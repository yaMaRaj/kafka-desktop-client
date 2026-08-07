import { useEffect, useMemo, useState, type Key } from 'react'
import {
  Button,
  Dropdown,
  Form,
  Input,
  InputNumber,
  Select,
  Switch,
  Table,
  Typography,
  message,
  Progress,
  Empty,
} from 'antd'
import { SearchOutlined, ExportOutlined, DownOutlined } from '@ant-design/icons'
import dayjs from 'dayjs'
import type { KafkaMessageView, SearchProgress, TopicInfo } from '@shared/types'
import { useAppStore } from '@/stores/appStore'
import { PageShell, useTableScrollY } from '@/shared/PageShell'
import { MessageDetailDrawer } from '@/shared/MessageDetailDrawer'
import { messageRowKey, saveMessagesExport } from '@/shared/exportMessages'

const { Text } = Typography

export default function SearchPage() {
  const { activeConnectionId } = useAppStore()
  const [topics, setTopics] = useState<TopicInfo[]>([])
  const [messages, setMessages] = useState<KafkaMessageView[]>([])
  const [loading, setLoading] = useState(false)
  const [exporting, setExporting] = useState(false)
  const [progress, setProgress] = useState<SearchProgress | null>(null)
  const [detail, setDetail] = useState<KafkaMessageView | null>(null)
  const [selectedKeys, setSelectedKeys] = useState<Key[]>([])
  const [form] = Form.useForm()
  const { containerRef, scrollY } = useTableScrollY(48)

  useEffect(() => {
    if (!activeConnectionId) return
    void window.kafkaApi.listTopics(activeConnectionId).then((res) => {
      if (res.ok) setTopics(res.data.filter((t) => !t.isInternal))
    })
    return window.kafkaApi.onSearchProgress(({ progress: p }) => setProgress(p))
  }, [activeConnectionId])

  useEffect(() => {
    setSelectedKeys([])
  }, [messages])

  const selectedMessages = useMemo(() => {
    const set = new Set(selectedKeys.map(String))
    return messages.filter((m) => set.has(messageRowKey(m)))
  }, [messages, selectedKeys])

  const runSearch = async (values: Record<string, unknown>) => {
    if (!activeConnectionId) throw new Error('未连接')
    const res = await window.kafkaApi.searchMessages(activeConnectionId, {
      topic: String(values.topic),
      query: String(values.query),
      useRegex: !!values.useRegex,
      searchKey: values.searchKey !== false,
      searchValue: values.searchValue !== false,
      fromOffset: (values.fromOffset as string) || undefined,
      maxScan: Number(values.maxScan) || 2000,
      decodeSchema: values.decodeSchema !== false,
    })
    if (!res.ok) throw new Error(res.error)
    return res.data
  }

  const onSearch = async () => {
    if (!activeConnectionId) return
    const values = await form.validateFields()
    setLoading(true)
    setMessages([])
    setProgress({ scanned: 0, matched: 0, done: false })
    try {
      const data = await runSearch(values)
      setMessages(data.messages)
      setProgress(data.progress)
      message.success(`扫描 ${data.progress.scanned} 条，匹配 ${data.messages.length} 条`)
    } catch (e) {
      message.error(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }

  const doExport = async (list: KafkaMessageView[], prefix: string) => {
    setExporting(true)
    try {
      const result = await saveMessagesExport(list, prefix)
      if (!result.ok) {
        if (result.error !== '已取消') message.warning(result.error)
        return
      }
      message.success(`已导出 ${result.count} 条 → ${result.filePath}`)
    } finally {
      setExporting(false)
    }
  }

  const exportByFilter = async () => {
    if (!activeConnectionId) return
    const values = await form.validateFields()
    setExporting(true)
    setProgress({ scanned: 0, matched: 0, done: false })
    try {
      const data = await runSearch(values)
      setMessages(data.messages)
      setProgress(data.progress)
      if (!data.messages.length) {
        message.warning('当前筛选条件下没有匹配消息')
        return
      }
      const result = await saveMessagesExport(data.messages, `kafka-search-${values.topic}`)
      if (!result.ok) {
        if (result.error !== '已取消') message.warning(result.error)
        return
      }
      message.success(`已导出 ${result.count} 条 → ${result.filePath}`)
    } catch (e) {
      message.error(e instanceof Error ? e.message : String(e))
    } finally {
      setExporting(false)
    }
  }

  return (
    <PageShell
      title="消息搜索"
      subtitle={`窗口内扫描 Key/Value · 匹配 ${messages.length} 条${
        selectedKeys.length ? ` · 已选 ${selectedKeys.length}` : ''
      }`}
      extra={
        <Dropdown
          menu={{
            items: [
              {
                key: 'selected',
                label: `导出选中（${selectedKeys.length}）`,
                disabled: selectedKeys.length === 0,
                onClick: () => void doExport(selectedMessages, 'kafka-search-selected'),
              },
              {
                key: 'current',
                label: `导出当前结果（${messages.length}）`,
                disabled: messages.length === 0,
                onClick: () => void doExport(messages, 'kafka-search-current'),
              },
              { type: 'divider' },
              {
                key: 'filter',
                label: '按筛选条件重新搜索并导出',
                onClick: () => void exportByFilter(),
              },
            ],
          }}
        >
          <Button icon={<ExportOutlined />} loading={exporting || loading}>
            导出 <DownOutlined />
          </Button>
        </Dropdown>
      }
      toolbar={
        <>
          <Form
            form={form}
            layout="inline"
            initialValues={{
              maxScan: 2000,
              searchKey: true,
              searchValue: true,
              decodeSchema: true,
            }}
          >
            <Form.Item name="topic" rules={[{ required: true }]}>
              <Select
                showSearch
                placeholder="Topic"
                style={{ width: 200 }}
                options={topics.map((t) => ({ value: t.name, label: t.name }))}
              />
            </Form.Item>
            <Form.Item name="query" rules={[{ required: true, message: '输入关键字' }]}>
              <Input placeholder="关键字 / 正则" style={{ width: 200 }} />
            </Form.Item>
            <Form.Item name="useRegex" label="正则" valuePropName="checked">
              <Switch />
            </Form.Item>
            <Form.Item name="searchKey" label="Key" valuePropName="checked">
              <Switch />
            </Form.Item>
            <Form.Item name="searchValue" label="Value" valuePropName="checked">
              <Switch />
            </Form.Item>
            <Form.Item name="fromOffset">
              <Input placeholder="起始 Offset" style={{ width: 110 }} />
            </Form.Item>
            <Form.Item name="maxScan">
              <InputNumber min={100} max={20000} />
            </Form.Item>
            <Form.Item name="decodeSchema" label="Schema" valuePropName="checked">
              <Switch />
            </Form.Item>
            <Form.Item>
              <div className="toolbar-actions">
                <Button
                  type="primary"
                  icon={<SearchOutlined />}
                  loading={loading}
                  onClick={() => void onSearch()}
                >
                  搜索
                </Button>
              </div>
            </Form.Item>
          </Form>
          {progress && (
            <div style={{ marginTop: 8 }}>
              <Text>
                已扫描 {progress.scanned} · 匹配 {progress.matched}
                {progress.done ? ' · 完成' : ' · 进行中…'}
              </Text>
              <Progress
                percent={
                  progress.done
                    ? 100
                    : Math.min(
                        99,
                        Math.round(
                          (progress.scanned / (form.getFieldValue('maxScan') || 2000)) * 100,
                        ),
                      )
                }
                status={progress.done ? 'success' : 'active'}
                size="small"
              />
            </div>
          )}
        </>
      }
    >
      <div ref={containerRef} className="table-fill">
        <Table
          rowKey={(r) => messageRowKey(r)}
          loading={loading}
          dataSource={messages}
          size="small"
          pagination={false}
          scroll={{ y: scrollY, x: 900 }}
          rowSelection={{
            selectedRowKeys: selectedKeys,
            onChange: (keys) => setSelectedKeys(keys),
            preserveSelectedRowKeys: true,
          }}
          locale={{
            emptyText: (
              <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="输入条件后开始搜索" />
            ),
          }}
          columns={[
            { title: 'P', dataIndex: 'partition', width: 60 },
            { title: 'Offset', dataIndex: 'offset', width: 100 },
            {
              title: '时间',
              dataIndex: 'timestamp',
              width: 170,
              render: (v: string) =>
                v && v !== '-1' ? dayjs(Number(v)).format('YYYY-MM-DD HH:mm:ss') : '—',
            },
            {
              title: 'Key',
              dataIndex: 'key',
              ellipsis: true,
              width: 140,
              render: (v) => v ?? '—',
            },
            { title: 'Value', dataIndex: 'value', ellipsis: true, render: (v) => v ?? '—' },
            {
              title: '',
              width: 70,
              render: (_, r) => (
                <Button type="link" size="small" onClick={() => setDetail(r)}>
                  详情
                </Button>
              ),
            },
          ]}
        />
      </div>

      <MessageDetailDrawer
        message={detail}
        open={!!detail}
        onClose={() => setDetail(null)}
      />
    </PageShell>
  )
}
