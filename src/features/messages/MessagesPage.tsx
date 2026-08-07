import { useEffect, useMemo, useState, type Key } from 'react'
import {
  Button,
  DatePicker,
  Dropdown,
  Form,
  Input,
  InputNumber,
  Select,
  Switch,
  Table,
  message,
  Empty,
} from 'antd'
import {
  ReloadOutlined,
  SearchOutlined,
  ExportOutlined,
  DownOutlined,
} from '@ant-design/icons'
import { useSearchParams } from 'react-router-dom'
import dayjs from 'dayjs'
import type { KafkaMessageView, TopicInfo } from '@shared/types'
import { useAppStore } from '@/stores/appStore'
import { PageShell, useTableScrollY } from '@/shared/PageShell'
import { MessageDetailDrawer } from '@/shared/MessageDetailDrawer'
import { messageRowKey, saveMessagesExport } from '@/shared/exportMessages'

export default function MessagesPage() {
  const { activeConnectionId } = useAppStore()
  const [searchParams] = useSearchParams()
  const [topics, setTopics] = useState<TopicInfo[]>([])
  const [messages, setMessages] = useState<KafkaMessageView[]>([])
  const [loading, setLoading] = useState(false)
  const [exporting, setExporting] = useState(false)
  const [detail, setDetail] = useState<KafkaMessageView | null>(null)
  const [selectedKeys, setSelectedKeys] = useState<Key[]>([])
  const [form] = Form.useForm()
  const { containerRef, scrollY } = useTableScrollY(48)

  useEffect(() => {
    if (!activeConnectionId) return
    void window.kafkaApi.listTopics(activeConnectionId).then((res) => {
      if (res.ok) setTopics(res.data.filter((t) => !t.isInternal))
    })
  }, [activeConnectionId])

  useEffect(() => {
    const topic = searchParams.get('topic')
    if (topic) form.setFieldsValue({ topic })
  }, [searchParams, form])

  useEffect(() => {
    setSelectedKeys([])
  }, [messages])

  const selectedTopic = Form.useWatch('topic', form)
  const partitions = useMemo(() => {
    const t = topics.find((x) => x.name === selectedTopic)
    return t?.partitions.map((p) => p.partitionId) || []
  }, [topics, selectedTopic])

  const selectedMessages = useMemo(() => {
    const set = new Set(selectedKeys.map(String))
    return messages.filter((m) => set.has(messageRowKey(m)))
  }, [messages, selectedKeys])

  const fetchWithValues = async (values: Record<string, unknown>, limit?: number) => {
    if (!activeConnectionId) throw new Error('未连接')
    const res = await window.kafkaApi.fetchMessages(activeConnectionId, {
      topic: String(values.topic),
      partition: values.partition as number | undefined,
      fromOffset: (values.fromOffset as string) || undefined,
      fromTimestamp: values.fromTimestamp
        ? dayjs(values.fromTimestamp as dayjs.Dayjs).valueOf()
        : undefined,
      limit: limit ?? (Number(values.limit) || 50),
      decodeSchema: values.decodeSchema !== false,
    })
    if (!res.ok) throw new Error(res.error)
    return res.data
  }

  const load = async () => {
    if (!activeConnectionId) return
    const values = await form.validateFields()
    setLoading(true)
    try {
      const data = await fetchWithValues(values)
      setMessages(data)
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
    try {
      const exportLimit = Math.min(Math.max(Number(values.limit) || 50, 50), 10000)
      const data = await fetchWithValues(values, exportLimit)
      if (!data.length) {
        message.warning('当前筛选条件下没有消息')
        return
      }
      setMessages(data)
      const result = await saveMessagesExport(data, `kafka-${values.topic}`)
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
      title="消息浏览"
      subtitle={`按分区 / offset / 时间拉取 · 当前 ${messages.length} 条${
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
                onClick: () => void doExport(selectedMessages, 'kafka-selected'),
              },
              {
                key: 'current',
                label: `导出当前结果（${messages.length}）`,
                disabled: messages.length === 0,
                onClick: () => void doExport(messages, 'kafka-current'),
              },
              { type: 'divider' },
              {
                key: 'filter',
                label: '按筛选条件重新拉取并导出',
                onClick: () => void exportByFilter(),
              },
            ],
          }}
        >
          <Button icon={<ExportOutlined />} loading={exporting}>
            导出 <DownOutlined />
          </Button>
        </Dropdown>
      }
      toolbar={
        <Form
          form={form}
          layout="inline"
          initialValues={{ limit: 50, decodeSchema: true }}
        >
          <Form.Item name="topic" rules={[{ required: true, message: '选择 Topic' }]}>
            <Select
              showSearch
              placeholder="Topic"
              style={{ width: 220 }}
              options={topics.map((t) => ({ value: t.name, label: t.name }))}
            />
          </Form.Item>
          <Form.Item name="partition">
            <Select
              allowClear
              placeholder="全部分区"
              style={{ width: 110 }}
              options={partitions.map((p) => ({ value: p, label: `P${p}` }))}
            />
          </Form.Item>
          <Form.Item name="fromOffset">
            <Input placeholder="起始 Offset" style={{ width: 120 }} />
          </Form.Item>
          <Form.Item name="fromTimestamp">
            <DatePicker showTime placeholder="起始时间" />
          </Form.Item>
          <Form.Item name="limit">
            <InputNumber min={1} max={10000} placeholder="条数" />
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
                onClick={() => void load()}
              >
                查询
              </Button>
              <Button icon={<ReloadOutlined />} onClick={() => void load()} />
            </div>
          </Form.Item>
        </Form>
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
              <Empty
                image={Empty.PRESENTED_IMAGE_SIMPLE}
                description="选择条件后点击查询"
              />
            ),
          }}
          columns={[
            { title: 'P', dataIndex: 'partition', width: 70 },
            { title: 'Offset', dataIndex: 'offset', width: 100 },
            {
              title: '时间',
              dataIndex: 'timestamp',
              width: 180,
              render: (v: string) =>
                v && v !== '-1' ? dayjs(Number(v)).format('YYYY-MM-DD HH:mm:ss.SSS') : '—',
            },
            {
              title: 'Key',
              dataIndex: 'key',
              ellipsis: true,
              width: 160,
              render: (v: string | null) => v ?? <span className="table-cell-secondary">—</span>,
            },
            {
              title: 'Value',
              dataIndex: 'value',
              ellipsis: true,
              render: (v: string | null) => <span className="mono">{v ?? '—'}</span>,
            },
            {
              title: '',
              width: 70,
              fixed: 'right',
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
