import { useEffect, useRef, useState } from 'react'
import {
  Button,
  Form,
  Input,
  Select,
  Space,
  Switch,
  Table,
  message,
  Tag,
  Empty,
} from 'antd'
import {
  PlayCircleOutlined,
  PauseCircleOutlined,
  ClearOutlined,
} from '@ant-design/icons'
import dayjs from 'dayjs'
import type { KafkaMessageView, TopicInfo } from '@shared/types'
import { useAppStore } from '@/stores/appStore'
import { PageShell, useTableScrollY } from '@/shared/PageShell'
import { MessageDetailDrawer } from '@/shared/MessageDetailDrawer'

const UI_CAP = 500

export default function TailPage() {
  const { activeConnectionId } = useAppStore()
  const [topics, setTopics] = useState<TopicInfo[]>([])
  const [messages, setMessages] = useState<KafkaMessageView[]>([])
  const [tailId, setTailId] = useState<string | null>(null)
  const [paused, setPaused] = useState(false)
  const [detail, setDetail] = useState<KafkaMessageView | null>(null)
  const pausedRef = useRef(false)
  const [form] = Form.useForm()
  const { containerRef, scrollY } = useTableScrollY(48)

  const running = !!tailId

  useEffect(() => {
    if (!activeConnectionId) return
    void window.kafkaApi.listTopics(activeConnectionId).then((res) => {
      if (res.ok) setTopics(res.data.filter((t) => !t.isInternal))
    })
  }, [activeConnectionId])

  useEffect(() => {
    const offMsg = window.kafkaApi.onTailMessage(({ tailId: id, message: msg }) => {
      if (pausedRef.current) return
      setTailId((current) => {
        if (current && current !== id) return current
        return current
      })
      setMessages((prev) => {
        const next = [msg, ...prev]
        return next.slice(0, UI_CAP)
      })
    })
    const offErr = window.kafkaApi.onTailError(({ error }) => {
      message.error(error)
    })
    return () => {
      offMsg()
      offErr()
    }
  }, [])

  const start = async () => {
    if (!activeConnectionId) return
    if (tailId) {
      await window.kafkaApi.stopTail(tailId)
      setTailId(null)
    }
    const values = await form.validateFields()
    const res = await window.kafkaApi.startTail(activeConnectionId, {
      topic: values.topic,
      fromBeginning: !!values.fromBeginning,
      filter: values.filter || undefined,
      useRegex: !!values.useRegex,
      decodeSchema: values.decodeSchema !== false,
    })
    if (!res.ok) {
      message.error(res.error)
      return
    }
    setMessages([])
    setTailId(res.data)
    setPaused(false)
    pausedRef.current = false
    message.success('已开始实时消费')
  }

  const stop = async () => {
    if (!tailId) return
    await window.kafkaApi.stopTail(tailId)
    setTailId(null)
    message.info('已停止')
  }

  const togglePause = () => {
    const next = !paused
    setPaused(next)
    pausedRef.current = next
  }

  useEffect(() => {
    return () => {
      if (tailId) void window.kafkaApi.stopTail(tailId)
    }
  }, [tailId])

  return (
    <PageShell
      title="实时消费 (Tail)"
      subtitle={`持续拉取新消息 · 已缓存 ${messages.length} 条（最多 ${UI_CAP}）`}
      extra={
        <Space size={8}>
          {running ? <Tag color="green">运行中</Tag> : <Tag>已停止</Tag>}
          {paused && running && <Tag color="orange">已暂停显示</Tag>}
        </Space>
      }
      toolbar={
        <Form form={form} layout="inline" initialValues={{ decodeSchema: true }}>
          <Form.Item name="topic" rules={[{ required: true }]}>
            <Select
              showSearch
              placeholder="Topic"
              style={{ width: 220 }}
              disabled={running}
              options={topics.map((t) => ({ value: t.name, label: t.name }))}
            />
          </Form.Item>
          <Form.Item name="fromBeginning" label="从最早" valuePropName="checked">
            <Switch disabled={running} />
          </Form.Item>
          <Form.Item name="filter">
            <Input placeholder="过滤关键字" style={{ width: 160 }} disabled={running} />
          </Form.Item>
          <Form.Item name="useRegex" label="正则" valuePropName="checked">
            <Switch disabled={running} />
          </Form.Item>
          <Form.Item name="decodeSchema" label="Schema" valuePropName="checked">
            <Switch disabled={running} />
          </Form.Item>
          <Form.Item>
            <div className="toolbar-actions">
              {!running ? (
                <Button type="primary" icon={<PlayCircleOutlined />} onClick={() => void start()}>
                  开始
                </Button>
              ) : (
                <>
                  <Button icon={<PauseCircleOutlined />} onClick={togglePause}>
                    {paused ? '继续显示' : '暂停显示'}
                  </Button>
                  <Button danger onClick={() => void stop()}>
                    停止
                  </Button>
                </>
              )}
              <Button icon={<ClearOutlined />} onClick={() => setMessages([])}>
                清空
              </Button>
            </div>
          </Form.Item>
        </Form>
      }
    >
      <div ref={containerRef} className="table-fill">
        <Table
          size="small"
          rowKey={(r) => `${r.partition}-${r.offset}-${r.timestamp}`}
          dataSource={messages}
          pagination={false}
          scroll={{ y: scrollY, x: 900 }}
          locale={{
            emptyText: (
              <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="点击开始后显示实时消息" />
            ),
          }}
          columns={[
            { title: 'P', dataIndex: 'partition', width: 60 },
            { title: 'Offset', dataIndex: 'offset', width: 100 },
            {
              title: '时间',
              dataIndex: 'timestamp',
              width: 120,
              render: (v: string) =>
                v && v !== '-1' ? dayjs(Number(v)).format('HH:mm:ss.SSS') : '—',
            },
            {
              title: 'Key',
              dataIndex: 'key',
              width: 160,
              ellipsis: true,
              render: (v) => v ?? '—',
            },
            {
              title: 'Value',
              dataIndex: 'value',
              ellipsis: true,
              render: (v: string | null) => (
                <span className="mono">
                  {v ? (v.length > 200 ? `${v.slice(0, 200)}…` : v) : '—'}
                </span>
              ),
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
