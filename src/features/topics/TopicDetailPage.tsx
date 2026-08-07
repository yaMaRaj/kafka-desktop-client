import { useEffect, useState } from 'react'
import {
  Button,
  Form,
  Input,
  InputNumber,
  Modal,
  Space,
  Table,
  Typography,
  message,
  Tabs,
} from 'antd'
import { ArrowLeftOutlined, ReloadOutlined } from '@ant-design/icons'
import { useNavigate, useParams } from 'react-router-dom'
import type { TopicInfo, TopicOffsets } from '@shared/types'
import { useAppStore } from '@/stores/appStore'
import { PageShell, useTableScrollY } from '@/shared/PageShell'

const { Text } = Typography

export default function TopicDetailPage() {
  const { name } = useParams()
  const topicName = decodeURIComponent(name || '')
  const { activeConnectionId } = useAppStore()
  const navigate = useNavigate()
  const [topic, setTopic] = useState<TopicInfo | null>(null)
  const [offsets, setOffsets] = useState<TopicOffsets | null>(null)
  const [loading, setLoading] = useState(false)
  const [configOpen, setConfigOpen] = useState(false)
  const [partOpen, setPartOpen] = useState(false)
  const [configForm] = Form.useForm()
  const [partForm] = Form.useForm()
  const { containerRef, scrollY } = useTableScrollY(100)

  const load = async () => {
    if (!activeConnectionId || !topicName) return
    setLoading(true)
    try {
      const [tRes, oRes] = await Promise.all([
        window.kafkaApi.getTopic(activeConnectionId, topicName),
        window.kafkaApi.getTopicOffsets(activeConnectionId, topicName),
      ])
      if (!tRes.ok) message.error(tRes.error)
      else setTopic(tRes.data)
      if (!oRes.ok) message.error(oRes.error)
      else setOffsets(oRes.data)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void load()
  }, [activeConnectionId, topicName])

  const saveConfig = async () => {
    if (!activeConnectionId || !topic) return
    const values = await configForm.validateFields()
    const lines = String(values.configText || '')
      .split('\n')
      .map((l: string) => l.trim())
      .filter(Boolean)
    const entries = lines.map((line: string) => {
      const idx = line.indexOf('=')
      if (idx < 0) throw new Error(`无效配置行: ${line}`)
      return { name: line.slice(0, idx).trim(), value: line.slice(idx + 1).trim() }
    })
    const res = await window.kafkaApi.alterTopicConfig(activeConnectionId, topicName, entries)
    if (!res.ok) message.error(res.error)
    else {
      message.success('配置已更新')
      setConfigOpen(false)
      await load()
    }
  }

  const expandPartitions = async () => {
    if (!activeConnectionId) return
    const values = await partForm.validateFields()
    const res = await window.kafkaApi.createPartitions(
      activeConnectionId,
      topicName,
      values.count,
    )
    if (!res.ok) message.error(res.error)
    else {
      message.success('分区已扩展')
      setPartOpen(false)
      await load()
    }
  }

  const configText = topic?.config
    ? Object.entries(topic.config)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([k, v]) => `${k}=${v}`)
        .join('\n')
    : ''

  return (
    <PageShell
      title={topicName}
      subtitle="分区、副本与配置"
      extra={
        <Space size={8} wrap>
          <Button icon={<ArrowLeftOutlined />} onClick={() => navigate('/topics')}>
            返回
          </Button>
          <Button icon={<ReloadOutlined />} loading={loading} onClick={() => void load()}>
            刷新
          </Button>
          <Button onClick={() => navigate(`/messages?topic=${encodeURIComponent(topicName)}`)}>
            浏览消息
          </Button>
          <Button
            onClick={() => {
              configForm.setFieldsValue({ configText })
              setConfigOpen(true)
            }}
          >
            修改配置
          </Button>
          <Button
            onClick={() => {
              partForm.setFieldsValue({ count: (topic?.partitions.length || 0) + 1 })
              setPartOpen(true)
            }}
          >
            扩展分区
          </Button>
        </Space>
      }
    >
      {topic && (
        <div ref={containerRef} style={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' }}>
          <Tabs
            style={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' }}
            items={[
              {
                key: 'partitions',
                label: `分区与副本 (${topic.partitions.length})`,
                children: (
                  <Table
                    rowKey="partitionId"
                    size="middle"
                    dataSource={topic.partitions}
                    pagination={false}
                    scroll={{ y: scrollY }}
                    columns={[
                      { title: 'Partition', dataIndex: 'partitionId', width: 100 },
                      { title: 'Leader', dataIndex: 'leader', width: 100 },
                      {
                        title: 'Replicas',
                        dataIndex: 'replicas',
                        render: (v: number[]) => v.join(', '),
                      },
                      {
                        title: 'ISR',
                        dataIndex: 'isr',
                        render: (v: number[]) => v.join(', '),
                      },
                      {
                        title: 'Low / High',
                        width: 180,
                        render: (_, r) => {
                          const o = offsets?.partitions.find((p) => p.partition === r.partitionId)
                          return o ? (
                            <span className="mono">
                              {o.low} / {o.high}
                            </span>
                          ) : (
                            '—'
                          )
                        },
                      },
                    ]}
                  />
                ),
              },
              {
                key: 'config',
                label: '配置',
                children: (
                  <pre className="mono" style={{ maxHeight: '100%', overflow: 'auto', margin: 0 }}>
                    {configText || '无配置'}
                  </pre>
                ),
              },
            ]}
          />
        </div>
      )}

      <Modal
        title="修改 Topic 配置"
        open={configOpen}
        onCancel={() => setConfigOpen(false)}
        onOk={() => void saveConfig()}
        width={720}
        okText="保存"
      >
        <Text type="secondary">每行一条 name=value</Text>
        <Form form={configForm} layout="vertical" style={{ marginTop: 8 }}>
          <Form.Item name="configText" rules={[{ required: true }]}>
            <Input.TextArea rows={16} className="mono" />
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title="扩展分区"
        open={partOpen}
        onCancel={() => setPartOpen(false)}
        onOk={() => void expandPartitions()}
        okText="确认"
      >
        <Form form={partForm} layout="vertical">
          <Form.Item
            name="count"
            label="目标分区总数（只能增加不能减少）"
            rules={[{ required: true }]}
          >
            <InputNumber min={(topic?.partitions.length || 0) + 1} style={{ width: '100%' }} />
          </Form.Item>
        </Form>
      </Modal>
    </PageShell>
  )
}
