import { useEffect, useMemo, useState } from 'react'
import {
  Button,
  Form,
  Input,
  InputNumber,
  Modal,
  Space,
  Table,
  Tag,
  Empty,
  message,
  Popconfirm,
  Switch,
} from 'antd'
import { PlusOutlined, ReloadOutlined, DeleteOutlined } from '@ant-design/icons'
import { useNavigate } from 'react-router-dom'
import type { TopicInfo } from '@shared/types'
import { useAppStore } from '@/stores/appStore'
import { PageShell, useTableScrollY } from '@/shared/PageShell'

export default function TopicsPage() {
  const { activeConnectionId } = useAppStore()
  const navigate = useNavigate()
  const [topics, setTopics] = useState<TopicInfo[]>([])
  const [loading, setLoading] = useState(false)
  const [keyword, setKeyword] = useState('')
  const [showInternal, setShowInternal] = useState(false)
  const [createOpen, setCreateOpen] = useState(false)
  const [form] = Form.useForm()
  const { containerRef, scrollY } = useTableScrollY(55)

  const load = async () => {
    if (!activeConnectionId) return
    setLoading(true)
    try {
      const res = await window.kafkaApi.listTopics(activeConnectionId)
      if (!res.ok) message.error(res.error)
      else setTopics(res.data)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void load()
  }, [activeConnectionId])

  const filtered = useMemo(() => {
    return topics.filter((t) => {
      if (!showInternal && t.isInternal) return false
      if (keyword && !t.name.toLowerCase().includes(keyword.toLowerCase())) return false
      return true
    })
  }, [topics, keyword, showInternal])

  const onCreate = async () => {
    if (!activeConnectionId) return
    const values = await form.validateFields()
    const configEntries: Array<{ name: string; value: string }> = []
    if (values.retentionMs) {
      configEntries.push({ name: 'retention.ms', value: String(values.retentionMs) })
    }
    if (values.cleanupPolicy) {
      configEntries.push({ name: 'cleanup.policy', value: String(values.cleanupPolicy) })
    }
    const res = await window.kafkaApi.createTopic(activeConnectionId, {
      name: values.name,
      numPartitions: values.numPartitions,
      replicationFactor: values.replicationFactor,
      configEntries: configEntries.length ? configEntries : undefined,
    })
    if (!res.ok) {
      message.error(res.error)
      return
    }
    message.success('Topic 已创建')
    setCreateOpen(false)
    form.resetFields()
    await load()
  }

  const onDelete = async (name: string) => {
    if (!activeConnectionId) return
    const res = await window.kafkaApi.deleteTopic(activeConnectionId, name)
    if (!res.ok) message.error(res.error)
    else {
      message.success('已删除')
      await load()
    }
  }

  return (
    <PageShell
      title="Topics"
      subtitle={`共 ${filtered.length} 个${keyword ? `（已筛选）` : ''}`}
      extra={
        <Space size={8} wrap>
          <Input.Search
            placeholder="搜索 Topic"
            allowClear
            style={{ width: 220 }}
            onSearch={setKeyword}
            onChange={(e) => setKeyword(e.target.value)}
          />
          <span style={{ color: 'rgba(0,0,0,0.65)', fontSize: 13 }}>
            内部 Topic <Switch checked={showInternal} onChange={setShowInternal} size="small" />
          </span>
          <Button icon={<ReloadOutlined />} onClick={() => void load()} loading={loading}>
            刷新
          </Button>
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={() => {
              form.setFieldsValue({
                numPartitions: 3,
                replicationFactor: 1,
                cleanupPolicy: 'delete',
              })
              setCreateOpen(true)
            }}
          >
            创建 Topic
          </Button>
        </Space>
      }
    >
      <div ref={containerRef} className="table-fill">
        <Table
          rowKey="name"
          size="middle"
          loading={loading}
          dataSource={filtered}
          pagination={false}
          scroll={{ y: scrollY }}
          locale={{
            emptyText: (
              <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="暂无 Topic" />
            ),
          }}
          columns={[
            {
              title: '名称',
              dataIndex: 'name',
              ellipsis: true,
              render: (name: string, r) => (
                <Space size={6}>
                  <a onClick={() => navigate(`/topics/${encodeURIComponent(name)}`)}>{name}</a>
                  {r.isInternal ? <Tag>内部</Tag> : null}
                </Space>
              ),
            },
            {
              title: '分区数',
              width: 100,
              render: (_, r) => r.partitions.length,
            },
            {
              title: '副本数',
              width: 100,
              render: (_, r) => r.partitions[0]?.replicas.length ?? '—',
            },
            {
              title: '操作',
              width: 200,
              render: (_, r) => (
                <Space size={4}>
                  <Button
                    size="small"
                    onClick={() =>
                      navigate(`/messages?topic=${encodeURIComponent(r.name)}`)
                    }
                  >
                    浏览消息
                  </Button>
                  <Popconfirm
                    title={`确认删除 Topic「${r.name}」？此操作不可恢复`}
                    onConfirm={() => void onDelete(r.name)}
                  >
                    <Button size="small" danger icon={<DeleteOutlined />}>
                      删除
                    </Button>
                  </Popconfirm>
                </Space>
              ),
            },
          ]}
        />
      </div>

      <Modal
        title="创建 Topic"
        open={createOpen}
        onCancel={() => setCreateOpen(false)}
        onOk={() => void onCreate()}
        okText="创建"
        cancelText="取消"
      >
        <Form form={form} layout="vertical">
          <Form.Item name="name" label="名称" rules={[{ required: true }]}>
            <Input placeholder="my-topic" />
          </Form.Item>
          <Form.Item name="numPartitions" label="分区数" rules={[{ required: true }]}>
            <InputNumber min={1} max={1000} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="replicationFactor" label="副本数" rules={[{ required: true }]}>
            <InputNumber min={1} max={20} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="cleanupPolicy" label="cleanup.policy">
            <Input placeholder="delete / compact" />
          </Form.Item>
          <Form.Item name="retentionMs" label="retention.ms">
            <InputNumber style={{ width: '100%' }} placeholder="例如 604800000" />
          </Form.Item>
        </Form>
      </Modal>
    </PageShell>
  )
}
