import { useEffect, useState } from 'react'
import {
  Button,
  DatePicker,
  Form,
  InputNumber,
  Modal,
  Select,
  Space,
  Table,
  Typography,
  message,
  Popconfirm,
  Descriptions,
  Tabs,
  Empty,
} from 'antd'
import { ReloadOutlined, UndoOutlined, DeleteOutlined } from '@ant-design/icons'
import dayjs from 'dayjs'
import type { ConsumerGroupInfo, OffsetResetStrategy, TopicInfo } from '@shared/types'
import { useAppStore } from '@/stores/appStore'
import { PageShell, useTableScrollY } from '@/shared/PageShell'

const { Text } = Typography

export default function ConsumerGroupsPage() {
  const { activeConnectionId } = useAppStore()
  const [groups, setGroups] = useState<string[]>([])
  const [selected, setSelected] = useState<string | null>(null)
  const [detail, setDetail] = useState<ConsumerGroupInfo | null>(null)
  const [topics, setTopics] = useState<TopicInfo[]>([])
  const [loading, setLoading] = useState(false)
  const [resetOpen, setResetOpen] = useState(false)
  const [resetForm] = Form.useForm()
  const strategy: OffsetResetStrategy = Form.useWatch('strategy', resetForm)
  const { containerRef, scrollY } = useTableScrollY(48)

  const loadGroups = async () => {
    if (!activeConnectionId) return
    setLoading(true)
    try {
      const [gRes, tRes] = await Promise.all([
        window.kafkaApi.listConsumerGroups(activeConnectionId),
        window.kafkaApi.listTopics(activeConnectionId),
      ])
      if (!gRes.ok) message.error(gRes.error)
      else setGroups(gRes.data)
      if (tRes.ok) setTopics(tRes.data.filter((t) => !t.isInternal))
    } finally {
      setLoading(false)
    }
  }

  const loadDetail = async (groupId: string) => {
    if (!activeConnectionId) return
    setLoading(true)
    try {
      const res = await window.kafkaApi.describeConsumerGroup(activeConnectionId, groupId)
      if (!res.ok) message.error(res.error)
      else {
        setSelected(groupId)
        setDetail(res.data)
      }
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void loadGroups()
  }, [activeConnectionId])

  const onReset = async () => {
    if (!activeConnectionId || !selected) return
    const values = await resetForm.validateFields()
    const res = await window.kafkaApi.resetOffsets(activeConnectionId, {
      groupId: selected,
      topic: values.topic,
      strategy: values.strategy,
      offset: values.offset != null ? String(values.offset) : undefined,
      timestamp: values.timestamp ? dayjs(values.timestamp).valueOf() : undefined,
    })
    if (!res.ok) {
      message.error(res.error)
      return
    }
    message.success('偏移已重置')
    setResetOpen(false)
    await loadDetail(selected)
  }

  const onDelete = async (groupId: string) => {
    if (!activeConnectionId) return
    const res = await window.kafkaApi.deleteConsumerGroup(activeConnectionId, groupId)
    if (!res.ok) message.error(res.error)
    else {
      message.success('已删除')
      if (selected === groupId) {
        setSelected(null)
        setDetail(null)
      }
      await loadGroups()
    }
  }

  return (
    <PageShell
      title="消费组"
      subtitle={`查看成员、lag，并重置偏移 · 共 ${groups.length} 个`}
      extra={
        <Button icon={<ReloadOutlined />} loading={loading} onClick={() => void loadGroups()}>
          刷新
        </Button>
      }
    >
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '300px 1fr',
          gap: 12,
          flex: 1,
          minHeight: 0,
          overflow: 'hidden',
        }}
      >
        <div ref={containerRef} style={{ minHeight: 0, overflow: 'hidden', border: '1px solid #f0f0f0', borderRadius: 6 }}>
          <Table
            size="small"
            rowKey={(r) => r}
            loading={loading}
            dataSource={groups}
            pagination={false}
            scroll={{ y: scrollY }}
            onRow={(groupId) => ({
              onClick: () => void loadDetail(groupId),
              style: {
                cursor: 'pointer',
                background: selected === groupId ? '#e6f4ff' : undefined,
              },
            })}
            locale={{
              emptyText: (
                <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="暂无消费组" />
              ),
            }}
            columns={[
              {
                title: 'Group ID',
                ellipsis: true,
                render: (_, groupId) => groupId,
              },
              {
                title: '',
                width: 40,
                render: (_, groupId) => (
                  <Popconfirm
                    title={`确认删除消费组「${groupId}」？`}
                    onConfirm={(e) => {
                      e?.stopPropagation()
                      void onDelete(groupId)
                    }}
                  >
                    <Button
                      size="small"
                      danger
                      type="text"
                      icon={<DeleteOutlined />}
                      onClick={(e) => e.stopPropagation()}
                    />
                  </Popconfirm>
                ),
              },
            ]}
          />
        </div>

        <div
          style={{
            minHeight: 0,
            overflow: 'auto',
            border: '1px solid #f0f0f0',
            borderRadius: 6,
            padding: 12,
          }}
        >
          {!detail ? (
            <div className="page-empty">
              <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="选择左侧消费组查看详情" />
            </div>
          ) : (
            <>
              <Space size={8} style={{ marginBottom: 12 }} wrap>
                <span style={{ fontWeight: 600, fontSize: 15 }}>{detail.groupId}</span>
                <Button
                  size="small"
                  icon={<UndoOutlined />}
                  onClick={() => {
                    resetForm.setFieldsValue({ strategy: 'earliest' })
                    setResetOpen(true)
                  }}
                >
                  重置偏移
                </Button>
                <Button size="small" onClick={() => void loadDetail(detail.groupId)}>
                  刷新详情
                </Button>
              </Space>

              <Descriptions size="small" bordered column={3} style={{ marginBottom: 12 }}>
                <Descriptions.Item label="状态">{detail.state}</Descriptions.Item>
                <Descriptions.Item label="Protocol">{detail.protocol || '—'}</Descriptions.Item>
                <Descriptions.Item label="总 Lag">{detail.totalLag}</Descriptions.Item>
              </Descriptions>

              <Tabs
                items={[
                  {
                    key: 'offsets',
                    label: '偏移与 Lag',
                    children: (
                      <Table
                        size="small"
                        rowKey={(r) => `${r.topic}-${r.partition}`}
                        dataSource={detail.offsets}
                        pagination={false}
                        scroll={{ y: 360 }}
                        columns={[
                          { title: 'Topic', dataIndex: 'topic', ellipsis: true },
                          { title: 'P', dataIndex: 'partition', width: 60 },
                          { title: 'Committed', dataIndex: 'offset', width: 110 },
                          { title: 'High', dataIndex: 'high', width: 110 },
                          { title: 'Lag', dataIndex: 'lag', width: 90 },
                        ]}
                      />
                    ),
                  },
                  {
                    key: 'members',
                    label: `成员 (${detail.members.length})`,
                    children: (
                      <Table
                        size="small"
                        rowKey="memberId"
                        dataSource={detail.members}
                        pagination={false}
                        scroll={{ y: 360 }}
                        columns={[
                          { title: 'Member ID', dataIndex: 'memberId', ellipsis: true },
                          { title: 'Client ID', dataIndex: 'clientId', width: 140 },
                          { title: 'Host', dataIndex: 'clientHost', width: 120 },
                          {
                            title: 'Assignments',
                            ellipsis: true,
                            render: (_, r) =>
                              r.assignments.length
                                ? r.assignments
                                    .map((a) => `${a.topic}[${a.partitions.join(',')}]`)
                                    .join('; ')
                                : '—',
                          },
                        ]}
                      />
                    ),
                  },
                ]}
              />
            </>
          )}
        </div>
      </div>

      <Modal
        title={`重置偏移 — ${selected}`}
        open={resetOpen}
        onCancel={() => setResetOpen(false)}
        onOk={() => void onReset()}
        okText="确认重置"
        okButtonProps={{ danger: true }}
      >
        <Form form={resetForm} layout="vertical">
          <Form.Item name="topic" label="Topic" rules={[{ required: true }]}>
            <Select
              showSearch
              options={topics.map((t) => ({ value: t.name, label: t.name }))}
            />
          </Form.Item>
          <Form.Item name="strategy" label="策略" rules={[{ required: true }]}>
            <Select
              options={[
                { value: 'earliest', label: 'Earliest（最早）' },
                { value: 'latest', label: 'Latest（最新）' },
                { value: 'offset', label: '指定 Offset' },
                { value: 'timestamp', label: '按时间戳' },
              ]}
            />
          </Form.Item>
          {strategy === 'offset' && (
            <Form.Item name="offset" label="Offset" rules={[{ required: true }]}>
              <InputNumber min={0} style={{ width: '100%' }} />
            </Form.Item>
          )}
          {strategy === 'timestamp' && (
            <Form.Item name="timestamp" label="时间" rules={[{ required: true }]}>
              <DatePicker showTime style={{ width: '100%' }} />
            </Form.Item>
          )}
        </Form>
        <Text type="warning">请确保该消费组没有活跃成员，否则重置会失败。</Text>
      </Modal>
    </PageShell>
  )
}
