/** 集群概览：ClusterId、Controller、Broker 列表 */
import { useEffect, useState } from 'react'
import { Button, Descriptions, Table, Tag, Empty, message } from 'antd'
import { ReloadOutlined } from '@ant-design/icons'
import type { ClusterOverview } from '@shared/types'
import { useAppStore } from '@/stores/appStore'
import { PageShell, useTableScrollY } from '@/shared/PageShell'

export default function OverviewPage() {
  const { activeConnectionId, overview, setOverview } = useAppStore()
  const [loading, setLoading] = useState(false)
  const [data, setData] = useState<ClusterOverview | null>(overview)
  const { containerRef, scrollY } = useTableScrollY(48)

  const load = async () => {
    if (!activeConnectionId) return
    setLoading(true)
    try {
      const res = await window.kafkaApi.getClusterOverview(activeConnectionId)
      if (!res.ok) {
        message.error(res.error)
        return
      }
      setData(res.data)
      setOverview(res.data)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void load()
  }, [activeConnectionId])

  return (
    <PageShell
      title="集群概览"
      subtitle="Broker 与控制器信息"
      extra={
        <Button icon={<ReloadOutlined />} loading={loading} onClick={() => void load()}>
          刷新
        </Button>
      }
    >
      {!data ? (
        <div className="page-empty">
          <Empty description="暂无集群信息" />
        </div>
      ) : (
        <>
          <Descriptions
            bordered
            size="small"
            column={3}
            style={{ marginBottom: 12, flexShrink: 0 }}
          >
            <Descriptions.Item label="Cluster ID">
              <span className="mono">{data.clusterId || '—'}</span>
            </Descriptions.Item>
            <Descriptions.Item label="Controller">{data.controllerId ?? '—'}</Descriptions.Item>
            <Descriptions.Item label="Broker 数量">{data.brokers.length}</Descriptions.Item>
          </Descriptions>

          <div ref={containerRef} className="table-fill">
            <Table
              rowKey="nodeId"
              size="middle"
              loading={loading}
              dataSource={data.brokers}
              pagination={false}
              scroll={{ y: scrollY }}
              columns={[
                { title: 'Node ID', dataIndex: 'nodeId', width: 100 },
                { title: 'Host', dataIndex: 'host' },
                { title: 'Port', dataIndex: 'port', width: 100 },
                {
                  title: 'Rack',
                  dataIndex: 'rack',
                  width: 120,
                  render: (v) => v || <span className="table-cell-secondary">—</span>,
                },
                {
                  title: '角色',
                  width: 120,
                  render: (_, r) =>
                    r.nodeId === data.controllerId ? (
                      <Tag color="blue">Controller</Tag>
                    ) : (
                      <Tag>Broker</Tag>
                    ),
                },
              ]}
            />
          </div>
        </>
      )}
    </PageShell>
  )
}
