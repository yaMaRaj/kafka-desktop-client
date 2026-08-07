import { useEffect, useState } from 'react'
import { Button, Space, Table, Tag, Empty, message, Popconfirm } from 'antd'
import { ReloadOutlined, ClearOutlined } from '@ant-design/icons'
import dayjs from 'dayjs'
import type { OperationLogEntry } from '@shared/types'
import { PageShell, useTableScrollY } from '@/shared/PageShell'

export default function LogsPage() {
  const [logs, setLogs] = useState<OperationLogEntry[]>([])
  const [loading, setLoading] = useState(false)
  const { containerRef, scrollY } = useTableScrollY(55)

  const load = async () => {
    setLoading(true)
    try {
      const res = await window.kafkaApi.listOperationLogs()
      if (!res.ok) message.error(res.error)
      else setLogs(res.data)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void load()
  }, [])

  return (
    <PageShell
      title="操作日志"
      subtitle={`本地记录的危险/变更操作（最多 500 条）· 共 ${logs.length} 条`}
      extra={
        <Space size={8}>
          <Button icon={<ReloadOutlined />} onClick={() => void load()} loading={loading}>
            刷新
          </Button>
          <Popconfirm
            title="确认清空全部日志？"
            onConfirm={async () => {
              const res = await window.kafkaApi.clearOperationLogs()
              if (!res.ok) message.error(res.error)
              else {
                message.success('已清空')
                setLogs([])
              }
            }}
          >
            <Button danger icon={<ClearOutlined />} disabled={logs.length === 0}>
              清空
            </Button>
          </Popconfirm>
        </Space>
      }
    >
      <div ref={containerRef} className="table-fill">
        <Table
          rowKey="id"
          size="middle"
          dataSource={logs}
          loading={loading}
          pagination={false}
          scroll={{ y: scrollY, x: 900 }}
          locale={{
            emptyText: <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="暂无操作记录" />,
          }}
          columns={[
            {
              title: '时间',
              dataIndex: 'at',
              width: 170,
              render: (v: number) => dayjs(v).format('YYYY-MM-DD HH:mm:ss'),
            },
            { title: '操作', dataIndex: 'action', width: 150 },
            { title: '详情', dataIndex: 'detail', ellipsis: true },
            {
              title: '结果',
              dataIndex: 'success',
              width: 90,
              render: (v: boolean) =>
                v ? <Tag color="success">成功</Tag> : <Tag color="error">失败</Tag>,
            },
            {
              title: '错误',
              dataIndex: 'error',
              ellipsis: true,
              render: (v) => v || <span className="table-cell-secondary">—</span>,
            },
          ]}
        />
      </div>
    </PageShell>
  )
}
