import { useEffect, useState } from 'react'
import { Button, Table, Typography, message, Drawer, Empty } from 'antd'
import { ReloadOutlined } from '@ant-design/icons'
import type { SchemaSubjectInfo } from '@shared/types'
import { useAppStore } from '@/stores/appStore'
import { PageShell, useTableScrollY } from '@/shared/PageShell'

const { Text } = Typography

export default function SchemaRegistryPage() {
  const { activeConnectionId, connections } = useAppStore()
  const [subjects, setSubjects] = useState<string[]>([])
  const [loading, setLoading] = useState(false)
  const [detail, setDetail] = useState<SchemaSubjectInfo | null>(null)
  const { containerRef, scrollY } = useTableScrollY(48)

  const conn = connections.find((c) => c.id === activeConnectionId)
  const hasRegistry = !!conn?.schemaRegistry?.url

  const load = async () => {
    if (!activeConnectionId) return
    setLoading(true)
    try {
      const res = await window.kafkaApi.listSubjects(activeConnectionId)
      if (!res.ok) message.error(res.error)
      else setSubjects(res.data)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    if (hasRegistry) void load()
  }, [activeConnectionId, hasRegistry])

  const openSubject = async (subject: string) => {
    if (!activeConnectionId) return
    const res = await window.kafkaApi.getSubject(activeConnectionId, subject)
    if (!res.ok) message.error(res.error)
    else setDetail(res.data)
  }

  if (!hasRegistry) {
    return (
      <PageShell title="Schema Registry" subtitle="未配置 Registry">
        <div className="page-empty">
          <Empty description="当前连接未配置 Schema Registry，请在「连接管理」中填写 Registry URL。" />
        </div>
      </PageShell>
    )
  }

  return (
    <PageShell
      title="Schema Registry"
      subtitle={`${conn?.schemaRegistry?.url} · ${subjects.length} 个 Subject`}
      extra={
        <Button icon={<ReloadOutlined />} loading={loading} onClick={() => void load()}>
          刷新
        </Button>
      }
    >
      <div ref={containerRef} className="table-fill">
        <Table
          rowKey={(r) => r}
          size="middle"
          loading={loading}
          dataSource={subjects}
          pagination={false}
          scroll={{ y: scrollY }}
          locale={{
            emptyText: (
              <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="暂无 Subject" />
            ),
          }}
          columns={[
            {
              title: 'Subject',
              render: (_, subject) => (
                <a onClick={() => void openSubject(subject)}>{subject}</a>
              ),
            },
          ]}
        />
      </div>

      <Drawer title={detail?.subject} open={!!detail} width={720} onClose={() => setDetail(null)}>
        {detail && (
          <>
            <p>
              <Text strong>类型：</Text>
              {detail.schemaType || '—'}
            </p>
            <p>
              <Text strong>兼容性：</Text>
              {detail.compatibility || '—'}
            </p>
            <p>
              <Text strong>版本：</Text>
              {detail.versions.join(', ')}
            </p>
            <p>
              <Text strong>最新版本：</Text>
              {detail.latestVersion ?? '—'}
            </p>
            <Text strong>Schema</Text>
            <pre className="message-value">
              {(() => {
                try {
                  return JSON.stringify(JSON.parse(detail.latestSchema || '{}'), null, 2)
                } catch {
                  return detail.latestSchema || ''
                }
              })()}
            </pre>
          </>
        )}
      </Drawer>
    </PageShell>
  )
}
