import { useEffect, useMemo, useState } from 'react'
import { Layout, Menu, Select, Button, Typography, Space, Tag, Spin, message } from 'antd'
import {
  ClusterOutlined,
  DatabaseOutlined,
  MessageOutlined,
  TeamOutlined,
  FileSearchOutlined,
  RadarChartOutlined,
  SettingOutlined,
  ApiOutlined,
  HistoryOutlined,
} from '@ant-design/icons'
import { HashRouter, Navigate, Route, Routes, useLocation, useNavigate } from 'react-router-dom'
import { useAppStore } from '@/stores/appStore'
import ConnectionsPage from '@/features/clusters/ConnectionsPage'
import OverviewPage from '@/features/clusters/OverviewPage'
import TopicsPage from '@/features/topics/TopicsPage'
import TopicDetailPage from '@/features/topics/TopicDetailPage'
import MessagesPage from '@/features/messages/MessagesPage'
import ProducePage from '@/features/messages/ProducePage'
import ConsumerGroupsPage from '@/features/consumer-groups/ConsumerGroupsPage'
import SchemaRegistryPage from '@/features/schema-registry/SchemaRegistryPage'
import SearchPage from '@/features/messages/SearchPage'
import TailPage from '@/features/messages/TailPage'
import LogsPage from '@/features/clusters/LogsPage'

const { Header, Sider, Content } = Layout
const { Text, Title } = Typography

function Shell() {
  const navigate = useNavigate()
  const location = useLocation()
  const {
    connections,
    activeConnectionId,
    connected,
    overview,
    loading,
    refreshConnections,
    connect,
    disconnect,
  } = useAppStore()
  const [version, setVersion] = useState('')

  useEffect(() => {
    void refreshConnections()
    void window.kafkaApi.getVersion().then(setVersion)
  }, [refreshConnections])

  const selectedKey = useMemo(() => {
    const path = location.pathname
    if (path.startsWith('/topics')) return '/topics'
    if (path.startsWith('/messages')) return '/messages'
    if (path.startsWith('/produce')) return '/produce'
    if (path.startsWith('/groups')) return '/groups'
    if (path.startsWith('/schema')) return '/schema'
    if (path.startsWith('/search')) return '/search'
    if (path.startsWith('/tail')) return '/tail'
    if (path.startsWith('/logs')) return '/logs'
    if (path.startsWith('/connections')) return '/connections'
    return '/overview'
  }, [location.pathname])

  const requireConnection = !['/connections'].includes(selectedKey)

  const onConnect = async (id: string) => {
    const err = await connect(id)
    if (err) message.error(err)
    else message.success('已连接集群')
  }

  return (
    <Layout className="app-root">
      <Sider width={220} theme="dark" className="app-sider">
        <div className="app-sider-brand">
          <Title level={4} style={{ color: '#fff', margin: 0, fontSize: 18 }}>
            Kafka Desktop
          </Title>
          <Text style={{ color: 'rgba(255,255,255,0.55)', fontSize: 12 }}>v{version || '…'}</Text>
        </div>
        <Menu
          className="app-sider-menu"
          theme="dark"
          mode="inline"
          selectedKeys={[selectedKey]}
          onClick={({ key }) => navigate(key)}
          items={[
            { key: '/connections', icon: <SettingOutlined />, label: '连接管理' },
            { key: '/overview', icon: <ClusterOutlined />, label: '集群概览' },
            { key: '/topics', icon: <DatabaseOutlined />, label: 'Topics' },
            { key: '/messages', icon: <MessageOutlined />, label: '消息浏览' },
            { key: '/produce', icon: <ApiOutlined />, label: '消息生产' },
            { key: '/groups', icon: <TeamOutlined />, label: '消费组' },
            { key: '/search', icon: <FileSearchOutlined />, label: '消息搜索' },
            { key: '/tail', icon: <RadarChartOutlined />, label: '实时消费' },
            { key: '/schema', icon: <ApiOutlined />, label: 'Schema Registry' },
            { key: '/logs', icon: <HistoryOutlined />, label: '操作日志' },
          ]}
        />
      </Sider>
      <Layout className="app-main">
        <Header className="app-header">
          <Space size="middle">
            <span>
              <span className={`conn-status-dot ${connected ? 'on' : 'off'}`} />
              {connected ? '已连接' : '未连接'}
            </span>
            <Select
              style={{ width: 300 }}
              placeholder="选择连接配置"
              value={activeConnectionId ?? undefined}
              options={connections.map((c) => ({
                value: c.id,
                label: `${c.name} (${c.bootstrapServers})`,
              }))}
              onChange={(id) => void onConnect(id)}
              allowClear={false}
            />
            {connected && (
              <Button
                onClick={() => {
                  void disconnect().then(() => message.info('已断开连接'))
                }}
              >
                断开
              </Button>
            )}
            {overview && (
              <Tag color="blue">
                Brokers: {overview.brokers.length}
                {overview.clusterId ? ` · ${overview.clusterId.slice(0, 8)}…` : ''}
              </Tag>
            )}
          </Space>
        </Header>
        <Content className="app-content">
          <Spin spinning={loading} style={{ height: '100%' }}>
            {requireConnection && !connected ? (
              <div className="page-card">
                <div className="page-empty">
                  <div style={{ textAlign: 'center' }}>
                    <Title level={4} style={{ marginBottom: 8 }}>
                      请先连接 Kafka 集群
                    </Title>
                    <Text type="secondary">
                      在顶部选择连接，或前往「连接管理」新增 PLAINTEXT / SASL / SSL 配置。
                    </Text>
                    <div style={{ marginTop: 20 }}>
                      <Button type="primary" onClick={() => navigate('/connections')}>
                        去连接管理
                      </Button>
                    </div>
                  </div>
                </div>
              </div>
            ) : (
              <Routes>
                <Route path="/" element={<Navigate to="/overview" replace />} />
                <Route path="/connections" element={<ConnectionsPage />} />
                <Route path="/overview" element={<OverviewPage />} />
                <Route path="/topics" element={<TopicsPage />} />
                <Route path="/topics/:name" element={<TopicDetailPage />} />
                <Route path="/messages" element={<MessagesPage />} />
                <Route path="/produce" element={<ProducePage />} />
                <Route path="/groups" element={<ConsumerGroupsPage />} />
                <Route path="/schema" element={<SchemaRegistryPage />} />
                <Route path="/search" element={<SearchPage />} />
                <Route path="/tail" element={<TailPage />} />
                <Route path="/logs" element={<LogsPage />} />
              </Routes>
            )}
          </Spin>
        </Content>
      </Layout>
    </Layout>
  )
}

export default function App() {
  return (
    <HashRouter>
      <Shell />
    </HashRouter>
  )
}
