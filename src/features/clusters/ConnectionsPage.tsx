/** 连接管理：多集群配置 CRUD、测试连接（PLAINTEXT / SASL / SSL） */
import { useEffect, useState } from 'react'
import {
  Button,
  Form,
  Input,
  Modal,
  Select,
  Space,
  Switch,
  Table,
  Tag,
  Empty,
  message,
  Popconfirm,
  Divider,
} from 'antd'
import { PlusOutlined, EditOutlined, DeleteOutlined, ApiOutlined } from '@ant-design/icons'
import { v4 as uuid } from 'uuid'
import type { ConnectionProfile, SecurityProtocol, SaslMechanism } from '@shared/types'
import { useAppStore } from '@/stores/appStore'
import { PageShell, useTableScrollY } from '@/shared/PageShell'

const emptyProfile = (): ConnectionProfile => ({
  id: uuid(),
  name: '',
  bootstrapServers: 'localhost:9092',
  securityProtocol: 'PLAINTEXT',
  clientId: 'kafka-desktop',
  createdAt: Date.now(),
  updatedAt: Date.now(),
})

function protocolColor(p: SecurityProtocol) {
  if (p === 'PLAINTEXT') return 'default'
  if (p === 'SSL') return 'blue'
  if (p === 'SASL_PLAINTEXT') return 'orange'
  return 'purple'
}

export default function ConnectionsPage() {
  const { connections, refreshConnections, connect, activeConnectionId } = useAppStore()
  const [open, setOpen] = useState(false)
  const [editing, setEditing] = useState<ConnectionProfile | null>(null)
  const [form] = Form.useForm()
  const [testing, setTesting] = useState(false)
  const protocol: SecurityProtocol = Form.useWatch('securityProtocol', form)
  const { containerRef, scrollY } = useTableScrollY(55)

  useEffect(() => {
    void refreshConnections()
  }, [refreshConnections])

  const openCreate = () => {
    const p = emptyProfile()
    setEditing(p)
    form.setFieldsValue({
      ...p,
      saslMechanism: 'plain',
      saslUsername: '',
      saslPassword: '',
      sslCaPath: '',
      sslCertPath: '',
      sslKeyPath: '',
      sslPassphrase: '',
      rejectUnauthorized: true,
      schemaUrl: '',
      schemaUsername: '',
      schemaPassword: '',
    })
    setOpen(true)
  }

  const openEdit = (record: ConnectionProfile) => {
    setEditing(record)
    form.setFieldsValue({
      ...record,
      saslMechanism: record.sasl?.mechanism || 'plain',
      saslUsername: record.sasl?.username || '',
      saslPassword: '',
      sslCaPath: record.ssl?.caPath || '',
      sslCertPath: record.ssl?.certPath || '',
      sslKeyPath: record.ssl?.keyPath || '',
      sslPassphrase: '',
      rejectUnauthorized: record.ssl?.rejectUnauthorized ?? true,
      schemaUrl: record.schemaRegistry?.url || '',
      schemaUsername: record.schemaRegistry?.username || '',
      schemaPassword: '',
    })
    setOpen(true)
  }

  const buildProfile = (values: Record<string, unknown>): ConnectionProfile => {
    const base = editing || emptyProfile()
    const securityProtocol = values.securityProtocol as SecurityProtocol
    const profile: ConnectionProfile = {
      ...base,
      name: String(values.name),
      bootstrapServers: String(values.bootstrapServers),
      securityProtocol,
      clientId: String(values.clientId || 'kafka-desktop'),
      updatedAt: Date.now(),
    }

    if (securityProtocol === 'SASL_PLAINTEXT' || securityProtocol === 'SASL_SSL') {
      const password = String(values.saslPassword || '') || editing?.sasl?.password || ''
      profile.sasl = {
        mechanism: (values.saslMechanism as SaslMechanism) || 'plain',
        username: String(values.saslUsername || ''),
        password,
      }
    } else {
      delete profile.sasl
    }

    if (securityProtocol === 'SSL' || securityProtocol === 'SASL_SSL') {
      profile.ssl = {
        caPath: String(values.sslCaPath || '') || undefined,
        certPath: String(values.sslCertPath || '') || undefined,
        keyPath: String(values.sslKeyPath || '') || undefined,
        passphrase: String(values.sslPassphrase || '') || undefined,
        rejectUnauthorized: Boolean(values.rejectUnauthorized),
      }
      if (!values.sslPassphrase && editing?.ssl?.passphrase) {
        profile.ssl.passphrase = editing.ssl.passphrase
      }
    } else {
      delete profile.ssl
    }

    const schemaUrl = String(values.schemaUrl || '')
    if (schemaUrl) {
      profile.schemaRegistry = {
        url: schemaUrl,
        username: String(values.schemaUsername || '') || undefined,
        password: String(values.schemaPassword || '') || undefined,
      }
      if (!values.schemaPassword && editing?.schemaRegistry?.password) {
        profile.schemaRegistry.password = editing.schemaRegistry.password
      }
    } else {
      delete profile.schemaRegistry
    }

    return profile
  }

  const onSave = async () => {
    const values = await form.validateFields()
    const profile = buildProfile(values)
    const res = await window.kafkaApi.saveConnection(profile)
    if (!res.ok) {
      message.error(res.error)
      return
    }
    message.success('已保存连接')
    setOpen(false)
    await refreshConnections()
  }

  const onTest = async () => {
    const values = await form.validateFields()
    const profile = buildProfile(values)
    setTesting(true)
    try {
      const res = await window.kafkaApi.testConnection(profile)
      if (!res.ok) {
        message.error(res.error)
        return
      }
      message.success(
        `连接成功：${res.data.brokers.length} 个 Broker` +
          (res.data.clusterId ? `，ClusterId=${res.data.clusterId}` : ''),
      )
    } finally {
      setTesting(false)
    }
  }

  const onDelete = async (id: string) => {
    const res = await window.kafkaApi.deleteConnection(id)
    if (!res.ok) message.error(res.error)
    else {
      message.success('已删除')
      await refreshConnections()
    }
  }

  return (
    <PageShell
      title="连接管理"
      subtitle={`支持 PLAINTEXT / SASL / SSL，配置本地加密保存 · 共 ${connections.length} 条`}
      extra={
        <Button type="primary" icon={<PlusOutlined />} onClick={openCreate}>
          新建连接
        </Button>
      }
    >
      <div ref={containerRef} className="table-fill">
        <Table
          rowKey="id"
          size="middle"
          dataSource={connections}
          pagination={false}
          scroll={{ y: scrollY, x: 900 }}
          locale={{
            emptyText: (
              <Empty
                image={Empty.PRESENTED_IMAGE_SIMPLE}
                description="暂无连接配置，点击右上角新建"
              >
                <Button type="primary" icon={<PlusOutlined />} onClick={openCreate}>
                  新建连接
                </Button>
              </Empty>
            ),
          }}
          columns={[
            {
              title: '名称',
              dataIndex: 'name',
              width: 180,
              ellipsis: true,
              render: (name: string, r) => (
                <Space size={6}>
                  <span style={{ fontWeight: 500 }}>{name}</span>
                  {r.id === activeConnectionId ? <Tag color="success">当前</Tag> : null}
                </Space>
              ),
            },
            {
              title: 'Bootstrap Servers',
              dataIndex: 'bootstrapServers',
              ellipsis: true,
              render: (v: string) => <span className="mono">{v}</span>,
            },
            {
              title: '协议',
              dataIndex: 'securityProtocol',
              width: 150,
              render: (v: SecurityProtocol) => (
                <Tag className="protocol-tag" color={protocolColor(v)}>
                  {v}
                </Tag>
              ),
            },
            {
              title: 'Schema Registry',
              width: 220,
              ellipsis: true,
              render: (_, r) =>
                r.schemaRegistry?.url ? (
                  <span className="mono">{r.schemaRegistry.url}</span>
                ) : (
                  <span className="table-cell-secondary">未配置</span>
                ),
            },
            {
              title: '操作',
              width: 220,
              fixed: 'right',
              render: (_, r) => (
                <Space size={4}>
                  <Button
                    size="small"
                    type="primary"
                    ghost
                    icon={<ApiOutlined />}
                    onClick={async () => {
                      const err = await connect(r.id)
                      if (err) message.error(err)
                      else message.success('已连接')
                    }}
                  >
                    连接
                  </Button>
                  <Button size="small" icon={<EditOutlined />} onClick={() => openEdit(r)}>
                    编辑
                  </Button>
                  <Popconfirm title="确认删除该连接？" onConfirm={() => void onDelete(r.id)}>
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
        title={editing ? '编辑连接' : '新建连接'}
        open={open}
        onCancel={() => setOpen(false)}
        width={720}
        destroyOnClose
        footer={[
          <Button key="test" loading={testing} onClick={() => void onTest()}>
            测试连接
          </Button>,
          <Button key="cancel" onClick={() => setOpen(false)}>
            取消
          </Button>,
          <Button key="save" type="primary" onClick={() => void onSave()}>
            保存
          </Button>,
        ]}
      >
        <Form form={form} layout="vertical">
          <Form.Item name="name" label="名称" rules={[{ required: true, message: '请输入名称' }]}>
            <Input placeholder="例如：本地开发 / 测试集群" />
          </Form.Item>
          <Form.Item
            name="bootstrapServers"
            label="Bootstrap Servers"
            rules={[{ required: true, message: '请输入 Bootstrap Servers' }]}
          >
            <Input placeholder="host1:9092,host2:9092" />
          </Form.Item>
          <Form.Item name="clientId" label="Client ID">
            <Input />
          </Form.Item>
          <Form.Item name="securityProtocol" label="安全协议" rules={[{ required: true }]}>
            <Select
              options={[
                { value: 'PLAINTEXT', label: 'PLAINTEXT' },
                { value: 'SASL_PLAINTEXT', label: 'SASL_PLAINTEXT' },
                { value: 'SASL_SSL', label: 'SASL_SSL' },
                { value: 'SSL', label: 'SSL' },
              ]}
            />
          </Form.Item>

          {(protocol === 'SASL_PLAINTEXT' || protocol === 'SASL_SSL') && (
            <>
              <Divider>SASL</Divider>
              <Form.Item name="saslMechanism" label="机制" rules={[{ required: true }]}>
                <Select
                  options={[
                    { value: 'plain', label: 'PLAIN' },
                    { value: 'scram-sha-256', label: 'SCRAM-SHA-256' },
                    { value: 'scram-sha-512', label: 'SCRAM-SHA-512' },
                  ]}
                />
              </Form.Item>
              <Form.Item name="saslUsername" label="用户名" rules={[{ required: true }]}>
                <Input />
              </Form.Item>
              <Form.Item
                name="saslPassword"
                label="密码"
                extra={editing?.sasl ? '留空则保留原密码' : undefined}
              >
                <Input.Password placeholder={editing?.sasl ? '••••••••' : ''} />
              </Form.Item>
            </>
          )}

          {(protocol === 'SSL' || protocol === 'SASL_SSL') && (
            <>
              <Divider>SSL 证书</Divider>
              <Form.Item name="sslCaPath" label="CA 证书路径">
                <Input placeholder="C:\\certs\\ca.pem" />
              </Form.Item>
              <Form.Item name="sslCertPath" label="客户端证书路径">
                <Input placeholder="C:\\certs\\client.crt" />
              </Form.Item>
              <Form.Item name="sslKeyPath" label="客户端私钥路径">
                <Input placeholder="C:\\certs\\client.key" />
              </Form.Item>
              <Form.Item name="sslPassphrase" label="私钥口令">
                <Input.Password />
              </Form.Item>
              <Form.Item name="rejectUnauthorized" label="校验服务端证书" valuePropName="checked">
                <Switch />
              </Form.Item>
            </>
          )}

          <Divider>Schema Registry（可选）</Divider>
          <Form.Item name="schemaUrl" label="Registry URL">
            <Input placeholder="http://localhost:8081" />
          </Form.Item>
          <Form.Item name="schemaUsername" label="用户名">
            <Input />
          </Form.Item>
          <Form.Item name="schemaPassword" label="密码">
            <Input.Password />
          </Form.Item>
        </Form>
      </Modal>
    </PageShell>
  )
}
