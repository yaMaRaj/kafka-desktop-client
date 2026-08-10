/** 消息生产：Key/Value/Headers、分区、可选 Schema 编码；底部吸底发送 */
import { useEffect, useState } from 'react'
import {
  Button,
  Form,
  Input,
  InputNumber,
  Select,
  Space,
  Switch,
  message,
  Divider,
} from 'antd'
import { SendOutlined, PlusOutlined, MinusCircleOutlined } from '@ant-design/icons'
import type { TopicInfo } from '@shared/types'
import { useAppStore } from '@/stores/appStore'
import { PageShell } from '@/shared/PageShell'

export default function ProducePage() {
  const { activeConnectionId } = useAppStore()
  const [topics, setTopics] = useState<TopicInfo[]>([])
  const [subjects, setSubjects] = useState<string[]>([])
  const [loading, setLoading] = useState(false)
  const [form] = Form.useForm()
  const encodeWithSchema = Form.useWatch('encodeWithSchema', form)

  useEffect(() => {
    if (!activeConnectionId) return
    void window.kafkaApi.listTopics(activeConnectionId).then((res) => {
      if (res.ok) setTopics(res.data.filter((t) => !t.isInternal))
    })
    void window.kafkaApi.listSubjects(activeConnectionId).then((res) => {
      if (res.ok) setSubjects(res.data)
    })
  }, [activeConnectionId])

  const onSend = async () => {
    if (!activeConnectionId) return
    const values = await form.validateFields()
    const headers = (values.headers || [])
      .filter((h: { key?: string; value?: string }) => h?.key)
      .map((h: { key: string; value?: string }) => ({
        key: h.key,
        value: h.value || '',
      }))

    const batchCount = values.batchCount || 1
    const messages = []
    for (let i = 0; i < batchCount; i++) {
      let value = values.value as string
      if (batchCount > 1 && values.batchIndexPlaceholder) {
        value = value.split('{{i}}').join(String(i))
      }
      messages.push({
        key: values.key || undefined,
        value,
        partition: values.partition,
        headers: headers.length ? headers : undefined,
        encodeWithSchema: !!values.encodeWithSchema,
        subject: values.subject,
      })
    }

    setLoading(true)
    try {
      const res = await window.kafkaApi.produceMessages(activeConnectionId, {
        topic: values.topic,
        messages,
      })
      if (!res.ok) {
        message.error(res.error)
        return
      }
      message.success(
        `已发送 ${res.data.length} 条：` +
          res.data.map((r) => `P${r.partition}@${r.offset}`).join(', '),
      )
    } finally {
      setLoading(false)
    }
  }

  return (
    <PageShell
      title="消息生产"
      subtitle="单条或批量发送，支持 Headers 与 Schema 编码"
      bodyScroll
      footer={
        <Button
          type="primary"
          size="large"
          icon={<SendOutlined />}
          loading={loading}
          onClick={() => void onSend()}
        >
          发送消息
        </Button>
      }
    >
      <Form
        form={form}
        layout="vertical"
        style={{ maxWidth: 840 }}
        initialValues={{ batchCount: 1, encodeWithSchema: false }}
        onFinish={() => void onSend()}
      >
        <Form.Item name="topic" label="Topic" rules={[{ required: true, message: '请选择 Topic' }]}>
          <Select
            showSearch
            placeholder="选择 Topic"
            options={topics.map((t) => ({ value: t.name, label: t.name }))}
          />
        </Form.Item>
        <Form.Item name="partition" label="指定分区（可选）">
          <InputNumber min={0} style={{ width: '100%' }} placeholder="留空则自动分配" />
        </Form.Item>
        <Form.Item name="key" label="Key">
          <Input placeholder="可选" />
        </Form.Item>
        <Form.Item name="value" label="Value" rules={[{ required: true, message: '请输入消息内容' }]}>
          <Input.TextArea rows={12} className="mono" placeholder='{"hello":"world"}' />
        </Form.Item>

        <Form.List name="headers">
          {(fields, { add, remove }) => (
            <>
              <Divider orientation="left">Headers</Divider>
              {fields.map((field) => (
                <Space key={field.key} align="baseline" style={{ display: 'flex' }}>
                  <Form.Item {...field} name={[field.name, 'key']} rules={[{ required: true }]}>
                    <Input placeholder="key" style={{ width: 180 }} />
                  </Form.Item>
                  <Form.Item {...field} name={[field.name, 'value']}>
                    <Input placeholder="value" style={{ width: 280 }} />
                  </Form.Item>
                  <MinusCircleOutlined onClick={() => remove(field.name)} />
                </Space>
              ))}
              <Button type="dashed" onClick={() => add()} icon={<PlusOutlined />} block>
                添加 Header
              </Button>
            </>
          )}
        </Form.List>

        <Divider />
        <Form.Item name="batchCount" label="批量条数">
          <InputNumber min={1} max={1000} style={{ width: '100%' }} />
        </Form.Item>
        <Form.Item
          name="batchIndexPlaceholder"
          label="批量时替换 {{i}} 为序号"
          valuePropName="checked"
        >
          <Switch />
        </Form.Item>
        <Form.Item
          name="encodeWithSchema"
          label="使用 Schema Registry 编码"
          valuePropName="checked"
        >
          <Switch />
        </Form.Item>
        {encodeWithSchema && (
          <Form.Item name="subject" label="Subject" rules={[{ required: true }]}>
            <Select
              showSearch
              options={subjects.map((s) => ({ value: s, label: s }))}
              placeholder="选择 Subject"
            />
          </Form.Item>
        )}
      </Form>
    </PageShell>
  )
}
