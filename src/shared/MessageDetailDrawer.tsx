import { useMemo, useState, useEffect } from 'react'
import { Button, Drawer, Tabs, Tooltip, Typography, message } from 'antd'
import { CheckOutlined, CopyOutlined } from '@ant-design/icons'
import dayjs from 'dayjs'
import type { KafkaMessageView } from '@shared/types'

const { Text } = Typography

function tryFormatJson(raw: string | null): { ok: boolean; text: string } {
  if (raw == null || raw === '') return { ok: false, text: '(null)' }
  try {
    const parsed = JSON.parse(raw)
    return { ok: true, text: JSON.stringify(parsed, null, 2) }
  } catch {
    try {
      const parsed = JSON.parse(raw.trim())
      return { ok: true, text: JSON.stringify(parsed, null, 2) }
    } catch {
      return { ok: false, text: raw }
    }
  }
}

async function copyText(text: string) {
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text)
    } else {
      const ta = document.createElement('textarea')
      ta.value = text
      ta.style.position = 'fixed'
      ta.style.left = '-9999px'
      document.body.appendChild(ta)
      ta.select()
      document.execCommand('copy')
      document.body.removeChild(ta)
    }
    return true
  } catch {
    return false
  }
}

/** Code block with in-box copy icon (GitHub / docs style) */
function CodeBlock({
  text,
  jsonTone = false,
}: {
  text: string
  jsonTone?: boolean
}) {
  const [copied, setCopied] = useState(false)

  const onCopy = async () => {
    const ok = await copyText(text)
    if (!ok) {
      message.error('复制失败')
      return
    }
    setCopied(true)
    window.setTimeout(() => setCopied(false), 1500)
  }

  return (
    <div className="code-block">
      <Tooltip title={copied ? '已复制' : '复制'}>
        <button type="button" className="code-block-copy" onClick={() => void onCopy()}>
          {copied ? <CheckOutlined /> : <CopyOutlined />}
        </button>
      </Tooltip>
      <pre className={`message-value${jsonTone ? ' message-value-json' : ''}`}>{text}</pre>
    </div>
  )
}

function SimpleField({ title, raw }: { title: string; raw: string | null }) {
  return (
    <div className="detail-field">
      <Text strong>{title}</Text>
      <CodeBlock text={raw ?? '(null)'} />
    </div>
  )
}

function ValueField({ raw }: { raw: string | null }) {
  const formatted = useMemo(() => tryFormatJson(raw), [raw])
  const [tab, setTab] = useState(formatted.ok ? 'json' : 'raw')

  useEffect(() => {
    setTab(formatted.ok ? 'json' : 'raw')
  }, [raw, formatted.ok])

  const displayRaw = raw ?? '(null)'

  return (
    <div className="detail-field">
      <Text strong>Value</Text>
      <Tabs
        size="small"
        className="value-tabs"
        activeKey={tab}
        onChange={setTab}
        items={[
          {
            key: 'raw',
            label: '原文',
            children: <CodeBlock text={displayRaw} />,
          },
          {
            key: 'json',
            label: 'JSON',
            children: (
              <CodeBlock
                jsonTone
                text={formatted.ok ? formatted.text : '无法解析为 JSON，请查看「原文」'}
              />
            ),
          },
        ]}
      />
    </div>
  )
}

export function MessageDetailDrawer({
  message: msg,
  open,
  onClose,
}: {
  message: KafkaMessageView | null
  open: boolean
  onClose: () => void
}) {
  const headersText = useMemo(() => {
    if (!msg?.headers.length) return '(none)'
    return JSON.stringify(
      Object.fromEntries(msg.headers.map((h) => [h.key, h.value])),
      null,
      2,
    )
  }, [msg])

  const allJson = useMemo(() => {
    if (!msg) return ''
    return JSON.stringify(
      {
        topic: msg.topic,
        partition: msg.partition,
        offset: msg.offset,
        timestamp: msg.timestamp,
        key: msg.key,
        value: tryParse(msg.value),
        headers: Object.fromEntries(msg.headers.map((h) => [h.key, h.value])),
        keySchemaId: msg.keySchemaId,
        valueSchemaId: msg.valueSchemaId,
      },
      null,
      2,
    )
  }, [msg])

  return (
    <Drawer
      title={msg ? `消息详情  P${msg.partition} @ ${msg.offset}` : '消息详情'}
      open={open}
      width={720}
      onClose={onClose}
      extra={
        msg ? (
          <Tooltip title="复制整条消息 JSON">
            <Button
              type="text"
              icon={<CopyOutlined />}
              onClick={async () => {
                const ok = await copyText(allJson)
                if (ok) message.success('已复制')
                else message.error('复制失败')
              }}
            />
          </Tooltip>
        ) : null
      }
    >
      {msg && (
        <>
          <div className="message-meta">
            <div>
              <Text type="secondary">Topic</Text>
              <div className="mono">{msg.topic}</div>
            </div>
            <div>
              <Text type="secondary">Partition / Offset</Text>
              <div className="mono">
                {msg.partition} / {msg.offset}
              </div>
            </div>
            <div>
              <Text type="secondary">Timestamp</Text>
              <div className="mono">
                {msg.timestamp && msg.timestamp !== '-1'
                  ? dayjs(Number(msg.timestamp)).format('YYYY-MM-DD HH:mm:ss.SSS')
                  : '—'}
              </div>
            </div>
            {msg.valueSchemaId != null && (
              <div>
                <Text type="secondary">Value Schema ID</Text>
                <div className="mono">{msg.valueSchemaId}</div>
              </div>
            )}
          </div>

          <SimpleField title="Key" raw={msg.key} />
          <ValueField raw={msg.value} />
          <SimpleField title="Headers" raw={headersText} />
        </>
      )}
    </Drawer>
  )
}

function tryParse(raw: string | null): unknown {
  if (raw == null) return null
  try {
    return JSON.parse(raw)
  } catch {
    return raw
  }
}
