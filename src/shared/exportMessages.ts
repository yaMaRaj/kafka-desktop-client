/**
 * 消息导出为 JSONL：组装行数据并调用 kafkaApi.saveTextFile 弹出保存对话框。
 */
import type { KafkaMessageView } from '@shared/types'
import dayjs from 'dayjs'

function tryParse(raw: string | null): unknown {
  if (raw == null) return null
  try {
    return JSON.parse(raw)
  } catch {
    return raw
  }
}

/** One message -> one plain object for export */
export function messageToExportRow(msg: KafkaMessageView) {
  return {
    topic: msg.topic,
    partition: msg.partition,
    offset: msg.offset,
    timestamp: msg.timestamp,
    datetime:
      msg.timestamp && msg.timestamp !== '-1'
        ? dayjs(Number(msg.timestamp)).format('YYYY-MM-DD HH:mm:ss.SSS')
        : null,
    key: msg.key,
    value: tryParse(msg.value),
    valueRaw: msg.value,
    headers: Object.fromEntries(msg.headers.map((h) => [h.key, h.value])),
    keySchemaId: msg.keySchemaId ?? null,
    valueSchemaId: msg.valueSchemaId ?? null,
  }
}

/** JSONL: one message per line */
export function toJsonl(messages: KafkaMessageView[]): string {
  return messages.map((m) => JSON.stringify(messageToExportRow(m))).join('\n') + (messages.length ? '\n' : '')
}

export function defaultExportFileName(prefix = 'kafka-messages'): string {
  return `${prefix}-${dayjs().format('YYYYMMDD-HHmmss')}.jsonl`
}

export function messageRowKey(m: KafkaMessageView): string {
  return `${m.topic}|${m.partition}|${m.offset}`
}

export async function saveMessagesExport(messages: KafkaMessageView[], filePrefix?: string) {
  if (!messages.length) {
    return { ok: false as const, error: '没有可导出的消息' }
  }
  const content = toJsonl(messages)
  const res = await window.kafkaApi.saveTextFile({
    defaultName: defaultExportFileName(filePrefix),
    content,
    filters: [
      { name: 'JSON Lines (一行一条)', extensions: ['jsonl'] },
      { name: '文本文件', extensions: ['txt'] },
      { name: '所有文件', extensions: ['*'] },
    ],
  })
  if (!res.ok) return { ok: false as const, error: res.error }
  if (!res.data.saved) return { ok: false as const, error: '已取消' }
  return { ok: true as const, filePath: res.data.filePath!, count: messages.length }
}
