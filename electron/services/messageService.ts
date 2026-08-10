/**
 * 消息浏览与生产。
 * - fetch：临时 Consumer，按分区 / offset / 时间戳拉取，可选 Schema 解码
 * - produce：支持 Headers、指定分区、Schema 编码
 * - toMessageView：统一为前端展示结构 KafkaMessageView
 */
import { CompressionTypes, type EachMessagePayload } from 'kafkajs'
import type {
  FetchMessagesParams,
  ProduceMessageParams,
  KafkaMessageView,
  KafkaHeader,
} from '../../shared/types'
import { getOrCreateClient } from './kafkaClient'
import { decodeMaybe, encodeWithSubject } from './schemaService'
import { bufferToDisplay, tryParseJson } from './utils'

function headersToView(
  headers?: Record<string, Buffer | string | (Buffer | string)[] | undefined>,
): KafkaHeader[] {
  if (!headers) return []
  const result: KafkaHeader[] = []
  for (const [key, value] of Object.entries(headers)) {
    if (value == null) continue
    const values = Array.isArray(value) ? value : [value]
    for (const v of values) {
      result.push({
        key,
        value: Buffer.isBuffer(v) ? v.toString('utf8') : String(v),
      })
    }
  }
  return result
}

/** 将 KafkaJS 原始消息转为 UI 可用的 KafkaMessageView */
export async function toMessageView(
  connectionId: string,
  topic: string,
  partition: number,
  message: EachMessagePayload['message'],
  decodeSchema: boolean,
): Promise<KafkaMessageView> {
  const keyBuf = message.key
  const valueBuf = message.value

  const keyDecoded = await decodeMaybe(
    connectionId,
    keyBuf && Buffer.isBuffer(keyBuf) ? keyBuf : keyBuf ? Buffer.from(keyBuf) : null,
    decodeSchema,
  )
  const valueDecoded = await decodeMaybe(
    connectionId,
    valueBuf && Buffer.isBuffer(valueBuf) ? valueBuf : valueBuf ? Buffer.from(valueBuf) : null,
    decodeSchema,
  )

  return {
    topic,
    partition,
    offset: message.offset,
    timestamp: message.timestamp,
    key:
      keyDecoded.text ??
      (keyBuf ? bufferToDisplay(Buffer.isBuffer(keyBuf) ? keyBuf : Buffer.from(keyBuf)) : null),
    value:
      valueDecoded.text ??
      (valueBuf
        ? bufferToDisplay(Buffer.isBuffer(valueBuf) ? valueBuf : Buffer.from(valueBuf))
        : null),
    headers: headersToView(message.headers),
    keySchemaId: keyDecoded.schemaId,
    valueSchemaId: valueDecoded.schemaId,
  }
}

/** 按条件拉取消息；limit 默认 50，上限 10000 */
export async function fetchMessages(
  connectionId: string,
  params: FetchMessagesParams,
): Promise<KafkaMessageView[]> {
  const { kafka, admin } = await getOrCreateClient(connectionId)
  const limit = Math.min(params.limit ?? 50, 10000)
  const decodeSchema = params.decodeSchema ?? true

  const offsets = await admin.fetchTopicOffsets(params.topic)
  const targetPartitions =
    params.partition != null
      ? offsets.filter((o) => o.partition === params.partition)
      : offsets

  if (targetPartitions.length === 0) return []

  const fromByPartition = new Map<number, string>()

  if (params.fromTimestamp != null) {
    const tsOffsets = await admin.fetchTopicOffsetsByTimestamp(
      params.topic,
      params.fromTimestamp,
    )
    for (const o of tsOffsets) {
      if (params.partition == null || o.partition === params.partition) {
        fromByPartition.set(o.partition, o.offset)
      }
    }
  } else if (params.fromOffset != null) {
    for (const o of targetPartitions) {
      fromByPartition.set(o.partition, params.fromOffset)
    }
  } else {
    for (const o of targetPartitions) {
      const high = BigInt(o.high)
      const low = BigInt(o.low)
      const start = high > BigInt(limit) ? high - BigInt(limit) : low
      const clamped = start < low ? low : start
      fromByPartition.set(o.partition, clamped.toString())
    }
  }

  const highByPartition = new Map(targetPartitions.map((o) => [o.partition, o.high]))
  const remaining = new Set(fromByPartition.keys())
  const results: KafkaMessageView[] = []

  const consumer = kafka.consumer({
    groupId: `kafka-desktop-fetch-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    allowAutoTopicCreation: false,
  })

  await consumer.connect()

  try {
    await consumer.subscribe({ topic: params.topic, fromBeginning: true })

    let seekDone = false
    consumer.on(consumer.events.GROUP_JOIN, () => {
      if (seekDone) return
      seekDone = true
      for (const [partition, offset] of fromByPartition) {
        try {
          consumer.seek({ topic: params.topic, partition, offset })
        } catch {
          /* ignore */
        }
      }
    })

    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => resolve(), 20000)

      consumer
        .run({
          autoCommit: false,
          eachMessage: async ({ topic, partition, message }) => {
            if (!remaining.has(partition)) return
            const high = highByPartition.get(partition)
            if (high != null && BigInt(message.offset) >= BigInt(high)) {
              remaining.delete(partition)
              if (remaining.size === 0 || results.length >= limit) {
                clearTimeout(timer)
                resolve()
              }
              return
            }

            results.push(
              await toMessageView(connectionId, topic, partition, message, decodeSchema),
            )

            if (results.length >= limit) {
              clearTimeout(timer)
              resolve()
            }
          },
        })
        .catch(reject)
    })
  } finally {
    try {
      await consumer.stop()
    } catch {
      /* ignore */
    }
    await consumer.disconnect().catch(() => undefined)
  }

  results.sort((a, b) => {
    if (a.partition !== b.partition) return a.partition - b.partition
    return BigInt(a.offset) < BigInt(b.offset) ? -1 : BigInt(a.offset) > BigInt(b.offset) ? 1 : 0
  })

  return results.slice(0, limit)
}

export async function produceMessages(
  connectionId: string,
  params: ProduceMessageParams,
): Promise<Array<{ topic: string; partition: number; offset: string }>> {
  const { producer } = await getOrCreateClient(connectionId)

  const messages = []
  for (const m of params.messages) {
    let value: Buffer | string = m.value
    const key: Buffer | string | undefined = m.key ?? undefined

    if (m.encodeWithSchema && m.subject) {
      const payload = tryParseJson(m.value)
      value = await encodeWithSubject(connectionId, m.subject, payload)
    }

    messages.push({
      key,
      value,
      partition: m.partition,
      headers: m.headers
        ? Object.fromEntries(m.headers.map((h) => [h.key, h.value]))
        : undefined,
    })
  }

  const result = await producer.send({
    topic: params.topic,
    compression: CompressionTypes.GZIP,
    messages,
  })

  return result.map((r) => ({
    topic: r.topicName,
    partition: r.partition,
    offset: r.baseOffset || '0',
  }))
}

export { headersToView }
