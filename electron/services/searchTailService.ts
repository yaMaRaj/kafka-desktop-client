import { v4 as uuid } from 'uuid'
import type { Consumer, EachMessagePayload } from 'kafkajs'
import type {
  SearchMessagesParams,
  SearchProgress,
  TailParams,
  KafkaMessageView,
} from '../../shared/types'
import { getOrCreateClient } from './kafkaClient'
import { toMessageView } from './messageService'

type ProgressEmitter = (searchId: string, progress: SearchProgress) => void
type MessageEmitter = (tailId: string, message: KafkaMessageView) => void
type ErrorEmitter = (tailId: string, error: string) => void

const activeSearches = new Map<string, { cancelled: boolean }>()
const activeTails = new Map<string, { consumer: Consumer; stopped: boolean }>()

function matchesQuery(
  message: KafkaMessageView,
  query: string,
  useRegex: boolean,
  searchKey: boolean,
  searchValue: boolean,
): boolean {
  if (!query) return true
  const parts: string[] = []
  if (searchKey && message.key) parts.push(message.key)
  if (searchValue && message.value) parts.push(message.value)
  const haystack = parts.join('\n')

  if (useRegex) {
    try {
      const re = new RegExp(query, 'i')
      return re.test(haystack)
    } catch {
      return haystack.toLowerCase().includes(query.toLowerCase())
    }
  }
  return haystack.toLowerCase().includes(query.toLowerCase())
}

export async function searchMessages(
  connectionId: string,
  params: SearchMessagesParams,
  searchId: string,
  emitProgress: ProgressEmitter,
): Promise<{ messages: KafkaMessageView[]; progress: SearchProgress }> {
  const maxScan = Math.min(params.maxScan ?? 2000, 20000)
  const decodeSchema = params.decodeSchema ?? true
  const searchKey = params.searchKey ?? true
  const searchValue = params.searchValue ?? true
  const state = { cancelled: false }
  activeSearches.set(searchId, state)

  const { kafka, admin } = await getOrCreateClient(connectionId)
  const offsets = await admin.fetchTopicOffsets(params.topic)
  const targetPartitions =
    params.partitions && params.partitions.length > 0
      ? offsets.filter((o) => params.partitions!.includes(o.partition))
      : offsets

  const matched: KafkaMessageView[] = []
  let scanned = 0

  const consumer = kafka.consumer({
    groupId: `kafka-desktop-search-${searchId}`,
    allowAutoTopicCreation: false,
  })

  await consumer.connect()

  try {
    await consumer.subscribe({ topic: params.topic, fromBeginning: true })

    const seekPlan = new Map<number, string>()
    for (const p of targetPartitions) {
      seekPlan.set(p.partition, params.fromOffset ?? p.low)
    }

    let joined = false
    const joinPromise = new Promise<void>((resolve) => {
      consumer.on(consumer.events.GROUP_JOIN, () => {
        if (!joined) {
          joined = true
          for (const [partition, offset] of seekPlan) {
            try {
              consumer.seek({ topic: params.topic, partition, offset })
            } catch {
              /* ignore */
            }
          }
          resolve()
        }
      })
    })

    const runPromise = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => resolve(), 60000)

      consumer
        .run({
          autoCommit: false,
          eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
            if (state.cancelled) {
              clearTimeout(timer)
              resolve()
              return
            }
            if (!seekPlan.has(partition)) return

            const high = targetPartitions.find((p) => p.partition === partition)?.high
            if (high != null && BigInt(message.offset) >= BigInt(high)) {
              seekPlan.delete(partition)
              if (seekPlan.size === 0) {
                clearTimeout(timer)
                resolve()
              }
              return
            }

            scanned++
            const view = await toMessageView(
              connectionId,
              topic,
              partition,
              message,
              decodeSchema,
            )
            if (
              matchesQuery(view, params.query, !!params.useRegex, searchKey, searchValue)
            ) {
              matched.push(view)
            }

            if (scanned % 50 === 0) {
              emitProgress(searchId, {
                scanned,
                matched: matched.length,
                done: false,
              })
            }

            if (scanned >= maxScan) {
              clearTimeout(timer)
              resolve()
            }
          },
        })
        .catch(reject)

      void joinPromise
    })

    await runPromise
  } finally {
    await consumer.disconnect().catch(() => undefined)
    activeSearches.delete(searchId)
  }

  const progress: SearchProgress = {
    scanned,
    matched: matched.length,
    done: true,
    cancelled: state.cancelled,
  }
  emitProgress(searchId, progress)
  return { messages: matched, progress }
}

export function cancelSearch(searchId: string): boolean {
  const state = activeSearches.get(searchId)
  if (state) {
    state.cancelled = true
    return true
  }
  return false
}

export async function startTail(
  connectionId: string,
  params: TailParams,
  emitMessage: MessageEmitter,
  emitError: ErrorEmitter,
): Promise<string> {
  const tailId = uuid()
  const { kafka } = await getOrCreateClient(connectionId)
  const decodeSchema = params.decodeSchema ?? true

  const consumer = kafka.consumer({
    groupId: `kafka-desktop-tail-${tailId}`,
    allowAutoTopicCreation: false,
  })

  await consumer.connect()
  await consumer.subscribe({
    topic: params.topic,
    fromBeginning: !!params.fromBeginning,
  })

  const entry = { consumer, stopped: false }
  activeTails.set(tailId, entry)

  void consumer
    .run({
      autoCommit: true,
      eachMessage: async ({ topic, partition, message }) => {
        if (entry.stopped) return
        try {
          const view = await toMessageView(
            connectionId,
            topic,
            partition,
            message,
            decodeSchema,
          )
          if (
            params.filter &&
            !matchesQuery(view, params.filter, !!params.useRegex, true, true)
          ) {
            return
          }
          emitMessage(tailId, view)
        } catch (err) {
          emitError(tailId, err instanceof Error ? err.message : String(err))
        }
      },
    })
    .catch((err) => {
      emitError(tailId, err instanceof Error ? err.message : String(err))
    })

  return tailId
}

export async function stopTail(tailId: string): Promise<boolean> {
  const entry = activeTails.get(tailId)
  if (!entry) return false
  entry.stopped = true
  try {
    await entry.consumer.stop()
  } catch {
    /* ignore */
  }
  try {
    await entry.consumer.disconnect()
  } catch {
    /* ignore */
  }
  activeTails.delete(tailId)
  return true
}
