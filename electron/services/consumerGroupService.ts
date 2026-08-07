import { v4 as uuid } from 'uuid'
import type {
  ConsumerGroupInfo,
  ConsumerGroupOffset,
  ResetOffsetsParams,
} from '../../shared/types'
import { getOrCreateClient } from './kafkaClient'
import { appendOperationLog } from '../store/configStore'

export async function listConsumerGroups(connectionId: string): Promise<string[]> {
  const { admin } = await getOrCreateClient(connectionId)
  const groups = await admin.listGroups()
  return groups.groups.map((g) => g.groupId).sort()
}

export async function describeConsumerGroup(
  connectionId: string,
  groupId: string,
): Promise<ConsumerGroupInfo> {
  const { admin } = await getOrCreateClient(connectionId)
  const described = await admin.describeGroups([groupId])
  const group = described.groups[0]
  if (!group) throw new Error(`Consumer Group 不存在: ${groupId}`)

  const offsetMap = await admin.fetchOffsets({ groupId, resolveOffsets: true })
  const offsets: ConsumerGroupOffset[] = []
  let totalLag = 0n

  for (const topicOffsets of offsetMap) {
    const topic = topicOffsets.topic
    const highWatermarks = await admin.fetchTopicOffsets(topic)
    const highMap = new Map(highWatermarks.map((h) => [h.partition, h.high]))

    for (const p of topicOffsets.partitions) {
      const high = highMap.get(p.partition) ?? p.offset
      const committed = BigInt(p.offset)
      const highBi = BigInt(high)
      const lag = highBi > committed ? highBi - committed : 0n
      totalLag += lag
      offsets.push({
        topic,
        partition: p.partition,
        offset: p.offset,
        high,
        lag: lag.toString(),
        metadata: p.metadata || undefined,
      })
    }
  }

  const members = (group.members || []).map((m) => {
    let assignments: Array<{ topic: string; partitions: number[] }> = []
    try {
      // memberAssignment is a Buffer in KafkaJS describeGroups
      const assignment = m.memberAssignment
      if (assignment && Buffer.isBuffer(assignment) && assignment.length > 0) {
        // KafkaJS may expose memberAssignment as decoded in some versions;
        // fallback: leave empty if cannot parse
      }
    } catch {
      /* ignore */
    }

    // Prefer memberAssignment from KafkaJS decoded structure when available
    const anyMember = m as unknown as {
      memberAssignment?: { topic?: string; partitions?: number[] }[] | Buffer
    }
    if (Array.isArray(anyMember.memberAssignment)) {
      assignments = anyMember.memberAssignment
        .filter((a) => a.topic)
        .map((a) => ({
          topic: a.topic!,
          partitions: a.partitions || [],
        }))
    }

    return {
      memberId: m.memberId,
      clientId: m.clientId,
      clientHost: m.clientHost,
      assignments,
    }
  })

  return {
    groupId: group.groupId,
    state: group.state,
    protocol: group.protocol || undefined,
    protocolType: group.protocolType || undefined,
    members,
    offsets: offsets.sort((a, b) =>
      a.topic === b.topic ? a.partition - b.partition : a.topic.localeCompare(b.topic),
    ),
    totalLag: totalLag.toString(),
  }
}

export async function resetOffsets(
  connectionId: string,
  params: ResetOffsetsParams,
): Promise<boolean> {
  const { admin } = await getOrCreateClient(connectionId)

  // Warn path: check active members
  const described = await admin.describeGroups([params.groupId])
  const state = described.groups[0]?.state
  if (state === 'Stable' || state === 'PreparingRebalance' || state === 'CompletingRebalance') {
    const memberCount = described.groups[0]?.members?.length ?? 0
    if (memberCount > 0) {
      throw new Error(
        `NON_EMPTY_GROUP: Consumer Group「${params.groupId}」当前有 ${memberCount} 个活跃成员（状态 ${state}），请先停止消费者再重置偏移`,
      )
    }
  }

  const topicOffsets = await admin.fetchTopicOffsets(params.topic)
  const partitions =
    params.partitions && params.partitions.length > 0
      ? topicOffsets.filter((p) => params.partitions!.includes(p.partition))
      : topicOffsets

  if (partitions.length === 0) {
    throw new Error('没有可重置的分区')
  }

  let partitionsToReset: Array<{ partition: number; offset: string }> = []

  if (params.strategy === 'earliest') {
    partitionsToReset = partitions.map((p) => ({ partition: p.partition, offset: p.low }))
  } else if (params.strategy === 'latest') {
    partitionsToReset = partitions.map((p) => ({ partition: p.partition, offset: p.high }))
  } else if (params.strategy === 'offset') {
    if (params.offset == null) throw new Error('请指定 offset')
    partitionsToReset = partitions.map((p) => ({
      partition: p.partition,
      offset: params.offset!,
    }))
  } else if (params.strategy === 'timestamp') {
    if (params.timestamp == null) throw new Error('请指定时间戳')
    const byTs = await admin.fetchTopicOffsetsByTimestamp(params.topic, params.timestamp)
    const wanted = new Set(partitions.map((p) => p.partition))
    partitionsToReset = byTs
      .filter((p) => wanted.has(p.partition))
      .map((p) => ({ partition: p.partition, offset: p.offset }))
  }

  await admin.setOffsets({
    groupId: params.groupId,
    topic: params.topic,
    partitions: partitionsToReset,
  })

  appendOperationLog({
    id: uuid(),
    at: Date.now(),
    connectionId,
    action: 'resetOffsets',
    detail: `${params.groupId} / ${params.topic} -> ${params.strategy}`,
    success: true,
  })
  return true
}

export async function deleteConsumerGroup(
  connectionId: string,
  groupId: string,
): Promise<boolean> {
  const { admin } = await getOrCreateClient(connectionId)
  await admin.deleteGroups([groupId])
  appendOperationLog({
    id: uuid(),
    at: Date.now(),
    connectionId,
    action: 'deleteConsumerGroup',
    detail: groupId,
    success: true,
  })
  return true
}
