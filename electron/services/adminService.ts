import { v4 as uuid } from 'uuid'
import type { ClusterOverview, TopicInfo, TopicCreateParams, TopicOffsets } from '../../shared/types'
import { getOrCreateClient, createTransientClient } from './kafkaClient'
import { appendOperationLog } from '../store/configStore'
import { ConfigResourceTypes, type Admin } from 'kafkajs'

export async function fetchClusterOverview(admin: Admin): Promise<ClusterOverview> {
  const describe = await admin.describeCluster()
  return {
    clusterId: describe.clusterId,
    controllerId: describe.controller ?? undefined,
    brokers: describe.brokers.map((b) => ({
      nodeId: b.nodeId,
      host: b.host,
      port: b.port,
      rack: (b as { rack?: string }).rack || undefined,
    })),
  }
}

export async function testConnectionOverview(profile: Parameters<typeof createTransientClient>[0]) {
  const bundle = await createTransientClient(profile)
  try {
    return await fetchClusterOverview(bundle.admin)
  } finally {
    await bundle.admin.disconnect().catch(() => undefined)
    await bundle.producer.disconnect().catch(() => undefined)
  }
}

export async function getOverview(connectionId: string): Promise<ClusterOverview> {
  const { admin } = await getOrCreateClient(connectionId)
  return fetchClusterOverview(admin)
}

export async function listTopics(connectionId: string): Promise<TopicInfo[]> {
  const { admin } = await getOrCreateClient(connectionId)
  const names = await admin.listTopics()
  if (names.length === 0) return []

  const meta = await admin.fetchTopicMetadata({ topics: names })
  return meta.topics.map((t) => ({
    name: t.name,
    isInternal: t.name.startsWith('__'),
    partitions: t.partitions.map((p) => ({
      partitionId: p.partitionId,
      leader: p.leader,
      replicas: p.replicas,
      isr: p.isr,
    })),
  }))
}

export async function getTopic(connectionId: string, topic: string): Promise<TopicInfo> {
  const { admin } = await getOrCreateClient(connectionId)
  const meta = await admin.fetchTopicMetadata({ topics: [topic] })
  const t = meta.topics[0]
  if (!t) throw new Error(`Topic 不存在: ${topic}`)

  const configs = await admin.describeConfigs({
    resources: [{ type: ConfigResourceTypes.TOPIC, name: topic }],
    includeSynonyms: false,
  })

  const configMap: Record<string, string> = {}
  const entries = configs.resources[0]?.configEntries || []
  for (const entry of entries) {
    if (entry.configName && entry.configValue != null) {
      configMap[entry.configName] = entry.configValue
    }
  }

  return {
    name: t.name,
    isInternal: t.name.startsWith('__'),
    partitions: t.partitions.map((p) => ({
      partitionId: p.partitionId,
      leader: p.leader,
      replicas: p.replicas,
      isr: p.isr,
    })),
    config: configMap,
  }
}

export async function createTopic(connectionId: string, params: TopicCreateParams): Promise<boolean> {
  const { admin } = await getOrCreateClient(connectionId)
  await admin.createTopics({
    topics: [
      {
        topic: params.name,
        numPartitions: params.numPartitions,
        replicationFactor: params.replicationFactor,
        configEntries: params.configEntries,
      },
    ],
    waitForLeaders: true,
  })
  appendOperationLog({
    id: uuid(),
    at: Date.now(),
    connectionId,
    action: 'createTopic',
    detail: `${params.name} (partitions=${params.numPartitions}, rf=${params.replicationFactor})`,
    success: true,
  })
  return true
}

export async function deleteTopic(connectionId: string, topic: string): Promise<boolean> {
  const { admin } = await getOrCreateClient(connectionId)
  await admin.deleteTopics({ topics: [topic], timeout: 10000 })
  appendOperationLog({
    id: uuid(),
    at: Date.now(),
    connectionId,
    action: 'deleteTopic',
    detail: topic,
    success: true,
  })
  return true
}

export async function alterTopicConfig(
  connectionId: string,
  topic: string,
  entries: Array<{ name: string; value: string }>,
): Promise<boolean> {
  const { admin } = await getOrCreateClient(connectionId)
  await admin.alterConfigs({
    resources: [
      {
        type: ConfigResourceTypes.TOPIC,
        name: topic,
        configEntries: entries.map((e) => ({ name: e.name, value: e.value })),
      },
    ],
    validateOnly: false,
  })
  appendOperationLog({
    id: uuid(),
    at: Date.now(),
    connectionId,
    action: 'alterTopicConfig',
    detail: `${topic}: ${entries.map((e) => `${e.name}=${e.value}`).join(', ')}`,
    success: true,
  })
  return true
}

export async function createPartitions(
  connectionId: string,
  topic: string,
  count: number,
): Promise<boolean> {
  const { admin } = await getOrCreateClient(connectionId)
  await admin.createPartitions({
    topicPartitions: [{ topic, count }],
  })
  appendOperationLog({
    id: uuid(),
    at: Date.now(),
    connectionId,
    action: 'createPartitions',
    detail: `${topic} -> ${count}`,
    success: true,
  })
  return true
}

export async function getTopicOffsets(connectionId: string, topic: string): Promise<TopicOffsets> {
  const { admin } = await getOrCreateClient(connectionId)
  const offsets = await admin.fetchTopicOffsets(topic)
  return {
    topic,
    partitions: offsets.map((o) => ({
      partition: o.partition,
      low: o.low,
      high: o.high,
    })),
  }
}
