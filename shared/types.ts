/** Shared types between Electron main and renderer */

export type SecurityProtocol = 'PLAINTEXT' | 'SASL_PLAINTEXT' | 'SASL_SSL' | 'SSL'

export type SaslMechanism = 'plain' | 'scram-sha-256' | 'scram-sha-512'

export interface SaslConfig {
  mechanism: SaslMechanism
  username: string
  password: string
}

export interface SslConfig {
  caPath?: string
  certPath?: string
  keyPath?: string
  passphrase?: string
  rejectUnauthorized?: boolean
}

export interface SchemaRegistryConfig {
  url: string
  username?: string
  password?: string
}

export interface ConnectionProfile {
  id: string
  name: string
  bootstrapServers: string
  securityProtocol: SecurityProtocol
  clientId?: string
  sasl?: SaslConfig
  ssl?: SslConfig
  schemaRegistry?: SchemaRegistryConfig
  createdAt: number
  updatedAt: number
}

export interface BrokerInfo {
  nodeId: number
  host: string
  port: number
  rack?: string
}

export interface ClusterOverview {
  clusterId?: string
  controllerId?: number
  brokers: BrokerInfo[]
}

export interface PartitionInfo {
  partitionId: number
  leader: number
  replicas: number[]
  isr: number[]
  offlineReplicas?: number[]
}

export interface TopicInfo {
  name: string
  partitions: PartitionInfo[]
  isInternal: boolean
  config?: Record<string, string>
}

export interface TopicCreateParams {
  name: string
  numPartitions: number
  replicationFactor: number
  configEntries?: Array<{ name: string; value: string }>
}

export interface TopicOffsets {
  topic: string
  partitions: Array<{
    partition: number
    low: string
    high: string
  }>
}

export interface KafkaHeader {
  key: string
  value: string
}

export interface KafkaMessageView {
  topic: string
  partition: number
  offset: string
  timestamp: string
  key: string | null
  value: string | null
  headers: KafkaHeader[]
  keySchemaId?: number
  valueSchemaId?: number
}

export interface FetchMessagesParams {
  topic: string
  partition?: number
  fromOffset?: string
  fromTimestamp?: number
  limit?: number
  decodeSchema?: boolean
}

export interface ProduceMessageParams {
  topic: string
  messages: Array<{
    key?: string
    value: string
    partition?: number
    headers?: KafkaHeader[]
    encodeWithSchema?: boolean
    subject?: string
  }>
}

export interface ConsumerGroupMember {
  memberId: string
  clientId: string
  clientHost: string
  assignments: Array<{ topic: string; partitions: number[] }>
}

export interface ConsumerGroupOffset {
  topic: string
  partition: number
  offset: string
  high: string
  lag: string
  metadata?: string
}

export interface ConsumerGroupInfo {
  groupId: string
  state: string
  protocol?: string
  protocolType?: string
  members: ConsumerGroupMember[]
  offsets: ConsumerGroupOffset[]
  totalLag: string
}

export type OffsetResetStrategy = 'earliest' | 'latest' | 'offset' | 'timestamp'

export interface ResetOffsetsParams {
  groupId: string
  topic: string
  strategy: OffsetResetStrategy
  partitions?: number[]
  offset?: string
  timestamp?: number
}

export interface SearchMessagesParams {
  topic: string
  partitions?: number[]
  query: string
  useRegex?: boolean
  searchKey?: boolean
  searchValue?: boolean
  fromOffset?: string
  maxScan?: number
  decodeSchema?: boolean
}

export interface SearchProgress {
  scanned: number
  matched: number
  done: boolean
  cancelled?: boolean
  error?: string
}

export interface TailParams {
  topic: string
  partitions?: number[]
  fromBeginning?: boolean
  filter?: string
  useRegex?: boolean
  decodeSchema?: boolean
}

export interface SchemaSubjectInfo {
  subject: string
  versions: number[]
  latestVersion?: number
  latestSchema?: string
  compatibility?: string
  schemaType?: string
}

export interface OperationLogEntry {
  id: string
  at: number
  connectionId: string
  action: string
  detail: string
  success: boolean
  error?: string
}

export type IpcResult<T> =
  | { ok: true; data: T }
  | { ok: false; error: string }
