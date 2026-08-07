import type {
  ConnectionProfile,
  TopicCreateParams,
  FetchMessagesParams,
  ProduceMessageParams,
  ResetOffsetsParams,
  SearchMessagesParams,
  TailParams,
  IpcResult,
  ClusterOverview,
  TopicInfo,
  TopicOffsets,
  KafkaMessageView,
  ConsumerGroupInfo,
  SchemaSubjectInfo,
  OperationLogEntry,
  SearchProgress,
} from '../shared/types'

export interface KafkaApi {
  getVersion: () => Promise<string>
  listConnections: () => Promise<IpcResult<ConnectionProfile[]>>
  saveConnection: (profile: ConnectionProfile) => Promise<IpcResult<ConnectionProfile>>
  deleteConnection: (id: string) => Promise<IpcResult<boolean>>
  testConnection: (profile: ConnectionProfile) => Promise<IpcResult<ClusterOverview>>
  connect: (id: string) => Promise<IpcResult<ClusterOverview>>
  disconnect: (id: string) => Promise<IpcResult<boolean>>
  getClusterOverview: (connectionId: string) => Promise<IpcResult<ClusterOverview>>
  listTopics: (connectionId: string) => Promise<IpcResult<TopicInfo[]>>
  getTopic: (connectionId: string, topic: string) => Promise<IpcResult<TopicInfo>>
  createTopic: (connectionId: string, params: TopicCreateParams) => Promise<IpcResult<boolean>>
  deleteTopic: (connectionId: string, topic: string) => Promise<IpcResult<boolean>>
  alterTopicConfig: (
    connectionId: string,
    topic: string,
    entries: Array<{ name: string; value: string }>,
  ) => Promise<IpcResult<boolean>>
  createPartitions: (
    connectionId: string,
    topic: string,
    count: number,
  ) => Promise<IpcResult<boolean>>
  getTopicOffsets: (connectionId: string, topic: string) => Promise<IpcResult<TopicOffsets>>
  fetchMessages: (
    connectionId: string,
    params: FetchMessagesParams,
  ) => Promise<IpcResult<KafkaMessageView[]>>
  produceMessages: (
    connectionId: string,
    params: ProduceMessageParams,
  ) => Promise<IpcResult<{ topic: string; partition: number; offset: string }[]>>
  listConsumerGroups: (connectionId: string) => Promise<IpcResult<string[]>>
  describeConsumerGroup: (
    connectionId: string,
    groupId: string,
  ) => Promise<IpcResult<ConsumerGroupInfo>>
  resetOffsets: (connectionId: string, params: ResetOffsetsParams) => Promise<IpcResult<boolean>>
  deleteConsumerGroup: (connectionId: string, groupId: string) => Promise<IpcResult<boolean>>
  listSubjects: (connectionId: string) => Promise<IpcResult<string[]>>
  getSubject: (connectionId: string, subject: string) => Promise<IpcResult<SchemaSubjectInfo>>
  searchMessages: (
    connectionId: string,
    params: SearchMessagesParams,
  ) => Promise<IpcResult<{ messages: KafkaMessageView[]; progress: SearchProgress }>>
  cancelSearch: (searchId: string) => Promise<IpcResult<boolean>>
  startTail: (connectionId: string, params: TailParams) => Promise<IpcResult<string>>
  stopTail: (tailId: string) => Promise<IpcResult<boolean>>
  onTailMessage: (
    callback: (payload: { tailId: string; message: KafkaMessageView }) => void,
  ) => () => void
  onTailError: (callback: (payload: { tailId: string; error: string }) => void) => () => void
  onSearchProgress: (
    callback: (payload: { searchId: string; progress: SearchProgress }) => void,
  ) => () => void
  listOperationLogs: () => Promise<IpcResult<OperationLogEntry[]>>
  clearOperationLogs: () => Promise<IpcResult<boolean>>
  saveTextFile: (payload: {
    defaultName: string
    content: string
    filters?: Array<{ name: string; extensions: string[] }>
  }) => Promise<IpcResult<{ saved: false } | { saved: true; filePath: string }>>
}

declare global {
  interface Window {
    kafkaApi: KafkaApi
  }
}

export {}
