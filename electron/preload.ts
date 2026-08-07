import { contextBridge, ipcRenderer, IpcRendererEvent } from 'electron'
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

const api = {
  getVersion: (): Promise<string> => ipcRenderer.invoke('app:getVersion'),

  // connections
  listConnections: (): Promise<IpcResult<ConnectionProfile[]>> =>
    ipcRenderer.invoke('connections:list'),
  saveConnection: (profile: ConnectionProfile): Promise<IpcResult<ConnectionProfile>> =>
    ipcRenderer.invoke('connections:save', profile),
  deleteConnection: (id: string): Promise<IpcResult<boolean>> =>
    ipcRenderer.invoke('connections:delete', id),
  testConnection: (profile: ConnectionProfile): Promise<IpcResult<ClusterOverview>> =>
    ipcRenderer.invoke('connections:test', profile),
  connect: (id: string): Promise<IpcResult<ClusterOverview>> =>
    ipcRenderer.invoke('connections:connect', id),
  disconnect: (id: string): Promise<IpcResult<boolean>> =>
    ipcRenderer.invoke('connections:disconnect', id),

  // admin / topics
  getClusterOverview: (connectionId: string): Promise<IpcResult<ClusterOverview>> =>
    ipcRenderer.invoke('admin:overview', connectionId),
  listTopics: (connectionId: string): Promise<IpcResult<TopicInfo[]>> =>
    ipcRenderer.invoke('admin:listTopics', connectionId),
  getTopic: (connectionId: string, topic: string): Promise<IpcResult<TopicInfo>> =>
    ipcRenderer.invoke('admin:getTopic', connectionId, topic),
  createTopic: (connectionId: string, params: TopicCreateParams): Promise<IpcResult<boolean>> =>
    ipcRenderer.invoke('admin:createTopic', connectionId, params),
  deleteTopic: (connectionId: string, topic: string): Promise<IpcResult<boolean>> =>
    ipcRenderer.invoke('admin:deleteTopic', connectionId, topic),
  alterTopicConfig: (
    connectionId: string,
    topic: string,
    entries: Array<{ name: string; value: string }>,
  ): Promise<IpcResult<boolean>> =>
    ipcRenderer.invoke('admin:alterTopicConfig', connectionId, topic, entries),
  createPartitions: (
    connectionId: string,
    topic: string,
    count: number,
  ): Promise<IpcResult<boolean>> =>
    ipcRenderer.invoke('admin:createPartitions', connectionId, topic, count),
  getTopicOffsets: (connectionId: string, topic: string): Promise<IpcResult<TopicOffsets>> =>
    ipcRenderer.invoke('admin:topicOffsets', connectionId, topic),

  // messages
  fetchMessages: (
    connectionId: string,
    params: FetchMessagesParams,
  ): Promise<IpcResult<KafkaMessageView[]>> =>
    ipcRenderer.invoke('messages:fetch', connectionId, params),
  produceMessages: (
    connectionId: string,
    params: ProduceMessageParams,
  ): Promise<IpcResult<{ topic: string; partition: number; offset: string }[]>> =>
    ipcRenderer.invoke('messages:produce', connectionId, params),

  // consumer groups
  listConsumerGroups: (connectionId: string): Promise<IpcResult<string[]>> =>
    ipcRenderer.invoke('groups:list', connectionId),
  describeConsumerGroup: (
    connectionId: string,
    groupId: string,
  ): Promise<IpcResult<ConsumerGroupInfo>> =>
    ipcRenderer.invoke('groups:describe', connectionId, groupId),
  resetOffsets: (
    connectionId: string,
    params: ResetOffsetsParams,
  ): Promise<IpcResult<boolean>> =>
    ipcRenderer.invoke('groups:resetOffsets', connectionId, params),
  deleteConsumerGroup: (connectionId: string, groupId: string): Promise<IpcResult<boolean>> =>
    ipcRenderer.invoke('groups:delete', connectionId, groupId),

  // schema registry
  listSubjects: (connectionId: string): Promise<IpcResult<string[]>> =>
    ipcRenderer.invoke('schema:listSubjects', connectionId),
  getSubject: (connectionId: string, subject: string): Promise<IpcResult<SchemaSubjectInfo>> =>
    ipcRenderer.invoke('schema:getSubject', connectionId, subject),

  // search & tail
  searchMessages: (
    connectionId: string,
    params: SearchMessagesParams,
  ): Promise<IpcResult<{ messages: KafkaMessageView[]; progress: SearchProgress }>> =>
    ipcRenderer.invoke('search:run', connectionId, params),
  cancelSearch: (searchId: string): Promise<IpcResult<boolean>> =>
    ipcRenderer.invoke('search:cancel', searchId),
  startTail: (connectionId: string, params: TailParams): Promise<IpcResult<string>> =>
    ipcRenderer.invoke('tail:start', connectionId, params),
  stopTail: (tailId: string): Promise<IpcResult<boolean>> =>
    ipcRenderer.invoke('tail:stop', tailId),
  onTailMessage: (callback: (payload: { tailId: string; message: KafkaMessageView }) => void) => {
    const listener = (_: IpcRendererEvent, payload: { tailId: string; message: KafkaMessageView }) =>
      callback(payload)
    ipcRenderer.on('tail:message', listener)
    return () => ipcRenderer.removeListener('tail:message', listener)
  },
  onTailError: (callback: (payload: { tailId: string; error: string }) => void) => {
    const listener = (_: IpcRendererEvent, payload: { tailId: string; error: string }) =>
      callback(payload)
    ipcRenderer.on('tail:error', listener)
    return () => ipcRenderer.removeListener('tail:error', listener)
  },
  onSearchProgress: (callback: (payload: { searchId: string; progress: SearchProgress }) => void) => {
    const listener = (
      _: IpcRendererEvent,
      payload: { searchId: string; progress: SearchProgress },
    ) => callback(payload)
    ipcRenderer.on('search:progress', listener)
    return () => ipcRenderer.removeListener('search:progress', listener)
  },

  // logs
  listOperationLogs: (): Promise<IpcResult<OperationLogEntry[]>> =>
    ipcRenderer.invoke('logs:list'),
  clearOperationLogs: (): Promise<IpcResult<boolean>> => ipcRenderer.invoke('logs:clear'),

  // export
  saveTextFile: (payload: {
    defaultName: string
    content: string
    filters?: Array<{ name: string; extensions: string[] }>
  }): Promise<IpcResult<{ saved: false } | { saved: true; filePath: string }>> =>
    ipcRenderer.invoke('export:saveTextFile', payload),
}

contextBridge.exposeInMainWorld('kafkaApi', api)

export type KafkaApi = typeof api
