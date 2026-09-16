/**
 * Wails Go 绑定包装：开发期无绑定则返回友好错误。
 */
function api() {
  return window.go?.main?.App
}

async function call(name, ...args) {
  const app = api()
  if (!app || typeof app[name] !== 'function') {
    return { ok: false, error: `Go 方法未绑定: ${name}（请用 wails dev 启动）` }
  }
  try {
    const res = await app[name](...args)
    return res
  } catch (e) {
    return { ok: false, error: e?.message || String(e) }
  }
}

export const kafkaApi = {
  getVersion: () => api()?.GetVersion?.() ?? Promise.resolve('dev'),
  getConfigPath: () => api()?.GetConfigPath?.() ?? Promise.resolve(''),
  listConnections: () => call('ListConnections'),
  saveConnection: (p) => call('SaveConnection', p),
  deleteConnection: (id) => call('DeleteConnection', id),
  testConnection: (p) => call('TestConnection', p),
  connect: (id) => call('Connect', id),
  disconnect: (id) => call('Disconnect', id),
  getActiveConnectionID: () => api()?.GetActiveConnectionID?.() ?? Promise.resolve(''),
  getOverview: () => call('GetOverview'),
  listTopics: () => call('ListTopics'),
  createTopic: (p) => call('CreateTopic', p),
  deleteTopic: (topic) => call('DeleteTopic', topic),
  updatePartitions: (topic, total) => call('UpdatePartitions', topic, total),
  fetchMessages: (p) => call('FetchMessages', p),
  produceMessage: (p) => call('ProduceMessage', p),
  bulkProduce: (p) => call('BulkProduce', p),
  listConsumerGroups: () => call('ListConsumerGroups'),
  describeConsumerGroup: (id) => call('DescribeConsumerGroup', id),
  deleteConsumerGroup: (id) => call('DeleteConsumerGroup', id),
  listOperationLogs: () => call('ListOperationLogs'),
  clearOperationLogs: () => call('ClearOperationLogs'),
  testSSH: (id) => call('TestSSH', id),
  readBrokerConfig: (id) => call('ReadBrokerConfig', id),
  saveBrokerConfig: (id, content) => call('SaveBrokerConfig', id, content),
  kafkaServiceStatus: (id) => call('KafkaServiceStatus', id),
  restartKafkaService: (id) => call('RestartKafkaService', id),
}
