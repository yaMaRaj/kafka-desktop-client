export function ok<T>(data: T) {
  return { ok: true as const, data }
}

export function fail(error: unknown) {
  const message =
    error instanceof Error
      ? error.message
      : typeof error === 'string'
        ? error
        : '未知错误'
  return { ok: false as const, error: friendlyKafkaError(message) }
}

export function friendlyKafkaError(message: string): string {
  const map: Array<[RegExp | string, string]> = [
    ['ECONNREFUSED', '无法连接 Broker，请检查地址与端口'],
    ['ENOTFOUND', '无法解析主机名，请检查 Bootstrap Servers'],
    ['SASL', 'SASL 认证失败，请检查用户名/密码/机制'],
    ['SSL', 'SSL 握手失败，请检查证书配置'],
    ['TOPIC_ALREADY_EXISTS', 'Topic 已存在'],
    ['UNKNOWN_TOPIC_OR_PARTITION', 'Topic 或分区不存在'],
    ['GROUP_AUTHORIZATION_FAILED', '没有该 Consumer Group 的操作权限'],
    ['TOPIC_AUTHORIZATION_FAILED', '没有该 Topic 的操作权限'],
    ['INVALID_REPLICATION_FACTOR', '副本数无效（不能大于 Broker 数量）'],
    ['NON_EMPTY_GROUP', 'Consumer Group 仍有活跃成员，请先停止消费者再重置偏移'],
    ['GROUP_ID_NOT_FOUND', 'Consumer Group 不存在'],
    ['Timeout', '请求超时，请检查网络或集群负载'],
  ]

  for (const [key, text] of map) {
    if (typeof key === 'string') {
      if (message.includes(key)) return `${text}（${message}）`
    } else if (key.test(message)) {
      return `${text}（${message}）`
    }
  }
  return message
}

export function bufferToDisplay(value?: Buffer | null): string | null {
  if (value == null) return null
  try {
    const text = value.toString('utf8')
    // Prefer JSON pretty if possible
    const trimmed = text.trim()
    if (
      (trimmed.startsWith('{') && trimmed.endsWith('}')) ||
      (trimmed.startsWith('[') && trimmed.endsWith(']'))
    ) {
      try {
        return JSON.stringify(JSON.parse(trimmed), null, 2)
      } catch {
        return text
      }
    }
    return text
  } catch {
    return value.toString('base64')
  }
}

export function tryParseJson(text: string): unknown {
  try {
    return JSON.parse(text)
  } catch {
    return text
  }
}
