/**
 * Kafka 客户端生命周期管理。
 * - 按 connectionId 缓存 Admin + Producer，避免重复建连
 * - Consumer 不在此池化（拉取/搜索/Tail 各自创建临时 Consumer）
 * - 读写配置前通过 unlockProfile 解密密钥
 */
import fs from 'fs'
import { Kafka, logLevel, type Admin, type Producer, type KafkaConfig, type SASLOptions } from 'kafkajs'
import type { ConnectionProfile } from '../../shared/types'
import { getConnection, unlockProfile } from '../store/configStore'

interface ClientBundle {
  kafka: Kafka
  admin: Admin
  producer: Producer
  profile: ConnectionProfile
}

/** connectionId → 已连接的 Admin/Producer */
const clients = new Map<string, ClientBundle>()

function readFileIfExists(filePath?: string): Buffer | undefined {
  if (!filePath) return undefined
  if (!fs.existsSync(filePath)) {
    throw new Error(`证书文件不存在: ${filePath}`)
  }
  return fs.readFileSync(filePath)
}

/** 将连接配置转为 KafkaJS KafkaConfig（含 SSL 文件与 SASL） */
export function buildKafkaConfig(profile: ConnectionProfile): KafkaConfig {
  const unlocked = unlockProfile(profile)
  const brokers = unlocked.bootstrapServers
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)

  if (brokers.length === 0) {
    throw new Error('请填写 Bootstrap Servers')
  }

  const config: KafkaConfig = {
    clientId: unlocked.clientId || 'kafka-desktop',
    brokers,
    connectionTimeout: 10000,
    requestTimeout: 30000,
    logLevel: logLevel.ERROR,
  }

  const protocol = unlocked.securityProtocol

  if (protocol === 'SSL' || protocol === 'SASL_SSL') {
    config.ssl = {
      rejectUnauthorized: unlocked.ssl?.rejectUnauthorized ?? true,
      ca: readFileIfExists(unlocked.ssl?.caPath),
      cert: readFileIfExists(unlocked.ssl?.certPath),
      key: readFileIfExists(unlocked.ssl?.keyPath),
      passphrase: unlocked.ssl?.passphrase || undefined,
    }
  }

  if (protocol === 'SASL_PLAINTEXT' || protocol === 'SASL_SSL') {
    if (!unlocked.sasl?.username) {
      throw new Error('SASL 需要填写用户名')
    }
    const mechanism = unlocked.sasl.mechanism || 'plain'
    config.sasl = {
      mechanism,
      username: unlocked.sasl.username,
      password: unlocked.sasl.password || '',
    } as SASLOptions
  }

  return config
}

/** 获取或创建持久客户端；连接配置变更后应先 disconnect 再取 */
export async function getOrCreateClient(connectionId: string): Promise<ClientBundle> {
  const existing = clients.get(connectionId)
  if (existing) return existing

  const stored = getConnection(connectionId)
  if (!stored) throw new Error('连接配置不存在')

  const profile = unlockProfile(stored)
  const kafka = new Kafka(buildKafkaConfig(profile))
  const admin = kafka.admin()
  const producer = kafka.producer()

  await admin.connect()
  await producer.connect()

  const bundle: ClientBundle = { kafka, admin, producer, profile }
  clients.set(connectionId, bundle)
  return bundle
}

/** 一次性客户端（如测试连接），调用方负责 disconnect */
export async function createTransientClient(profile: ConnectionProfile): Promise<ClientBundle> {
  const unlocked = unlockProfile(profile)
  const kafka = new Kafka(buildKafkaConfig(unlocked))
  const admin = kafka.admin()
  const producer = kafka.producer()
  await admin.connect()
  await producer.connect()
  return { kafka, admin, producer, profile: unlocked }
}

export async function disconnectClient(connectionId: string): Promise<void> {
  const bundle = clients.get(connectionId)
  if (!bundle) return
  try {
    await bundle.admin.disconnect()
  } catch {
    /* ignore */
  }
  try {
    await bundle.producer.disconnect()
  } catch {
    /* ignore */
  }
  clients.delete(connectionId)
}

export async function closeAllClients(): Promise<void> {
  const ids = [...clients.keys()]
  await Promise.all(ids.map((id) => disconnectClient(id)))
}

export function getActiveConnectionIds(): string[] {
  return [...clients.keys()]
}

export function getProfileForConnection(connectionId: string): ConnectionProfile | undefined {
  return clients.get(connectionId)?.profile ?? getConnection(connectionId)
}
