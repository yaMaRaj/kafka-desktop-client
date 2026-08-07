import { SchemaRegistry } from '@kafkajs/confluent-schema-registry'
import type { ConnectionProfile, SchemaSubjectInfo } from '../../shared/types'
import { getConnection, unlockProfile } from '../store/configStore'

const registries = new Map<string, SchemaRegistry>()

function resolveProfile(connectionId: string): ConnectionProfile {
  const stored = getConnection(connectionId)
  if (!stored) throw new Error('连接配置不存在')
  return unlockProfile(stored)
}

export function getSchemaRegistry(connectionId: string): SchemaRegistry {
  const existing = registries.get(connectionId)
  if (existing) return existing

  const profile = resolveProfile(connectionId)
  if (!profile.schemaRegistry?.url) {
    throw new Error('未配置 Schema Registry 地址')
  }

  const auth =
    profile.schemaRegistry.username
      ? {
          username: profile.schemaRegistry.username,
          password: profile.schemaRegistry.password || '',
        }
      : undefined

  const registry = new SchemaRegistry({
    host: profile.schemaRegistry.url.replace(/\/$/, ''),
    auth,
  })
  registries.set(connectionId, registry)
  return registry
}

export function clearSchemaRegistry(connectionId: string) {
  registries.delete(connectionId)
}

async function registryFetch(
  connectionId: string,
  path: string,
  init?: RequestInit,
): Promise<Response> {
  const profile = resolveProfile(connectionId)
  const host = profile.schemaRegistry!.url.replace(/\/$/, '')
  const headers: Record<string, string> = {
    Accept: 'application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json',
    ...(init?.headers as Record<string, string> | undefined),
  }
  if (profile.schemaRegistry?.username) {
    const token = Buffer.from(
      `${profile.schemaRegistry.username}:${profile.schemaRegistry.password || ''}`,
    ).toString('base64')
    headers.Authorization = `Basic ${token}`
  }
  return fetch(`${host}${path}`, { ...init, headers })
}

export async function fetchSubjects(connectionId: string): Promise<string[]> {
  const res = await registryFetch(connectionId, '/subjects')
  if (!res.ok) throw new Error(`获取 Subject 列表失败: ${res.status} ${await res.text()}`)
  return (await res.json()) as string[]
}

export async function getSubjectInfo(
  connectionId: string,
  subject: string,
): Promise<SchemaSubjectInfo> {
  const versionsRes = await registryFetch(
    connectionId,
    `/subjects/${encodeURIComponent(subject)}/versions`,
  )
  if (!versionsRes.ok) {
    throw new Error(`获取版本失败: ${versionsRes.status}`)
  }
  const versions = (await versionsRes.json()) as number[]
  const latestVersion = versions[versions.length - 1]

  let latestSchema: string | undefined
  let schemaType: string | undefined
  if (latestVersion != null) {
    const schemaRes = await registryFetch(
      connectionId,
      `/subjects/${encodeURIComponent(subject)}/versions/${latestVersion}`,
    )
    if (schemaRes.ok) {
      const body = (await schemaRes.json()) as {
        schema: string
        schemaType?: string
      }
      latestSchema = body.schema
      schemaType = body.schemaType || 'AVRO'
    }
  }

  let compatibility: string | undefined
  const compatRes = await registryFetch(
    connectionId,
    `/config/${encodeURIComponent(subject)}`,
  )
  if (compatRes.ok) {
    const body = (await compatRes.json()) as { compatibilityLevel?: string }
    compatibility = body.compatibilityLevel
  }

  return {
    subject,
    versions,
    latestVersion,
    latestSchema,
    compatibility,
    schemaType,
  }
}

/** Decode Confluent wire format if present; otherwise return utf8 string */
export async function decodeMaybe(
  connectionId: string,
  buffer: Buffer | null,
  enabled: boolean,
): Promise<{ text: string | null; schemaId?: number }> {
  if (buffer == null) return { text: null }
  if (!enabled || buffer.length < 5 || buffer[0] !== 0) {
    return { text: buffer.toString('utf8') }
  }

  try {
    const profile = resolveProfile(connectionId)
    if (!profile.schemaRegistry?.url) {
      return { text: buffer.toString('utf8') }
    }
    const registry = getSchemaRegistry(connectionId)
    const schemaId = buffer.readUInt32BE(1)
    const decoded = await registry.decode(buffer)
    const text =
      typeof decoded === 'string' ? decoded : JSON.stringify(decoded, null, 2)
    return { text, schemaId }
  } catch {
    return { text: buffer.toString('utf8') }
  }
}

export async function encodeWithSubject(
  connectionId: string,
  subject: string,
  payload: unknown,
): Promise<Buffer> {
  const registry = getSchemaRegistry(connectionId)
  const id = await registry.getLatestSchemaId(subject)
  return registry.encode(id, payload)
}
