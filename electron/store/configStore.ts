import Store from 'electron-store'
import { safeStorage } from 'electron'
import type { ConnectionProfile, OperationLogEntry } from '../../shared/types'

interface StoreSchema {
  connections: ConnectionProfile[]
  operationLogs: OperationLogEntry[]
}

const store = new Store<StoreSchema>({
  name: 'kafka-desktop',
  defaults: {
    connections: [],
    operationLogs: [],
  },
})

function encryptSecret(value: string): string {
  if (!value) return value
  if (safeStorage.isEncryptionAvailable()) {
    return `enc:${safeStorage.encryptString(value).toString('base64')}`
  }
  return `plain:${Buffer.from(value, 'utf8').toString('base64')}`
}

function decryptSecret(value: string | undefined): string {
  if (!value) return ''
  if (value.startsWith('enc:')) {
    const raw = Buffer.from(value.slice(4), 'base64')
    return safeStorage.decryptString(raw)
  }
  if (value.startsWith('plain:')) {
    return Buffer.from(value.slice(6), 'base64').toString('utf8')
  }
  return value
}

function alreadyProtected(value: string): boolean {
  return value.startsWith('enc:') || value.startsWith('plain:')
}

function protectSecret(value: string | undefined): string | undefined {
  if (!value) return value
  if (alreadyProtected(value)) return value
  return encryptSecret(value)
}

function maskProfileForStorage(profile: ConnectionProfile): ConnectionProfile {
  const cloned: ConnectionProfile = JSON.parse(JSON.stringify(profile))
  if (cloned.sasl?.password) {
    cloned.sasl.password = protectSecret(cloned.sasl.password)!
  }
  if (cloned.ssl?.passphrase) {
    cloned.ssl.passphrase = protectSecret(cloned.ssl.passphrase)!
  }
  if (cloned.schemaRegistry?.password) {
    cloned.schemaRegistry.password = protectSecret(cloned.schemaRegistry.password)!
  }
  return cloned
}

export function unlockProfile(profile: ConnectionProfile): ConnectionProfile {
  const cloned: ConnectionProfile = JSON.parse(JSON.stringify(profile))
  if (cloned.sasl?.password) {
    cloned.sasl.password = decryptSecret(cloned.sasl.password)
  }
  if (cloned.ssl?.passphrase) {
    cloned.ssl.passphrase = decryptSecret(cloned.ssl.passphrase)
  }
  if (cloned.schemaRegistry?.password) {
    cloned.schemaRegistry.password = decryptSecret(cloned.schemaRegistry.password)
  }
  return cloned
}

export function listConnections(): ConnectionProfile[] {
  return store.get('connections')
}

export function getConnection(id: string): ConnectionProfile | undefined {
  return store.get('connections').find((c) => c.id === id)
}

export function saveConnection(profile: ConnectionProfile): ConnectionProfile {
  const connections = store.get('connections')
  const idx = connections.findIndex((c) => c.id === profile.id)
  const stored = maskProfileForStorage(profile)
  if (idx >= 0) connections[idx] = stored
  else connections.push(stored)
  store.set('connections', connections)
  return profile
}

export function deleteConnection(id: string): boolean {
  const connections = store.get('connections').filter((c) => c.id !== id)
  store.set('connections', connections)
  return true
}

export function appendOperationLog(entry: OperationLogEntry) {
  const logs = store.get('operationLogs')
  logs.unshift(entry)
  store.set('operationLogs', logs.slice(0, 500))
}

export function listOperationLogs(): OperationLogEntry[] {
  return store.get('operationLogs')
}

export function clearOperationLogs(): boolean {
  store.set('operationLogs', [])
  return true
}
