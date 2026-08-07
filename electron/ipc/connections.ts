import { ipcMain } from 'electron'
import { v4 as uuid } from 'uuid'
import type { ConnectionProfile } from '../../shared/types'
import {
  listConnections,
  saveConnection,
  deleteConnection,
  getConnection,
} from '../store/configStore'
import { getOrCreateClient, disconnectClient } from '../services/kafkaClient'
import { testConnectionOverview, getOverview } from '../services/adminService'
import { clearSchemaRegistry } from '../services/schemaService'
import { ok, fail } from '../services/utils'

export function registerConnectionHandlers() {
  ipcMain.handle('connections:list', async () => {
    try {
      return ok(listConnections())
    } catch (e) {
      return fail(e)
    }
  })

  ipcMain.handle('connections:save', async (_e, profile: ConnectionProfile) => {
    try {
      if (!profile.id) profile.id = uuid()
      const now = Date.now()
      if (!profile.createdAt) profile.createdAt = now
      profile.updatedAt = now
      const saved = saveConnection(profile)
      await disconnectClient(profile.id).catch(() => undefined)
      clearSchemaRegistry(profile.id)
      return ok(saved)
    } catch (e) {
      return fail(e)
    }
  })

  ipcMain.handle('connections:delete', async (_e, id: string) => {
    try {
      await disconnectClient(id)
      clearSchemaRegistry(id)
      return ok(deleteConnection(id))
    } catch (e) {
      return fail(e)
    }
  })

  ipcMain.handle('connections:test', async (_e, profile: ConnectionProfile) => {
    try {
      const overview = await testConnectionOverview(profile)
      return ok(overview)
    } catch (e) {
      return fail(e)
    }
  })

  ipcMain.handle('connections:connect', async (_e, id: string) => {
    try {
      if (!getConnection(id)) throw new Error('连接配置不存在')
      await getOrCreateClient(id)
      const overview = await getOverview(id)
      return ok(overview)
    } catch (e) {
      return fail(e)
    }
  })

  ipcMain.handle('connections:disconnect', async (_e, id: string) => {
    try {
      await disconnectClient(id)
      clearSchemaRegistry(id)
      return ok(true)
    } catch (e) {
      return fail(e)
    }
  })
}
