/** IPC：集群概览与 Topic 管理（转发 adminService） */
import { ipcMain } from 'electron'
import type { TopicCreateParams } from '../../shared/types'
import {
  getOverview,
  listTopics,
  getTopic,
  createTopic,
  deleteTopic,
  alterTopicConfig,
  createPartitions,
  getTopicOffsets,
} from '../services/adminService'
import { ok, fail } from '../services/utils'
import { appendOperationLog } from '../store/configStore'
import { v4 as uuid } from 'uuid'

export function registerAdminHandlers() {
  ipcMain.handle('admin:overview', async (_e, connectionId: string) => {
    try {
      return ok(await getOverview(connectionId))
    } catch (e) {
      return fail(e)
    }
  })

  ipcMain.handle('admin:listTopics', async (_e, connectionId: string) => {
    try {
      return ok(await listTopics(connectionId))
    } catch (e) {
      return fail(e)
    }
  })

  ipcMain.handle('admin:getTopic', async (_e, connectionId: string, topic: string) => {
    try {
      return ok(await getTopic(connectionId, topic))
    } catch (e) {
      return fail(e)
    }
  })

  ipcMain.handle(
    'admin:createTopic',
    async (_e, connectionId: string, params: TopicCreateParams) => {
      try {
        return ok(await createTopic(connectionId, params))
      } catch (e) {
        appendOperationLog({
          id: uuid(),
          at: Date.now(),
          connectionId,
          action: 'createTopic',
          detail: params.name,
          success: false,
          error: e instanceof Error ? e.message : String(e),
        })
        return fail(e)
      }
    },
  )

  ipcMain.handle('admin:deleteTopic', async (_e, connectionId: string, topic: string) => {
    try {
      return ok(await deleteTopic(connectionId, topic))
    } catch (e) {
      appendOperationLog({
        id: uuid(),
        at: Date.now(),
        connectionId,
        action: 'deleteTopic',
        detail: topic,
        success: false,
        error: e instanceof Error ? e.message : String(e),
      })
      return fail(e)
    }
  })

  ipcMain.handle(
    'admin:alterTopicConfig',
    async (
      _e,
      connectionId: string,
      topic: string,
      entries: Array<{ name: string; value: string }>,
    ) => {
      try {
        return ok(await alterTopicConfig(connectionId, topic, entries))
      } catch (e) {
        return fail(e)
      }
    },
  )

  ipcMain.handle(
    'admin:createPartitions',
    async (_e, connectionId: string, topic: string, count: number) => {
      try {
        return ok(await createPartitions(connectionId, topic, count))
      } catch (e) {
        return fail(e)
      }
    },
  )

  ipcMain.handle('admin:topicOffsets', async (_e, connectionId: string, topic: string) => {
    try {
      return ok(await getTopicOffsets(connectionId, topic))
    } catch (e) {
      return fail(e)
    }
  })
}
