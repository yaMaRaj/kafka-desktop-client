import { ipcMain } from 'electron'
import type { ResetOffsetsParams } from '../../shared/types'
import {
  listConsumerGroups,
  describeConsumerGroup,
  resetOffsets,
  deleteConsumerGroup,
} from '../services/consumerGroupService'
import { ok, fail } from '../services/utils'

export function registerConsumerGroupHandlers() {
  ipcMain.handle('groups:list', async (_e, connectionId: string) => {
    try {
      return ok(await listConsumerGroups(connectionId))
    } catch (e) {
      return fail(e)
    }
  })

  ipcMain.handle('groups:describe', async (_e, connectionId: string, groupId: string) => {
    try {
      return ok(await describeConsumerGroup(connectionId, groupId))
    } catch (e) {
      return fail(e)
    }
  })

  ipcMain.handle(
    'groups:resetOffsets',
    async (_e, connectionId: string, params: ResetOffsetsParams) => {
      try {
        return ok(await resetOffsets(connectionId, params))
      } catch (e) {
        return fail(e)
      }
    },
  )

  ipcMain.handle('groups:delete', async (_e, connectionId: string, groupId: string) => {
    try {
      return ok(await deleteConsumerGroup(connectionId, groupId))
    } catch (e) {
      return fail(e)
    }
  })
}
