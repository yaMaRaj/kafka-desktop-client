import { ipcMain } from 'electron'
import type { FetchMessagesParams, ProduceMessageParams } from '../../shared/types'
import { fetchMessages, produceMessages } from '../services/messageService'
import { ok, fail } from '../services/utils'

export function registerMessageHandlers() {
  ipcMain.handle(
    'messages:fetch',
    async (_e, connectionId: string, params: FetchMessagesParams) => {
      try {
        return ok(await fetchMessages(connectionId, params))
      } catch (e) {
        return fail(e)
      }
    },
  )

  ipcMain.handle(
    'messages:produce',
    async (_e, connectionId: string, params: ProduceMessageParams) => {
      try {
        return ok(await produceMessages(connectionId, params))
      } catch (e) {
        return fail(e)
      }
    },
  )
}
