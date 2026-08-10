/**
 * IPC：消息搜索与实时 Tail。
 * 搜索进度 / Tail 消息通过 webContents.send 推送，需传入 getMainWindow。
 */
import { BrowserWindow, ipcMain } from 'electron'
import { v4 as uuid } from 'uuid'
import type { SearchMessagesParams, TailParams } from '../../shared/types'
import {
  searchMessages,
  cancelSearch,
  startTail,
  stopTail,
} from '../services/searchTailService'
import { ok, fail } from '../services/utils'

export function registerSearchTailHandlers(getMainWindow: () => BrowserWindow | null) {
  ipcMain.handle(
    'search:run',
    async (_e, connectionId: string, params: SearchMessagesParams) => {
      try {
        const searchId = uuid()
        const win = getMainWindow()
        const result = await searchMessages(
          connectionId,
          params,
          searchId,
          (id, progress) => {
            win?.webContents.send('search:progress', { searchId: id, progress })
          },
        )
        return ok(result)
      } catch (e) {
        return fail(e)
      }
    },
  )

  ipcMain.handle('search:cancel', async (_e, searchId: string) => {
    try {
      return ok(cancelSearch(searchId))
    } catch (e) {
      return fail(e)
    }
  })

  ipcMain.handle('tail:start', async (_e, connectionId: string, params: TailParams) => {
    try {
      const win = getMainWindow()
      const tailId = await startTail(
        connectionId,
        params,
        (id, message) => {
          win?.webContents.send('tail:message', { tailId: id, message })
        },
        (id, error) => {
          win?.webContents.send('tail:error', { tailId: id, error })
        },
      )
      return ok(tailId)
    } catch (e) {
      return fail(e)
    }
  })

  ipcMain.handle('tail:stop', async (_e, tailId: string) => {
    try {
      return ok(await stopTail(tailId))
    } catch (e) {
      return fail(e)
    }
  })
}
