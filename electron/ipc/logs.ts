/** IPC：本地操作日志列表与清空 */
import { ipcMain } from 'electron'
import { listOperationLogs, clearOperationLogs } from '../store/configStore'
import { ok, fail } from '../services/utils'

export function registerLogHandlers() {
  ipcMain.handle('logs:list', async () => {
    try {
      return ok(listOperationLogs())
    } catch (e) {
      return fail(e)
    }
  })

  ipcMain.handle('logs:clear', async () => {
    try {
      return ok(clearOperationLogs())
    } catch (e) {
      return fail(e)
    }
  })
}
