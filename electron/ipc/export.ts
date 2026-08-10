/** IPC：弹出系统保存对话框并写入文本文件（消息导出 JSONL 等） */
import { dialog, ipcMain, BrowserWindow } from 'electron'
import fs from 'fs/promises'
import { ok, fail } from '../services/utils'

export function registerExportHandlers() {
  ipcMain.handle(
    'export:saveTextFile',
    async (
      event,
      payload: {
        defaultName: string
        content: string
        filters?: Array<{ name: string; extensions: string[] }>
      },
    ) => {
      try {
        const win = BrowserWindow.fromWebContents(event.sender)
        const options = {
          title: '导出消息',
          defaultPath: payload.defaultName,
          filters: payload.filters ?? [
            { name: 'JSON Lines', extensions: ['jsonl'] },
            { name: 'JSON', extensions: ['json'] },
            { name: '文本文件', extensions: ['txt'] },
            { name: '所有文件', extensions: ['*'] },
          ],
        }
        const result = win
          ? await dialog.showSaveDialog(win, options)
          : await dialog.showSaveDialog(options)

        if (result.canceled || !result.filePath) {
          return ok({ saved: false as const })
        }
        await fs.writeFile(result.filePath, payload.content, 'utf8')
        return ok({ saved: true as const, filePath: result.filePath })
      } catch (e) {
        return fail(e)
      }
    },
  )
}
