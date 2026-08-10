/**
 * Electron 主进程入口。
 * - 创建主窗口（preload + contextIsolation）
 * - 注册全部 IPC handlers
 * - 退出时断开 Kafka 客户端连接
 *
 * 开发期由 Vite 注入 VITE_DEV_SERVER_URL；生产加载 dist/index.html。
 */
import { app, BrowserWindow, ipcMain, shell } from 'electron'
import path from 'path'
import { registerConnectionHandlers } from './ipc/connections'
import { registerAdminHandlers } from './ipc/admin'
import { registerMessageHandlers } from './ipc/messages'
import { registerConsumerGroupHandlers } from './ipc/consumerGroups'
import { registerSchemaHandlers } from './ipc/schema'
import { registerSearchTailHandlers } from './ipc/searchTail'
import { registerLogHandlers } from './ipc/logs'
import { registerExportHandlers } from './ipc/export'
import { closeAllClients } from './services/kafkaClient'

process.env.DIST = path.join(__dirname, '../dist')
process.env.VITE_PUBLIC = app.isPackaged
  ? process.env.DIST
  : path.join(process.env.DIST, '../public')

let mainWindow: BrowserWindow | null = null

const VITE_DEV_SERVER_URL = process.env.VITE_DEV_SERVER_URL

/** 创建应用主窗口；外链用系统浏览器打开 */
function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1400,
    height: 900,
    minWidth: 1100,
    minHeight: 700,
    title: 'Kafka Desktop',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: false,
    },
    show: false,
  })

  mainWindow.once('ready-to-show', () => {
    mainWindow?.show()
  })

  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    shell.openExternal(url)
    return { action: 'deny' }
  })

  if (VITE_DEV_SERVER_URL) {
    mainWindow.loadURL(VITE_DEV_SERVER_URL)
  } else {
    mainWindow.loadFile(path.join(process.env.DIST!, 'index.html'))
  }
}

/** 集中注册 IPC，便于新增域时在此挂载 */
function registerAllHandlers() {
  registerConnectionHandlers()
  registerAdminHandlers()
  registerMessageHandlers()
  registerConsumerGroupHandlers()
  registerSchemaHandlers()
  registerSearchTailHandlers(getMainWindow)
  registerLogHandlers()
  registerExportHandlers()

  ipcMain.handle('app:getVersion', () => app.getVersion())
}

/** 供需要向渲染进程推送事件的 handler（如 Tail / 搜索进度）获取窗口 */
function getMainWindow() {
  return mainWindow
}

app.whenReady().then(() => {
  registerAllHandlers()
  createWindow()

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow()
  })
})

app.on('window-all-closed', () => {
  void closeAllClients()
  if (process.platform !== 'darwin') app.quit()
})

app.on('before-quit', () => {
  void closeAllClients()
})
