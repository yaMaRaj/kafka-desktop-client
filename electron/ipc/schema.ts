import { ipcMain } from 'electron'
import { fetchSubjects, getSubjectInfo } from '../services/schemaService'
import { ok, fail } from '../services/utils'

export function registerSchemaHandlers() {
  ipcMain.handle('schema:listSubjects', async (_e, connectionId: string) => {
    try {
      return ok(await fetchSubjects(connectionId))
    } catch (e) {
      return fail(e)
    }
  })

  ipcMain.handle('schema:getSubject', async (_e, connectionId: string, subject: string) => {
    try {
      return ok(await getSubjectInfo(connectionId, subject))
    } catch (e) {
      return fail(e)
    }
  })
}
