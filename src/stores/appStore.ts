import { create } from 'zustand'
import type { ConnectionProfile, ClusterOverview } from '@shared/types'

interface AppState {
  connections: ConnectionProfile[]
  activeConnectionId: string | null
  overview: ClusterOverview | null
  connected: boolean
  loading: boolean
  setConnections: (list: ConnectionProfile[]) => void
  setActiveConnectionId: (id: string | null) => void
  setOverview: (overview: ClusterOverview | null) => void
  setConnected: (v: boolean) => void
  setLoading: (v: boolean) => void
  refreshConnections: () => Promise<void>
  connect: (id: string) => Promise<string | null>
  disconnect: () => Promise<void>
}

export const useAppStore = create<AppState>((set, get) => ({
  connections: [],
  activeConnectionId: null,
  overview: null,
  connected: false,
  loading: false,

  setConnections: (list) => set({ connections: list }),
  setActiveConnectionId: (id) => set({ activeConnectionId: id }),
  setOverview: (overview) => set({ overview }),
  setConnected: (v) => set({ connected: v }),
  setLoading: (v) => set({ loading: v }),

  refreshConnections: async () => {
    const res = await window.kafkaApi.listConnections()
    if (res.ok) set({ connections: res.data })
  },

  connect: async (id: string) => {
    set({ loading: true })
    try {
      const res = await window.kafkaApi.connect(id)
      if (!res.ok) {
        set({ connected: false, overview: null })
        return res.error
      }
      set({
        activeConnectionId: id,
        overview: res.data,
        connected: true,
      })
      return null
    } finally {
      set({ loading: false })
    }
  },

  disconnect: async () => {
    const id = get().activeConnectionId
    if (id) await window.kafkaApi.disconnect(id)
    set({ connected: false, overview: null, activeConnectionId: null })
  },
}))
