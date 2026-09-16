<template>
  <el-container class="app-root">
    <el-aside width="220px" class="app-aside">
      <div class="brand">
        <div class="brand-title">Kafka Client</div>
        <div class="brand-sub">Go · Vue3 · v{{ version || '…' }}</div>
      </div>
      <el-menu :default-active="activeMenu" router background-color="#1f2d3d" text-color="#bfcbd9" active-text-color="#409eff">
        <el-menu-item v-for="m in menu" :key="m.path" :index="m.path">
          <el-icon><component :is="m.icon" /></el-icon>
          <span>{{ m.label }}</span>
        </el-menu-item>
      </el-menu>
    </el-aside>
    <el-container>
      <el-header class="app-header" height="56px">
        <div class="header-left">
          <span class="dot" :class="connected ? 'on' : 'off'" />
          <span>{{ connected ? '已连接' : '未连接' }}</span>
          <el-select
            v-model="activeId"
            placeholder="选择连接配置"
            style="width: 320px; margin-left: 12px"
            @change="onConnect"
          >
            <el-option
              v-for="c in connections"
              :key="c.id"
              :label="`${c.name} (${c.bootstrapServers})`"
              :value="c.id"
            />
          </el-select>
          <el-button v-if="connected" style="margin-left: 8px" @click="onDisconnect">断开</el-button>
          <el-tag v-if="overview" type="info" style="margin-left: 12px">
            Brokers: {{ overview.brokers?.length || 0 }}
          </el-tag>
        </div>
      </el-header>
      <el-main class="app-main" v-loading="loading">
        <router-view />
      </el-main>
    </el-container>
  </el-container>
</template>

<script setup>
import { ref, computed, provide } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { kafkaApi } from './api/kafka'
import router from './router'

const menu = [
  { path: '/connections', label: '连接管理', icon: 'Setting' },
  { path: '/overview', label: '集群概览', icon: 'Monitor' },
  { path: '/topics', label: 'Topics', icon: 'Collection' },
  { path: '/messages', label: '消息浏览', icon: 'ChatDotRound' },
  { path: '/produce', label: '消息生产', icon: 'Upload' },
  { path: '/bulk', label: '批量造数', icon: 'Lightning' },
  { path: '/groups', label: '消费组', icon: 'User' },
	{ path: '/ops', label: '集群运维', icon: 'Tools' },
  { path: '/logs', label: '操作日志', icon: 'Document' },
]

const route = useRoute()
const vueRouter = useRouter()
const version = ref('')
const connections = ref([])
const activeId = ref('')
const connected = ref(false)
const overview = ref(null)
const loading = ref(false)
const activeMenu = computed(() => route.path)

// 未连接时拦截需集群的页面，避免一进页就一堆报错
router.beforeEach((to) => {
  if (to.meta?.needConn && !connected.value) {
    ElMessage.warning('请先在顶部选择连接配置')
    return { path: '/connections' }
  }
})

async function refreshConnections() {
  const res = await kafkaApi.listConnections()
  if (res.ok) connections.value = res.data || []
}

// 顶部下拉选择配置即建立连接（不必只在连接管理里点「连接」）
async function onConnect(id) {
  if (!id) return
  loading.value = true
  try {
    const res = await kafkaApi.connect(id)
    if (!res.ok) {
      ElMessage.error(res.error)
      connected.value = false
      overview.value = null
      // 清空选中，便于再次选择同一配置重试（el-select 同值不触发 change）
      activeId.value = ''
      return
    }
    activeId.value = id
    connected.value = true
    overview.value = res.data
    ElMessage.success('已连接集群')
  } finally {
    loading.value = false
  }
}

async function onDisconnect() {
  if (!activeId.value) return
  await kafkaApi.disconnect(activeId.value)
  connected.value = false
  overview.value = null
  activeId.value = ''
  ElMessage.info('已断开')
  if (route.meta?.needConn) vueRouter.push('/connections')
}

function clearConnectionState(id) {
  if (id && activeId.value && id !== activeId.value) return
  connected.value = false
  overview.value = null
  if (!id || activeId.value === id) activeId.value = ''
  if (route.meta?.needConn) vueRouter.push('/connections')
}

provide('appState', {
  connections,
  activeId,
  connected,
  overview,
  refreshConnections,
  onConnect,
  clearConnectionState,
})

kafkaApi.getVersion().then((v) => (version.value = v))
refreshConnections()
kafkaApi.getActiveConnectionID().then((id) => {
  if (id) {
    activeId.value = id
    connected.value = true
    kafkaApi.getOverview().then((res) => {
      if (res.ok) overview.value = res.data
    })
  }
})
</script>

<style scoped>
.app-root { height: 100vh; }
.app-aside { background: #1f2d3d; color: #fff; }
.brand { padding: 18px 16px 12px; border-bottom: 1px solid rgba(255,255,255,0.08); }
.brand-title { font-size: 18px; font-weight: 700; color: #fff; }
.brand-sub { font-size: 12px; color: rgba(255,255,255,0.55); margin-top: 4px; }
.app-header {
  display: flex; align-items: center; background: #fff;
  border-bottom: 1px solid #ebeef5; padding: 0 16px;
}
.header-left { display: flex; align-items: center; }
.dot { width: 8px; height: 8px; border-radius: 50%; margin-right: 8px; display: inline-block; }
.dot.on { background: #67c23a; }
.dot.off { background: #909399; }
.app-main { background: #f5f7fa; height: calc(100vh - 56px); overflow: auto; }
</style>
