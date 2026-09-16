<template>
  <div class="page topics-page">
    <div class="page-header">
      <div>
        <div class="page-title">Topics</div>
        <div class="page-sub">
          共 {{ filtered.length }} 个
          <span v-if="unhealthyCount" class="sub-alert"> · {{ unhealthyCount }} 个异常</span>
          <span> · 消息 {{ formatCount(totalRecords) }} · 磁盘 {{ formatBytes(totalBytes) }}</span>
        </div>
      </div>
      <div class="header-right">
        <el-checkbox v-model="onlyUnhealthy">仅看异常</el-checkbox>
        <el-checkbox v-model="hideInternal">隐藏内部</el-checkbox>
        <el-input v-model="q" placeholder="过滤名称" clearable style="width: 180px" />
        <el-button type="primary" @click="showCreate = true">创建</el-button>
        <el-button @click="load" :loading="loading">刷新</el-button>
      </div>
    </div>
    <el-table
      :data="filtered"
      stripe
      border
      height="calc(100vh - 200px)"
      class="topics-table"
    >
      <el-table-column type="expand" width="40">
        <template #default="{ row }">
          <div class="expand">
            <div class="expand-meta">
              <span>清理策略：{{ row.cleanupPolicy || '-' }}</span>
              <span>保留时间：{{ formatRetention(row.retentionMs) }}</span>
              <span>保留大小：{{ formatRetentionBytes(row.retentionBytes) }}</span>
              <span>min.ISR：{{ row.minIsr || '-' }}</span>
              <span>最大消息：{{ formatMaxMsg(row.maxMessageBytes) }}</span>
            </div>
            <el-table :data="row.partitions || []" size="small" border>
              <el-table-column prop="partitionId" label="分区" width="80" />
              <el-table-column prop="leader" label="Leader" width="80" />
              <el-table-column label="副本" min-width="120">
                <template #default="{ row: p }">{{ (p.replicas || []).join(', ') }}</template>
              </el-table-column>
              <el-table-column label="ISR" min-width="120">
                <template #default="{ row: p }">{{ (p.isr || []).join(', ') }}</template>
              </el-table-column>
              <el-table-column prop="startOffset" label="Start" width="100" />
              <el-table-column prop="endOffset" label="End" width="100" />
              <el-table-column label="消息数" width="110" align="right">
                <template #default="{ row: p }">{{ formatCount(p.records) }}</template>
              </el-table-column>
              <el-table-column label="磁盘" width="110" align="right">
                <template #default="{ row: p }">{{ formatBytes(p.sizeBytes) }}</template>
              </el-table-column>
            </el-table>
          </div>
        </template>
      </el-table-column>
      <el-table-column label="状态" width="140" sortable :sort-method="sortHealth">
        <template #default="{ row }">
          <span class="health" :class="healthClass(row)">
            <i class="health-dot" />
            {{ healthText(row) }}
          </span>
        </template>
      </el-table-column>
      <el-table-column label="Topic" min-width="220" show-overflow-tooltip>
        <template #default="{ row }">
          <el-button link type="primary" class="topic-name" @click="openDetail(row)">{{ row.name }}</el-button>
          <el-tag v-if="row.isInternal" size="small" type="info" class="internal-tag">内部</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="消息数" min-width="110" align="right" sortable :sort-method="(a, b) => (a.recordCount || 0) - (b.recordCount || 0)">
        <template #default="{ row }">
          <span class="num">{{ formatCount(row.recordCount) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="磁盘占用" min-width="110" align="right" sortable :sort-method="(a, b) => (a.sizeBytes || 0) - (b.sizeBytes || 0)">
        <template #default="{ row }">
          <span class="num">{{ formatBytes(row.sizeBytes) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="分区" width="72" align="center">
        <template #default="{ row }">{{ row.partitions?.length || 0 }}</template>
      </el-table-column>
      <el-table-column prop="replicationFactor" label="副本" width="72" align="center" />
      <el-table-column label="操作" width="148" class-name="ops-col">
        <template #default="{ row }">
          <div class="ops-cell">
            <button type="button" class="ops-link" @click.stop="jump('/messages', row)">浏览</button>
            <el-dropdown trigger="click" @command="(cmd) => onMore(cmd, row)">
              <button type="button" class="ops-link" @click.stop>更多</button>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item command="produce">生产消息</el-dropdown-item>
                  <el-dropdown-item command="bulk">批量造数</el-dropdown-item>
                  <el-dropdown-item command="expand" divided>扩分区</el-dropdown-item>
                  <el-dropdown-item command="delete" divided>
                    <span style="color: #f56c6c">删除</span>
                  </el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
          </div>
        </template>
      </el-table-column>
    </el-table>

    <el-drawer v-model="detailVisible" :title="detail?.name || 'Topic 详情'" size="52%">
      <template v-if="detail">
        <el-descriptions :column="2" border>
          <el-descriptions-item label="状态">
            <span class="health" :class="healthClass(detail)">
              <i class="health-dot" />
              {{ healthText(detail) }}
            </span>
          </el-descriptions-item>
          <el-descriptions-item label="分区 / 副本">{{ detail.partitions?.length || 0 }} / {{ detail.replicationFactor }}</el-descriptions-item>
          <el-descriptions-item label="消息数">{{ formatCount(detail.recordCount) }}</el-descriptions-item>
          <el-descriptions-item label="磁盘占用">{{ formatBytes(detail.sizeBytes) }}</el-descriptions-item>
          <el-descriptions-item label="清理策略">{{ detail.cleanupPolicy || '-' }}</el-descriptions-item>
          <el-descriptions-item label="保留时间">{{ formatRetention(detail.retentionMs) }}</el-descriptions-item>
          <el-descriptions-item label="保留大小">{{ formatRetentionBytes(detail.retentionBytes) }}</el-descriptions-item>
          <el-descriptions-item label="min.ISR">{{ detail.minIsr || '-' }}</el-descriptions-item>
          <el-descriptions-item label="最大消息">{{ formatMaxMsg(detail.maxMessageBytes) }}</el-descriptions-item>
          <el-descriptions-item label="内部">{{ detail.isInternal ? '是' : '否' }}</el-descriptions-item>
        </el-descriptions>
        <div class="drawer-actions">
          <el-button type="primary" @click="jump('/messages', detail)">浏览消息</el-button>
          <el-button @click="jump('/produce', detail)">生产消息</el-button>
          <el-button @click="jump('/bulk', detail)">批量造数</el-button>
        </div>
        <el-table :data="detail.partitions || []" size="small" border style="margin-top: 12px">
          <el-table-column prop="partitionId" label="分区" width="70" />
          <el-table-column prop="leader" label="Leader" width="80" />
          <el-table-column label="副本" min-width="110">
            <template #default="{ row }">{{ (row.replicas || []).join(', ') }}</template>
          </el-table-column>
          <el-table-column label="ISR" min-width="110">
            <template #default="{ row }">{{ (row.isr || []).join(', ') }}</template>
          </el-table-column>
          <el-table-column prop="startOffset" label="Start" width="90" />
          <el-table-column prop="endOffset" label="End" width="90" />
          <el-table-column label="消息数" width="100" align="right">
            <template #default="{ row }">{{ formatCount(row.records) }}</template>
          </el-table-column>
          <el-table-column label="磁盘" width="100" align="right">
            <template #default="{ row }">{{ formatBytes(row.sizeBytes) }}</template>
          </el-table-column>
        </el-table>
      </template>
    </el-drawer>

    <el-dialog v-model="showCreate" title="创建 Topic" width="480px">
      <el-form label-width="100px">
        <el-form-item label="名称"><el-input v-model="createForm.name" /></el-form-item>
        <el-form-item label="分区数"><el-input-number v-model="createForm.numPartitions" :min="1" /></el-form-item>
        <el-form-item label="副本数"><el-input-number v-model="createForm.replicationFactor" :min="1" /></el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showCreate = false">取消</el-button>
        <el-button type="primary" @click="create">创建</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { computed, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { kafkaApi } from '../api/kafka'

const router = useRouter()
const list = ref([])
const q = ref('')
const hideInternal = ref(true)
const onlyUnhealthy = ref(false)
const loading = ref(false)
const showCreate = ref(false)
const detailVisible = ref(false)
const detail = ref(null)
const createForm = ref({ name: '', numPartitions: 1, replicationFactor: 1 })

function healthRank(t) {
  if (t.offlinePartitions) return 0
  if (t.underReplicated) return 1
  return 2
}

function healthClass(row) {
  if (row.offlinePartitions) return 'bad'
  if (row.underReplicated) return 'warn'
  return 'ok'
}

function healthText(row) {
  if (row.offlinePartitions) return `离线 ${row.offlinePartitions}`
  if (row.underReplicated) return `副本不足 ${row.underReplicated}`
  return '正常'
}

function sortHealth(a, b) {
  return healthRank(a) - healthRank(b)
}

const filtered = computed(() => {
  const rows = list.value.filter((t) => {
    if (hideInternal.value && t.isInternal) return false
    if (onlyUnhealthy.value && healthRank(t) === 2) return false
    if (q.value && !t.name.toLowerCase().includes(q.value.toLowerCase())) return false
    return true
  })
  return rows.slice().sort((a, b) => {
    const d = healthRank(a) - healthRank(b)
    if (d !== 0) return d
    return String(a.name || '').localeCompare(String(b.name || ''))
  })
})

const unhealthyCount = computed(() => list.value.filter((t) => {
  if (hideInternal.value && t.isInternal) return false
  return healthRank(t) < 2
}).length)

const totalRecords = computed(() => filtered.value.reduce((s, t) => s + (t.recordCount || 0), 0))
const totalBytes = computed(() => filtered.value.reduce((s, t) => s + (t.sizeBytes || 0), 0))

function formatCount(n) {
  const v = Number(n) || 0
  return v.toLocaleString()
}

function formatBytes(n) {
  const v = Number(n)
  if (!Number.isFinite(v) || v < 0) return '-'
  if (v < 1024) return `${v} B`
  const units = ['KB', 'MB', 'GB', 'TB']
  let x = v / 1024
  let i = 0
  while (x >= 1024 && i < units.length - 1) {
    x /= 1024
    i++
  }
  return `${x.toFixed(x >= 10 ? 1 : 2)} ${units[i]}`
}

function formatRetention(ms) {
  const v = Number(ms)
  if (!Number.isFinite(v)) return ms || '-'
  if (v < 0) return '永久'
  if (v < 1000) return `${v} ms`
  const sec = v / 1000
  if (sec < 60) return `${sec} 秒`
  const min = sec / 60
  if (min < 60) return `${min} 分钟`
  const hour = min / 60
  if (hour < 48) return `${hour} 小时`
  return `${(hour / 24).toFixed(1)} 天`
}

function formatRetentionBytes(b) {
  const v = Number(b)
  if (!Number.isFinite(v)) return b || '-'
  if (v < 0) return '不限制'
  return formatBytes(v)
}

function formatMaxMsg(b) {
  const v = Number(b)
  if (!Number.isFinite(v) || v <= 0) return b || '-'
  return formatBytes(v)
}

async function load() {
  loading.value = true
  try {
    const res = await kafkaApi.listTopics()
    if (!res.ok) return ElMessage.error(res.error)
    list.value = res.data || []
  } finally {
    loading.value = false
  }
}

function openDetail(row) {
  detail.value = row
  detailVisible.value = true
}

function jump(path, row) {
  detailVisible.value = false
  router.push({ path, query: { topic: row.name } })
}

async function onMore(cmd, row) {
  if (cmd === 'produce') return jump('/produce', row)
  if (cmd === 'bulk') return jump('/bulk', row)
  if (cmd === 'expand') return expand(row)
  if (cmd === 'delete') {
    try {
      await ElMessageBox.confirm(`确认删除 Topic「${row.name}」？`, '删除 Topic', { type: 'warning' })
    } catch {
      return
    }
    return remove(row.name)
  }
}

async function create() {
  const res = await kafkaApi.createTopic(createForm.value)
  if (!res.ok) return ElMessage.error(res.error)
  ElMessage.success('已创建')
  showCreate.value = false
  await load()
}

async function remove(name) {
  const res = await kafkaApi.deleteTopic(name)
  if (!res.ok) return ElMessage.error(res.error)
  ElMessage.success('已删除')
  if (detail.value?.name === name) detailVisible.value = false
  await load()
}

async function expand(row) {
  const cur = row.partitions?.length || 0
  const { value } = await ElMessageBox.prompt(`当前分区 ${cur}，输入目标分区总数`, '扩分区', {
    inputValue: String(cur + 1),
    inputPattern: /^\d+$/,
  })
  const total = Number(value)
  const res = await kafkaApi.updatePartitions(row.name, total)
  if (!res.ok) return ElMessage.error(res.error)
  ElMessage.success('已扩分区')
  await load()
}

onMounted(load)
</script>

<style scoped>
.header-right { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.sub-alert { color: #f56c6c; font-weight: 600; }
.expand { padding: 8px 16px 12px 48px; }
.expand-meta {
  display: flex; flex-wrap: wrap; gap: 16px;
  color: #606266; font-size: 13px; margin-bottom: 8px;
}
.drawer-actions { margin: 14px 0 8px; display: flex; gap: 8px; }
.topic-name { font-weight: 600; vertical-align: middle; }
.internal-tag { margin-left: 6px; vertical-align: middle; }
.num { font-variant-numeric: tabular-nums; }

.health {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  white-space: nowrap;
  font-size: 13px;
  font-weight: 600;
  line-height: 1;
}
.health-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  display: inline-block;
  flex: none;
}
.health.ok { color: #67c23a; }
.health.ok .health-dot { background: #67c23a; }
.health.warn { color: #e6a23c; }
.health.warn .health-dot { background: #e6a23c; }
.health.bad { color: #f56c6c; }
.health.bad .health-dot { background: #f56c6c; }

.topics-table :deep(.el-table__cell) {
  padding: 8px 12px;
  vertical-align: middle;
}
.topics-table :deep(th.el-table__cell) {
  background: #fafafa;
  color: #606266;
  font-weight: 600;
}
.topics-table :deep(.el-table__header .cell) {
  display: flex;
  align-items: center;
  min-height: 24px;
  line-height: 24px;
}
.topics-table :deep(.el-table__body .cell) {
  display: flex;
  align-items: center;
  min-height: 24px;
  line-height: 24px;
}
.topics-table :deep(.is-right .cell) { justify-content: flex-end; }
.topics-table :deep(.is-center .cell) { justify-content: center; }
.topics-table :deep(.el-table__header .caret-wrapper) {
  height: 16px;
}

.ops-cell {
  display: inline-flex;
  align-items: center;
  justify-content: flex-end;
  gap: 12px;
  height: 24px;
  width: 100%;
}
.ops-link {
  border: 0;
  background: none;
  padding: 0;
  margin: 0;
  height: 24px;
  line-height: 24px;
  font-size: 13px;
  color: #409eff;
  cursor: pointer;
}
.ops-link:hover { color: #79bbff; }
.ops-cell :deep(.el-dropdown) {
  display: inline-flex;
  align-items: center;
  height: 24px;
  line-height: 24px;
}
</style>
