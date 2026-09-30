<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">消息浏览</div>
        <div class="page-sub">按 Topic / 分区 / Offset 拉取</div>
      </div>
    </div>
    <div class="toolbar">
      <el-select v-model="topic" filterable placeholder="选择 Topic" style="width: 260px" @focus="loadTopics">
        <el-option v-for="t in topics" :key="t.name" :label="t.name" :value="t.name" />
      </el-select>
      <div class="field">
        <span class="label">分区</span>
        <el-select v-model="partition" :empty-values="[]" style="width: 110px">
          <el-option label="全部分区" :value="null" />
          <el-option v-for="p in partitionIds" :key="p" :label="p" :value="p" />
        </el-select>
      </div>
      <div class="field">
        <span class="label">起点</span>
        <el-select v-model="startMode" style="width: 120px">
          <el-option label="从最早开始" value="earliest" />
          <el-option label="从指定位置" value="custom" />
        </el-select>
        <el-input-number
          v-if="startMode === 'custom'"
          v-model="customOffset"
          :min="0"
          :precision="0"
          placeholder="Offset"
          style="width: 130px"
        />
      </div>
      <div class="field">
        <span class="label">条数</span>
        <el-input-number v-model="limit" :min="1" :max="5000" style="width: 110px" />
      </div>
      <el-button class="btn-stable" type="primary" :loading="loading" @click="fetch">拉取</el-button>
    </div>
    <el-table :data="pagedRows" stripe border height="calc(100vh - 280px)" @row-click="showDetail">
      <el-table-column prop="partition" label="P" width="60" />
      <el-table-column prop="offset" label="Offset" width="100" />
      <el-table-column prop="timestamp" label="时间" width="180">
        <template #default="{ row }">{{ formatTs(row.timestamp) }}</template>
      </el-table-column>
      <el-table-column prop="key" label="Key" min-width="120" show-overflow-tooltip />
      <el-table-column prop="value" label="Value" min-width="260" show-overflow-tooltip />
    </el-table>
    <div v-if="rows.length > pageSize" class="pager">
      <el-pagination
        v-model:current-page="page"
        :page-size="pageSize"
        :total="rows.length"
        layout="total, prev, pager, next, jumper"
        background
      />
    </div>

    <el-drawer v-model="drawer" title="消息详情" size="44%">
      <div class="detail-toolbar">
        <el-radio-group v-model="detailMode" size="small">
          <el-radio-button value="meta">元数据</el-radio-button>
          <el-radio-button value="json">JSON</el-radio-button>
        </el-radio-group>
      </div>

      <template v-if="detailMode === 'meta' && current">
        <el-descriptions :column="1" border size="small" class="meta-desc">
          <el-descriptions-item label="Topic">{{ current.topic || '-' }}</el-descriptions-item>
          <el-descriptions-item label="Partition">{{ current.partition }}</el-descriptions-item>
          <el-descriptions-item label="Offset">{{ current.offset }}</el-descriptions-item>
          <el-descriptions-item label="Timestamp">{{ formatTs(current.timestamp) }}</el-descriptions-item>
          <el-descriptions-item label="Key">{{ current.key ?? '(null)' }}</el-descriptions-item>
          <el-descriptions-item label="Headers">
            <span v-if="!(current.headers || []).length">-</span>
            <div v-else class="hdr-list">
              <div v-for="(h, i) in current.headers" :key="i" class="hdr-item">
                <code>{{ h.key }}</code>: {{ h.value }}
              </div>
            </div>
          </el-descriptions-item>
        </el-descriptions>
        <div class="msg-label">Value</div>
        <div class="msg-box">
          <el-tooltip content="复制" placement="top">
            <button type="button" class="msg-copy" @click="copyText(valueText)">
              <el-icon :size="16"><DocumentCopy /></el-icon>
            </button>
          </el-tooltip>
          <pre class="msg-body">{{ valueText }}</pre>
        </div>
      </template>

      <template v-else>
        <div class="msg-box msg-box-json">
          <el-tooltip content="复制" placement="top">
            <button type="button" class="msg-copy" @click="copyText(jsonText)">
              <el-icon :size="16"><DocumentCopy /></el-icon>
            </button>
          </el-tooltip>
          <pre class="msg-body json-pre" v-html="jsonHtml"></pre>
        </div>
      </template>
    </el-drawer>
  </div>
</template>

<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'
import { kafkaApi } from '../api/kafka'

const PAGE_SIZE = 50

const route = useRoute()
const topics = ref([])
const topic = ref('')
const partition = ref(null) // null = 全部分区
const startMode = ref('earliest')
const customOffset = ref(0)
const limit = ref(50)
const rows = ref([])
const page = ref(1)
const pageSize = PAGE_SIZE
const loading = ref(false)
const drawer = ref(false)
const current = ref(null)
const detailMode = ref('meta')

const jsonText = computed(() => JSON.stringify(current.value, null, 2))
const jsonHtml = computed(() => highlightJson(jsonText.value))
const valueText = computed(() => {
  if (!current.value) return ''
  if (current.value.value == null) return '(null)'
  return String(current.value.value)
})

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
}

/** 轻量 JSON 着色：key / string / number / bool·null */
function highlightJson(text) {
  if (!text) return ''
  const escaped = escapeHtml(text)
  return escaped.replace(
    /("(?:\\.|[^"\\])*")(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g,
    (match, str, isKey, lit) => {
      if (str && isKey) return `<span class="jk">${str}</span>${isKey}`
      if (str) return `<span class="js">${str}</span>`
      if (lit) return `<span class="jl">${lit}</span>`
      return `<span class="jn">${match}</span>`
    },
  )
}

const pagedRows = computed(() => {
  const start = (page.value - 1) * pageSize
  return rows.value.slice(start, start + pageSize)
})

const partitionIds = computed(() => {
  const t = topics.value.find((x) => x.name === topic.value)
  return (t?.partitions || []).map((p) => p.partitionId).sort((a, b) => a - b)
})

watch(topic, () => {
  if (partition.value != null && !partitionIds.value.includes(partition.value)) {
    partition.value = null
  }
})

watch(() => rows.value.length, (n) => {
  const maxPage = Math.max(1, Math.ceil(n / pageSize) || 1)
  if (page.value > maxPage) page.value = maxPage
})

function formatTs(ts) {
  if (!ts || ts < 0) return '-'
  return new Date(ts).toLocaleString()
}

function applyQueryTopic() {
  const q = route.query.topic
  if (q) topic.value = String(q)
}

async function loadTopics() {
  const res = await kafkaApi.listTopics()
  if (res.ok) topics.value = (res.data || []).filter((t) => !t.isInternal)
  applyQueryTopic()
}

async function fetch() {
  if (!topic.value) return ElMessage.warning('请选择 Topic')
  if (startMode.value === 'custom' && customOffset.value == null) {
    return ElMessage.warning('请填写起始 Offset')
  }
  loading.value = true
  try {
    const params = { topic: topic.value, limit: limit.value }
    if (partition.value != null) params.partition = partition.value
    if (startMode.value === 'custom') params.fromOffset = customOffset.value
    const res = await kafkaApi.fetchMessages(params)
    if (!res.ok) return ElMessage.error(res.error)
    rows.value = res.data || []
    page.value = 1
  } finally {
    loading.value = false
  }
}

function showDetail(row) {
  current.value = row
  detailMode.value = 'meta'
  drawer.value = true
}

async function copyText(text) {
  try {
    await navigator.clipboard.writeText(text ?? '')
    ElMessage.success('已复制')
  } catch {
    ElMessage.error('复制失败')
  }
}

watch(() => route.query.topic, applyQueryTopic)
onMounted(loadTopics)
</script>

<style scoped>
.field { display: flex; align-items: center; gap: 6px; }
.label { color: #606266; font-size: 13px; white-space: nowrap; }
.btn-stable { min-width: 80px; }
.pager { display: flex; justify-content: flex-end; margin-top: 12px; }

.detail-toolbar { margin-bottom: 12px; }
.meta-desc { margin-bottom: 14px; }
.hdr-list { display: flex; flex-direction: column; gap: 4px; }
.hdr-item { font-size: 12px; word-break: break-all; }
.msg-label {
  font-size: 13px;
  color: #606266;
  margin: 4px 0 8px;
  font-weight: 600;
}
.msg-box {
  position: relative;
  background: #f5f7fa;
  border: 1px solid #e4e7ed;
  border-radius: 6px;
  padding: 36px 12px 12px;
  min-height: 120px;
}
.msg-copy {
  position: absolute;
  top: 8px;
  right: 8px;
  z-index: 1;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 28px;
  height: 28px;
  padding: 0;
  border: none;
  border-radius: 4px;
  background: transparent;
  color: #909399;
  cursor: pointer;
}
.msg-copy:hover {
  color: #409eff;
  background: #ecf5ff;
}
.msg-box-json {
  background: #1e1e1e;
  border-color: #333;
}
.msg-box-json .msg-copy {
  color: #a0a0a0;
}
.msg-box-json .msg-copy:hover {
  color: #fff;
  background: rgba(255, 255, 255, 0.12);
}
.json-pre {
  color: #d4d4d4;
  font-family: Consolas, "Courier New", monospace;
}
.json-pre :deep(.jk) { color: #9cdcfe; }   /* key */
.json-pre :deep(.js) { color: #ce9178; }   /* string */
.json-pre :deep(.jn) { color: #b5cea8; }   /* number */
.json-pre :deep(.jl) { color: #569cd6; }   /* true/false/null */
.msg-body {
  margin: 0;
  white-space: pre-wrap;
  word-break: break-all;
  font-size: 12px;
  line-height: 1.5;
  color: #303133;
}
.msg-box-json .msg-body {
  color: #d4d4d4;
}
</style>
