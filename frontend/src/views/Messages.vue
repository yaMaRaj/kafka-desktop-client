<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">消息浏览</div>
        <div class="page-sub">预览拉取与条件扫描导出</div>
      </div>
    </div>

    <div class="panel">
      <div class="panel-head">
        <span class="panel-title">拉取范围</span>
      </div>
      <div class="panel-grid">
        <div class="field">
          <span class="label">Topic</span>
          <el-select v-model="topic" filterable placeholder="选择 Topic" class="ctl-topic" @focus="loadTopics">
            <el-option v-for="t in topics" :key="t.name" :label="t.name" :value="t.name" />
          </el-select>
        </div>
        <div class="field">
          <span class="label">分区</span>
          <el-select v-model="partition" :empty-values="[]" class="ctl-sm">
            <el-option label="全部分区" :value="null" />
            <el-option v-for="p in partitionIds" :key="p" :label="p" :value="p" />
          </el-select>
        </div>
        <div class="field field-start">
          <span class="label">起点</span>
          <el-select v-model="startMode" class="ctl-sm">
            <el-option label="从最早开始" value="earliest" />
            <el-option label="从指定位置" value="custom" />
            <el-option label="从时间开始" value="time" />
          </el-select>
          <el-input-number
            v-if="startMode === 'custom'"
            v-model="customOffset"
            :min="0"
            :precision="0"
            placeholder="Offset"
            class="ctl-sm"
          />
          <el-date-picker
            v-if="startMode === 'time'"
            v-model="fromTime"
            type="datetime"
            placeholder="起始时间"
            value-format="x"
            class="ctl-time"
          />
        </div>
      </div>
      <div class="panel-actions">
        <div class="field">
          <span class="label">预览条数</span>
          <el-input-number v-model="limit" :min="1" :max="5000" class="ctl-sm" />
        </div>
        <div class="actions">
          <el-button class="btn-stable" type="primary" :loading="loading" @click="fetch">拉取</el-button>
          <el-button class="btn-stable" type="success" :loading="exporting" @click="openExport">导出</el-button>
        </div>
      </div>
    </div>

    <div class="panel panel-filter">
      <div class="panel-head">
        <span class="panel-title">导出筛选</span>
        <span class="panel-hint">仅导出扫描时生效，可选填</span>
      </div>
      <div class="panel-grid panel-grid-filter">
        <div class="field">
          <span class="label">Key 包含</span>
          <el-input v-model="filters.keyContains" clearable placeholder="可选" />
        </div>
        <div class="field">
          <span class="label">Value 包含</span>
          <el-input v-model="filters.valueContains" clearable placeholder="可选" />
        </div>
        <div class="field">
          <span class="label">Value 正则</span>
          <el-input v-model="filters.valueRegex" clearable placeholder="可选" />
        </div>
        <div class="field">
          <span class="label">结束时间</span>
          <el-date-picker
            v-model="filters.toTime"
            type="datetime"
            placeholder="可选"
            value-format="x"
            clearable
            class="ctl-time"
          />
        </div>
      </div>
    </div>

    <el-table :data="pagedRows" stripe border height="calc(100vh - 380px)" @row-click="showDetail">
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

    <div v-if="exportLogs.length" class="export-log">
      <div class="export-log-header">导出进度</div>
      <el-progress :percentage="exportPercent" :stroke-width="10" style="margin: 8px 0" />
      <div class="export-log-body">
        <div v-for="line in exportLogs" :key="line.id" class="export-log-line">
          <span class="ts">{{ line.ts }}</span>
          <span>{{ line.message }}</span>
        </div>
      </div>
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

    <el-dialog v-model="exportVisible" title="扫描导出消息" width="520px">
      <el-form label-width="110px">
        <el-form-item label="导出格式">
          <el-radio-group v-model="exportForm.format">
            <el-radio-button value="jsonl">JSONL</el-radio-button>
            <el-radio-button value="csv">CSV</el-radio-button>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="最大扫描">
          <el-input-number v-model="exportForm.maxScan" :min="1" :max="100000" />
          <span class="hint">上限 100000</span>
        </el-form-item>
        <el-form-item label="最大导出">
          <el-input-number v-model="exportForm.maxExport" :min="1" :max="20000" />
          <span class="hint">上限 20000（命中条数）</span>
        </el-form-item>
        <el-alert
          type="info"
          :closable="false"
          show-icon
          title="将按「拉取范围」与「导出筛选」扫描集群，边过滤边写入文件。"
        />
      </el-form>
      <template #footer>
        <el-button @click="exportVisible = false">取消</el-button>
        <el-button type="primary" :loading="exporting" @click="runExport">开始导出</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'
import { kafkaApi } from '../api/kafka'
import { EventsOff, EventsOn } from '../../wailsjs/runtime/runtime'

const PAGE_SIZE = 50

const route = useRoute()
const topics = ref([])
const topic = ref('')
const partition = ref(null)
const startMode = ref('earliest')
const customOffset = ref(0)
const fromTime = ref(null)
const limit = ref(50)
const rows = ref([])
const page = ref(1)
const pageSize = PAGE_SIZE
const loading = ref(false)
const drawer = ref(false)
const current = ref(null)
const detailMode = ref('meta')

const filters = ref({
  keyContains: '',
  valueContains: '',
  valueRegex: '',
  toTime: null,
})

const exportVisible = ref(false)
const exporting = ref(false)
const exportForm = ref({ format: 'jsonl', maxScan: 20000, maxExport: 5000 })
const exportLogs = ref([])
const exportLatest = ref(null)
let exportLogSeq = 0

const jsonText = computed(() => JSON.stringify(current.value, null, 2))
const jsonHtml = computed(() => highlightJson(jsonText.value))
const valueText = computed(() => {
  if (!current.value) return ''
  if (current.value.value == null) return '(null)'
  return String(current.value.value)
})

const exportPercent = computed(() => {
  const p = exportLatest.value
  if (!p || !p.maxScan) return 0
  if (p.phase === 'done') return 100
  return Math.min(99, Math.round((p.scanned / p.maxScan) * 100))
})

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
}

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

function pad(n) {
  return String(n).padStart(2, '0')
}

function nowTs() {
  const d = new Date()
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
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

function buildRangeParams() {
  const params = { topic: topic.value }
  if (partition.value != null) params.partition = partition.value
  if (startMode.value === 'custom') params.fromOffset = customOffset.value
  if (startMode.value === 'time' && fromTime.value != null) {
    params.fromTimestamp = Number(fromTime.value)
  }
  return params
}

async function fetch() {
  if (!topic.value) return ElMessage.warning('请选择 Topic')
  if (startMode.value === 'custom' && customOffset.value == null) {
    return ElMessage.warning('请填写起始 Offset')
  }
  if (startMode.value === 'time' && fromTime.value == null) {
    return ElMessage.warning('请选择起始时间')
  }
  loading.value = true
  try {
    const params = { ...buildRangeParams(), limit: limit.value }
    const res = await kafkaApi.fetchMessages(params)
    if (!res.ok) return ElMessage.error(res.error)
    rows.value = res.data || []
    page.value = 1
  } finally {
    loading.value = false
  }
}

function openExport() {
  if (!topic.value) return ElMessage.warning('请选择 Topic')
  exportVisible.value = true
}

function onExportProgress(prog) {
  if (!prog) return
  exportLatest.value = prog
  exportLogs.value.push({
    id: ++exportLogSeq,
    ts: nowTs(),
    message: prog.message || '',
  })
  if (exportLogs.value.length > 80) {
    exportLogs.value = exportLogs.value.slice(-80)
  }
}

async function runExport() {
  if (!topic.value) return ElMessage.warning('请选择 Topic')
  if (startMode.value === 'custom' && customOffset.value == null) {
    return ElMessage.warning('请填写起始 Offset')
  }
  if (startMode.value === 'time' && fromTime.value == null) {
    return ElMessage.warning('请选择起始时间')
  }
  exporting.value = true
  exportLogs.value = []
  exportLatest.value = { scanned: 0, maxScan: exportForm.value.maxScan, phase: 'start' }
  exportVisible.value = false
  try {
    const params = {
      ...buildRangeParams(),
      keyContains: filters.value.keyContains || '',
      valueContains: filters.value.valueContains || '',
      valueRegex: filters.value.valueRegex || '',
      maxScan: exportForm.value.maxScan,
      maxExport: exportForm.value.maxExport,
      format: exportForm.value.format,
    }
    if (filters.value.toTime != null) {
      params.toTimestamp = Number(filters.value.toTime)
    }
    const res = await kafkaApi.exportMessages(params)
    if (!res.ok) return ElMessage.error(res.error)
    const d = res.data || {}
    const tip = `已导出 ${d.exported || 0} 条 → ${d.path || ''}`
    if (d.truncated) ElMessage.warning(`${tip}（已达上限）`)
    else ElMessage.success(tip)
  } finally {
    exporting.value = false
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
onMounted(() => {
  loadTopics()
  EventsOn('export-messages-progress', onExportProgress)
})
onUnmounted(() => {
  EventsOff('export-messages-progress')
})
</script>

<style scoped>
.panel {
  border: 1px solid #ebeef5;
  border-radius: 8px;
  padding: 12px 14px 14px;
  margin-bottom: 12px;
  background: #fff;
}
.panel-filter {
  background: #fafbfc;
}
.panel-head {
  display: flex;
  align-items: baseline;
  gap: 10px;
  margin-bottom: 12px;
}
.panel-title {
  font-size: 13px;
  font-weight: 600;
  color: #303133;
}
.panel-hint {
  font-size: 12px;
  color: #909399;
}
.panel-grid {
  display: grid;
  grid-template-columns: minmax(220px, 1.4fr) minmax(120px, 0.7fr) minmax(280px, 1.6fr);
  gap: 12px 16px;
  align-items: center;
}
.panel-grid-filter {
  grid-template-columns: repeat(4, minmax(140px, 1fr));
}
.panel-actions {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-top: 12px;
  padding-top: 12px;
  border-top: 1px dashed #e4e7ed;
}
.actions {
  display: flex;
  align-items: center;
  gap: 8px;
}
.field {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
}
.field-start {
  flex-wrap: wrap;
}
.field :deep(.el-input),
.field :deep(.el-select),
.field :deep(.el-date-editor),
.field :deep(.el-input-number) {
  flex: 1;
  min-width: 0;
  width: 100%;
}
.label {
  color: #606266;
  font-size: 13px;
  white-space: nowrap;
  flex: none;
  width: 64px;
  text-align: right;
}
.panel-actions .label { width: auto; }
.ctl-topic { width: 100%; }
.ctl-sm { width: 140px !important; flex: none !important; }
.ctl-time { width: 200px !important; flex: none !important; }
.hint { margin-left: 8px; color: #909399; font-size: 12px; }
.btn-stable { min-width: 80px; }
.pager { display: flex; justify-content: flex-end; margin-top: 12px; }
.export-log { margin-top: 12px; max-width: 900px; }
.export-log-header { font-weight: 600; color: #303133; }
.export-log-body {
  max-height: 140px;
  overflow: auto;
  background: #1f2d3d;
  color: #d8dee9;
  border-radius: 6px;
  padding: 8px 12px;
  font-family: Consolas, "Courier New", monospace;
  font-size: 12px;
  line-height: 1.6;
}
.export-log-line { display: flex; gap: 10px; }
.export-log-line .ts { color: #88c0d0; flex: none; }

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
.json-pre :deep(.jk) { color: #9cdcfe; }
.json-pre :deep(.js) { color: #ce9178; }
.json-pre :deep(.jn) { color: #b5cea8; }
.json-pre :deep(.jl) { color: #569cd6; }
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

@media (max-width: 1100px) {
  .panel-grid,
  .panel-grid-filter {
    grid-template-columns: 1fr 1fr;
  }
}
@media (max-width: 720px) {
  .panel-grid,
  .panel-grid-filter {
    grid-template-columns: 1fr;
  }
  .panel-actions {
    flex-direction: column;
    align-items: stretch;
  }
  .actions {
    justify-content: flex-end;
  }
}
</style>
