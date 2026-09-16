<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">批量造数 / 轻量压测</div>
        <div class="page-sub">Go 直连集群，无需本机安装 Kafka 发行版</div>
      </div>
    </div>
    <el-form label-width="120px" style="max-width: 640px">
      <el-form-item label="Topic" required>
        <el-select v-model="form.topic" filterable style="width: 100%" @focus="loadTopics">
          <el-option v-for="t in topics" :key="t.name" :label="t.name" :value="t.name" />
        </el-select>
      </el-form-item>
      <el-form-item label="条数">
        <el-input-number v-model="form.numRecords" :min="1" :max="5000000" />
      </el-form-item>
      <el-form-item label="单条大小(B)">
        <el-input-number v-model="form.recordSize" :min="1" :max="1048576" />
      </el-form-item>
      <el-form-item label="吞吐(条/秒)">
        <el-input-number v-model="form.throughput" :min="0" />
        <span style="margin-left: 8px; color: #909399">0 = 不限速</span>
      </el-form-item>
      <el-form-item label="Key 前缀">
        <el-input v-model="form.keyPrefix" placeholder="可选" />
      </el-form-item>
      <el-form-item>
        <el-button type="primary" :loading="loading" @click="run">开始</el-button>
      </el-form-item>
    </el-form>

    <div v-if="logs.length" class="bulk-log">
      <div class="bulk-log-header">
        <span>造数日志</span>
        <span class="bulk-log-sub">每 1000 条刷新一次</span>
      </div>
      <el-progress
        :percentage="percent"
        :status="progressStatus"
        :stroke-width="10"
        style="margin: 8px 0 12px"
      />
      <div ref="logBody" class="bulk-log-body">
        <div v-for="line in logs" :key="line.id" class="bulk-log-line" :class="line.phase">
          <span class="ts">{{ line.ts }}</span>
          <span class="msg">{{ line.message }}</span>
        </div>
      </div>
    </div>

    <el-descriptions v-if="result" title="结果" :column="2" border style="max-width: 720px; margin-top: 16px">
      <el-descriptions-item label="成功">{{ result.sent }}</el-descriptions-item>
      <el-descriptions-item label="失败">{{ result.failed }}</el-descriptions-item>
      <el-descriptions-item label="耗时(ms)">{{ result.elapsedMs }}</el-descriptions-item>
      <el-descriptions-item label="吞吐(条/秒)">{{ result.recordsPerSec?.toFixed?.(2) ?? result.recordsPerSec }}</el-descriptions-item>
      <el-descriptions-item label="平均延迟(ms)">{{ result.avgLatencyMs?.toFixed?.(3) ?? result.avgLatencyMs }}</el-descriptions-item>
    </el-descriptions>
  </div>
</template>

<script setup>
import { computed, nextTick, onMounted, onUnmounted, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { kafkaApi } from '../api/kafka'
import { EventsOff, EventsOn } from '../../wailsjs/runtime/runtime'
import { useRoute } from 'vue-router'

const route = useRoute()
const topics = ref([])
const form = ref({
  topic: '',
  numRecords: 1000,
  recordSize: 100,
  throughput: 0,
  keyPrefix: 'bulk',
})
const loading = ref(false)
const result = ref(null)
const logs = ref([])
const latest = ref(null)
const logBody = ref(null)
let logSeq = 0

const percent = computed(() => {
  const p = latest.value
  if (!p || !p.total) return 0
  return Math.min(100, Math.round((p.processed / p.total) * 100))
})

const progressStatus = computed(() => {
  if (!latest.value) return undefined
  if (latest.value.phase !== 'done') return undefined
  if (latest.value.failed > 0 && latest.value.sent === 0) return 'exception'
  if (latest.value.failed > 0) return 'warning'
  return 'success'
})

function pad(n) {
  return String(n).padStart(2, '0')
}

function nowTs() {
  const d = new Date()
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
}

function appendLog(prog) {
  if (!prog) return
  latest.value = prog
  logs.value.push({
    id: ++logSeq,
    ts: nowTs(),
    phase: prog.phase || 'progress',
    message: prog.message || '',
  })
  if (logs.value.length > 200) {
    logs.value = logs.value.slice(-200)
  }
  nextTick(() => {
    const el = logBody.value
    if (el) el.scrollTop = el.scrollHeight
  })
}

function onProgress(prog) {
  appendLog(prog)
}

async function loadTopics() {
  const res = await kafkaApi.listTopics()
  if (res.ok) topics.value = (res.data || []).filter((t) => !t.isInternal)
  applyQueryTopic()
}

function applyQueryTopic() {
  const q = route.query.topic
  if (q) form.value.topic = String(q)
}

watch(() => route.query.topic, applyQueryTopic)

async function run() {
  if (!form.value.topic) return ElMessage.warning('请选择 Topic')
  loading.value = true
  result.value = null
  logs.value = []
  latest.value = { processed: 0, total: form.value.numRecords, phase: 'start', failed: 0, sent: 0 }
  try {
    const res = await kafkaApi.bulkProduce(form.value)
    if (!res.ok) {
      appendLog({
        phase: 'done',
        processed: latest.value?.processed || 0,
        total: form.value.numRecords,
        sent: latest.value?.sent || 0,
        failed: latest.value?.failed || 0,
        message: `失败：${res.error}`,
      })
      return ElMessage.error(res.error)
    }
    result.value = res.data
    if (res.data?.failed > 0 && res.data?.sent === 0) {
      ElMessage.error(`全部失败（${res.data.failed} 条）`)
    } else if (res.data?.failed > 0) {
      ElMessage.warning(`完成：成功 ${res.data.sent}，失败 ${res.data.failed}`)
    } else {
      ElMessage.success('完成')
    }
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  loadTopics()
  EventsOn('bulk-produce-progress', onProgress)
})

onUnmounted(() => {
  EventsOff('bulk-produce-progress')
})
</script>

<style scoped>
.bulk-log {
  max-width: 720px;
  margin-top: 8px;
}
.bulk-log-header {
  display: flex;
  align-items: baseline;
  gap: 10px;
  font-weight: 600;
  color: #303133;
}
.bulk-log-sub {
  font-weight: 400;
  font-size: 12px;
  color: #909399;
}
.bulk-log-body {
  height: 220px;
  overflow: auto;
  background: #1f2d3d;
  color: #d8dee9;
  border-radius: 6px;
  padding: 10px 12px;
  font-family: Consolas, "Courier New", monospace;
  font-size: 12px;
  line-height: 1.7;
}
.bulk-log-line { display: flex; gap: 10px; }
.bulk-log-line .ts { color: #88c0d0; flex: none; }
.bulk-log-line.start .msg { color: #ebcb8b; }
.bulk-log-line.done .msg { color: #a3be8c; }
.bulk-log-line .msg { word-break: break-all; }
</style>
