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
      <el-button type="primary" :loading="loading" @click="fetch">拉取</el-button>
    </div>
    <el-table :data="rows" stripe border height="calc(100vh - 240px)" @row-click="showDetail">
      <el-table-column prop="partition" label="P" width="60" />
      <el-table-column prop="offset" label="Offset" width="100" />
      <el-table-column prop="timestamp" label="时间" width="180">
        <template #default="{ row }">{{ formatTs(row.timestamp) }}</template>
      </el-table-column>
      <el-table-column prop="key" label="Key" min-width="120" show-overflow-tooltip />
      <el-table-column prop="value" label="Value" min-width="260" show-overflow-tooltip />
    </el-table>

    <el-drawer v-model="drawer" title="消息详情" size="40%">
      <pre class="detail">{{ detailText }}</pre>
    </el-drawer>
  </div>
</template>

<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'
import { kafkaApi } from '../api/kafka'

const route = useRoute()
const topics = ref([])
const topic = ref('')
const partition = ref(null) // null = 全部分区
const startMode = ref('earliest')
const customOffset = ref(0)
const limit = ref(50)
const rows = ref([])
const loading = ref(false)
const drawer = ref(false)
const current = ref(null)
const detailText = computed(() => JSON.stringify(current.value, null, 2))

const partitionIds = computed(() => {
  const t = topics.value.find((x) => x.name === topic.value)
  return (t?.partitions || []).map((p) => p.partitionId).sort((a, b) => a - b)
})

watch(topic, () => {
  if (partition.value != null && !partitionIds.value.includes(partition.value)) {
    partition.value = null
  }
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
  } finally {
    loading.value = false
  }
}

function showDetail(row) {
  current.value = row
  drawer.value = true
}

watch(() => route.query.topic, applyQueryTopic)
onMounted(loadTopics)
</script>

<style scoped>
.field { display: flex; align-items: center; gap: 6px; }
.label { color: #606266; font-size: 13px; white-space: nowrap; }
.detail { white-space: pre-wrap; word-break: break-all; font-size: 12px; }
</style>
