<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">消息生产</div>
        <div class="page-sub">发送单条消息到 Topic</div>
      </div>
    </div>
    <el-form label-width="100px" style="max-width: 720px">
      <el-form-item label="Topic" required>
        <el-select v-model="form.topic" filterable style="width: 100%" @focus="loadTopics">
          <el-option v-for="t in topics" :key="t.name" :label="t.name" :value="t.name" />
        </el-select>
      </el-form-item>
      <el-form-item label="Key"><el-input v-model="form.key" /></el-form-item>
      <el-form-item label="分区">
        <el-select v-model="part" :empty-values="[]" style="width: 220px">
          <el-option label="自动分配" :value="null" />
          <el-option v-for="p in partitionIds" :key="p" :label="p" :value="p" />
        </el-select>
      </el-form-item>
      <el-form-item label="Value" required>
        <el-input v-model="form.value" type="textarea" :rows="10" />
      </el-form-item>
      <el-form-item>
        <el-button type="primary" :loading="loading" @click="send">发送</el-button>
      </el-form-item>
    </el-form>
  </div>
</template>

<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'
import { kafkaApi } from '../api/kafka'

const route = useRoute()
const topics = ref([])
const form = ref({ topic: '', key: '', value: '' })
const part = ref(null) // null = 自动分配
const loading = ref(false)

const partitionIds = computed(() => {
  const t = topics.value.find((x) => x.name === form.value.topic)
  return (t?.partitions || []).map((p) => p.partitionId).sort((a, b) => a - b)
})

watch(() => form.value.topic, () => {
  if (part.value != null && !partitionIds.value.includes(part.value)) part.value = null
})

async function loadTopics() {
  const res = await kafkaApi.listTopics()
  if (res.ok) topics.value = (res.data || []).filter((t) => !t.isInternal)
  applyQueryTopic()
}

function applyQueryTopic() {
  const q = route.query.topic
  if (q) form.value.topic = String(q)
}

async function send() {
  if (!form.value.topic || !form.value.value) return ElMessage.warning('请填写 Topic 与 Value')
  loading.value = true
  try {
    const payload = { ...form.value }
    if (part.value != null) payload.partition = part.value
    const res = await kafkaApi.produceMessage(payload)
    if (!res.ok) return ElMessage.error(res.error)
    ElMessage.success(`已发送 P${res.data.partition} @ ${res.data.offset}`)
  } finally {
    loading.value = false
  }
}

watch(() => route.query.topic, applyQueryTopic)
onMounted(loadTopics)
</script>
