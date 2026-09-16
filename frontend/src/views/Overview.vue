<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">集群概览</div>
        <div class="page-sub">Cluster ID / Controller / Brokers</div>
      </div>
      <el-button @click="load" :loading="loading">刷新</el-button>
    </div>
    <el-descriptions v-if="data" :column="2" border style="margin-bottom: 16px">
      <el-descriptions-item label="Cluster ID">{{ data.clusterId || '-' }}</el-descriptions-item>
      <el-descriptions-item label="Controller">{{ data.controllerId }}</el-descriptions-item>
      <el-descriptions-item label="Broker 数">{{ data.brokers?.length || 0 }}</el-descriptions-item>
    </el-descriptions>
    <el-table :data="data?.brokers || []" stripe border>
      <el-table-column prop="nodeId" label="NodeID" width="100" />
      <el-table-column prop="host" label="Host" />
      <el-table-column prop="port" label="Port" width="100" />
      <el-table-column prop="rack" label="Rack" width="120" />
    </el-table>
  </div>
</template>

<script setup>
import { onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { kafkaApi } from '../api/kafka'

const data = ref(null)
const loading = ref(false)

async function load() {
  loading.value = true
  try {
    const res = await kafkaApi.getOverview()
    if (!res.ok) return ElMessage.error(res.error)
    data.value = res.data
  } finally {
    loading.value = false
  }
}

onMounted(load)
</script>
