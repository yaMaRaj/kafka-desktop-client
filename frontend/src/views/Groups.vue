<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">消费组</div>
        <div class="page-sub">查看成员与 lag</div>
      </div>
      <el-button class="btn-stable" @click="load" :loading="loading">刷新</el-button>
    </div>
    <el-row :gutter="12">
      <el-col :span="8">
        <el-table :data="groups" stripe border height="calc(100vh - 200px)" highlight-current-row @current-change="onSelect">
          <el-table-column prop="groupId" label="Group" />
          <el-table-column prop="state" label="状态" width="100" />
        </el-table>
      </el-col>
      <el-col :span="16">
        <template v-if="detail">
          <el-descriptions :column="3" border style="margin-bottom: 12px">
            <el-descriptions-item label="Group">{{ detail.groupId }}</el-descriptions-item>
            <el-descriptions-item label="状态">{{ detail.state }}</el-descriptions-item>
            <el-descriptions-item label="成员">{{ detail.members }}</el-descriptions-item>
            <el-descriptions-item label="总 Lag">{{ detail.totalLag }}</el-descriptions-item>
          </el-descriptions>
          <el-table :data="detail.offsets || []" stripe border height="calc(100vh - 320px)">
            <el-table-column prop="topic" label="Topic" />
            <el-table-column prop="partition" label="P" width="70" />
            <el-table-column prop="offset" label="Offset" width="120" />
            <el-table-column prop="lag" label="Lag" width="120" />
          </el-table>
          <div style="margin-top: 12px">
            <el-popconfirm title="确认删除消费组？" @confirm="remove(detail.groupId)">
              <template #reference>
                <el-button type="danger">删除组</el-button>
              </template>
            </el-popconfirm>
          </div>
        </template>
        <el-empty v-else description="选择左侧消费组" />
      </el-col>
    </el-row>
  </div>
</template>

<script setup>
import { onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { kafkaApi } from '../api/kafka'

const groups = ref([])
const detail = ref(null)
const loading = ref(false)

async function load() {
  loading.value = true
  try {
    const res = await kafkaApi.listConsumerGroups()
    if (!res.ok) return ElMessage.error(res.error)
    groups.value = res.data || []
  } finally {
    loading.value = false
  }
}

async function onSelect(row) {
  if (!row) return
  const res = await kafkaApi.describeConsumerGroup(row.groupId)
  if (!res.ok) return ElMessage.error(res.error)
  detail.value = res.data
}

async function remove(id) {
  const res = await kafkaApi.deleteConsumerGroup(id)
  if (!res.ok) return ElMessage.error(res.error)
  ElMessage.success('已删除')
  detail.value = null
  await load()
}

onMounted(load)
</script>

<style scoped>
.btn-stable { min-width: 80px; }
</style>
