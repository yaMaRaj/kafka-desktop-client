<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">操作日志</div>
        <div class="page-sub">本地记录最近 500 条操作</div>
      </div>
      <div class="header-actions">
        <el-date-picker
          v-model="timeRange"
          type="datetimerange"
          range-separator="至"
          start-placeholder="开始时间"
          end-placeholder="结束时间"
          format="YYYY-MM-DD HH:mm:ss"
          value-format="x"
          :default-time="defaultTime"
          :clearable="true"
          style="width: 360px"
        />
        <el-button @click="load">刷新</el-button>
        <el-popconfirm title="确认清空？" @confirm="clear">
          <template #reference>
            <el-button type="danger">清空</el-button>
          </template>
        </el-popconfirm>
      </div>
    </div>
    <el-table :data="pagedRows" stripe border height="calc(100vh - 260px)">
      <el-table-column label="时间" width="170">
        <template #default="{ row }">{{ formatTs(row.at) }}</template>
      </el-table-column>
      <el-table-column label="连接" width="140" show-overflow-tooltip>
        <template #default="{ row }">{{ connName(row) }}</template>
      </el-table-column>
      <el-table-column label="动作" width="120">
        <template #default="{ row }">
          <el-tag :type="actionMeta(row.action).type" size="small">
            {{ actionMeta(row.action).label }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="detail" label="详情" min-width="220" show-overflow-tooltip />
      <el-table-column label="结果" width="80">
        <template #default="{ row }">
          <el-tag :type="row.success ? 'success' : 'danger'" size="small">
            {{ row.success ? '成功' : '失败' }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="失败原因" min-width="220" show-overflow-tooltip>
        <template #default="{ row }">
          <span :class="{ 'err-text': row.error }">{{ row.error || '-' }}</span>
        </template>
      </el-table-column>
    </el-table>
    <div class="pager">
      <el-pagination
        v-model:current-page="page"
        v-model:page-size="pageSize"
        :total="filtered.length"
        :page-sizes="[10, 20, 50, 100]"
        layout="total, sizes, prev, pager, next, jumper"
        background
      />
    </div>
  </div>
</template>

<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { kafkaApi } from '../api/kafka'

// 动作代码 -> 中文标签与分类颜色（连接类=蓝、Topic 类=橙、消息类=绿、删除类=红）
const ACTION_META = {
  connect: { label: '连接集群', type: 'info' },
  disconnect: { label: '断开连接', type: 'info' },
  testConnection: { label: '测试连接', type: 'info' },
  saveConnection: { label: '保存连接', type: 'info' },
  deleteConnection: { label: '删除连接', type: 'danger' },
  createTopic: { label: '创建 Topic', type: 'warning' },
  deleteTopic: { label: '删除 Topic', type: 'danger' },
  updatePartitions: { label: '扩分区', type: 'warning' },
  produceMessage: { label: '发送消息', type: 'success' },
  bulkProduce: { label: '批量造数', type: 'success' },
  deleteGroup: { label: '删除消费组', type: 'danger' },
  testSSH: { label: '测试 SSH', type: 'info' },
  readBrokerConfig: { label: '读取配置', type: 'warning' },
  saveBrokerConfig: { label: '保存配置', type: 'warning' },
  kafkaStatus: { label: '服务状态', type: 'info' },
  restartKafka: { label: '重启 Kafka', type: 'danger' },
}

const rows = ref([])
const connNames = ref({})
const timeRange = ref(null) // [开始毫秒, 结束毫秒]，value-format="x" 返回字符串
const page = ref(1)
const pageSize = ref(20)

// 只选日期时默认覆盖整天：开始 00:00:00、结束 23:59:59
const defaultTime = [new Date(2000, 0, 1, 0, 0, 0), new Date(2000, 0, 1, 23, 59, 59)]

const filtered = computed(() => {
  let list = rows.value
  if (timeRange.value && timeRange.value.length === 2) {
    const s = Number(timeRange.value[0])
    const e = Number(timeRange.value[1])
    if (!Number.isNaN(s) && !Number.isNaN(e)) {
      list = list.filter((r) => r.at >= s && r.at <= e)
    }
  }
  return list
})

const pagedRows = computed(() => {
  const start = (page.value - 1) * pageSize.value
  return filtered.value.slice(start, start + pageSize.value)
})

function actionMeta(action) {
  return ACTION_META[action] || { label: action || '-', type: 'info' }
}

// 优先用记录时冗余的连接名；旧日志回退到按 connectionId 查当前连接
function connName(row) {
  return row.connectionName || connNames.value[row.connectionId] || '-'
}

function formatTs(ts) {
  if (!ts) return '-'
  const d = new Date(ts)
  const p = (n) => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`
}

async function load() {
  const [logRes, connRes] = await Promise.all([kafkaApi.listOperationLogs(), kafkaApi.listConnections()])
  if (!logRes.ok) return ElMessage.error(logRes.error)
  rows.value = logRes.data || []
  const map = {}
  for (const c of connRes.data || []) map[c.id] = c.name
  connNames.value = map
}

async function clear() {
  const res = await kafkaApi.clearOperationLogs()
  if (!res.ok) return ElMessage.error(res.error)
  ElMessage.success('已清空')
  await load()
}

// 换筛选条件或页大小回到第一页；数据变化后当前页越界时自动收敛
watch(timeRange, () => { page.value = 1 })
watch(pageSize, () => { page.value = 1 })
watch(() => filtered.value.length, (n) => {
  const maxPage = Math.max(1, Math.ceil(n / pageSize.value))
  if (page.value > maxPage) page.value = maxPage
})

onMounted(load)
</script>

<style scoped>
.header-actions { display: flex; align-items: center; gap: 8px; }
.err-text { color: #f56c6c; }
.pager { display: flex; justify-content: flex-end; margin-top: 12px; }
</style>
