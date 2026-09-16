<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">集群运维</div>
        <div class="page-sub">通过 SSH 查看 / 修改 Broker 配置文件，并重启 Kafka 服务</div>
      </div>
    </div>

    <el-form label-width="110px" style="max-width: 920px">
      <el-form-item label="连接">
        <el-select v-model="connId" placeholder="选择已保存的连接" style="width: 420px" @change="onPick">
          <el-option
            v-for="c in connections"
            :key="c.id"
            :label="`${c.name} (${c.bootstrapServers})`"
            :value="c.id"
          />
        </el-select>
        <el-button style="margin-left: 8px" @click="goEdit">去配置 SSH</el-button>
      </el-form-item>
    </el-form>

    <el-alert
      v-if="connId && !hasSSH"
      type="warning"
      show-icon
      :closable="false"
      title="该连接尚未配置 SSH。请在「连接管理」中编辑连接，填写 SSH 用户、密码/私钥、配置文件路径和重启命令。"
      style="margin-bottom: 12px"
    />

    <template v-if="hasSSH">
      <el-descriptions :column="2" border style="max-width: 920px; margin-bottom: 12px">
        <el-descriptions-item label="SSH">{{ sshSummary }}</el-descriptions-item>
        <el-descriptions-item label="配置文件">{{ admin.configPath || '-' }}</el-descriptions-item>
        <el-descriptions-item label="重启命令" :span="2">{{ admin.restartCommand || '-' }}</el-descriptions-item>
      </el-descriptions>

      <div class="toolbar">
        <el-button :loading="testing" @click="testSSH">测试 SSH</el-button>
        <el-button type="primary" :loading="loadingCfg" @click="loadConfig">读取配置</el-button>
        <el-button type="success" :loading="saving" :disabled="!loaded" @click="saveConfig">保存配置</el-button>
        <el-button :loading="statusing" @click="loadStatus">服务状态</el-button>
        <el-popconfirm title="确认在远程主机上重启 Kafka 服务？正在写入的生产者会中断。" @confirm="restart">
          <template #reference>
            <el-button type="danger" :loading="restarting">重启 Kafka</el-button>
          </template>
        </el-popconfirm>
      </div>

      <div class="cfg-label">{{ fileMeta || '尚未读取配置文件' }}</div>
      <el-input
        v-model="content"
        type="textarea"
        :rows="22"
        :disabled="!loaded"
        class="cfg-editor"
        spellcheck="false"
        placeholder="点击「读取配置」后可在此编辑 server.properties"
      />

      <div v-if="output" class="out-wrap">
        <div class="cfg-label">命令输出</div>
        <pre class="out">{{ output }}</pre>
      </div>
    </template>
  </div>
</template>

<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { kafkaApi } from '../api/kafka'

const route = useRoute()
const router = useRouter()
const connections = ref([])
const connId = ref('')
const content = ref('')
const loaded = ref(false)
const fileMeta = ref('')
const output = ref('')
const loadingCfg = ref(false)
const saving = ref(false)
const testing = ref(false)
const statusing = ref(false)
const restarting = ref(false)

const current = computed(() => connections.value.find((c) => c.id === connId.value) || null)
const admin = computed(() => current.value?.hostAdmin || {})
const hasSSH = computed(() => !!admin.value?.username)

const sshSummary = computed(() => {
  const a = admin.value
  if (!a?.username) return '-'
  const host = a.host || (current.value?.bootstrapServers || '').split(',')[0]?.split(':')[0] || ''
  const port = a.port || 22
  return `${a.username}@${host}:${port}`
})

async function loadList() {
  const res = await kafkaApi.listConnections()
  if (!res.ok) return ElMessage.error(res.error)
  connections.value = res.data || []
  const q = route.query.id
  if (q && connections.value.some((c) => c.id === q)) {
    connId.value = String(q)
  } else if (!connId.value && connections.value.length) {
    connId.value = connections.value[0].id
  }
}

function onPick() {
  loaded.value = false
  content.value = ''
  fileMeta.value = ''
  output.value = ''
}

function goEdit() {
  router.push('/connections')
}

async function testSSH() {
  testing.value = true
  try {
    const res = await kafkaApi.testSSH(connId.value)
    if (!res.ok) return ElMessage.error(res.error)
    ElMessage.success(res.data?.output || 'SSH 连通成功')
    output.value = res.data?.output || 'SSH 连通成功'
  } finally {
    testing.value = false
  }
}

async function loadConfig() {
  loadingCfg.value = true
  try {
    const res = await kafkaApi.readBrokerConfig(connId.value)
    if (!res.ok) return ElMessage.error(res.error)
    content.value = res.data?.content ?? ''
    loaded.value = true
    fileMeta.value = `${res.data?.path}  @  ${res.data?.host}`
    ElMessage.success('已读取配置文件')
  } finally {
    loadingCfg.value = false
  }
}

async function saveConfig() {
  saving.value = true
  try {
    const res = await kafkaApi.saveBrokerConfig(connId.value, content.value)
    if (!res.ok) return ElMessage.error(res.error)
    ElMessage.success('已保存（远程会先备份为 .bak.时间戳）')
  } finally {
    saving.value = false
  }
}

async function loadStatus() {
  statusing.value = true
  try {
    const res = await kafkaApi.kafkaServiceStatus(connId.value)
    if (!res.ok) return ElMessage.error(res.error)
    output.value = res.data?.output || ''
  } finally {
    statusing.value = false
  }
}

async function restart() {
  restarting.value = true
  try {
    const res = await kafkaApi.restartKafkaService(connId.value)
    if (!res.ok) return ElMessage.error(res.error)
    output.value = res.data?.output || '重启命令已执行'
    ElMessage.success('已执行重启命令')
  } finally {
    restarting.value = false
  }
}

watch(() => route.query.id, loadList)
onMounted(loadList)
</script>

<style scoped>
.cfg-label { color: #909399; font-size: 13px; margin: 8px 0; }
.cfg-editor :deep(textarea) {
  font-family: Consolas, "Courier New", monospace;
  font-size: 12px;
  line-height: 1.5;
}
.out-wrap { margin-top: 16px; max-width: 920px; }
.out {
  background: #1f2d3d;
  color: #d8dee9;
  border-radius: 6px;
  padding: 12px;
  white-space: pre-wrap;
  word-break: break-all;
  font-size: 12px;
  max-height: 240px;
  overflow: auto;
}
</style>
