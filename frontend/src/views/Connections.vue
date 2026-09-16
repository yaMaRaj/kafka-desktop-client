<template>
  <div class="page">
    <div class="page-header">
      <div>
        <div class="page-title">连接管理</div>
        <div class="page-sub">配置 PLAINTEXT / SASL / SSL 集群连接</div>
      </div>
      <el-button type="primary" @click="openEdit()">新建连接</el-button>
    </div>
    <el-table :data="list" stripe border>
      <el-table-column prop="name" label="名称" min-width="140" />
      <el-table-column prop="bootstrapServers" label="Bootstrap" min-width="220" />
      <el-table-column prop="securityProtocol" label="协议" width="140" />
      <el-table-column label="操作" width="340" fixed="right">
        <template #default="{ row }">
          <el-button link type="primary" @click="test(row)">测试</el-button>
          <el-button link type="success" @click="connect(row)">连接</el-button>
          <el-button link type="warning" @click="openOps(row)">运维</el-button>
          <el-button link @click="openEdit(row)">编辑</el-button>
          <el-popconfirm title="确认删除？" @confirm="remove(row.id)">
            <template #reference>
              <el-button link type="danger">删除</el-button>
            </template>
          </el-popconfirm>
        </template>
      </el-table-column>
    </el-table>

    <el-dialog v-model="visible" :title="form.id ? '编辑连接' : '新建连接'" width="680px">
      <el-form label-width="120px">
        <el-form-item label="名称" required>
          <el-input v-model="form.name" />
        </el-form-item>
        <el-form-item label="Bootstrap" required>
          <el-input v-model="form.bootstrapServers" placeholder="host1:9092,host2:9092" />
        </el-form-item>
        <el-form-item label="协议">
          <el-select v-model="form.securityProtocol" style="width: 100%">
            <el-option label="PLAINTEXT" value="PLAINTEXT" />
            <el-option label="SASL_PLAINTEXT" value="SASL_PLAINTEXT" />
            <el-option label="SASL_SSL" value="SASL_SSL" />
            <el-option label="SSL" value="SSL" />
          </el-select>
        </el-form-item>
        <el-form-item label="Client ID">
          <el-input v-model="form.clientId" placeholder="kafka-client-go" />
        </el-form-item>
        <template v-if="form.securityProtocol?.includes('SASL')">
          <el-form-item label="SASL 机制">
            <el-select v-model="form.sasl.mechanism" style="width: 100%">
              <el-option label="PLAIN" value="plain" />
              <el-option label="SCRAM-SHA-256" value="scram-sha-256" />
              <el-option label="SCRAM-SHA-512" value="scram-sha-512" />
            </el-select>
          </el-form-item>
          <el-form-item label="用户名">
            <el-input v-model="form.sasl.username" />
          </el-form-item>
          <el-form-item label="密码">
            <el-input v-model="form.sasl.password" type="password" show-password />
          </el-form-item>
        </template>
        <template v-if="form.securityProtocol?.includes('SSL')">
          <el-form-item label="CA 路径">
            <el-input v-model="form.ssl.caPath" />
          </el-form-item>
          <el-form-item label="Cert 路径">
            <el-input v-model="form.ssl.certPath" />
          </el-form-item>
          <el-form-item label="Key 路径">
            <el-input v-model="form.ssl.keyPath" />
          </el-form-item>
        </template>
        <el-collapse v-model="sshOpen" class="ssh-collapse">
          <el-collapse-item name="ssh">
            <template #title>
              <span class="ssh-title">SSH 运维（可选）</span>
              <span class="ssh-hint">远程配置 / 重启 Kafka</span>
            </template>
            <el-form-item label="SSH 主机">
              <el-input v-model="form.hostAdmin.host" placeholder="默认取 Bootstrap 主机" />
            </el-form-item>
            <el-form-item label="SSH 端口">
              <el-input-number v-model="form.hostAdmin.port" :min="1" :max="65535" />
            </el-form-item>
            <el-form-item label="SSH 用户">
              <el-input v-model="form.hostAdmin.username" placeholder="如 root / kafka" />
            </el-form-item>
            <el-form-item label="SSH 密码">
              <el-input v-model="form.hostAdmin.password" type="password" show-password />
            </el-form-item>
            <el-form-item label="私钥路径">
              <el-input v-model="form.hostAdmin.keyPath" placeholder="可选，本机私钥文件" />
            </el-form-item>
            <el-form-item label="配置文件">
              <el-input v-model="form.hostAdmin.configPath" placeholder="/opt/kafka/config/server.properties" />
            </el-form-item>
            <el-form-item label="重启命令">
              <el-input v-model="form.hostAdmin.restartCommand" placeholder="sudo systemctl restart kafka" />
            </el-form-item>
            <el-form-item label="状态命令">
              <el-input v-model="form.hostAdmin.statusCommand" placeholder="systemctl status kafka --no-pager -n 15" />
            </el-form-item>
            <el-form-item label="跳过指纹">
              <el-switch v-model="form.hostAdmin.skipHostKey" />
              <span style="margin-left: 8px; color: #909399">内网建议开启</span>
            </el-form-item>
          </el-collapse-item>
        </el-collapse>
      </el-form>
      <template #footer>
        <el-button @click="visible = false">取消</el-button>
        <el-button type="primary" :loading="saving" @click="save">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { inject, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { kafkaApi } from '../api/kafka'

const router = useRouter()
const appState = inject('appState')
const list = ref([])
const visible = ref(false)
const saving = ref(false)
const sshOpen = ref([]) // 默认折叠
const form = ref(emptyForm())

function emptyForm() {
  return {
    id: '',
    name: '',
    bootstrapServers: '',
    securityProtocol: 'PLAINTEXT',
    clientId: '',
    sasl: { mechanism: 'plain', username: '', password: '' },
    ssl: { caPath: '', certPath: '', keyPath: '' },
    hostAdmin: emptyHostAdmin(),
  }
}

function emptyHostAdmin() {
  return {
    host: '',
    port: 22,
    username: '',
    password: '',
    keyPath: '',
    skipHostKey: true,
    configPath: '/opt/kafka/config/server.properties',
    restartCommand: 'sudo systemctl restart kafka',
    statusCommand: 'systemctl is-active kafka; systemctl status kafka --no-pager -n 15',
  }
}

async function load() {
  const res = await kafkaApi.listConnections()
  if (!res.ok) return ElMessage.error(res.error)
  list.value = res.data || []
  await appState.refreshConnections()
}

function openEdit(row) {
  form.value = row
    ? JSON.parse(JSON.stringify({
        ...emptyForm(),
        ...row,
        sasl: row.sasl || emptyForm().sasl,
        ssl: row.ssl || emptyForm().ssl,
        hostAdmin: { ...emptyHostAdmin(), ...(row.hostAdmin || {}) },
      }))
    : emptyForm()
  sshOpen.value = [] // 每次打开弹窗默认折叠
  visible.value = true
}

async function save() {
  if (!form.value.name || !form.value.bootstrapServers) {
    return ElMessage.warning('请填写名称与 Bootstrap')
  }
  saving.value = true
  try {
    const payload = { ...form.value }
    if (!payload.securityProtocol.includes('SASL')) payload.sasl = null
    if (!payload.securityProtocol.includes('SSL')) payload.ssl = null
    if (!payload.hostAdmin?.username) payload.hostAdmin = null
    const res = await kafkaApi.saveConnection(payload)
    if (!res.ok) return ElMessage.error(res.error)
    ElMessage.success('已保存')
    visible.value = false
    await load()
  } finally {
    saving.value = false
  }
}

async function remove(id) {
  const res = await kafkaApi.deleteConnection(id)
  if (!res.ok) return ElMessage.error(res.error)
  appState.clearConnectionState?.(id)
  ElMessage.success('已删除')
  await load()
}

async function test(row) {
  const res = await kafkaApi.testConnection(row)
  if (!res.ok) return ElMessage.error(res.error)
  ElMessage.success(`连通成功，Brokers=${res.data?.brokers?.length || 0}`)
}

async function connect(row) {
  await appState.onConnect(row.id)
}

function openOps(row) {
  router.push({ path: '/ops', query: { id: row.id } })
}

onMounted(load)
</script>

<style scoped>
.ssh-collapse {
  border: none;
  margin-top: 4px;
}
.ssh-collapse :deep(.el-collapse-item__header) {
  height: 40px;
  line-height: 40px;
  border: none;
  background: #f5f7fa;
  border-radius: 6px;
  padding: 0 12px;
  font-weight: 600;
  color: #606266;
}
.ssh-collapse :deep(.el-collapse-item__wrap) {
  border: none;
}
.ssh-collapse :deep(.el-collapse-item__content) {
  padding: 12px 0 0;
}
.ssh-title { margin-right: 8px; }
.ssh-hint {
  font-weight: 400;
  font-size: 12px;
  color: #909399;
}
</style>
