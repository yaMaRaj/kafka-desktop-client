import { createRouter, createWebHashHistory } from 'vue-router'
import Connections from '../views/Connections.vue'
import Overview from '../views/Overview.vue'
import Topics from '../views/Topics.vue'
import Messages from '../views/Messages.vue'
import Produce from '../views/Produce.vue'
import BulkProduce from '../views/BulkProduce.vue'
import Groups from '../views/Groups.vue'
import Logs from '../views/Logs.vue'
import Ops from '../views/Ops.vue'

const routes = [
  { path: '/', redirect: '/connections' },
  { path: '/connections', component: Connections, meta: { title: '连接管理' } },
  { path: '/overview', component: Overview, meta: { title: '集群概览', needConn: true } },
  { path: '/topics', component: Topics, meta: { title: 'Topics', needConn: true } },
  { path: '/messages', component: Messages, meta: { title: '消息浏览', needConn: true } },
  { path: '/produce', component: Produce, meta: { title: '消息生产', needConn: true } },
  { path: '/bulk', component: BulkProduce, meta: { title: '批量造数', needConn: true } },
  { path: '/groups', component: Groups, meta: { title: '消费组', needConn: true } },
  { path: '/ops', component: Ops, meta: { title: '集群运维' } },
  { path: '/logs', component: Logs, meta: { title: '操作日志' } },
]

export default createRouter({
  history: createWebHashHistory(),
  routes,
})
