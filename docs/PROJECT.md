# Kafka Client Go — 架构说明

## 技术选型

| 层 | 选型 | 说明 |
|----|------|------|
| 桌面壳 | Wails v2 | Go 编译为原生窗口，内嵌 WebView |
| 后端 | Go + franz-go | 直连 Broker，Admin 用 `kadm` |
| 前端 | Vue 3 + Element Plus | 中文 UI，**不使用 TypeScript** |
| 存储 | 用户配置目录 JSON | Windows: `%AppData%\kafka-client-go\` |

## 调用链

```
Vue 页面 → frontend/src/api/kafka.js → window.go.main.App.Xxx
       → app.go → internal/kafka/* → Kafka 集群
```

统一返回：`{ ok: true, data } | { ok: false, error }`（`model.Result`）。

## 包职责

| 包 | 职责 |
|----|------|
| `internal/model` | DTO / Result |
| `internal/store` | 连接配置、操作日志持久化 |
| `internal/kafka` | 建连、Topic、消息、消费组、BulkProduce |
| `app.go` | 对外 API，薄封装 |

## 批量造数（无需本机 Kafka）

`internal/kafka/perf.go` 的 `BulkProduce`：

- 使用已连接的 Producer 循环发送
- 支持条数、消息大小、限速（throughput）
- 返回 sent/failed、RPS、平均延迟

这替代了「调用官方 `kafka-producer-perf-test` 脚本」的路径，用户只需能连上集群。

## 新增功能步骤

1. 在 `internal/model` 加参数/结果类型
2. 在 `internal/kafka` 实现逻辑
3. 在 `app.go` 增加导出方法（首字母大写）
4. `wails generate module`（可选）
5. 在 `frontend/src/api/kafka.js` 与 `views/` 接 UI

## 后续可迭代

- 消费组偏移重置、消息搜索 / Tail
- Schema Registry
- ACL / 分区重分配
- 异步压测 + 进度事件（Wails Events）
