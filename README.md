# Kafka Client Go

基于 **Go + Wails + Vue 3** 的 Kafka 桌面客户端（`kafka-desktop-client` 的 Go 重构版）。

- **后端**：Go（`franz-go` 直连 Kafka，业务逻辑全在 Go）
- **前端**：Vue 3 + Element Plus（无 TypeScript，便于维护）
- **桌面壳**：Wails v2（比 Electron 更轻）

## 与旧项目对比

| | kafka-desktop-client (TS) | kafka-client-go |
|--|--|--|
| 语言 | Electron + React + TS | Go + Vue 3 |
| Kafka 客户端 | KafkaJS | franz-go |
| 批量造数 | 可调官方脚本或自研 | **Go 直连自研，无需本机装 Kafka** |
| 配置存储 | electron-store | `%AppData%/kafka-client-go/config.json` |

## 已实现功能（v0.1）

- 连接管理：PLAINTEXT / SASL / SSL，测试连接
- 集群概览、Topic 列表/创建/删除/扩分区
- 消息浏览、单条生产
- **批量造数 / 轻量压测**（吞吐、延迟统计）
- 消费组列表、Lag、删除组
- 本地操作日志

## 开发

前置：Go 1.22+、Node 18+、[Wails CLI](https://wails.io)

```bash
# 安装 Wails（如未安装）
go install github.com/wailsapp/wails/v2/cmd/wails@latest

cd kafka-client-go
wails dev
```

仅前端：

```bash
cd frontend
npm install
npm run dev
```

## 打包

```bash
wails build
```

产物在 `build/bin/`。

## 目录结构

```
kafka-client-go/
├── app.go                 # Wails 绑定（前端可调用的 Go 方法）
├── main.go                # 窗口入口
├── internal/
│   ├── model/             # 共享数据结构
│   ├── store/             # 本地 JSON 配置与日志
│   └── kafka/             # 连接、Admin、消息、消费组、压测
├── frontend/              # Vue3 + Element Plus
│   └── src/
│       ├── api/kafka.js   # 调用 Go 绑定
│       ├── views/         # 页面
│       └── App.vue
└── docs/PROJECT.md        # 架构与扩展说明
```

## 扩展建议

新增功能优先写在 `internal/kafka/`，再在 `app.go` 暴露方法，最后加 Vue 页面。  
运行 `wails generate module` 可刷新 `frontend/wailsjs` 绑定。
