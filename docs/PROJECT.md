# Kafka Desktop — 项目结构与维护说明

面向后续维护与功能迭代的技术说明。日常使用说明见根目录 [README.md](../README.md)。

---

## 1. 技术栈

| 层级 | 技术 |
|------|------|
| 桌面壳 | Electron 34 |
| UI | React 18 + TypeScript + Ant Design 5 + React Router（HashRouter） |
| 构建 | Vite 6 + vite-plugin-electron |
| Kafka | KafkaJS 2.x |
| Schema Registry | `@kafkajs/confluent-schema-registry` |
| 本地存储 | electron-store（连接配置 + 操作日志） |
| 密钥保护 | Electron `safeStorage`（不可用时退化为 Base64 `plain:` 前缀） |
| 状态 | Zustand（仅渲染进程全局连接态） |

---

## 2. 总体架构

```
┌─────────────────────────────────────────────────────────────┐
│  Renderer (React)                                            │
│  src/features/*  →  window.kafkaApi  →  preload bridge       │
└────────────────────────────┬────────────────────────────────┘
                             │ IPC (invoke / on)
┌────────────────────────────▼────────────────────────────────┐
│  Main Process (Electron)                                     │
│  electron/ipc/*  →  electron/services/*  →  KafkaJS / SR     │
│                   →  electron/store/configStore              │
└─────────────────────────────────────────────────────────────┘
```

**原则：**

1. **渲染进程不直连 Kafka**。所有集群操作经 `preload` 暴露的 `window.kafkaApi`，由主进程执行。
2. **IPC 统一返回** `IpcResult<T>`：`{ ok: true, data }` / `{ ok: false, error }`。
3. **共享类型**放在 `shared/types.ts`，主进程与渲染进程共用，避免两边漂移。
4. **按连接 ID 缓存** Kafka Admin / Producer（`kafkaClient.ts`）；Consumer 多用于临时拉取、搜索、Tail，用完断开。

---

## 3. 目录结构

```
kafka-desktop-client/
├── docs/
│   └── PROJECT.md              # 本文件：结构与维护说明
├── electron/                   # 主进程源码
│   ├── main.ts                 # 窗口创建、注册全部 IPC、退出时断开客户端
│   ├── preload.ts              # contextBridge：暴露 kafkaApi
│   ├── ipc/                    # IPC 注册（薄封装：参数校验 → 调 service → ok/fail）
│   │   ├── connections.ts      # 连接 CRUD / 测试 / 连接断开
│   │   ├── admin.ts            # 集群概览、Topic CRUD、配置、扩分区、offsets
│   │   ├── messages.ts         # 消息拉取 / 生产
│   │   ├── consumerGroups.ts   # 消费组列表、详情、重置偏移、删除
│   │   ├── schema.ts           # Schema Registry Subject
│   │   ├── searchTail.ts       # 搜索（可取消）+ Tail（推送事件）
│   │   ├── logs.ts             # 操作日志列表 / 清空
│   │   └── export.ts           # 另存为文本文件（系统对话框）
│   ├── services/               # 业务逻辑（KafkaJS 调用）
│   │   ├── kafkaClient.ts      # 配置构建、客户端池、加解密后的连接配置
│   │   ├── adminService.ts     # Topic / 集群元数据
│   │   ├── messageService.ts   # 浏览拉取、生产、消息视图转换
│   │   ├── consumerGroupService.ts
│   │   ├── schemaService.ts    # Registry 客户端缓存、编解码
│   │   ├── searchTailService.ts# 限窗扫描搜索、实时 Tail Consumer
│   │   └── utils.ts            # ok/fail、错误中文化、Buffer 展示
│   └── store/
│       └── configStore.ts      # electron-store：连接与日志；密钥加解密
├── shared/
│   └── types.ts                # 跨进程共享类型与 IpcResult
├── src/                        # 渲染进程
│   ├── main.tsx                # React 入口（Ant Design 中文 locale）
│   ├── styles.css              # 全局布局与页面样式
│   ├── vite-env.d.ts           # window.kafkaApi 类型声明
│   ├── app/
│   │   └── App.tsx             # 侧栏布局、路由、顶栏连接选择
│   ├── stores/
│   │   └── appStore.ts         # 当前连接 / 概览 / loading
│   ├── shared/                 # 渲染侧复用组件与工具
│   │   ├── PageShell.tsx       # 页面标题区 + 表格滚动高度
│   │   ├── MessageDetailDrawer.tsx  # 消息详情抽屉
│   │   └── exportMessages.ts   # 导出 JSONL（调用 export:saveTextFile）
│   └── features/               # 按业务域拆分页面
│       ├── clusters/           # 连接管理、集群概览、操作日志
│       ├── topics/             # Topic 列表与详情
│       ├── messages/           # 浏览、生产、搜索、Tail
│       ├── consumer-groups/
│       └── schema-registry/
├── public/                     # 静态资源（开发期）
├── index.html
├── vite.config.ts              # 别名 @ / @shared；Electron 双进程构建
├── package.json                # 脚本与 electron-builder 配置
├── LICENSE
└── README.md
```

构建产物（不入库）：

- `dist/` — 渲染进程打包
- `dist-electron/` — main / preload 打包
- `release/` — 安装包 / 便携版
- `node_modules/`

用户数据（不入库，本机路径）：

| 平台 | 路径 |
|------|------|
| Windows | `%APPDATA%\kafka-desktop\`（如 `kafka-desktop.json`） |
| macOS | `~/Library/Application Support/kafka-desktop/` |

---

## 4. 数据流与 IPC 约定

### 4.1 通道命名

`域:动作`，例如：

- `connections:list` / `connections:save` / `connections:connect`
- `admin:listTopics` / `messages:fetch` / `groups:resetOffsets`
- `search:run` / `tail:start`
- 推送事件：`search:progress`、`tail:message`、`tail:error`

### 4.2 典型调用链

**消息浏览：**

`MessagesPage` → `kafkaApi.fetchMessages` → `messages:fetch` → `messageService.fetchMessages` → 临时 Consumer 按 offset/时间拉取 → `KafkaMessageView[]`

**重置偏移：**

`ConsumerGroupsPage` → `kafkaApi.resetOffsets` → `groups:resetOffsets` → `consumerGroupService.resetOffsets` → KafkaJS `admin.setOffsets`  
注意：组状态必须为 Empty（无活跃成员），否则失败。Stable 表示仍有消费者在跑。

**实时 Tail：**

`tail:start` 返回 `tailId`；主进程 `webContents.send('tail:message')`；页面用 `onTailMessage` 订阅，卸载时 `stopTail` 并取消监听。

### 4.3 新增 IPC 的标准步骤

1. 在 `shared/types.ts` 补充参数/返回类型（如需）。
2. 在 `electron/services/` 实现业务（尽量纯函数式、可单测思路）。
3. 在对应 `electron/ipc/*.ts` 注册 `ipcMain.handle`，统一 `ok` / `fail`。
4. 在 `electron/preload.ts` 与 `src/vite-env.d.ts` **同步**增加 `kafkaApi` 方法。
5. 在 `src/features/...` 调用；危险操作加 `Modal` / `Popconfirm` 确认。
6. 写操作建议 `appendOperationLog`（见 `configStore`）。

---

## 5. 核心模块说明

### 5.1 `kafkaClient.ts`

- `buildKafkaConfig`：Bootstrap、SSL 证书文件、SASL 机制。
- `getOrCreateClient`：按 `connectionId` 复用 Admin + Producer。
- `createTransientClient`：测试连接等一次性场景。
- 退出应用时 `closeAllClients`。

### 5.2 `configStore.ts`

- 持久化连接列表与最近 500 条操作日志。
- 密码字段存盘前加密：`enc:`（safeStorage）或 `plain:`（降级）。
- 使用前 `unlockProfile` 解密；**切勿把解密后的配置写回磁盘或打进日志**。

### 5.3 `messageService.ts`

- 按分区/offset/时间戳拉取；可选 Schema 解码。
- 生产支持 Headers、分区指定、Schema 编码（Subject）。

### 5.4 `searchTailService.ts`

- 搜索：限 `maxScan`（上限 20000），可取消。
- Tail：独立 Consumer，支持关键字/正则过滤；注意勿与业务消费组共用会冲突的 `groupId` 策略（实现内为临时 group）。

### 5.5 `schemaService.ts`

- 按连接缓存 Registry 客户端；连接变更时 `clearSchemaRegistry`。
- `decodeMaybe` / `encodeWithSubject` 供消息读写复用。

### 5.6 渲染层

- `App.tsx`：侧栏路由；除「连接管理」外需已连接。
- `appStore`：只持有连接级全局状态；页面内列表数据用本地 `useState`。
- `PageShell`：统一页头与表格可视区高度。

---

## 6. 开发与打包

```bash
npm install
npm run dev          # Vite + Electron 开发
npm run build        # 类型检查 + 打包渲染/主进程
npm run dist:win     # Windows：NSIS Setup + Portable
npm run dist:mac     # 需在 macOS 上执行
```

路径别名（`vite.config.ts` / tsconfig）：

- `@` → `src/`
- `@shared` → `shared/`

国内镜像可参考 README 中的 `ELECTRON_MIRROR` 设置。

---

## 7. 功能迭代建议

| 方向 | 建议切入点 | 注意 |
|------|------------|------|
| 新运维能力（ACL、配额等） | `adminService` + `ipc/admin` + 新 feature 页 | 权限错误已在 `friendlyKafkaError` 映射，可继续扩充 |
| 消息格式（Protobuf/自定义） | `schemaService` / `messageService.toMessageView` | 保持 `KafkaMessageView` 稳定，扩展可选字段 |
| 多窗口 / 多连接并行 | `kafkaClient` 池已按 connectionId 隔离；UI 需支持多 active | Tail/搜索的 Map 需按连接清理 |
| 导入/导出连接配置 | `configStore` + 新 IPC | 导出时勿明文写密码，或二次加密 |
| 主题/英文 UI | `main.tsx` ConfigProvider、文案抽 i18n | 路由与 IPC 无需改动 |
| 云厂商 IAM | 新 `SecurityProtocol` + `buildKafkaConfig` | 当前明确不做；扩展时改共享类型 |

**改动时优先保持：**

1. IPC 返回形状不变（或做兼容）。
2. `shared/types.ts` 为契约中心。
3. preload 与 `vite-env.d.ts` 同步。
4. 危险写操作：确认框 + 操作日志。

---

## 8. 已知约束与坑

1. **重置消费组偏移**：组须无活跃成员（Empty）。Stable 会失败（Kafka / KafkaJS 限制）。
2. **域名 Bootstrap**：支持；还需集群 `advertised.listeners` 对客户端可达，SSL 证书域名匹配。
3. **macOS 包**：须在 macOS 上 `dist:mac`；Windows 上无法打出可用 dmg。
4. **Windows 签名**：`signAndEditExecutable: false`，避免本机符号链接权限问题。
5. **Tail / 搜索**：大流量时注意内存；Tail 页已去掉导出以减轻压力，导出走浏览/搜索页。

---

## 9. 快速定位表

| 要改的行为 | 优先打开的文件 |
|------------|----------------|
| 侧栏菜单 / 路由 | `src/app/App.tsx` |
| 连接表单字段 | `ConnectionsPage.tsx` + `shared/types.ts` ConnectionProfile |
| Topic 管理 | `adminService.ts` + `TopicsPage.tsx` / `TopicDetailPage.tsx` |
| 消息表格列 / 详情 | `MessagesPage.tsx` + `MessageDetailDrawer.tsx` |
| 消费组重置策略 | `consumerGroupService.ts` + `ConsumerGroupsPage.tsx` |
| 错误文案 | `electron/services/utils.ts` → `friendlyKafkaError` |
| 安装包名称 | `package.json` → `build.nsis` / `build.portable` |

---

## 10. 版本与协议

- 应用版本：`package.json` → `version`
- 开源协议：根目录 `LICENSE`（MIT）
