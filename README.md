# Kafka Desktop

跨 Windows / macOS 的 Kafka 桌面客户端：连接集群、浏览/生产消息、管理 Topic 与消费组偏移，支持 Schema Registry、消息搜索与实时 Tail。

## 功能

- 多集群连接：PLAINTEXT / SASL_PLAINTEXT / SASL_SSL / SSL（证书路径配置）
- Topic：创建、删除、配置修改、扩分区、分区/副本/ISR 详情
- 消息浏览（分区 / offset / 时间）与生产（Headers、批量、Schema 编码）
- Consumer Group：成员、lag、重置偏移（earliest / latest / 指定 offset / 时间戳）
- Schema Registry：Subject 列表与 schema 查看、编解码
- 消息内容搜索（关键字 / 正则，限扫描窗口）
- 实时 Tail 消费（过滤、暂停）
- 中文界面，危险操作二次确认，本地操作日志

## 开发

```bash
npm install
npm run dev
```

开发时 Vite 会同时拉起 Electron 窗口。

## 打包

```bash
# Windows（NSIS 安装包 + 便携版）
npm run dist:win

# macOS（需在 macOS 上执行）
npm run dist:mac
```

产物位于 `release/`：

- `Kafka Desktop-Setup-1.0.0.exe` — Windows 安装包
- `Kafka Desktop-Portable-1.0.0.exe` — Windows 便携版
- macOS dmg（在 Mac 上构建）

若 Electron 下载较慢，可设置镜像：

```bash
# PowerShell
$env:ELECTRON_MIRROR="https://npmmirror.com/mirrors/electron/"
$env:ELECTRON_BUILDER_BINARIES_MIRROR="https://npmmirror.com/mirrors/electron-builder-binaries/"
```

## 使用提示

1. 打开「连接管理」新建连接并「测试连接」
2. 右上角选择连接后即可使用 Topics / 消息 / 消费组等功能
3. Schema Registry 在连接配置中填写 URL；未配置时仍可按文本/JSON 工作
4. 重置消费组偏移前请先停止该组的活跃消费者
