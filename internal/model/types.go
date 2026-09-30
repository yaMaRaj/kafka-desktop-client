package model

// SecurityProtocol 连接安全协议
type SecurityProtocol string

const (
	ProtocolPlaintext     SecurityProtocol = "PLAINTEXT"
	ProtocolSASLPlaintext SecurityProtocol = "SASL_PLAINTEXT"
	ProtocolSASLSSL       SecurityProtocol = "SASL_SSL"
	ProtocolSSL           SecurityProtocol = "SSL"
)

// SaslMechanism SASL 机制
type SaslMechanism string

const (
	SaslPlain       SaslMechanism = "plain"
	SaslScramSHA256 SaslMechanism = "scram-sha-256"
	SaslScramSHA512 SaslMechanism = "scram-sha-512"
)

// ConnectionProfile 集群连接配置（持久化到本地）
type ConnectionProfile struct {
	ID                string           `json:"id"`
	Name              string           `json:"name"`
	BootstrapServers  string           `json:"bootstrapServers"`
	SecurityProtocol  SecurityProtocol `json:"securityProtocol"`
	ClientID          string           `json:"clientId,omitempty"`
	SASL              *SaslConfig      `json:"sasl,omitempty"`
	SSL               *SslConfig       `json:"ssl,omitempty"`
	SchemaRegistryURL string           `json:"schemaRegistryUrl,omitempty"`
	HostAdmin         *HostAdminConfig `json:"hostAdmin,omitempty"`
	CreatedAt         int64            `json:"createdAt"`
	UpdatedAt         int64            `json:"updatedAt"`
}

// HostAdminConfig 通过 SSH 管理 Broker 主机上的 Kafka 配置与服务
type HostAdminConfig struct {
	Host           string `json:"host,omitempty"`
	Port           int    `json:"port,omitempty"`
	Username       string `json:"username"`
	Password       string `json:"password,omitempty"`
	KeyPath        string `json:"keyPath,omitempty"`
	SkipHostKey    bool   `json:"skipHostKey"`
	ConfigPath     string `json:"configPath"`
	RestartCommand string `json:"restartCommand"`
	StatusCommand  string `json:"statusCommand,omitempty"`
}

type BrokerConfigFile struct {
	Host    string `json:"host"`
	Path    string `json:"path"`
	Content string `json:"content"`
}

type HostCommandResult struct {
	Host    string `json:"host"`
	Command string `json:"command"`
	Output  string `json:"output"`
}

type SaslConfig struct {
	Mechanism SaslMechanism `json:"mechanism"`
	Username  string        `json:"username"`
	Password  string        `json:"password"`
}

type SslConfig struct {
	CAPath             string `json:"caPath,omitempty"`
	CertPath           string `json:"certPath,omitempty"`
	KeyPath            string `json:"keyPath,omitempty"`
	Passphrase         string `json:"passphrase,omitempty"`
	RejectUnauthorized *bool  `json:"rejectUnauthorized,omitempty"`
}

type BrokerInfo struct {
	NodeID int32  `json:"nodeId"`
	Host   string `json:"host"`
	Port   int32  `json:"port"`
	Rack   string `json:"rack,omitempty"`
}

type ClusterOverview struct {
	ClusterID    string       `json:"clusterId,omitempty"`
	ControllerID int32        `json:"controllerId,omitempty"`
	Brokers      []BrokerInfo `json:"brokers"`
}

type PartitionInfo struct {
	PartitionID     int32   `json:"partitionId"`
	Leader          int32   `json:"leader"`
	Replicas        []int32 `json:"replicas"`
	ISR             []int32 `json:"isr"`
	OfflineReplicas []int32 `json:"offlineReplicas,omitempty"`
	StartOffset     int64   `json:"startOffset"`
	EndOffset       int64   `json:"endOffset"`
	Records         int64   `json:"records"`
	SizeBytes       int64   `json:"sizeBytes"`
}

type TopicInfo struct {
	Name              string          `json:"name"`
	Partitions        []PartitionInfo `json:"partitions"`
	IsInternal        bool            `json:"isInternal"`
	ReplicationFactor int             `json:"replicationFactor"`
	RecordCount       int64           `json:"recordCount"`
	SizeBytes         int64           `json:"sizeBytes"` // 所有副本磁盘占用
	UnderReplicated   int             `json:"underReplicated"`
	OfflinePartitions int             `json:"offlinePartitions"`
	CleanupPolicy     string          `json:"cleanupPolicy,omitempty"`
	RetentionMs       string          `json:"retentionMs,omitempty"`
	RetentionBytes    string          `json:"retentionBytes,omitempty"`
	MinISR            string          `json:"minIsr,omitempty"`
	MaxMessageBytes   string          `json:"maxMessageBytes,omitempty"`
}

type TopicCreateParams struct {
	Name              string `json:"name"`
	NumPartitions     int32  `json:"numPartitions"`
	ReplicationFactor int16  `json:"replicationFactor"`
}

type KafkaHeader struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type KafkaMessageView struct {
	Topic     string        `json:"topic"`
	Partition int32         `json:"partition"`
	Offset    int64         `json:"offset"`
	Timestamp int64         `json:"timestamp"`
	Key       *string       `json:"key"`
	Value     *string       `json:"value"`
	Headers   []KafkaHeader `json:"headers"`
}

type FetchMessagesParams struct {
	Topic         string `json:"topic"`
	Partition     *int32 `json:"partition,omitempty"`
	FromOffset    *int64 `json:"fromOffset,omitempty"`
	FromTimestamp *int64 `json:"fromTimestamp,omitempty"`
	Limit         int    `json:"limit,omitempty"`
}

type ProduceMessageParams struct {
	Topic     string `json:"topic"`
	Key       string `json:"key,omitempty"`
	Value     string `json:"value"`
	Partition *int32 `json:"partition,omitempty"`
}

type ProduceResult struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

// BulkProduceParams 批量造数 / 轻量压测（无需本机安装 Kafka）
type BulkProduceParams struct {
	Topic         string `json:"topic"`
	NumRecords    int    `json:"numRecords"`
	RecordSize    int    `json:"recordSize"`              // >0 时：无模板按固定字节填充；有模板则不足时用随机文本补齐
	Throughput    int    `json:"throughput"`              // 每秒条数，0 或 -1 表示不限速
	KeyPrefix     string `json:"keyPrefix,omitempty"`
	ValuePrefix   string `json:"valuePrefix,omitempty"`   // 仅无模板时生效，写入固定 payload 前缀
	ValueTemplate string `json:"valueTemplate,omitempty"` // JSON/文本模板，支持 {{seq}} {{uuid}} 等占位符
}

type BulkProduceResult struct {
	Sent          int     `json:"sent"`
	Failed        int     `json:"failed"`
	ElapsedMs     int64   `json:"elapsedMs"`
	RecordsPerSec float64 `json:"recordsPerSec"`
	AvgLatencyMs  float64 `json:"avgLatencyMs"`
}

// BulkProduceProgress 批量造数过程进度（通过 Wails 事件推到前端）
type BulkProduceProgress struct {
	Phase         string  `json:"phase"` // start | progress | done
	Topic         string  `json:"topic"`
	Processed     int     `json:"processed"`
	Sent          int     `json:"sent"`
	Failed        int     `json:"failed"`
	Total         int     `json:"total"`
	ElapsedMs     int64   `json:"elapsedMs"`
	RecordsPerSec float64 `json:"recordsPerSec"`
	Message       string  `json:"message"`
}

type ConsumerGroupSummary struct {
	GroupID string `json:"groupId"`
	State   string `json:"state,omitempty"`
}

type ConsumerGroupOffset struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Lag       int64  `json:"lag"`
}

type ConsumerGroupInfo struct {
	GroupID  string                `json:"groupId"`
	State    string                `json:"state"`
	Members  int                   `json:"members"`
	Offsets  []ConsumerGroupOffset `json:"offsets"`
	TotalLag int64                 `json:"totalLag"`
}

type OperationLogEntry struct {
	ID             string `json:"id"`
	At             int64  `json:"at"`
	ConnectionID   string `json:"connectionId"`
	ConnectionName string `json:"connectionName,omitempty"` // 记录时冗余连接名，连接删除后日志仍可读
	Action         string `json:"action"`
	Detail         string `json:"detail"`
	Success        bool   `json:"success"`
	Error          string `json:"error,omitempty"`
}

// 操作日志动作代码（前端 ACTION_META 映射为中文标签）
const (
	ActionConnect          = "connect"
	ActionDisconnect       = "disconnect"
	ActionTestConnection   = "testConnection"
	ActionSaveConnection   = "saveConnection"
	ActionDeleteConnection = "deleteConnection"
	ActionCreateTopic      = "createTopic"
	ActionDeleteTopic      = "deleteTopic"
	ActionUpdatePartitions = "updatePartitions"
	ActionProduceMessage   = "produceMessage"
	ActionBulkProduce      = "bulkProduce"
	ActionDeleteGroup      = "deleteGroup"
	ActionReadBrokerConfig = "readBrokerConfig"
	ActionSaveBrokerConfig = "saveBrokerConfig"
	ActionRestartKafka     = "restartKafka"
	ActionKafkaStatus      = "kafkaStatus"
	ActionTestSSH          = "testSSH"
)

// Result 统一前端返回（Wails 友好，避免泛型绑定问题）
type Result struct {
	OK    bool        `json:"ok"`
	Data  interface{} `json:"data,omitempty"`
	Error string      `json:"error,omitempty"`
}

func OK(data interface{}) Result {
	return Result{OK: true, Data: data}
}

func Fail(err error) Result {
	msg := "未知错误"
	if err != nil {
		msg = err.Error()
	}
	return Result{OK: false, Error: msg}
}
