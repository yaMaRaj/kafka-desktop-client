package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"kafka-client-go/internal/hostssh"
	"kafka-client-go/internal/kafka"
	"kafka-client-go/internal/model"
	"kafka-client-go/internal/store"

	"github.com/wailsapp/wails/v2/pkg/runtime"
)

// App Wails 绑定入口：前端通过 window.go.main.App.* 调用
type App struct {
	ctx     context.Context
	store   *store.Store
	manager *kafka.Manager
	active  string
}

func NewApp() *App {
	return &App{manager: kafka.NewManager()}
}

func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
	s, err := store.New()
	if err != nil {
		fmt.Println("store init error:", err)
		return
	}
	a.store = s
}

// domReady 将窗口限制在当前屏幕工作区内并重新居中，避免标题栏被顶出屏幕。
func (a *App) domReady(ctx context.Context) {
	screens, err := runtime.ScreenGetAll(ctx)
	if err != nil || len(screens) == 0 {
		runtime.WindowCenter(ctx)
		return
	}

	screen := screens[0]
	for _, s := range screens {
		if s.IsCurrent {
			screen = s
			break
		}
		if s.IsPrimary {
			screen = s
		}
	}

	// 预留任务栏与窗口边框空间，防止居中后标题栏超出可见区域
	maxW := screen.Size.Width - 48
	maxH := screen.Size.Height - 96
	if maxW < 800 {
		maxW = screen.Size.Width
	}
	if maxH < 500 {
		maxH = screen.Size.Height
	}

	w, h := runtime.WindowGetSize(ctx)
	nw, nh := w, h
	if nw > maxW {
		nw = maxW
	}
	if nh > maxH {
		nh = maxH
	}
	if nw != w || nh != h {
		runtime.WindowSetSize(ctx, nw, nh)
	}
	runtime.WindowCenter(ctx)
}

func (a *App) shutdown(ctx context.Context) {
	a.manager.CloseAll()
}

func (a *App) GetVersion() string { return "0.1.0" }

func (a *App) GetConfigPath() string {
	if a.store == nil {
		return ""
	}
	return a.store.Path()
}

func (a *App) ListConnections() model.Result {
	if a.store == nil {
		return model.Fail(fmt.Errorf("存储未初始化"))
	}
	return model.OK(a.store.ListConnections())
}

func (a *App) SaveConnection(p model.ConnectionProfile) model.Result {
	if a.store == nil {
		return model.Fail(fmt.Errorf("存储未初始化"))
	}
	isNew := p.ID == ""
	saved, err := a.store.SaveConnection(p)
	if err != nil {
		a.logOp(p.ID, p.Name, model.ActionSaveConnection, connDetail(p), err)
		return model.Fail(err)
	}
	a.manager.Disconnect(saved.ID)
	verb := "更新"
	if isNew {
		verb = "新增"
	}
	a.logOp(saved.ID, saved.Name, model.ActionSaveConnection, fmt.Sprintf("%s %s", verb, connDetail(saved)), nil)
	return model.OK(saved)
}

func (a *App) DeleteConnection(id string) model.Result {
	if a.store == nil {
		return model.Fail(fmt.Errorf("存储未初始化"))
	}
	name, detail := "", id
	if p := a.store.GetConnection(id); p != nil {
		name = p.Name
		detail = connDetail(*p)
	}
	a.manager.Disconnect(id)
	if a.active == id {
		a.active = ""
	}
	if err := a.store.DeleteConnection(id); err != nil {
		a.logOp(id, name, model.ActionDeleteConnection, detail, err)
		return model.Fail(err)
	}
	a.logOp(id, name, model.ActionDeleteConnection, detail, nil)
	return model.OK(true)
}

func (a *App) TestConnection(p model.ConnectionProfile) model.Result {
	ov, err := a.manager.Test(p)
	a.logOp(p.ID, p.Name, model.ActionTestConnection, connDetail(p), err)
	if err != nil {
		return model.Fail(err)
	}
	return model.OK(ov)
}

func (a *App) Connect(id string) model.Result {
	if a.store == nil {
		return model.Fail(fmt.Errorf("存储未初始化"))
	}
	p := a.store.GetConnection(id)
	if p == nil {
		return model.Fail(fmt.Errorf("连接不存在"))
	}
	c, err := a.manager.GetOrCreate(*p)
	if err != nil {
		a.logOp(id, p.Name, model.ActionConnect, p.BootstrapServers, err)
		return model.Fail(err)
	}
	ov, err := kafka.FetchOverview(a.ctx, c)
	if err != nil {
		// 建连成功但拿不到元数据时丢掉缓存，避免下次复用坏连接
		a.manager.Disconnect(id)
		a.logOp(id, p.Name, model.ActionConnect, p.BootstrapServers, err)
		return model.Fail(err)
	}
	a.active = id
	a.logOp(id, p.Name, model.ActionConnect, p.BootstrapServers, nil)
	return model.OK(ov)
}

func (a *App) Disconnect(id string) model.Result {
	name, detail := "", id
	if a.store != nil {
		if p := a.store.GetConnection(id); p != nil {
			name = p.Name
			detail = p.BootstrapServers
		}
	}
	a.manager.Disconnect(id)
	if a.active == id {
		a.active = ""
	}
	a.logOp(id, name, model.ActionDisconnect, detail, nil)
	return model.OK(true)
}

func (a *App) GetActiveConnectionID() string { return a.active }

func (a *App) client() (*kafka.Client, error) {
	if a.active == "" {
		return nil, fmt.Errorf("请先连接集群")
	}
	if a.store == nil {
		return nil, fmt.Errorf("存储未初始化")
	}
	p := a.store.GetConnection(a.active)
	if p == nil {
		a.active = ""
		return nil, fmt.Errorf("连接不存在")
	}
	return a.manager.GetOrCreate(*p)
}

func (a *App) GetOverview() model.Result {
	c, err := a.client()
	if err != nil {
		return model.Fail(err)
	}
	ov, err := kafka.FetchOverview(a.ctx, c)
	if err != nil {
		return model.Fail(err)
	}
	return model.OK(ov)
}

func (a *App) ListTopics() model.Result {
	c, err := a.client()
	if err != nil {
		return model.Fail(err)
	}
	list, err := kafka.ListTopics(a.ctx, c)
	if err != nil {
		return model.Fail(err)
	}
	return model.OK(list)
}

func (a *App) CreateTopic(p model.TopicCreateParams) model.Result {
	c, err := a.client()
	if err != nil {
		return model.Fail(err)
	}
	detail := fmt.Sprintf("%s（%d 分区 × %d 副本）", p.Name, p.NumPartitions, p.ReplicationFactor)
	if err := kafka.CreateTopic(a.ctx, c, p); err != nil {
		a.logOp(a.active, "", model.ActionCreateTopic, detail, err)
		return model.Fail(err)
	}
	a.logOp(a.active, "", model.ActionCreateTopic, detail, nil)
	return model.OK(true)
}

func (a *App) DeleteTopic(topic string) model.Result {
	c, err := a.client()
	if err != nil {
		return model.Fail(err)
	}
	if err := kafka.DeleteTopic(a.ctx, c, topic); err != nil {
		a.logOp(a.active, "", model.ActionDeleteTopic, topic, err)
		return model.Fail(err)
	}
	a.logOp(a.active, "", model.ActionDeleteTopic, topic, nil)
	return model.OK(true)
}

func (a *App) UpdatePartitions(topic string, total int32) model.Result {
	c, err := a.client()
	if err != nil {
		return model.Fail(err)
	}
	detail := fmt.Sprintf("%s → %d 分区", topic, total)
	if err := kafka.CreatePartitions(a.ctx, c, topic, total); err != nil {
		a.logOp(a.active, "", model.ActionUpdatePartitions, detail, err)
		return model.Fail(err)
	}
	a.logOp(a.active, "", model.ActionUpdatePartitions, detail, nil)
	return model.OK(true)
}

func (a *App) FetchMessages(p model.FetchMessagesParams) model.Result {
	c, err := a.client()
	if err != nil {
		return model.Fail(err)
	}
	list, err := kafka.FetchMessages(a.ctx, c, p)
	if err != nil {
		return model.Fail(err)
	}
	return model.OK(list)
}

func (a *App) ProduceMessage(p model.ProduceMessageParams) model.Result {
	c, err := a.client()
	if err != nil {
		return model.Fail(err)
	}
	r, err := kafka.ProduceMessage(a.ctx, c, p)
	detail := p.Topic
	if p.Key != "" {
		detail = fmt.Sprintf("%s（key=%s）", p.Topic, p.Key)
	}
	a.logOp(a.active, "", model.ActionProduceMessage, detail, err)
	if err != nil {
		return model.Fail(err)
	}
	return model.OK(r)
}

func (a *App) BulkProduce(p model.BulkProduceParams) model.Result {
	c, err := a.client()
	if err != nil {
		return model.Fail(err)
	}
	r, err := kafka.BulkProduce(a.ctx, c, p, func(prog model.BulkProduceProgress) {
		runtime.EventsEmit(a.ctx, "bulk-produce-progress", prog)
	})
	if r == nil {
		a.logOp(a.active, "", model.ActionBulkProduce, bulkDetail(p), err)
		return model.Fail(err)
	}
	detail := bulkDetail(p)
	if err != nil && r.Sent > 0 {
		detail = fmt.Sprintf("%s（已发送 %d 条）", detail, r.Sent)
	}
	a.logOp(a.active, "", model.ActionBulkProduce, detail, err)
	return model.OK(r)
}

func (a *App) ListConsumerGroups() model.Result {
	c, err := a.client()
	if err != nil {
		return model.Fail(err)
	}
	list, err := kafka.ListConsumerGroups(a.ctx, c)
	if err != nil {
		return model.Fail(err)
	}
	return model.OK(list)
}

func (a *App) DescribeConsumerGroup(groupID string) model.Result {
	c, err := a.client()
	if err != nil {
		return model.Fail(err)
	}
	info, err := kafka.DescribeConsumerGroup(a.ctx, c, groupID)
	if err != nil {
		return model.Fail(err)
	}
	return model.OK(info)
}

func (a *App) DeleteConsumerGroup(groupID string) model.Result {
	c, err := a.client()
	if err != nil {
		return model.Fail(err)
	}
	if err := kafka.DeleteConsumerGroup(a.ctx, c, groupID); err != nil {
		a.logOp(a.active, "", model.ActionDeleteGroup, groupID, err)
		return model.Fail(err)
	}
	a.logOp(a.active, "", model.ActionDeleteGroup, groupID, nil)
	return model.OK(true)
}

func (a *App) ListOperationLogs() model.Result {
	if a.store == nil {
		return model.Fail(fmt.Errorf("存储未初始化"))
	}
	return model.OK(a.store.ListLogs())
}

func (a *App) ClearOperationLogs() model.Result {
	if a.store == nil {
		return model.Fail(fmt.Errorf("存储未初始化"))
	}
	if err := a.store.ClearLogs(); err != nil {
		return model.Fail(err)
	}
	return model.OK(true)
}

func (a *App) hostProfile(id string) (*model.ConnectionProfile, *hostssh.Resolved, error) {
	if a.store == nil {
		return nil, nil, fmt.Errorf("存储未初始化")
	}
	if id == "" {
		id = a.active
	}
	if id == "" {
		return nil, nil, fmt.Errorf("请先选择连接")
	}
	p := a.store.GetConnection(id)
	if p == nil {
		return nil, nil, fmt.Errorf("连接不存在")
	}
	r, err := hostssh.Resolve(*p)
	if err != nil {
		return p, nil, err
	}
	return p, r, nil
}

func (a *App) TestSSH(id string) model.Result {
	p, r, err := a.hostProfile(id)
	if err != nil {
		a.logOp(id, "", model.ActionTestSSH, id, err)
		return model.Fail(err)
	}
	cl, err := hostssh.Dial(r)
	if err != nil {
		a.logOp(p.ID, p.Name, model.ActionTestSSH, r.Addr, err)
		return model.Fail(err)
	}
	_ = cl.Close()
	a.logOp(p.ID, p.Name, model.ActionTestSSH, r.Addr, nil)
	return model.OK(model.HostCommandResult{Host: r.Addr, Output: "SSH 连通成功"})
}

func (a *App) ReadBrokerConfig(id string) model.Result {
	p, r, err := a.hostProfile(id)
	if err != nil {
		a.logOp(id, "", model.ActionReadBrokerConfig, "", err)
		return model.Fail(err)
	}
	body, err := hostssh.ReadFile(r, r.ConfigPath)
	a.logOp(p.ID, p.Name, model.ActionReadBrokerConfig, r.ConfigPath+" @ "+r.Addr, err)
	if err != nil {
		return model.Fail(err)
	}
	return model.OK(model.BrokerConfigFile{Host: r.Addr, Path: r.ConfigPath, Content: body})
}

func (a *App) SaveBrokerConfig(id string, content string) model.Result {
	p, r, err := a.hostProfile(id)
	if err != nil {
		a.logOp(id, "", model.ActionSaveBrokerConfig, "", err)
		return model.Fail(err)
	}
	err = hostssh.WriteFile(r, r.ConfigPath, content)
	a.logOp(p.ID, p.Name, model.ActionSaveBrokerConfig, r.ConfigPath+" @ "+r.Addr, err)
	if err != nil {
		return model.Fail(err)
	}
	return model.OK(true)
}

func (a *App) KafkaServiceStatus(id string) model.Result {
	p, r, err := a.hostProfile(id)
	if err != nil {
		a.logOp(id, "", model.ActionKafkaStatus, "", err)
		return model.Fail(err)
	}
	out, err := hostssh.Run(r, r.StatusCommand, 25*time.Second)
	a.logOp(p.ID, p.Name, model.ActionKafkaStatus, r.Addr, err)
	if err != nil {
		return model.Fail(err)
	}
	return model.OK(model.HostCommandResult{Host: r.Addr, Command: r.StatusCommand, Output: out})
}

func (a *App) RestartKafkaService(id string) model.Result {
	p, r, err := a.hostProfile(id)
	if err != nil {
		a.logOp(id, "", model.ActionRestartKafka, "", err)
		return model.Fail(err)
	}
	out, err := hostssh.Run(r, r.RestartCommand, 60*time.Second)
	a.logOp(p.ID, p.Name, model.ActionRestartKafka, r.RestartCommand+" @ "+r.Addr, err)
	if err != nil {
		return model.Fail(err)
	}
	return model.OK(model.HostCommandResult{Host: r.Addr, Command: r.RestartCommand, Output: out})
}

// logOp 记录一条操作日志：connName 为目标连接名（谁），action 为动作代码，
// detail 为操作对象与参数（做了什么），err 非空即失败并记录原因（为什么失败）。
func (a *App) logOp(connID, connName, action, detail string, err error) {
	if a.store == nil {
		return
	}
	if connName == "" && connID != "" {
		if p := a.store.GetConnection(connID); p != nil {
			connName = p.Name
		}
	}
	e := model.OperationLogEntry{
		ConnectionID:   connID,
		ConnectionName: connName,
		Action:         action,
		Detail:         detail,
		Success:        err == nil,
	}
	if err != nil {
		e.Error = err.Error()
	}
	a.store.AppendLog(e)
}

// connDetail 连接日志详情：名称（地址）
func connDetail(p model.ConnectionProfile) string {
	return fmt.Sprintf("%s（%s）", p.Name, p.BootstrapServers)
}

// bulkDetail 批量造数日志详情：对象与参数
func bulkDetail(p model.BulkProduceParams) string {
	mode := fmt.Sprintf("%dB/条", p.RecordSize)
	if strings.TrimSpace(p.ValueTemplate) != "" {
		if p.RecordSize > 0 {
			mode = fmt.Sprintf("JSON模板，补齐至 %dB", p.RecordSize)
		} else {
			mode = "JSON模板"
		}
	}
	detail := fmt.Sprintf("%s × %d 条（%s", p.Topic, p.NumRecords, mode)
	if p.Throughput > 0 {
		detail += fmt.Sprintf("，限速 %d 条/秒", p.Throughput)
	}
	return detail + "）"
}
