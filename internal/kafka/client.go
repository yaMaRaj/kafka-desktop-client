package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"kafka-client-go/internal/model"
)

// Manager 按 connectionId 管理 Kafka 客户端
type Manager struct {
	mu      sync.Mutex
	clients map[string]*Client
}

type Client struct {
	Profile model.ConnectionProfile
	Raw     *kgo.Client
	Admin   *kadm.Client
}

func NewManager() *Manager {
	return &Manager{clients: make(map[string]*Client)}
}

func (m *Manager) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, c := range m.clients {
		c.Raw.Close()
		delete(m.clients, id)
	}
}

func (m *Manager) Disconnect(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.clients[id]; ok {
		c.Raw.Close()
		delete(m.clients, id)
	}
}

func (m *Manager) GetOrCreate(profile model.ConnectionProfile) (*Client, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.clients[profile.ID]; ok {
		return c, nil
	}
	c, err := dial(profile)
	if err != nil {
		return nil, err
	}
	m.clients[profile.ID] = c
	return c, nil
}

func (m *Manager) Test(profile model.ConnectionProfile) (*model.ClusterOverview, error) {
	c, err := dial(profile)
	if err != nil {
		return nil, err
	}
	defer c.Raw.Close()
	return FetchOverview(context.Background(), c)
}

func dial(profile model.ConnectionProfile) (*Client, error) {
	opts, err := buildOpts(profile)
	if err != nil {
		return nil, err
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("无法连接集群: %w", err)
	}

	return &Client{Profile: profile, Raw: cl, Admin: kadm.NewClient(cl)}, nil
}

// buildOpts 根据连接配置生成 kgo 选项（供临时 Consumer / 压测复用）
func buildOpts(profile model.ConnectionProfile) ([]kgo.Opt, error) {
	brokers := splitBrokers(profile.BootstrapServers)
	if len(brokers) == 0 {
		return nil, fmt.Errorf("请填写 Bootstrap Servers")
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(firstNonEmpty(profile.ClientID, "kafka-client-go")),
		kgo.RequestTimeoutOverhead(10 * time.Second),
		kgo.DialTimeout(10 * time.Second),
	}

	switch profile.SecurityProtocol {
	case model.ProtocolSSL, model.ProtocolSASLSSL:
		tlsCfg, err := buildTLS(profile.SSL)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	switch profile.SecurityProtocol {
	case model.ProtocolSASLPlaintext, model.ProtocolSASLSSL:
		if profile.SASL == nil || profile.SASL.Username == "" {
			return nil, fmt.Errorf("SASL 需要填写用户名")
		}
		mech := profile.SASL.Mechanism
		user, pass := profile.SASL.Username, profile.SASL.Password
		switch mech {
		case model.SaslScramSHA256:
			opts = append(opts, kgo.SASL(scram.Auth{User: user, Pass: pass}.AsSha256Mechanism()))
		case model.SaslScramSHA512:
			opts = append(opts, kgo.SASL(scram.Auth{User: user, Pass: pass}.AsSha512Mechanism()))
		default:
			opts = append(opts, kgo.SASL(plain.Auth{User: user, Pass: pass}.AsMechanism()))
		}
	}
	return opts, nil
}

func buildTLS(cfg *model.SslConfig) (*tls.Config, error) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg != nil && cfg.RejectUnauthorized != nil {
		tlsCfg.InsecureSkipVerify = !*cfg.RejectUnauthorized
	}
	if cfg == nil {
		return tlsCfg, nil
	}
	if cfg.CAPath != "" {
		pem, err := os.ReadFile(cfg.CAPath)
		if err != nil {
			return nil, fmt.Errorf("读取 CA 失败: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("无效的 CA 证书: %s", cfg.CAPath)
		}
		tlsCfg.RootCAs = pool
	}
	if cfg.CertPath != "" && cfg.KeyPath != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertPath, cfg.KeyPath)
		if err != nil {
			return nil, fmt.Errorf("加载客户端证书失败: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return tlsCfg, nil
}

func splitBrokers(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
