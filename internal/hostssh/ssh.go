package hostssh

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
	"kafka-client-go/internal/model"
)

const maxConfigBytes = 2 * 1024 * 1024

// Resolved 把连接配置整理成可拨号的 SSH 参数。
type Resolved struct {
	Addr           string
	User           string
	Password       string
	KeyPath        string
	SkipHostKey    bool
	ConfigPath     string
	RestartCommand string
	StatusCommand  string
}

func Resolve(p model.ConnectionProfile) (*Resolved, error) {
	h := p.HostAdmin
	if h == nil || strings.TrimSpace(h.Username) == "" {
		return nil, fmt.Errorf("请先在连接配置中填写 SSH 用户名（集群运维）")
	}
	host := strings.TrimSpace(h.Host)
	if host == "" {
		host = HostFromBootstrap(p.BootstrapServers)
	}
	if host == "" {
		return nil, fmt.Errorf("无法确定 SSH 主机，请填写 Host")
	}
	port := h.Port
	if port <= 0 {
		port = 22
	}
	cfgPath := strings.TrimSpace(h.ConfigPath)
	if cfgPath == "" {
		cfgPath = "/opt/kafka/config/server.properties"
	}
	restart := strings.TrimSpace(h.RestartCommand)
	if restart == "" {
		restart = "sudo systemctl restart kafka"
	}
	status := strings.TrimSpace(h.StatusCommand)
	if status == "" {
		status = "systemctl is-active kafka; systemctl status kafka --no-pager -n 15"
	}
	if strings.TrimSpace(h.Password) == "" && strings.TrimSpace(h.KeyPath) == "" {
		return nil, fmt.Errorf("请填写 SSH 密码或私钥路径")
	}
	return &Resolved{
		Addr:           net.JoinHostPort(host, fmt.Sprintf("%d", port)),
		User:           strings.TrimSpace(h.Username),
		Password:       h.Password,
		KeyPath:        strings.TrimSpace(h.KeyPath),
		SkipHostKey:    h.SkipHostKey,
		ConfigPath:     cfgPath,
		RestartCommand: restart,
		StatusCommand:  status,
	}, nil
}

func HostFromBootstrap(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	first := strings.TrimSpace(strings.Split(s, ",")[0])
	host, _, err := net.SplitHostPort(first)
	if err != nil {
		return first
	}
	return host
}

func Quote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'"'"'`) + "'"
}

func Dial(r *Resolved) (*ssh.Client, error) {
	var auths []ssh.AuthMethod
	if r.KeyPath != "" {
		key, err := os.ReadFile(r.KeyPath)
		if err != nil {
			return nil, fmt.Errorf("读取 SSH 私钥失败: %w", err)
		}
		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			return nil, fmt.Errorf("解析 SSH 私钥失败: %w", err)
		}
		auths = append(auths, ssh.PublicKeys(signer))
	}
	if r.Password != "" {
		auths = append(auths, ssh.Password(r.Password))
	}
	if !r.SkipHostKey {
		return nil, fmt.Errorf("当前版本请勾选“跳过主机指纹”（仅建议内网使用）")
	}
	cfg := &ssh.ClientConfig{
		User:            r.User,
		Auth:            auths,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         12 * time.Second,
	}
	return ssh.Dial("tcp", r.Addr, cfg)
}

func ReadFile(r *Resolved, path string) (string, error) {
	cl, err := Dial(r)
	if err != nil {
		return "", err
	}
	defer cl.Close()
	out, errOut, err := run(cl, "cat -- "+Quote(path), 20*time.Second)
	if err != nil {
		if errOut != "" {
			return "", fmt.Errorf("%w: %s", err, strings.TrimSpace(errOut))
		}
		return "", err
	}
	if len(out) > maxConfigBytes {
		return "", fmt.Errorf("配置文件过大（>%d bytes）", maxConfigBytes)
	}
	return out, nil
}

func WriteFile(r *Resolved, path, content string) error {
	if len(content) > maxConfigBytes {
		return fmt.Errorf("配置文件过大（>%d bytes）", maxConfigBytes)
	}
	cl, err := Dial(r)
	if err != nil {
		return err
	}
	defer cl.Close()
	cmd := fmt.Sprintf("cp -p -- %s %s.bak.$(date +%%s) 2>/dev/null; cat > %s", Quote(path), Quote(path), Quote(path))
	sess, err := cl.NewSession()
	if err != nil {
		return err
	}
	defer sess.Close()
	sess.Stdin = strings.NewReader(content)
	var stderr bytes.Buffer
	sess.Stderr = &stderr
	if err := sess.Run(cmd); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return fmt.Errorf("%w: %s", err, msg)
		}
		return err
	}
	return nil
}

func Run(r *Resolved, cmd string, timeout time.Duration) (string, error) {
	cl, err := Dial(r)
	if err != nil {
		return "", err
	}
	defer cl.Close()
	out, errOut, err := run(cl, cmd, timeout)
	if err != nil {
		both := strings.TrimSpace(out + "\n" + errOut)
		if both != "" {
			return both, fmt.Errorf("%w: %s", err, both)
		}
		return "", err
	}
	if strings.TrimSpace(errOut) != "" {
		out = strings.TrimSpace(out + "\n" + errOut)
	}
	return out, nil
}

func run(cl *ssh.Client, cmd string, timeout time.Duration) (string, string, error) {
	sess, err := cl.NewSession()
	if err != nil {
		return "", "", err
	}
	defer sess.Close()
	var stdout, stderr bytes.Buffer
	sess.Stdout = &stdout
	sess.Stderr = &stderr
	done := make(chan error, 1)
	go func() { done <- sess.Run(cmd) }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return stdout.String(), stderr.String(), err
	case <-timer.C:
		_ = sess.Signal(ssh.SIGKILL)
		return stdout.String(), stderr.String(), fmt.Errorf("命令超时（%s）: %s", timeout, cmd)
	}
}
