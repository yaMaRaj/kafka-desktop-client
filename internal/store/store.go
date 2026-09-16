package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"kafka-client-go/internal/model"
)

const appDirName = "kafka-client-go"
const configFile = "config.json"

type dataFile struct {
	Connections   []model.ConnectionProfile  `json:"connections"`
	OperationLogs []model.OperationLogEntry  `json:"operationLogs"`
}

// Store 本地 JSON 配置与操作日志
type Store struct {
	mu   sync.Mutex
	path string
	data dataFile
}

func New() (*Store, error) {
	cfg, err := os.UserConfigDir()
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(cfg, appDirName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{path: filepath.Join(dir, configFile)}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			s.data = dataFile{Connections: []model.ConnectionProfile{}, OperationLogs: []model.OperationLogEntry{}}
			return nil
		}
		return err
	}
	if len(b) == 0 {
		s.data = dataFile{Connections: []model.ConnectionProfile{}, OperationLogs: []model.OperationLogEntry{}}
		return nil
	}
	return json.Unmarshal(b, &s.data)
}

func (s *Store) saveLocked() error {
	b, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, b, 0o600)
}

func (s *Store) ListConnections() []model.ConnectionProfile {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.ConnectionProfile, len(s.data.Connections))
	copy(out, s.data.Connections)
	return out
}

func (s *Store) GetConnection(id string) *model.ConnectionProfile {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.data.Connections {
		if s.data.Connections[i].ID == id {
			c := s.data.Connections[i]
			return &c
		}
	}
	return nil
}

func (s *Store) SaveConnection(p model.ConnectionProfile) (model.ConnectionProfile, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixMilli()
	if p.ID == "" {
		p.ID = uuid.NewString()
		p.CreatedAt = now
	}
	p.UpdatedAt = now
	found := false
	for i := range s.data.Connections {
		if s.data.Connections[i].ID == p.ID {
			if p.CreatedAt == 0 {
				p.CreatedAt = s.data.Connections[i].CreatedAt
			}
			s.data.Connections[i] = p
			found = true
			break
		}
	}
	if !found {
		if p.CreatedAt == 0 {
			p.CreatedAt = now
		}
		s.data.Connections = append(s.data.Connections, p)
	}
	return p, s.saveLocked()
}

func (s *Store) DeleteConnection(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.data.Connections[:0]
	for _, c := range s.data.Connections {
		if c.ID != id {
			next = append(next, c)
		}
	}
	s.data.Connections = next
	return s.saveLocked()
}

func (s *Store) AppendLog(entry model.OperationLogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry.ID == "" {
		entry.ID = uuid.NewString()
	}
	if entry.At == 0 {
		entry.At = time.Now().UnixMilli()
	}
	s.data.OperationLogs = append([]model.OperationLogEntry{entry}, s.data.OperationLogs...)
	if len(s.data.OperationLogs) > 500 {
		s.data.OperationLogs = s.data.OperationLogs[:500]
	}
	_ = s.saveLocked()
}

func (s *Store) ListLogs() []model.OperationLogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.OperationLogEntry, len(s.data.OperationLogs))
	copy(out, s.data.OperationLogs)
	return out
}

func (s *Store) ClearLogs() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.OperationLogs = nil
	return s.saveLocked()
}

func (s *Store) Path() string {
	return s.path
}
