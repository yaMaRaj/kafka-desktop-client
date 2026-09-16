package store

import (
	"os"
	"path/filepath"
	"testing"

	"kafka-client-go/internal/model"
)

func TestStoreCRUDAndLogs(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("APPDATA", dir) // Windows UserConfigDir uses APPDATA
	// Also cover non-Windows: UserConfigDir may use HOME/XDG — force via rewriting New is hard,
	// so construct store against a custom path by mimicking New.
	s := &Store{path: filepath.Join(dir, "kafka-client-go", "config.json")}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		t.Fatal(err)
	}
	s.data.Connections = []model.ConnectionProfile{}
	s.data.OperationLogs = nil

	p, err := s.SaveConnection(model.ConnectionProfile{
		Name:             "本地",
		BootstrapServers: "127.0.0.1:9092",
		SecurityProtocol: model.ProtocolPlaintext,
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.ID == "" || p.CreatedAt == 0 {
		t.Fatalf("expected id/createdAt, got %+v", p)
	}
	got := s.GetConnection(p.ID)
	if got == nil || got.Name != "本地" {
		t.Fatalf("get: %+v", got)
	}

	p.BootstrapServers = "127.0.0.1:9093"
	saved, err := s.SaveConnection(p)
	if err != nil {
		t.Fatal(err)
	}
	if saved.CreatedAt != p.CreatedAt {
		t.Fatalf("createdAt changed on update")
	}
	if len(s.ListConnections()) != 1 {
		t.Fatalf("want 1 connection")
	}

	s.AppendLog(model.OperationLogEntry{
		ConnectionID: p.ID, ConnectionName: p.Name,
		Action: model.ActionConnect, Detail: "x", Success: true,
	})
	if len(s.ListLogs()) != 1 {
		t.Fatal("want 1 log")
	}

	if err := s.DeleteConnection(p.ID); err != nil {
		t.Fatal(err)
	}
	if s.GetConnection(p.ID) != nil || len(s.ListConnections()) != 0 {
		t.Fatal("delete failed")
	}
	if err := s.ClearLogs(); err != nil {
		t.Fatal(err)
	}
	if len(s.ListLogs()) != 0 {
		t.Fatal("clear failed")
	}

	// reload from disk
	s2 := &Store{path: s.path}
	if err := s2.load(); err != nil {
		t.Fatal(err)
	}
	if len(s2.ListConnections()) != 0 {
		t.Fatal("reloaded unexpected connections")
	}
}
