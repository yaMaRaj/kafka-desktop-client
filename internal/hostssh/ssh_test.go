package hostssh

import (
	"testing"

	"kafka-client-go/internal/model"
)

func TestHostFromBootstrap(t *testing.T) {
	if g := HostFromBootstrap("172.20.73.42:9505"); g != "172.20.73.42" {
		t.Fatalf("got %q", g)
	}
	if g := HostFromBootstrap("a:9092,b:9092"); g != "a" {
		t.Fatalf("got %q", g)
	}
}

func TestQuote(t *testing.T) {
	if g := Quote(`/opt/kafka/config/server.properties`); g != `'/opt/kafka/config/server.properties'` {
		t.Fatalf("got %s", g)
	}
	if g := Quote(`it's`); g != `'it'"'"'s'` {
		t.Fatalf("got %s", g)
	}
}

func TestResolveRequiresSSH(t *testing.T) {
	_, err := Resolve(model.ConnectionProfile{BootstrapServers: "127.0.0.1:9092"})
	if err == nil {
		t.Fatal("expected error")
	}
}
