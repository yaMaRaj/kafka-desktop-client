//go:build integration

package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"kafka-client-go/internal/model"
)

func TestAppAgainstSavedConnection(t *testing.T) {
	app := NewApp()
	app.startup(context.Background())
	if app.store == nil {
		t.Fatal("store nil")
	}

	topic := fmt.Sprintf("app-topic-%d", time.Now().Unix())
	list := app.ListConnections()
	if !list.OK {
		t.Fatalf("ListConnections: %s", list.Error)
	}
	conns, _ := list.Data.([]model.ConnectionProfile)
	if len(conns) == 0 {
		t.Skip("no saved connections")
	}
	conn := conns[0]
	t.Logf("conn=%s brokers=%s path=%s", conn.Name, conn.BootstrapServers, app.store.Path())

	step := func(name string, r model.Result) model.Result {
		t.Helper()
		t.Logf("%s => ok=%v err=%q", name, r.OK, r.Error)
		return assertOK(t, name, r)
	}

	step("TestConnection", app.TestConnection(conn))
	step("Connect", app.Connect(conn.ID))
	if app.GetActiveConnectionID() != conn.ID {
		t.Fatal("active id mismatch")
	}
	step("GetOverview", app.GetOverview())
	step("ListTopics", app.ListTopics())
	step("CreateTopic", app.CreateTopic(model.TopicCreateParams{
		Name: topic, NumPartitions: 1, ReplicationFactor: 1,
	}))
	t.Cleanup(func() { _ = app.DeleteTopic(topic) })

	step("ProduceMessage", app.ProduceMessage(model.ProduceMessageParams{
		Topic: topic, Key: "a", Value: "hello",
	}))
	step("BulkProduce", app.BulkProduce(model.BulkProduceParams{
		Topic: topic, NumRecords: 5, RecordSize: 16,
	}))
	fetch := step("FetchMessages", app.FetchMessages(model.FetchMessagesParams{
		Topic: topic, Limit: 10,
	}))
	msgs, ok := fetch.Data.([]model.KafkaMessageView)
	t.Logf("fetch typeAssert=%v len=%d dataType=%T", ok, len(msgs), fetch.Data)
	if len(msgs) == 0 {
		t.Fatal("expected messages")
	}
	step("ListConsumerGroups", app.ListConsumerGroups())
	step("ListOperationLogs", app.ListOperationLogs())
	step("DeleteTopic", app.DeleteTopic(topic))
	step("Disconnect", app.Disconnect(conn.ID))

	fail := app.ListTopics()
	if fail.OK {
		t.Fatal("ListTopics should fail when disconnected")
	}
}

func assertOK(t *testing.T, name string, r model.Result) model.Result {
	t.Helper()
	if !r.OK {
		t.Fatalf("%s failed: %s", name, r.Error)
	}
	return r
}
