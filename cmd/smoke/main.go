// Smoke test against the saved connection in %AppData%/kafka-client-go/config.json.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"kafka-client-go/internal/kafka"
	"kafka-client-go/internal/model"
)

type dataFile struct {
	Connections []model.ConnectionProfile `json:"connections"`
}

func main() {
	cfg := filepath.Join(must(os.UserConfigDir()), "kafka-client-go", "config.json")
	raw, err := os.ReadFile(cfg)
	if err != nil {
		fatal("read config: %v", err)
	}
	var df dataFile
	if err := json.Unmarshal(raw, &df); err != nil {
		fatal("parse config: %v", err)
	}
	if len(df.Connections) == 0 {
		fatal("no connections in %s", cfg)
	}
	p := df.Connections[0]
	fmt.Printf("== smoke: %s (%s)\n", p.Name, p.BootstrapServers)

	mgr := kafka.NewManager()
	defer mgr.CloseAll()
	ctx := context.Background()

	step("Test", func() error {
		ov, err := mgr.Test(p)
		if err != nil {
			return err
		}
		fmt.Printf("  brokers=%d controller=%d\n", len(ov.Brokers), ov.ControllerID)
		return nil
	})

	c, err := mgr.GetOrCreate(p)
	if err != nil {
		fatal("connect: %v", err)
	}

	topic := fmt.Sprintf("smoke-%d", time.Now().Unix())
	step("CreateTopic", func() error {
		return kafka.CreateTopic(ctx, c, model.TopicCreateParams{
			Name: topic, NumPartitions: 2, ReplicationFactor: 1,
		})
	})
	defer func() {
		_ = kafka.DeleteTopic(ctx, c, topic)
		fmt.Println("== cleanup topic:", topic)
	}()

	step("ListTopics", func() error {
		list, err := kafka.ListTopics(ctx, c)
		if err != nil {
			return err
		}
		found := false
		for _, t := range list {
			if t.Name == topic {
				found = true
				if len(t.Partitions) != 2 {
					return fmt.Errorf("want 2 partitions, got %d", len(t.Partitions))
				}
			}
		}
		if !found {
			return fmt.Errorf("topic %s not listed", topic)
		}
		fmt.Printf("  topics=%d\n", len(list))
		return nil
	})

	step("ProduceMessage", func() error {
		r, err := kafka.ProduceMessage(ctx, c, model.ProduceMessageParams{
			Topic: topic, Key: "k1", Value: `{"hello":"smoke"}`,
		})
		if err != nil {
			return err
		}
		fmt.Printf("  p=%d offset=%d\n", r.Partition, r.Offset)
		return nil
	})

	step("ProduceToPartition", func() error {
		part := int32(1)
		r, err := kafka.ProduceMessage(ctx, c, model.ProduceMessageParams{
			Topic: topic, Key: "k2", Value: "part1", Partition: &part,
		})
		if err != nil {
			return err
		}
		if r.Partition != 1 {
			return fmt.Errorf("want partition 1, got %d", r.Partition)
		}
		return nil
	})

	step("BulkProduce", func() error {
		r, err := kafka.BulkProduce(ctx, c, model.BulkProduceParams{
			Topic: topic, NumRecords: 20, RecordSize: 32, KeyPrefix: "b",
		}, nil)
		if err != nil {
			return err
		}
		if r.Sent != 20 {
			return fmt.Errorf("sent=%d want 20 failed=%d", r.Sent, r.Failed)
		}
		fmt.Printf("  sent=%d rps=%.1f\n", r.Sent, r.RecordsPerSec)
		return nil
	})

	step("TopicStats", func() error {
		list, err := kafka.ListTopics(ctx, c)
		if err != nil {
			return err
		}
		for _, t := range list {
			if t.Name != topic {
				continue
			}
			fmt.Printf("  records=%d size=%d rf=%d\n", t.RecordCount, t.SizeBytes, t.ReplicationFactor)
			if t.RecordCount < 22 {
				return fmt.Errorf("recordCount=%d want >= 22", t.RecordCount)
			}
			if t.ReplicationFactor < 1 {
				return fmt.Errorf("replicationFactor=%d", t.ReplicationFactor)
			}
			return nil
		}
		return fmt.Errorf("topic %s not listed", topic)
	})

	step("UpdatePartitions", func() error {
		return kafka.CreatePartitions(ctx, c, topic, 4)
	})

	step("FetchMessagesLimit", func() error {
		msgs, err := kafka.FetchMessages(ctx, c, model.FetchMessagesParams{
			Topic: topic, Limit: 5,
		})
		if err != nil {
			return err
		}
		fmt.Printf("  got=%d (limit=5)\n", len(msgs))
		if len(msgs) == 0 {
			return fmt.Errorf("expected some messages")
		}
		if len(msgs) > 5 {
			return fmt.Errorf("limit exceeded: got %d > 5", len(msgs))
		}
		return nil
	})

	step("ListConsumerGroups", func() error {
		gs, err := kafka.ListConsumerGroups(ctx, c)
		if err != nil {
			return err
		}
		fmt.Printf("  groups=%d\n", len(gs))
		return nil
	})

	step("FetchOverview", func() error {
		ov, err := kafka.FetchOverview(ctx, c)
		if err != nil {
			return err
		}
		if len(ov.Brokers) == 0 {
			return fmt.Errorf("no brokers")
		}
		return nil
	})

	fmt.Println("== ALL PASSED")
}

func step(name string, fn func() error) {
	fmt.Printf("-- %s\n", name)
	if err := fn(); err != nil {
		fatal("%s failed: %v", name, err)
	}
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "FAIL: "+format+"\n", args...)
	os.Exit(1)
}

func must[T any](v T, err error) T {
	if err != nil {
		fatal("%v", err)
	}
	return v
}
