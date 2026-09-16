package kafka

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"kafka-client-go/internal/model"
)

func FetchOverview(ctx context.Context, c *Client) (*model.ClusterOverview, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	meta, err := c.Admin.BrokerMetadata(ctx)
	if err != nil {
		return nil, err
	}
	ov := &model.ClusterOverview{
		ClusterID:    meta.Cluster,
		ControllerID: meta.Controller,
		Brokers:      make([]model.BrokerInfo, 0, len(meta.Brokers)),
	}
	for _, b := range meta.Brokers {
		rack := ""
		if b.Rack != nil {
			rack = *b.Rack
		}
		ov.Brokers = append(ov.Brokers, model.BrokerInfo{
			NodeID: b.NodeID,
			Host:   b.Host,
			Port:   b.Port,
			Rack:   rack,
		})
	}
	sort.Slice(ov.Brokers, func(i, j int) bool { return ov.Brokers[i].NodeID < ov.Brokers[j].NodeID })
	return ov, nil
}

func ListTopics(ctx context.Context, c *Client) ([]model.TopicInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()

	td, err := c.Admin.ListTopicsWithInternal(ctx)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(td))
	for name := range td {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]model.TopicInfo, 0, len(names))
	for _, name := range names {
		d := td[name]
		if d.Err != nil {
			continue
		}
		info := model.TopicInfo{
			Name:       name,
			IsInternal: d.IsInternal,
			Partitions: make([]model.PartitionInfo, 0, len(d.Partitions)),
		}
		rf := 0
		for _, p := range d.Partitions.Sorted() {
			if n := len(p.Replicas); n > rf {
				rf = n
			}
			if p.Leader < 0 {
				info.OfflinePartitions++
			}
			if len(p.ISR) < len(p.Replicas) {
				info.UnderReplicated++
			}
			info.Partitions = append(info.Partitions, model.PartitionInfo{
				PartitionID:     p.Partition,
				Leader:          p.Leader,
				Replicas:        append([]int32(nil), p.Replicas...),
				ISR:             append([]int32(nil), p.ISR...),
				OfflineReplicas: append([]int32(nil), p.OfflineReplicas...),
			})
		}
		info.ReplicationFactor = rf
		out = append(out, info)
	}

	enrichTopicOffsets(ctx, c, out)
	enrichTopicSizes(ctx, c, out)
	enrichTopicConfigs(ctx, c, names, out)
	return out, nil
}

func enrichTopicOffsets(ctx context.Context, c *Client, topics []model.TopicInfo) {
	if len(topics) == 0 {
		return
	}
	names := make([]string, len(topics))
	for i, t := range topics {
		names[i] = t.Name
	}
	startOffsets, err := c.Admin.ListStartOffsets(ctx, names...)
	if err != nil && len(startOffsets) == 0 {
		return
	}
	endOffsets, err := c.Admin.ListEndOffsets(ctx, names...)
	if err != nil && len(endOffsets) == 0 {
		return
	}
	for i := range topics {
		t := &topics[i]
		var rec int64
		for j := range t.Partitions {
			p := &t.Partitions[j]
			st, ok1 := startOffsets.Lookup(t.Name, p.PartitionID)
			en, ok2 := endOffsets.Lookup(t.Name, p.PartitionID)
			if !ok1 || !ok2 || st.Err != nil || en.Err != nil {
				continue
			}
			p.StartOffset = st.Offset
			p.EndOffset = en.Offset
			if en.Offset > st.Offset && st.Offset >= 0 {
				p.Records = en.Offset - st.Offset
			}
			rec += p.Records
		}
		t.RecordCount = rec
	}
}

func enrichTopicSizes(ctx context.Context, c *Client, topics []model.TopicInfo) {
	if len(topics) == 0 {
		return
	}
	dirs, err := c.Admin.DescribeAllLogDirs(ctx, nil)
	if err != nil && len(dirs) == 0 {
		return
	}
	type tp struct {
		t string
		p int32
	}
	sizes := make(map[tp]int64)
	dirs.Each(func(d kadm.DescribedLogDir) {
		if d.Err != nil {
			return
		}
		d.Topics.Each(func(part kadm.DescribedLogDirPartition) {
			if part.IsFuture {
				return
			}
			sizes[tp{part.Topic, part.Partition}] += part.Size
		})
	})
	for i := range topics {
		t := &topics[i]
		var tot int64
		for j := range t.Partitions {
			p := &t.Partitions[j]
			sz := sizes[tp{t.Name, p.PartitionID}]
			p.SizeBytes = sz
			tot += sz
		}
		t.SizeBytes = tot
	}
}

func enrichTopicConfigs(ctx context.Context, c *Client, names []string, topics []model.TopicInfo) {
	if len(names) == 0 {
		return
	}
	cfgs, err := c.Admin.DescribeTopicConfigs(ctx, names...)
	if err != nil && len(cfgs) == 0 {
		return
	}
	byName := make(map[string]kadm.ResourceConfig, len(cfgs))
	for _, rc := range cfgs {
		byName[rc.Name] = rc
	}
	for i := range topics {
		rc, ok := byName[topics[i].Name]
		if !ok || rc.Err != nil {
			continue
		}
		for _, cfg := range rc.Configs {
			v := cfg.MaybeValue()
			switch cfg.Key {
			case "cleanup.policy":
				topics[i].CleanupPolicy = v
			case "retention.ms":
				topics[i].RetentionMs = v
			case "retention.bytes":
				topics[i].RetentionBytes = v
			case "min.insync.replicas":
				topics[i].MinISR = v
			case "max.message.bytes":
				topics[i].MaxMessageBytes = v
			}
		}
	}
}

func CreateTopic(ctx context.Context, c *Client, p model.TopicCreateParams) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if p.Name == "" {
		return fmt.Errorf("Topic 名称不能为空")
	}
	if p.NumPartitions <= 0 {
		p.NumPartitions = 1
	}
	if p.ReplicationFactor <= 0 {
		p.ReplicationFactor = 1
	}
	resp, err := c.Admin.CreateTopics(ctx, p.NumPartitions, p.ReplicationFactor, nil, p.Name)
	if err != nil {
		return err
	}
	for _, r := range resp {
		if r.Err != nil {
			return r.Err
		}
	}
	return nil
}

func DeleteTopic(ctx context.Context, c *Client, topic string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := c.Admin.DeleteTopics(ctx, topic)
	if err != nil {
		return err
	}
	for _, r := range resp {
		if r.Err != nil {
			return r.Err
		}
	}
	return nil
}

// CreatePartitions 将分区数设置为 total（需 >= 当前分区数）
func CreatePartitions(ctx context.Context, c *Client, topic string, total int32) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := c.Admin.UpdatePartitions(ctx, int(total), topic)
	if err != nil {
		return err
	}
	for _, r := range resp {
		if r.Err != nil {
			return r.Err
		}
	}
	return nil
}
