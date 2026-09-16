package kafka

import (
	"context"
	"fmt"
	"sort"
	"time"

	"kafka-client-go/internal/model"
)

func ListConsumerGroups(ctx context.Context, c *Client) ([]model.ConsumerGroupSummary, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	groups, err := c.Admin.ListGroups(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]model.ConsumerGroupSummary, 0, len(groups))
	for _, g := range groups.Sorted() {
		out = append(out, model.ConsumerGroupSummary{GroupID: g.Group, State: g.State})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GroupID < out[j].GroupID })
	return out, nil
}

func DescribeConsumerGroup(ctx context.Context, c *Client, groupID string) (*model.ConsumerGroupInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if groupID == "" {
		return nil, fmt.Errorf("请指定消费组")
	}

	lags, err := c.Admin.Lag(ctx, groupID)
	if err != nil {
		return nil, err
	}
	gl, ok := lags[groupID]
	if !ok {
		return nil, fmt.Errorf("消费组不存在或无法描述: %s", groupID)
	}
	if err := gl.Error(); err != nil {
		return nil, err
	}

	info := &model.ConsumerGroupInfo{
		GroupID:  groupID,
		State:    gl.State,
		Members:  len(gl.Members),
		Offsets:  make([]model.ConsumerGroupOffset, 0),
		TotalLag: 0,
	}
	for _, byPart := range gl.Lag {
		for _, pl := range byPart {
			lag := pl.Lag
			if lag < 0 {
				lag = 0
			}
			info.Offsets = append(info.Offsets, model.ConsumerGroupOffset{
				Topic:     pl.Topic,
				Partition: pl.Partition,
				Offset:    pl.Commit.At,
				Lag:       lag,
			})
			info.TotalLag += lag
		}
	}
	sort.Slice(info.Offsets, func(i, j int) bool {
		if info.Offsets[i].Topic == info.Offsets[j].Topic {
			return info.Offsets[i].Partition < info.Offsets[j].Partition
		}
		return info.Offsets[i].Topic < info.Offsets[j].Topic
	})
	return info, nil
}

func DeleteConsumerGroup(ctx context.Context, c *Client, groupID string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := c.Admin.DeleteGroup(ctx, groupID)
	if err != nil {
		return err
	}
	return resp.Err
}
