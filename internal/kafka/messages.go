package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"kafka-client-go/internal/model"
)

func FetchMessages(ctx context.Context, c *Client, p model.FetchMessagesParams) ([]model.KafkaMessageView, error) {
	limit := p.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 5000 {
		limit = 5000
	}
	if p.Topic == "" {
		return nil, fmt.Errorf("请指定 Topic")
	}

	partOffsets, err := resolveConsumeOffsets(ctx, c, p.Topic, p.Partition, p.FromOffset, p.FromTimestamp)
	if err != nil {
		return nil, err
	}
	if len(partOffsets) == 0 {
		return nil, nil
	}

	need := limit
	base, err := buildOpts(c.Profile)
	if err != nil {
		return nil, err
	}
	opts := append(base,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{p.Topic: partOffsets}),
		kgo.FetchMaxBytes(8*1024*1024),
		kgo.ClientID(firstNonEmpty(c.Profile.ClientID, "kafka-client-go")+"-fetch"),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	out := make([]model.KafkaMessageView, 0, need)
	for len(out) < need {
		pollCtx, cancelPoll := ctx, func() {}
		if len(out) > 0 {
			pollCtx, cancelPoll = context.WithTimeout(ctx, 5*time.Second)
		}
		fetches := cl.PollFetches(pollCtx)
		cancelPoll()

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == nil || errors.Is(e.Err, context.Canceled) || errors.Is(e.Err, context.DeadlineExceeded) {
					continue
				}
				return out, e.Err
			}
			if len(out) > 0 || ctx.Err() != nil {
				break
			}
			continue
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if len(out) >= need {
				return
			}
			out = append(out, recordToView(r))
		})
	}
	return out, nil
}

func ProduceMessage(ctx context.Context, c *Client, p model.ProduceMessageParams) (*model.ProduceResult, error) {
	if p.Topic == "" {
		return nil, fmt.Errorf("请指定 Topic")
	}
	rec := &kgo.Record{Topic: p.Topic, Value: []byte(p.Value)}
	if p.Key != "" {
		rec.Key = []byte(p.Key)
	}

	cl := c.Raw
	if p.Partition != nil {
		base, err := buildOpts(c.Profile)
		if err != nil {
			return nil, err
		}
		opts := append(base, kgo.RecordPartitioner(kgo.ManualPartitioner()))
		tmp, err := kgo.NewClient(opts...)
		if err != nil {
			return nil, err
		}
		defer tmp.Close()
		cl = tmp
		rec.Partition = *p.Partition
	}

	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	res := cl.ProduceSync(ctx, rec)
	r, err := res.First()
	if err != nil {
		return nil, err
	}
	return &model.ProduceResult{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset}, nil
}

func recordToView(r *kgo.Record) model.KafkaMessageView {
	var key, val *string
	if r.Key != nil {
		s := string(r.Key)
		key = &s
	}
	if r.Value != nil {
		s := string(r.Value)
		val = &s
	}
	headers := make([]model.KafkaHeader, 0, len(r.Headers))
	for _, h := range r.Headers {
		headers = append(headers, model.KafkaHeader{Key: h.Key, Value: string(h.Value)})
	}
	return model.KafkaMessageView{
		Topic:     r.Topic,
		Partition: r.Partition,
		Offset:    r.Offset,
		Timestamp: r.Timestamp.UnixMilli(),
		Key:       key,
		Value:     val,
		Headers:   headers,
	}
}
