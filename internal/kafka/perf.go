package kafka

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"kafka-client-go/internal/model"
)

// BulkProgressEvery 每处理这么多条推一次进度日志（首尾另报）
const BulkProgressEvery = 1000

const maxBulkValueBytes = 1024 * 1024

// BulkProduce 批量造数 / 轻量压测（Go 直连集群，无需本机安装 Kafka 发行版）
func BulkProduce(ctx context.Context, c *Client, p model.BulkProduceParams, onProgress func(model.BulkProduceProgress)) (*model.BulkProduceResult, error) {
	if p.Topic == "" {
		return nil, fmt.Errorf("请指定 Topic")
	}
	if p.NumRecords <= 0 {
		return nil, fmt.Errorf("条数必须大于 0")
	}
	if p.NumRecords > 5_000_000 {
		return nil, fmt.Errorf("单次最多 500 万条，请分批执行")
	}

	tpl := strings.TrimSpace(p.ValueTemplate)
	useTemplate := tpl != ""
	if useTemplate {
		if len(tpl) > maxBulkValueBytes {
			return nil, fmt.Errorf("消息模板不能超过 1MB")
		}
	}
	if p.RecordSize < 0 {
		return nil, fmt.Errorf("单条大小不能为负数")
	}
	if p.RecordSize > maxBulkValueBytes {
		return nil, fmt.Errorf("单条大小不能超过 1MB")
	}
	// 无模板且未指定大小时，退回旧行为：固定字节填充
	if !useTemplate {
		if p.RecordSize <= 0 {
			p.RecordSize = 100
		}
	}

	// 无模板时：预先生成固定 payload（兼容旧行为）
	var fixedPayload []byte
	if !useTemplate {
		fixedPayload = bytes.Repeat([]byte("x"), p.RecordSize)
		if p.ValuePrefix != "" {
			prefix := []byte(p.ValuePrefix)
			if len(prefix) >= p.RecordSize {
				fixedPayload = prefix[:p.RecordSize]
			} else {
				copy(fixedPayload, prefix)
			}
		}
	}

	start := time.Now()
	var totalLatency time.Duration
	sent, failed := 0, 0

	report := func(phase string) {
		if onProgress == nil {
			return
		}
		processed := sent + failed
		elapsed := time.Since(start)
		rps := 0.0
		if elapsed.Seconds() > 0 {
			rps = float64(sent) / elapsed.Seconds()
		}
		onProgress(model.BulkProduceProgress{
			Phase:         phase,
			Topic:         p.Topic,
			Processed:     processed,
			Sent:          sent,
			Failed:        failed,
			Total:         p.NumRecords,
			ElapsedMs:     elapsed.Milliseconds(),
			RecordsPerSec: rps,
			Message:       formatBulkProgress(phase, p.Topic, processed, sent, failed, p.NumRecords, elapsed, rps),
		})
	}

	report("start")

	var interval time.Duration
	if p.Throughput > 0 {
		interval = time.Second / time.Duration(p.Throughput)
	}

	for i := 0; i < p.NumRecords; i++ {
		select {
		case <-ctx.Done():
			report("done")
			return summarize(sent, failed, start, totalLatency), ctx.Err()
		default:
		}

		var payload []byte
		if useTemplate {
			payload = renderBulkValue(tpl, i)
			if p.RecordSize > 0 {
				payload = padBulkValueToSize(payload, p.RecordSize)
			}
			if len(payload) > maxBulkValueBytes {
				return nil, fmt.Errorf("渲染后的消息超过 1MB（第 %d 条）", i)
			}
		} else {
			payload = fixedPayload
		}

		rec := &kgo.Record{Topic: p.Topic, Value: payload}
		if p.KeyPrefix != "" {
			rec.Key = []byte(fmt.Sprintf("%s-%d", p.KeyPrefix, i))
		}

		t0 := time.Now()
		r := c.Raw.ProduceSync(ctx, rec)
		lat := time.Since(t0)
		totalLatency += lat
		if err := r.FirstErr(); err != nil {
			failed++
		} else {
			sent++
		}

		if interval > 0 {
			elapsed := time.Since(t0)
			if elapsed < interval {
				time.Sleep(interval - elapsed)
			}
		}

		processed := sent + failed
		if processed%BulkProgressEvery == 0 && processed < p.NumRecords {
			report("progress")
		}
	}

	report("done")
	return summarize(sent, failed, start, totalLatency), nil
}

func formatBulkProgress(phase, topic string, processed, sent, failed, total int, elapsed time.Duration, rps float64) string {
	switch phase {
	case "start":
		return fmt.Sprintf("开始向 %s 造数，目标 %d 条", topic, total)
	case "done":
		if failed > 0 {
			return fmt.Sprintf("结束：已处理 %d/%d，成功 %d，失败 %d，耗时 %s，%.0f 条/秒",
				processed, total, sent, failed, elapsed.Truncate(time.Millisecond), rps)
		}
		return fmt.Sprintf("结束：已发送 %d/%d 条，耗时 %s，%.0f 条/秒",
			sent, total, elapsed.Truncate(time.Millisecond), rps)
	default:
		if failed > 0 {
			return fmt.Sprintf("进度 %d/%d（成功 %d，失败 %d），%.0f 条/秒",
				processed, total, sent, failed, rps)
		}
		return fmt.Sprintf("已发送 %d/%d 条，%.0f 条/秒", processed, total, rps)
	}
}

func summarize(sent, failed int, start time.Time, totalLatency time.Duration) *model.BulkProduceResult {
	elapsed := time.Since(start)
	ms := elapsed.Milliseconds()
	rps := 0.0
	if elapsed.Seconds() > 0 {
		rps = float64(sent) / elapsed.Seconds()
	}
	avg := 0.0
	n := sent + failed
	if n > 0 {
		avg = float64(totalLatency.Microseconds()) / float64(n) / 1000.0
	}
	return &model.BulkProduceResult{
		Sent:          sent,
		Failed:        failed,
		ElapsedMs:     ms,
		RecordsPerSec: rps,
		AvgLatencyMs:  avg,
	}
}
