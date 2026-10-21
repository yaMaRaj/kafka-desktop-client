package kafka

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"kafka-client-go/internal/model"
)

const (
	DefaultMaxScan   = 20000
	DefaultMaxExport = 5000
	HardMaxScan      = 100000
	HardMaxExport    = 20000
)

// ExportMessages 扫描 Topic，按条件过滤后写入 JSONL/CSV 文件。
func ExportMessages(ctx context.Context, c *Client, p model.ExportMessagesParams, path string, onProgress func(model.ExportMessagesProgress)) (*model.ExportMessagesResult, error) {
	if p.Topic == "" {
		return nil, fmt.Errorf("请指定 Topic")
	}
	if path == "" {
		return nil, fmt.Errorf("请指定导出路径")
	}
	format := strings.ToLower(strings.TrimSpace(p.Format))
	if format != "jsonl" && format != "csv" {
		return nil, fmt.Errorf("导出格式仅支持 jsonl / csv")
	}

	maxScan := p.MaxScan
	if maxScan <= 0 {
		maxScan = DefaultMaxScan
	}
	if maxScan > HardMaxScan {
		maxScan = HardMaxScan
	}
	maxExport := p.MaxExport
	if maxExport <= 0 {
		maxExport = DefaultMaxExport
	}
	if maxExport > HardMaxExport {
		maxExport = HardMaxExport
	}

	var valueRe *regexp.Regexp
	if strings.TrimSpace(p.ValueRegex) != "" {
		re, err := regexp.Compile(p.ValueRegex)
		if err != nil {
			return nil, fmt.Errorf("Value 正则无效: %w", err)
		}
		valueRe = re
	}

	partOffsets, err := resolveConsumeOffsets(ctx, c, p.Topic, p.Partition, p.FromOffset, p.FromTimestamp)
	if err != nil {
		return nil, err
	}
	if len(partOffsets) == 0 {
		return &model.ExportMessagesResult{Path: path, Format: format}, nil
	}

	base, err := buildOpts(c.Profile)
	if err != nil {
		return nil, err
	}
	opts := append(base,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{p.Topic: partOffsets}),
		kgo.FetchMaxBytes(8*1024*1024),
		kgo.ClientID(firstNonEmpty(c.Profile.ClientID, "kafka-client-go")+"-export"),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	defer cl.Close()

	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("创建文件失败: %w", err)
	}
	defer f.Close()

	var csvW *csv.Writer
	if format == "csv" {
		csvW = csv.NewWriter(f)
		if err := csvW.Write([]string{"topic", "partition", "offset", "timestamp", "key", "value", "headers"}); err != nil {
			return nil, err
		}
	}

	report := func(phase string, scanned, matched, exported int, truncated bool) {
		if onProgress == nil {
			return
		}
		msg := ""
		switch phase {
		case "start":
			msg = fmt.Sprintf("开始扫描 %s，最多扫描 %d、导出 %d", p.Topic, maxScan, maxExport)
		case "done":
			msg = fmt.Sprintf("完成：扫描 %d，命中 %d，导出 %d", scanned, matched, exported)
			if truncated {
				msg += "（已达上限）"
			}
		default:
			msg = fmt.Sprintf("已扫描 %d，命中 %d，已导出 %d", scanned, matched, exported)
		}
		onProgress(model.ExportMessagesProgress{
			Phase: phase, Scanned: scanned, Matched: matched, Exported: exported,
			MaxScan: maxScan, MaxExport: maxExport, Message: msg,
		})
	}

	report("start", 0, 0, 0, false)

	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	scanned, matched, exported := 0, 0, 0
	truncated := false
	lastReport := time.Now()
	var writeErr error

	for scanned < maxScan && exported < maxExport && writeErr == nil {
		select {
		case <-ctx.Done():
			if scanned == 0 && exported == 0 {
				return nil, ctx.Err()
			}
			truncated = true
			goto finish
		default:
		}

		pollCtx, cancelPoll := context.WithTimeout(ctx, 8*time.Second)
		fetches := cl.PollFetches(pollCtx)
		cancelPoll()

		if errs := fetches.Errors(); len(errs) > 0 {
			hasReal := false
			for _, e := range errs {
				if e.Err == nil || errors.Is(e.Err, context.Canceled) || errors.Is(e.Err, context.DeadlineExceeded) {
					continue
				}
				hasReal = true
				if scanned == 0 && exported == 0 {
					return nil, e.Err
				}
				truncated = true
				goto finish
			}
			if !hasReal && scanned > 0 {
				break
			}
			if !hasReal {
				continue
			}
		}

		emptyBatch := true
		fetches.EachRecord(func(r *kgo.Record) {
			if writeErr != nil || scanned >= maxScan || exported >= maxExport {
				return
			}
			emptyBatch = false
			scanned++
			view := recordToView(r)
			if !matchExportFilter(view, p, valueRe) {
				return
			}
			matched++
			if err := writeExportRecord(f, csvW, format, view); err != nil {
				writeErr = err
				return
			}
			exported++
		})
		if writeErr != nil {
			return nil, writeErr
		}

		if time.Since(lastReport) >= 800*time.Millisecond {
			report("progress", scanned, matched, exported, false)
			lastReport = time.Now()
		}

		if emptyBatch {
			break
		}
		if scanned >= maxScan || exported >= maxExport {
			truncated = true
			break
		}
	}

finish:
	if csvW != nil {
		csvW.Flush()
		if err := csvW.Error(); err != nil {
			return nil, err
		}
	}
	if err := f.Sync(); err != nil {
		return nil, err
	}
	report("done", scanned, matched, exported, truncated)
	return &model.ExportMessagesResult{
		Path: path, Scanned: scanned, Matched: matched, Exported: exported,
		Truncated: truncated, Format: format,
	}, nil
}

func resolveConsumeOffsets(ctx context.Context, c *Client, topic string, partition *int32, fromOffset, fromTimestamp *int64) (map[int32]kgo.Offset, error) {
	startOffsets, err := c.Admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	endOffsets, err := c.Admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}

	var timeOffsets kadm.ListedOffsets
	if fromTimestamp != nil {
		timeOffsets, err = c.Admin.ListOffsetsAfterMilli(ctx, *fromTimestamp, topic)
		if err != nil {
			return nil, fmt.Errorf("按时间定位 offset 失败: %w", err)
		}
	}

	partOffsets := make(map[int32]kgo.Offset)
	startOffsets.Each(func(o kadm.ListedOffset) {
		if o.Topic != topic {
			return
		}
		if partition != nil && o.Partition != *partition {
			return
		}
		end, ok := endOffsets.Lookup(topic, o.Partition)
		if !ok {
			return
		}
		at := o.Offset
		if fromTimestamp != nil {
			if to, ok := timeOffsets.Lookup(topic, o.Partition); ok && to.Offset >= 0 {
				at = to.Offset
			}
		}
		if fromOffset != nil {
			at = *fromOffset
			if at < o.Offset {
				at = o.Offset
			}
		}
		if end.Offset >= 0 && at >= end.Offset {
			return
		}
		partOffsets[o.Partition] = kgo.NewOffset().At(at)
	})
	return partOffsets, nil
}

func matchExportFilter(m model.KafkaMessageView, p model.ExportMessagesParams, valueRe *regexp.Regexp) bool {
	if p.FromTimestamp != nil && m.Timestamp > 0 && m.Timestamp < *p.FromTimestamp {
		return false
	}
	if p.ToTimestamp != nil && m.Timestamp > 0 && m.Timestamp > *p.ToTimestamp {
		return false
	}
	key := ""
	if m.Key != nil {
		key = *m.Key
	}
	val := ""
	if m.Value != nil {
		val = *m.Value
	}
	if p.KeyContains != "" && !strings.Contains(key, p.KeyContains) {
		return false
	}
	if p.ValueContains != "" && !strings.Contains(val, p.ValueContains) {
		return false
	}
	if valueRe != nil && !valueRe.MatchString(val) {
		return false
	}
	return true
}

func writeExportRecord(f *os.File, csvW *csv.Writer, format string, m model.KafkaMessageView) error {
	key := ""
	if m.Key != nil {
		key = *m.Key
	}
	val := ""
	if m.Value != nil {
		val = *m.Value
	}
	headersJSON, _ := json.Marshal(m.Headers)
	if format == "csv" {
		return csvW.Write([]string{
			m.Topic,
			strconv.FormatInt(int64(m.Partition), 10),
			strconv.FormatInt(m.Offset, 10),
			strconv.FormatInt(m.Timestamp, 10),
			key,
			val,
			string(headersJSON),
		})
	}
	line, err := json.Marshal(m)
	if err != nil {
		return err
	}
	_, err = f.Write(append(line, '\n'))
	return err
}
