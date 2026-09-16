package kafka

import (
	"testing"
	"time"
)

func TestFormatBulkProgress(t *testing.T) {
	got := formatBulkProgress("start", "demo", 0, 0, 0, 2500, 0, 0)
	if got != "开始向 demo 造数，目标 2500 条" {
		t.Fatalf("start: %s", got)
	}
	got = formatBulkProgress("progress", "demo", 1000, 1000, 0, 2500, time.Second, 800)
	if got != "已发送 1000/2500 条，800 条/秒" {
		t.Fatalf("progress: %s", got)
	}
	got = formatBulkProgress("done", "demo", 2500, 2500, 0, 2500, 3*time.Second, 833)
	if got != "结束：已发送 2500/2500 条，耗时 3s，833 条/秒" {
		t.Fatalf("done: %s", got)
	}
}
