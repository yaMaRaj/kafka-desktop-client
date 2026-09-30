package kafka

import (
	"encoding/json"
	"strings"
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

func TestRenderBulkValue(t *testing.T) {
	tpl := `{
  "id": "{{uuid}}",
  "seq": {{seq}},
  "seq1": {{seq1}},
  "name": "{{cname}}",
  "age": {{age}},
  "email": "{{email}}",
  "mobile": "{{mobile}}",
  "city": "{{ccity}}",
  "address": "{{caddress}}",
  "level": "{{pick:A|B|C}}",
  "score": {{integer:1:100}},
  "token": "{{string:8}}",
  "remark": "{{text:20}}",
  "ok": {{bool}}
}`
	out := string(renderBulkValue(tpl, 7))
	if strings.Contains(out, "{{") {
		t.Fatalf("placeholder left: %s", out)
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(out), &obj); err != nil {
		t.Fatalf("invalid json: %v\n%s", err, out)
	}
	if int(obj["seq"].(float64)) != 7 {
		t.Fatalf("seq=%v", obj["seq"])
	}
	if obj["name"] == "" || obj["email"] == "" || obj["remark"] == "" {
		t.Fatalf("empty fields: %s", out)
	}
}

func TestPadBulkValueToSize(t *testing.T) {
	base := []byte(`{"seq":1,"name":"张三"}`)
	got := padBulkValueToSize(base, 120)
	if len(got) != 120 {
		t.Fatalf("len=%d want 120: %s", len(got), got)
	}
	var obj map[string]any
	if err := json.Unmarshal(got, &obj); err != nil {
		t.Fatalf("padded json invalid: %v\n%s", err, got)
	}
	if _, ok := obj["_text"]; !ok {
		t.Fatalf("missing _text: %s", got)
	}
	// 已够大则不改
	same := padBulkValueToSize(got, 50)
	if string(same) != string(got) {
		t.Fatalf("should keep original when already large enough")
	}
}
