package kafka

import (
	"testing"

	"kafka-client-go/internal/model"
)

func TestMatchExportFilter(t *testing.T) {
	key := "user-42"
	val := `{"name":"张三","city":"北京"}`
	m := model.KafkaMessageView{Topic: "t", Partition: 0, Offset: 1, Timestamp: 1700000000000, Key: &key, Value: &val}

	if !matchExportFilter(m, model.ExportMessagesParams{KeyContains: "user"}, nil) {
		t.Fatal("key contains should match")
	}
	if matchExportFilter(m, model.ExportMessagesParams{KeyContains: "order"}, nil) {
		t.Fatal("key contains should miss")
	}
	if !matchExportFilter(m, model.ExportMessagesParams{ValueContains: "北京"}, nil) {
		t.Fatal("value contains should match")
	}
	to := int64(1600000000000)
	if matchExportFilter(m, model.ExportMessagesParams{ToTimestamp: &to}, nil) {
		t.Fatal("toTimestamp should filter out")
	}
}
