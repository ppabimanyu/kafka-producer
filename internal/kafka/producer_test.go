package kafka

import (
	"testing"
)

func TestBuildRecord(t *testing.T) {
	input := &RecordInput{
		Topic:     "test-topic",
		Partition: 3,
		Key:       "key-xyz",
		Value:     []byte(`{"status": "ok"}`),
		Headers: map[string]string{
			"env": "test",
		},
	}

	record := BuildRecord(input)

	if record.Topic != "test-topic" {
		t.Errorf("expected topic 'test-topic', got '%s'", record.Topic)
	}

	if record.Partition != 3 {
		t.Errorf("expected partition 3, got %d", record.Partition)
	}

	if string(record.Key) != "key-xyz" {
		t.Errorf("expected key 'key-xyz', got '%s'", string(record.Key))
	}

	if string(record.Value) != `{"status": "ok"}` {
		t.Errorf("expected value '{\"status\": \"ok\"}', got '%s'", string(record.Value))
	}

	if len(record.Headers) != 1 {
		t.Fatalf("expected 1 header, got %d", len(record.Headers))
	}

	if record.Headers[0].Key != "env" || string(record.Headers[0].Value) != "test" {
		t.Errorf("header mismatch: got %s=%s", record.Headers[0].Key, string(record.Headers[0].Value))
	}
}

func TestBuildRecordAutoPartition(t *testing.T) {
	input := &RecordInput{
		Topic:     "test-topic",
		Partition: -1,
		Value:     []byte("hello"),
	}

	record := BuildRecord(input)

	if record.Partition != -1 {
		t.Errorf("expected partition -1 for auto-partitioning, got %d", record.Partition)
	}
}
