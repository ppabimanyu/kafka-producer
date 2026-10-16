package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// RecordInput represents a message to be published to Kafka.
type RecordInput struct {
	Topic     string
	Partition int32 // -1 indicates default/auto partitioning
	Key       string
	Value     []byte
	Headers   map[string]string
}

// PublishResult contains metadata of a successfully produced record.
type PublishResult struct {
	Topic       string
	Partition   int32
	Offset      int64
	Timestamp   time.Time
	Key         string
	PayloadSize int
}

// BuildRecord constructs a Franz-Go kgo.Record from RecordInput.
func BuildRecord(input *RecordInput) *kgo.Record {
	r := &kgo.Record{
		Topic:     input.Topic,
		Value:     input.Value,
		Timestamp: time.Now(),
	}

	if input.Partition >= 0 {
		r.Partition = input.Partition
	} else {
		r.Partition = -1
	}

	if input.Key != "" {
		r.Key = []byte(input.Key)
	}

	if len(input.Headers) > 0 {
		headers := make([]kgo.RecordHeader, 0, len(input.Headers))
		for k, v := range input.Headers {
			headers = append(headers, kgo.RecordHeader{
				Key:   k,
				Value: []byte(v),
			})
		}
		r.Headers = headers
	}

	return r
}

// PublishMessage sends a single message synchronously to Kafka.
func PublishMessage(ctx context.Context, client *kgo.Client, input *RecordInput) (*PublishResult, error) {
	record := BuildRecord(input)
	res := client.ProduceSync(ctx, record)
	if err := res.FirstErr(); err != nil {
		return nil, fmt.Errorf("failed to produce message to topic '%s': %w", input.Topic, err)
	}

	rec, _ := res.First()
	return &PublishResult{
		Topic:       rec.Topic,
		Partition:   rec.Partition,
		Offset:      rec.Offset,
		Timestamp:   rec.Timestamp,
		Key:         string(rec.Key),
		PayloadSize: len(rec.Value),
	}, nil
}

// PublishBatch sends multiple messages synchronously in a batch to Kafka.
func PublishBatch(ctx context.Context, client *kgo.Client, inputs []*RecordInput) ([]*PublishResult, error) {
	if len(inputs) == 0 {
		return nil, nil
	}

	records := make([]*kgo.Record, len(inputs))
	for i, input := range inputs {
		records[i] = BuildRecord(input)
	}

	results := client.ProduceSync(ctx, records...)
	publishResults := make([]*PublishResult, len(results))

	for i, r := range results {
		if r.Err != nil {
			return nil, fmt.Errorf("error producing record #%d: %w", i+1, r.Err)
		}
		publishResults[i] = &PublishResult{
			Topic:       r.Record.Topic,
			Partition:   r.Record.Partition,
			Offset:      r.Record.Offset,
			Timestamp:   r.Record.Timestamp,
			Key:         string(r.Record.Key),
			PayloadSize: len(r.Record.Value),
		}
	}

	return publishResults, nil
}
