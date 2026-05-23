package kafka

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ProduceOptions configures acknowledgement semantics and serialization for
// producing messages to Kafka.
type ProduceOptions struct {
	Acks       string `json:"acks"`       // "0", "1", "all"
	Serializer string `json:"serializer"` // "auto", "json", "string"
	SchemaID   int    `json:"schema_id"`  // -1 for none
}

// Produce writes messages to a topic.
func (m *Manager) Produce(ctx context.Context, cluster, topic string, messages []ProduceMessage, opts ProduceOptions) ([]DeliveryResult, error) {
	// For non-default acks, create a temporary client
	var client *kgo.Client
	var needsClose bool

	if opts.Acks == "0" || opts.Acks == "1" {
		clCfg, err := m.GetClusterConfig(cluster)
		if err != nil {
			return nil, err
		}
		clientOpts, err := m.clientOpts(clCfg)
		if err != nil {
			return nil, err
		}
		switch opts.Acks {
		case "0":
			clientOpts = append(clientOpts, kgo.RequiredAcks(kgo.NoAck()))
		case "1":
			clientOpts = append(clientOpts, kgo.RequiredAcks(kgo.LeaderAck()))
		}
		client, err = kgo.NewClient(clientOpts...)
		if err != nil {
			return nil, fmt.Errorf("creating producer client: %w", err)
		}
		needsClose = true
	} else {
		var err error
		client, err = m.GetClient(cluster)
		if err != nil {
			return nil, err
		}
	}
	if needsClose {
		defer client.Close()
	}

	// Pre-allocate results to preserve input order
	results := make([]DeliveryResult, len(messages))
	var wg sync.WaitGroup
	var failCount int64

	for i, msg := range messages {
		value := serializeValue(msg.Value)
		record := &kgo.Record{
			Topic: topic,
			Value: []byte(value),
		}
		if msg.Key != "" {
			record.Key = []byte(msg.Key)
		}
		if msg.Partition != nil {
			record.Partition = *msg.Partition
		}
		for k, v := range msg.Headers {
			record.Headers = append(record.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}

		wg.Add(1)
		idx := i
		client.Produce(ctx, record, func(r *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				results[idx] = DeliveryResult{Status: "error: delivery failed"}
				atomic.AddInt64(&failCount, 1)
				return
			}
			results[idx] = DeliveryResult{
				Partition: r.Partition,
				Offset:    r.Offset,
				Timestamp: r.Timestamp.UTC().Format("2006-01-02T15:04:05.000Z"),
				Status:    "delivered",
			}
		})
	}

	wg.Wait()

	if n := atomic.LoadInt64(&failCount); n > 0 {
		return results, fmt.Errorf("%d of %d messages failed delivery", n, len(messages))
	}
	return results, nil
}

// ProduceMessage is a single message to be published to a Kafka topic,
// including an optional key, headers, and target partition override.
type ProduceMessage struct {
	Key       string            `json:"key"`
	Value     any               `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Partition *int32            `json:"partition,omitempty"`
}
