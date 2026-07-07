package kafka

import (
	"context"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Consume reads messages from a topic using direct partition fetch.
func (m *Manager) Consume(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]Message, error) {
	clCfg, err := m.GetClusterConfig(cluster)
	if err != nil {
		return nil, err
	}

	admin, err := m.GetAdmin(cluster)
	if err != nil {
		return nil, err
	}

	// Determine partitions to read from
	topics, err := admin.ListTopics(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("listing topic for consume: %w", err)
	}
	td, ok := topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic %q not found", topic)
	}

	targetPartitions := selectTargetPartitions(partition, td.Partitions.Sorted())

	perPartitionLimit := limit
	if partition < 0 && len(targetPartitions) > 0 {
		// Distribute limit across partitions; +1 per partition to ensure we meet
		// the total limit despite integer division truncation.
		perPartitionLimit = (limit / len(targetPartitions)) + 1
	}

	// Build consumer offsets
	consumeOffsets := make(map[string]map[int32]kgo.Offset)
	consumeOffsets[topic] = make(map[int32]kgo.Offset)

	for _, p := range targetPartitions {
		var kgoOffset kgo.Offset
		switch offset {
		case "earliest":
			kgoOffset = kgo.NewOffset().AtStart()
		case "latest":
			endOffsets, err := admin.ListEndOffsets(ctx, topic)
			if err != nil {
				return nil, fmt.Errorf("listing end offsets: %w", err)
			}
			eo, ok := endOffsets.Lookup(topic, p)
			if !ok {
				continue
			}
			startAt := eo.Offset - int64(perPartitionLimit)
			if startAt < 0 {
				startAt = 0
			}
			kgoOffset = kgo.NewOffset().At(startAt)
		default:
			if off, err := strconv.ParseInt(offset, 10, 64); err == nil {
				kgoOffset = kgo.NewOffset().At(off)
			} else if ts, tsErr := parseTimestamp(offset); tsErr == nil {
				listed, lErr := admin.ListOffsetsAfterMilli(ctx, ts.UnixMilli(), topic)
				if lErr == nil {
					if lo, ok := listed.Lookup(topic, p); ok && lo.Offset >= 0 {
						kgoOffset = kgo.NewOffset().At(lo.Offset)
					} else {
						kgoOffset = kgo.NewOffset().AtEnd()
					}
				} else {
					kgoOffset = kgo.NewOffset().AtEnd()
				}
			} else {
				kgoOffset = kgo.NewOffset().AtEnd()
			}
		}
		consumeOffsets[topic][p] = kgoOffset
	}

	// Reuse shared client options (SASL, TLS) then add consumer-specific opts
	opts, err := m.clientOpts(clCfg)
	if err != nil {
		return nil, fmt.Errorf("building client options: %w", err)
	}
	opts = append(opts, kgo.ConsumePartitions(consumeOffsets))

	consumer, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("creating consumer: %w", err)
	}
	defer consumer.Close()

	var messages []Message
	ctx, cancel := context.WithTimeout(ctx, parseDurationMs(timeoutMs))
	defer cancel()

	for len(messages) < limit {
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err != context.DeadlineExceeded {
					m.logger.Warn("fetch error", "topic", e.Topic, "partition", e.Partition, "error", e.Err)
				}
			}
		}

		fetches.EachRecord(func(r *kgo.Record) {
			if len(messages) >= limit {
				return
			}
			headers := make(map[string]string)
			for _, h := range r.Headers {
				headers[h.Key] = string(h.Value)
			}

			msg := Message{
				Partition: r.Partition,
				Offset:    r.Offset,
				Timestamp: r.Timestamp.UTC().Format("2006-01-02T15:04:05.000Z"),
				Key:       decodeValue(r.Key, deserializer),
				Value:     decodeValue(r.Value, deserializer),
				Headers:   headers,
			}
			messages = append(messages, msg)
		})

		if fetches.NumRecords() == 0 {
			break
		}
	}

	if len(messages) > limit {
		messages = messages[:limit]
	}

	return messages, nil
}

func selectTargetPartitions(requested int32, available []kadm.PartitionDetail) []int32 {
	if requested >= 0 {
		return []int32{requested}
	}
	var selected []int32
	for _, partition := range available {
		selected = append(selected, partition.Partition)
	}
	return selected
}
