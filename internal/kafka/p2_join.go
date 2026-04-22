package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// JoinRequest defines parameters for a cross-topic join.
type JoinRequest struct {
	LeftTopic  string `json:"left_topic"`
	RightTopic string `json:"right_topic"`
	KeyField   string `json:"key_field"`
	WindowMs   int64  `json:"window_ms"`
	Limit      int    `json:"limit"`
}

// JoinedMessage represents a joined message from two topics.
type JoinedMessage struct {
	Key       any    `json:"key"`
	Left      any    `json:"left_value"`
	Right     any    `json:"right_value"`
	LeftTopic string `json:"left_topic"`
	RightTopic string `json:"right_topic"`
	Timestamp string `json:"timestamp"`
}

// JoinResult is the result of a cross-topic join.
type JoinResult struct {
	Joined     []JoinedMessage `json:"joined"`
	LeftCount  int             `json:"left_count"`
	RightCount int             `json:"right_count"`
	MatchCount int             `json:"match_count"`
	WindowMs   int64           `json:"window_ms"`
}

// JoinTopics performs a time-windowed join across two topics on a key field.
func (m *Manager) JoinTopics(ctx context.Context, cluster string, req JoinRequest) (*JoinResult, error) {
	if req.WindowMs <= 0 {
		req.WindowMs = 60000 // 1 minute default
	}
	if req.Limit <= 0 {
		req.Limit = 100
	}

	windowStart := time.Now().Add(-time.Duration(req.WindowMs) * time.Millisecond)
	startISO := windowStart.UTC().Format(time.RFC3339)

	leftMsgs, err := m.Consume(ctx, cluster, req.LeftTopic, -1, startISO, 5000, 10000, "auto")
	if err != nil {
		return nil, fmt.Errorf("consuming left topic: %w", err)
	}

	rightMsgs, err := m.Consume(ctx, cluster, req.RightTopic, -1, startISO, 5000, 10000, "auto")
	if err != nil {
		return nil, fmt.Errorf("consuming right topic: %w", err)
	}

	// Build index on right topic by key
	rightByKey := make(map[string][]Message)
	for _, msg := range rightMsgs {
		key := extractJoinKey(msg, req.KeyField)
		if key != "" {
			rightByKey[key] = append(rightByKey[key], msg)
		}
	}

	// Join left with right
	var joined []JoinedMessage
	for _, leftMsg := range leftMsgs {
		if len(joined) >= req.Limit {
			break
		}
		key := extractJoinKey(leftMsg, req.KeyField)
		if key == "" {
			continue
		}
		if rightMatches, ok := rightByKey[key]; ok {
			for _, rightMsg := range rightMatches {
				if len(joined) >= req.Limit {
					break
				}
				joined = append(joined, JoinedMessage{
					Key:        key,
					Left:       leftMsg.Value,
					Right:      rightMsg.Value,
					LeftTopic:  req.LeftTopic,
					RightTopic: req.RightTopic,
					Timestamp:  leftMsg.Timestamp,
				})
			}
		}
	}

	return &JoinResult{
		Joined:     joined,
		LeftCount:  len(leftMsgs),
		RightCount: len(rightMsgs),
		MatchCount: len(joined),
		WindowMs:   req.WindowMs,
	}, nil
}

func extractJoinKey(msg Message, keyField string) string {
	// If keyField is empty, use the message key
	if keyField == "" {
		return fmt.Sprintf("%v", msg.Key)
	}
	// Extract from value using dot notation
	parts := strings.Split(keyField, ".")
	current := msg.Value
	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current, ok = m[part]
		if !ok {
			return ""
		}
	}
	return fmt.Sprintf("%v", current)
}
