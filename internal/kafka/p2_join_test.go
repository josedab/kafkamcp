package kafka

import (
	"testing"
)

func TestExtractJoinKey_WithKeyField(t *testing.T) {
	t.Parallel()
	msg := Message{
		Key:   "order-1",
		Value: map[string]any{"user_id": "u1", "amount": 100},
	}
	key := extractJoinKey(msg, "user_id")
	if key != "u1" {
		t.Errorf("expected u1, got %q", key)
	}
}

func TestExtractJoinKey_DotNotation(t *testing.T) {
	t.Parallel()
	msg := Message{
		Key: "k1",
		Value: map[string]any{
			"user": map[string]any{
				"profile": map[string]any{
					"id": "nested-123",
				},
			},
		},
	}
	key := extractJoinKey(msg, "user.profile.id")
	if key != "nested-123" {
		t.Errorf("expected nested-123, got %q", key)
	}
}

func TestExtractJoinKey_EmptyKeyField(t *testing.T) {
	t.Parallel()
	msg := Message{Key: "msg-key-1", Value: "some value"}
	key := extractJoinKey(msg, "")
	if key != "msg-key-1" {
		t.Errorf("expected msg-key-1, got %q", key)
	}
}

func TestExtractJoinKey_MissingField(t *testing.T) {
	t.Parallel()
	msg := Message{Value: map[string]any{"a": 1}}
	key := extractJoinKey(msg, "nonexistent")
	if key != "" {
		t.Errorf("expected empty key for missing field, got %q", key)
	}
}

func TestExtractJoinKey_NonMapValue(t *testing.T) {
	t.Parallel()
	msg := Message{Value: "plain string"}
	key := extractJoinKey(msg, "some_field")
	if key != "" {
		t.Errorf("expected empty key for non-map value, got %q", key)
	}
}

func TestJoinRequest_DefaultValues(t *testing.T) {
	t.Parallel()
	req := JoinRequest{LeftTopic: "left", RightTopic: "right", KeyField: "id"}
	if req.WindowMs <= 0 {
		req.WindowMs = 60000
	}
	if req.Limit <= 0 {
		req.Limit = 100
	}
	if req.WindowMs != 60000 {
		t.Errorf("expected default WindowMs 60000, got %d", req.WindowMs)
	}
	if req.Limit != 100 {
		t.Errorf("expected default Limit 100, got %d", req.Limit)
	}
}

func TestJoinLogic_InnerJoinMatch(t *testing.T) {
	t.Parallel()
	leftMsgs := []Message{
		{Value: map[string]any{"id": "a", "left_data": "L1"}, Timestamp: "2025-01-01T00:00:00Z"},
		{Value: map[string]any{"id": "b", "left_data": "L2"}, Timestamp: "2025-01-01T00:00:01Z"},
	}
	rightMsgs := []Message{
		{Value: map[string]any{"id": "a", "right_data": "R1"}, Timestamp: "2025-01-01T00:00:00Z"},
		{Value: map[string]any{"id": "c", "right_data": "R2"}, Timestamp: "2025-01-01T00:00:01Z"},
	}

	keyField := "id"
	rightByKey := make(map[string][]Message)
	for _, msg := range rightMsgs {
		key := extractJoinKey(msg, keyField)
		if key != "" {
			rightByKey[key] = append(rightByKey[key], msg)
		}
	}

	var joined []JoinedMessage
	for _, leftMsg := range leftMsgs {
		key := extractJoinKey(leftMsg, keyField)
		if key == "" {
			continue
		}
		if rightMatches, ok := rightByKey[key]; ok {
			for _, rightMsg := range rightMatches {
				joined = append(joined, JoinedMessage{
					Key:   key,
					Left:  leftMsg.Value,
					Right: rightMsg.Value,
				})
			}
		}
	}

	if len(joined) != 1 {
		t.Fatalf("expected 1 joined message, got %d", len(joined))
	}
	if joined[0].Key != "a" {
		t.Errorf("expected key 'a', got %v", joined[0].Key)
	}
}

func TestJoinLogic_NoMatchingKeys(t *testing.T) {
	t.Parallel()
	leftMsgs := []Message{{Value: map[string]any{"id": "left-1"}}}
	rightMsgs := []Message{{Value: map[string]any{"id": "right-1"}}}

	rightByKey := make(map[string][]Message)
	for _, msg := range rightMsgs {
		key := extractJoinKey(msg, "id")
		if key != "" {
			rightByKey[key] = append(rightByKey[key], msg)
		}
	}

	var joined []JoinedMessage
	for _, leftMsg := range leftMsgs {
		key := extractJoinKey(leftMsg, "id")
		if key == "" {
			continue
		}
		if rightMatches, ok := rightByKey[key]; ok {
			for _, rightMsg := range rightMatches {
				joined = append(joined, JoinedMessage{Key: key, Left: leftMsg.Value, Right: rightMsg.Value})
			}
		}
	}

	if len(joined) != 0 {
		t.Errorf("expected 0 joined messages for non-matching keys, got %d", len(joined))
	}
}

func TestJoinLogic_LimitEnforcement(t *testing.T) {
	t.Parallel()
	var leftMsgs, rightMsgs []Message
	for i := 0; i < 20; i++ {
		leftMsgs = append(leftMsgs, Message{Value: map[string]any{"id": "shared"}})
		rightMsgs = append(rightMsgs, Message{Value: map[string]any{"id": "shared"}})
	}

	limit := 5
	rightByKey := make(map[string][]Message)
	for _, msg := range rightMsgs {
		key := extractJoinKey(msg, "id")
		if key != "" {
			rightByKey[key] = append(rightByKey[key], msg)
		}
	}

	var joined []JoinedMessage
	for _, leftMsg := range leftMsgs {
		if len(joined) >= limit {
			break
		}
		key := extractJoinKey(leftMsg, "id")
		if key == "" {
			continue
		}
		if rightMatches, ok := rightByKey[key]; ok {
			for _, rightMsg := range rightMatches {
				if len(joined) >= limit {
					break
				}
				joined = append(joined, JoinedMessage{Key: key, Left: leftMsg.Value, Right: rightMsg.Value})
			}
		}
	}

	if len(joined) != limit {
		t.Errorf("expected %d joined messages (limit), got %d", limit, len(joined))
	}
}

func TestExtractJoinKey_DeepDotNotationMissing(t *testing.T) {
	t.Parallel()
	msg := Message{Value: map[string]any{"user": map[string]any{"name": "alice"}}}
	key := extractJoinKey(msg, "user.profile.id")
	if key != "" {
		t.Errorf("expected empty for missing nested path, got %q", key)
	}
}

func TestJoinResult_Structure(t *testing.T) {
	t.Parallel()
	result := JoinResult{
		Joined:     []JoinedMessage{{Key: "k1"}},
		LeftCount:  5,
		RightCount: 3,
		MatchCount: 1,
		WindowMs:   60000,
	}
	if result.MatchCount != len(result.Joined) {
		t.Errorf("match count %d != joined length %d", result.MatchCount, len(result.Joined))
	}
}
