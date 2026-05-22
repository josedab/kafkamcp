package server

import (
	"context"
	"strings"
	"testing"
)

// TestHandlers_StringValidation_Broad spot-checks the validateStringLength
// short-circuit across a representative subset of handlers. The early-return
// covers the validation slice of each handler before kafkaManager is touched.
func TestHandlers_StringValidation_Broad(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	ctx := context.Background()
	long := strings.Repeat("x", maxInputLength+1)

	handlers := []struct {
		name string
		args map[string]any
		call func(context.Context, map[string]any) error
	}{
		{"describe_topic", map[string]any{"name": long}, func(c context.Context, a map[string]any) error {
			r, err := s.handleDescribeTopic(c, makeToolRequest(a))
			assertIsError(t, r)
			return err
		}},
		{"create_topic_long_name", map[string]any{"name": long}, func(c context.Context, a map[string]any) error {
			r, err := s.handleCreateTopic(c, makeToolRequest(a))
			assertIsError(t, r)
			return err
		}},
		{"delete_topic", map[string]any{"name": long}, func(c context.Context, a map[string]any) error {
			r, err := s.handleDeleteTopic(c, makeToolRequest(a))
			assertIsError(t, r)
			return err
		}},
		{"describe_consumer_group", map[string]any{"group_id": long}, func(c context.Context, a map[string]any) error {
			r, err := s.handleDescribeConsumerGroup(c, makeToolRequest(a))
			assertIsError(t, r)
			return err
		}},
		{"reset_offsets", map[string]any{"group_id": long}, func(c context.Context, a map[string]any) error {
			r, err := s.handleResetOffsets(c, makeToolRequest(a))
			assertIsError(t, r)
			return err
		}},
		{"alter_topic", map[string]any{"name": long}, func(c context.Context, a map[string]any) error {
			r, err := s.handleAlterTopic(c, makeToolRequest(a))
			assertIsError(t, r)
			return err
		}},
		{"search", map[string]any{"topic": long}, func(c context.Context, a map[string]any) error {
			r, err := s.handleSearch(c, makeToolRequest(a))
			assertIsError(t, r)
			return err
		}},
		{"produce", map[string]any{"topic": long}, func(c context.Context, a map[string]any) error {
			r, err := s.handleProduce(c, makeToolRequest(a))
			assertIsError(t, r)
			return err
		}},
		{"subscribe", map[string]any{"cluster": long}, func(c context.Context, a map[string]any) error {
			r, err := s.handleSubscribe(c, makeToolRequest(a))
			assertIsError(t, r)
			return err
		}},
		{"consume", map[string]any{"topic": long}, func(c context.Context, a map[string]any) error {
			r, err := s.handleConsume(c, makeToolRequest(a))
			assertIsError(t, r)
			return err
		}},
		{"list_acls", map[string]any{"cluster": long}, func(c context.Context, a map[string]any) error {
			r, err := s.handleListACLs(c, makeToolRequest(a))
			assertIsError(t, r)
			return err
		}},
	}

	for _, h := range handlers {
		h := h
		t.Run(h.name, func(t *testing.T) {
			t.Parallel()
			if err := h.call(ctx, h.args); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
