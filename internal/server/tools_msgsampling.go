package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/msgsampling"
)

func (s *Server) handleMessageSample(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_message_sample", resourceType: auth.ResourceTopic, action: auth.ActionRead,
		resourceName: func(r mcp.CallToolRequest) string { return r.GetString("topic", "") },
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		if action == "" {
			return mcp.NewToolResultError("action is required"), nil
		}

		switch action {
		case "sample":
			topic := request.GetString("topic", "")
			cluster := request.GetString("cluster", "")
			if topic == "" {
				return mcp.NewToolResultError("topic is required"), nil
			}
			if deny := validateStringLength("topic", topic); deny != nil {
				return deny, nil
			}

			sampleSize := request.GetInt("sample_size", 100)
			if sampleSize > 1000 {
				sampleSize = 1000
			}
			if sampleSize < 1 {
				sampleSize = 1
			}

			messages, err := s.kafkaManager.Consume(ctx, cluster, topic, -1, "latest", sampleSize, 5000, "auto")
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_message_sample", args, cluster, "error", 0, err, start)
				return s.toolError("Failed to consume sample messages", err), nil
			}

			var samples []msgsampling.MessageSample
			for _, msg := range messages {
				sample := msgsampling.MessageSample{
					HeaderCount: len(msg.Headers),
				}

				// Compute key info
				if msg.Key != nil {
					keyBytes, _ := json.Marshal(msg.Key)
					sample.HasKey = true
					sample.KeySize = len(keyBytes)
					h := sha256.Sum256(keyBytes)
					sample.KeyHash = hex.EncodeToString(h[:8])
				}

				// Compute value info
				if msg.Value != nil {
					valBytes, _ := json.Marshal(msg.Value)
					sample.ValueSize = len(valBytes)
					sample.SchemaType = detectSchemaType(msg.Value)
				}

				samples = append(samples, sample)
			}

			result := s.msgSamplingEngine.Profile(topic, samples)
			s.logAudit(ctx, agentID, "kafka_message_sample", args, cluster, "success", len(samples), nil, start)
			return marshalResult(result)

		case "cached":
			topic := request.GetString("topic", "")
			if topic == "" {
				return mcp.NewToolResultError("topic is required"), nil
			}
			cached := s.msgSamplingEngine.GetCachedProfile(topic)
			if cached == nil {
				return marshalResult(map[string]any{"topic": topic, "cached": false, "message": "No cached profile. Use action=sample first."})
			}
			return marshalResult(cached)

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q, use: sample, cached", action)), nil
		}
	})(ctx, request)
}

func detectSchemaType(value any) string {
	switch value.(type) {
	case map[string]any:
		return "json"
	case []any:
		return "json"
	case string:
		return "string"
	case float64, int, int64:
		return "string"
	default:
		return "binary"
	}
}
