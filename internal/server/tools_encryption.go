package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/encryption"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleEncryptionKey(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_encryption_key", resourceType: auth.ResourceCluster, action: auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "list")
		keyID := request.GetString("key_id", "")

		switch action {
		case "create":
			name := request.GetString("name", "")
			if name == "" {
				return mcp.NewToolResultError("name is required"), nil
			}
			provider := encryption.KeyProvider(request.GetString("provider", "local"))
			algorithm := request.GetString("algorithm", "AES-256-GCM")

			key, err := s.encryptionEngine.CreateKey(name, provider, algorithm)
			if err != nil {
				return s.toolError("failed to create key", err), nil
			}
			s.logAudit(agentID, "kafka_encryption_key", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"key": key, "message": "Key created successfully"})

		case "rotate":
			if keyID == "" {
				return mcp.NewToolResultError("key_id is required"), nil
			}
			result, err := s.encryptionEngine.RotateKey(keyID)
			if err != nil {
				return s.toolError("rotation failed", err), nil
			}
			s.logAudit(agentID, "kafka_encryption_key", args, "", "success", 1, nil, start)
			return marshalResult(result)

		case "revoke":
			if keyID == "" {
				return mcp.NewToolResultError("key_id is required"), nil
			}
			if err := s.encryptionEngine.RevokeKey(keyID); err != nil {
				return s.toolError("revocation failed", err), nil
			}
			return marshalResult(map[string]any{"status": "revoked", "key_id": keyID})

		case "get":
			if keyID == "" {
				return mcp.NewToolResultError("key_id is required"), nil
			}
			key, err := s.encryptionEngine.GetKey(keyID)
			if err != nil {
				return s.toolError("key not found", err), nil
			}
			return marshalResult(key)

		default:
			keys := s.encryptionEngine.ListKeys()
			return marshalResult(map[string]any{"keys": keys, "total": len(keys)})
		}
	})(ctx, request)
}

func (s *Server) handleEncryptionConfigure(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_encryption_configure", resourceType: auth.ResourceCluster, action: auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "configure_fields")

		switch action {
		case "configure_fields":
			topic := request.GetString("topic", "")
			keyID := request.GetString("key_id", "")
			if topic == "" || keyID == "" {
				return mcp.NewToolResultError("topic and key_id are required"), nil
			}
			config, err := s.encryptionEngine.ConfigureFieldEncryption(topic, keyID, []string{"*"})
			if err != nil {
				return s.toolError("configuration failed", err), nil
			}
			return marshalResult(config)

		case "set_policy":
			targetAgent := request.GetString("agent_id", "")
			if targetAgent == "" {
				return mcp.NewToolResultError("agent_id is required"), nil
			}
			policy, err := s.encryptionEngine.SetAgentPolicy(targetAgent, nil, []string{"*"}, true, true)
			if err != nil {
				return s.toolError("failed to set policy", err), nil
			}
			return marshalResult(policy)

		case "get_policy":
			targetAgent := request.GetString("agent_id", "")
			if targetAgent == "" {
				return mcp.NewToolResultError("agent_id is required"), nil
			}
			policy, err := s.encryptionEngine.GetAgentPolicy(targetAgent)
			if err != nil {
				return s.toolError("policy not found", err), nil
			}
			return marshalResult(policy)

		default:
			return mcp.NewToolResultError("action must be: configure_fields, set_policy, get_policy"), nil
		}
	})(ctx, request)
}

func (s *Server) handleEncryptionProcess(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_encryption_process", resourceType: auth.ResourceTopic, action: auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "encrypt")
		topic := request.GetString("topic", "")
		message := request.GetString("message", "")

		if topic == "" || message == "" {
			return mcp.NewToolResultError("topic and message are required"), nil
		}
		if deny := validateStringLength("message", message); deny != nil {
			return deny, nil
		}

		switch action {
		case "encrypt":
			result, err := s.encryptionEngine.EncryptMessage(agentID, topic, message)
			if err != nil {
				return s.toolError("encryption failed", err), nil
			}
			s.logAudit(agentID, "kafka_encryption_process", args, "", "success", result.EncryptedFields, nil, start)
			return marshalResult(result)

		case "decrypt":
			result, err := s.encryptionEngine.DecryptMessage(agentID, topic, message)
			if err != nil {
				return s.toolError("decryption failed", err), nil
			}
			s.logAudit(agentID, "kafka_encryption_process", args, "", "success", result.DecryptedFields, nil, start)
			return marshalResult(result)

		default:
			return mcp.NewToolResultError("action must be: encrypt, decrypt"), nil
		}
	})(ctx, request)
}
