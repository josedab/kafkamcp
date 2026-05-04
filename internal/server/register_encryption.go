package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerEncryptionTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_encryption_key",
		withCategory("security"),
		mcp.WithDescription("Manage encryption keys: create, rotate, revoke, list, or get key details. Supports AES-256-GCM and AES-128-GCM with local, Vault, or KMS providers."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: create, rotate, revoke, list, get")),
		mcp.WithString("key_id", mcp.Description("Key ID (for rotate/revoke/get)")),
		mcp.WithString("name", mcp.Description("Key name (for create)")),
		mcp.WithString("provider", mcp.Description("Provider: local, vault, aws_kms, gcp_kms. Default: local")),
		mcp.WithString("algorithm", mcp.Description("Algorithm: AES-256-GCM, AES-128-GCM. Default: AES-256-GCM")),
	), s.handleEncryptionKey)

	s.mcpServer.AddTool(mcp.NewTool("kafka_encryption_configure",
		withCategory("security"),
		mcp.WithDescription("Configure field-level encryption for a topic or set per-agent encryption policies."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: configure_fields, set_policy, get_policy")),
		mcp.WithString("topic", mcp.Description("Topic name (for configure_fields)")),
		mcp.WithString("key_id", mcp.Description("Key ID (for configure_fields)")),
		mcp.WithString("agent_id", mcp.Description("Agent ID (for set_policy/get_policy)")),
	), s.handleEncryptionConfigure)

	s.mcpServer.AddTool(mcp.NewTool("kafka_encryption_process",
		withCategory("security"),
		mcp.WithDescription("Encrypt or decrypt message fields transparently. Uses configured field-level encryption with per-agent key policies."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: encrypt, decrypt")),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
		mcp.WithString("message", mcp.Required(), mcp.Description("JSON message to encrypt/decrypt")),
	), s.handleEncryptionProcess)
}
