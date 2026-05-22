package server

import (
	"testing"

	"github.com/mark3labs/mcp-go/server"
)

// TestRegisterAllTools ensures every registerXxxTools function runs without panic
// against a Server with a MockKafkaManager. This exercises the AddTool plumbing
// for the full surface (~300+ tools) and bumps line coverage in register_*.go.
func TestRegisterAllTools(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)

	s.mcpServer = server.NewMCPServer(
		"KafkaMCP-test",
		"test",
		server.WithToolCapabilities(true),
		server.WithResourceCapabilities(true, true),
	)
	s.registeredTools = make(map[string]string)

	s.registerTools()
	s.registerResources()
}
