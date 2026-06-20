package server

import (
	"context"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"

	"github.com/josedab/kafkamcp/internal/config"
)

func noopHandler(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return mcp.NewToolResultText("ok"), nil
}

func newBareServer() *Server {
	return &Server{
		mcpServer:       mcpserver.NewMCPServer("test", "0.0.0", mcpserver.WithToolCapabilities(true)),
		registeredTools: make(map[string]string),
		seenToolNames:   make(map[string]struct{}),
	}
}

func TestAddTool_UniqueNamesNoError(t *testing.T) {
	s := newBareServer()
	s.addTool(mcp.NewTool("kafka_one"), noopHandler)
	s.addTool(mcp.NewTool("kafka_two"), noopHandler)
	if err := s.DuplicateToolError(); err != nil {
		t.Fatalf("unexpected duplicate error: %v", err)
	}
	if s.RegisteredToolCount() != 2 {
		t.Fatalf("expected 2 registered tools, got %d", s.RegisteredToolCount())
	}
}

func TestAddTool_DuplicateNameFailsClosed(t *testing.T) {
	s := newBareServer()
	s.addTool(mcp.NewTool("kafka_dup"), noopHandler)
	s.addTool(mcp.NewTool("kafka_dup"), noopHandler)
	err := s.DuplicateToolError()
	if err == nil {
		t.Fatal("expected a duplicate registration error")
	}
	if got := err.Error(); got == "" || !contains(got, "kafka_dup") {
		t.Fatalf("error should name the duplicate tool, got: %v", got)
	}
}

func TestAddTool_DuplicateDetectedEvenWhenFiltered(t *testing.T) {
	// Even if the pack filter would skip a tool, a duplicate name must still be
	// detected because it is a code bug regardless of the active pack.
	s := newBareServer()
	s.toolPackFilter = NewToolPackFilter([]string{"core-read"}) // filters out kafka_dup
	s.addTool(mcp.NewTool("kafka_dup"), noopHandler)
	s.addTool(mcp.NewTool("kafka_dup"), noopHandler)
	if err := s.DuplicateToolError(); err == nil {
		t.Fatal("expected duplicate detection to work even for filtered tools")
	}
}

func TestDecorateWriteToolAddsApprovalTokenSchema(t *testing.T) {
	tool := mcp.NewTool("kafka_delete_topic", mcp.WithString("topic", mcp.Required()))
	decorateWriteTool(&tool)
	property, ok := tool.InputSchema.Properties[approvalTokenArg].(map[string]any)
	if !ok {
		t.Fatalf("write tool must advertise %s", approvalTokenArg)
	}
	if property["type"] != "string" {
		t.Fatalf("approval token must be a string schema, got %#v", property)
	}

	readTool := mcp.NewTool("kafka_list_topics")
	decorateWriteTool(&readTool)
	if _, ok := readTool.InputSchema.Properties[approvalTokenArg]; ok {
		t.Fatal("read-only tool must not advertise an approval token")
	}
}

func TestExperimentalPackAndSimulatedToolsRemoved(t *testing.T) {
	// The experimental pack no longer exists; requesting it yields no tools,
	// and simulated tool names are never registerable.
	s := newBareServer()
	s.config = &config.Config{}
	s.toolPackFilter = NewToolPackFilter([]string{"experimental"})
	s.addTool(mcp.NewTool("kafka_proxy"), noopHandler)
	s.addTool(mcp.NewTool("kafka_wasm_execute"), noopHandler)
	if s.RegisteredToolCount() != 0 {
		t.Fatal("removed simulated tools must never register")
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
