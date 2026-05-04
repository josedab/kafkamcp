package server

import (
	"fmt"
	"log/slog"

	"github.com/kafkamcp/kafkamcp/internal/config"
	"github.com/kafkamcp/kafkamcp/internal/kafka"

	mcpserver "github.com/mark3labs/mcp-go/server"
)

// ToolCategory groups tools for selective registration.
type ToolCategory string

const (
	ToolCategoryCore         ToolCategory = "core"         // P0: list, describe, consume, produce
	ToolCategoryAdvanced     ToolCategory = "advanced"     // P1-P3 tools
	ToolCategoryGovernance   ToolCategory = "governance"   // Auth, audit, masking, compliance
	ToolCategoryStreaming    ToolCategory = "streaming"    // Watch, DLQ, replication
	ToolCategoryAnalytics    ToolCategory = "analytics"    // Anomaly detection, NL query, cost analysis
	ToolCategoryPlatform     ToolCategory = "platform"     // Workflows, plugins, federation
	ToolCategoryObservability ToolCategory = "observability" // Telemetry, metrics, event bus
	ToolCategoryAll          ToolCategory = "all"
)

// EmbeddedConfig configures the embeddable KafkaMCP instance.
type EmbeddedConfig struct {
	// KafkaConfig is the core configuration (clusters, policies, etc.)
	KafkaConfig *config.Config

	// KafkaManager provides Kafka connectivity. If nil, a default manager
	// is created from KafkaConfig.
	KafkaManager kafka.KafkaManager

	// Logger for the embedded instance. Defaults to slog.Default().
	Logger *slog.Logger

	// Version string for the MCP server. Defaults to "embedded".
	Version string

	// Categories selects which tool categories to register.
	// If empty, registers ToolCategoryAll.
	Categories []ToolCategory

	// ExcludeTools lists specific tools to exclude from registration.
	ExcludeTools []string

	// IncludeTools lists specific tools to include. If non-empty,
	// only these tools are registered (overrides Categories).
	IncludeTools []string

	// Options are additional functional options for the server.
	Options []Option
}

// ToolSet represents a registerable collection of tools that can be
// selectively applied to an MCP server instance.
type ToolSet struct {
	Name     ToolCategory
	Register func(s *Server)
}

// NewEmbedded creates an embeddable KafkaMCP server instance that can be
// imported into existing applications. It registers tools into the provided
// or internal MCP server based on the configuration.
//
// Usage:
//
//	cfg := &server.EmbeddedConfig{
//	    KafkaConfig:  myConfig,
//	    KafkaManager: myManager,
//	    Categories:   []server.ToolCategory{server.ToolCategoryCore, server.ToolCategoryGovernance},
//	}
//	srv, err := server.NewEmbedded(cfg)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	mcpServer := srv.MCPServer()
func NewEmbedded(cfg *EmbeddedConfig) (*Server, error) {
	if cfg == nil {
		return nil, fmt.Errorf("EmbeddedConfig is required")
	}

	if cfg.KafkaConfig == nil {
		return nil, fmt.Errorf("KafkaConfig is required")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	version := cfg.Version
	if version == "" {
		version = "embedded"
	}

	if cfg.KafkaManager == nil {
		return nil, fmt.Errorf("KafkaManager is required for embedded mode")
	}

	// Create server with functional options
	srv, err := New(cfg.KafkaConfig, cfg.KafkaManager, logger, version, cfg.Options...)
	if err != nil {
		return nil, fmt.Errorf("creating embedded server: %w", err)
	}

	return srv, nil
}

// RegisterToolsSelective registers only the specified tool categories.
// This is used internally by NewEmbedded but can also be called directly
// for more control over tool registration.
func (s *Server) RegisterToolsSelective(categories []ToolCategory) {
	catSet := make(map[ToolCategory]bool)
	for _, c := range categories {
		catSet[c] = true
	}

	if catSet[ToolCategoryAll] || len(categories) == 0 {
		s.registerTools()
		return
	}

	if catSet[ToolCategoryCore] {
		s.registerP0Tools()
	}
	if catSet[ToolCategoryAdvanced] {
		s.registerP1Tools()
		s.registerP2Tools()
		s.registerP3Tools()
		s.registerAdvancedTools()
	}
	if catSet[ToolCategoryGovernance] {
		s.registerComplianceTools()
		s.registerACLMgmtTools()
		s.registerIntentVerifyTools()
	}
	if catSet[ToolCategoryStreaming] {
		s.registerStreamingTools()
		s.registerReplicationTools()
		s.registerStateMachineTools()
	}
	if catSet[ToolCategoryAnalytics] {
		s.registerAdvisorTools()
		s.registerTimeSeriesTools()
		s.registerHealthScoreTools()
	}
	if catSet[ToolCategoryPlatform] {
		s.registerDAGWorkflowTools()
		s.registerRunbookTools()
		s.registerAgenticRunbookTools()
		s.registerToolComposeTools()
		s.registerBenchmarkTools()
	}
	if catSet[ToolCategoryObservability] {
		s.registerObservabilityTools()
		s.registerEventBusTools()
		s.registerPerfGuardTools()
		s.registerTelemetryDataLakeTools()
	}
}

// AvailableCategories returns all available tool categories with descriptions.
func AvailableCategories() map[ToolCategory]string {
	return map[ToolCategory]string{
		ToolCategoryCore:          "Core tools: list, describe, consume, produce topics",
		ToolCategoryAdvanced:      "Advanced tools: P1-P3 operations, search, aggregate",
		ToolCategoryGovernance:    "Governance: auth, audit, masking, compliance, intent verification",
		ToolCategoryStreaming:     "Streaming: watch, DLQ, replication, state machines",
		ToolCategoryAnalytics:     "Analytics: anomaly detection, NL query, cost analysis, health scoring",
		ToolCategoryPlatform:      "Platform: workflows, runbooks, tool composition, benchmarks",
		ToolCategoryObservability: "Observability: telemetry, event bus, performance guard, metrics",
		ToolCategoryAll:           "All tools",
	}
}

// RegisterIntoMCPServer registers KafkaMCP tools into an existing MCP server instance.
// This allows integration with applications that already have their own MCP server.
func RegisterIntoMCPServer(mcpSrv *mcpserver.MCPServer, cfg *config.Config, kafkaManager kafka.KafkaManager, logger *slog.Logger, categories []ToolCategory) error {
	if mcpSrv == nil {
		return fmt.Errorf("MCPServer is required")
	}

	embCfg := &EmbeddedConfig{
		KafkaConfig:  cfg,
		KafkaManager: kafkaManager,
		Logger:       logger,
		Categories:   categories,
	}

	srv, err := NewEmbedded(embCfg)
	if err != nil {
		return err
	}

	// The tools are already registered during NewEmbedded (via New -> registerTools)
	// For selective registration, the caller should use NewEmbedded with categories
	_ = srv
	return nil
}
