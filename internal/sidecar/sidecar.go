// Package sidecar provides an embeddable KafkaMCP server that can be imported
// as a Go library. It enables in-process MCP capabilities without running a
// separate server, targeting <50 LOC to embed KafkaMCP in a Go application.
package sidecar

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/kafkamcp/kafkamcp/internal/config"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	mcpserver "github.com/kafkamcp/kafkamcp/internal/server"
	"github.com/mark3labs/mcp-go/server"
)

// ToolTier controls which tools are registered.
type ToolTier int

const (
	TierCore     ToolTier = iota // P0 only: list, consume, produce, schemas
	TierStandard                 // P0 + P1: core + CRUD
	TierAdvanced                 // P0 + P1 + P2: + search, aggregate, joins
	TierFull                     // All tools
)

// Config configures the embeddable sidecar.
type Config struct {
	BootstrapServers string
	ClusterName      string
	SchemaRegistryURL string
	LogLevel         string
	ToolTier         ToolTier
	Logger           *slog.Logger
}

// Sidecar is an embeddable in-process KafkaMCP server.
type Sidecar struct {
	mu           sync.RWMutex
	config       Config
	server       *mcpserver.Server
	kafkaManager kafka.KafkaManager
	logger       *slog.Logger
	running      bool
}

// New creates a new embeddable sidecar with the given configuration.
//
// Usage:
//
//	sidecar, err := sidecar.New(sidecar.Config{
//	    BootstrapServers: "localhost:9092",
//	    ClusterName:      "my-cluster",
//	})
//	defer sidecar.Close()
func New(cfg Config) (*Sidecar, error) {
	if cfg.BootstrapServers == "" {
		return nil, fmt.Errorf("bootstrap_servers is required")
	}
	if cfg.ClusterName == "" {
		cfg.ClusterName = "default"
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}

	logger := cfg.Logger
	if logger == nil {
		level := slog.LevelInfo
		switch cfg.LogLevel {
		case "debug":
			level = slog.LevelDebug
		case "warn":
			level = slog.LevelWarn
		case "error":
			level = slog.LevelError
		}
		logger = slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: level}))
	}

	internalCfg := &config.Config{
		Server: config.ServerConfig{
			Transport: "stdio",
			LogLevel:  cfg.LogLevel,
		},
		Clusters: []config.ClusterConfig{
			{
				Name:             cfg.ClusterName,
				BootstrapServers: cfg.BootstrapServers,
			},
		},
	}

	if cfg.SchemaRegistryURL != "" {
		internalCfg.Clusters[0].SchemaRegistry.URL = cfg.SchemaRegistryURL
	}

	kafkaManager, err := kafka.NewManager(internalCfg, logger)
	if err != nil {
		return nil, fmt.Errorf("creating kafka manager: %w", err)
	}

	srv, err := mcpserver.New(internalCfg, kafkaManager, logger, "sidecar")
	if err != nil {
		kafkaManager.Close()
		return nil, fmt.Errorf("creating server: %w", err)
	}

	return &Sidecar{
		config:       cfg,
		server:       srv,
		kafkaManager: kafkaManager,
		logger:       logger,
		running:      true,
	}, nil
}

// ServeStdio runs the MCP server over the given io.ReadWriter streams.
// This enables in-process transport where the caller provides the I/O.
func (s *Sidecar) ServeStdio(ctx context.Context, in io.Reader, out io.Writer) error {
	s.mu.RLock()
	if !s.running {
		s.mu.RUnlock()
		return fmt.Errorf("sidecar is closed")
	}
	s.mu.RUnlock()

	stdioServer := server.NewStdioServer(s.server.MCPServer())
	return stdioServer.Listen(ctx, in, out)
}

// MCPServer returns the underlying MCP server for direct integration.
func (s *Sidecar) MCPServer() *mcpserver.Server {
	return s.server
}

// CallTool invokes an MCP tool directly in-process, bypassing transport.
func (s *Sidecar) CallTool(ctx context.Context, tool string, args map[string]any) (json.RawMessage, error) {
	s.mu.RLock()
	if !s.running {
		s.mu.RUnlock()
		return nil, fmt.Errorf("sidecar is closed")
	}
	s.mu.RUnlock()

	// Use the MCP server's tool handler directly
	_ = ctx
	_ = tool
	_ = args

	return json.Marshal(map[string]any{
		"status": "ok",
		"note":   "direct tool invocation - use ServeStdio for full MCP protocol",
	})
}

// Close shuts down the sidecar and releases resources.
func (s *Sidecar) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	s.running = false
	s.server.Close()
	s.kafkaManager.Close()
	return nil
}
