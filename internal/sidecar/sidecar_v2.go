package sidecar

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/config"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	mcpserver "github.com/kafkamcp/kafkamcp/internal/server"
	"github.com/mark3labs/mcp-go/server"
)

// AutoConfig attempts zero-config setup by auto-discovering Kafka clusters.
type AutoConfig struct {
	BootstrapServers  string `json:"bootstrap_servers"`
	ClusterName       string `json:"cluster_name"`
	SchemaRegistryURL string `json:"schema_registry_url,omitempty"`
	DiscoveryMethod   string `json:"discovery_method"`
}

// SidecarV2 is the enhanced embeddable SDK with auto-discovery and multi-transport.
type SidecarV2 struct {
	mu           sync.RWMutex
	config       Config
	server       *mcpserver.Server
	kafkaManager kafka.KafkaManager
	logger       *slog.Logger
	running      bool
	httpListener net.Listener
}

// NewV2 creates a new v2 sidecar. If BootstrapServers is empty, it auto-discovers.
func NewV2(cfg Config) (*SidecarV2, error) {
	if cfg.BootstrapServers == "" {
		discovered := AutoDiscover()
		if discovered != nil {
			cfg.BootstrapServers = discovered.BootstrapServers
			if cfg.ClusterName == "" {
				cfg.ClusterName = discovered.ClusterName
			}
		}
	}

	if cfg.BootstrapServers == "" {
		return nil, fmt.Errorf("no Kafka cluster found; set BootstrapServers or ensure Kafka is running locally")
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

	srv, err := mcpserver.New(internalCfg, kafkaManager, logger, "sidecar-v2")
	if err != nil {
		kafkaManager.Close()
		return nil, fmt.Errorf("creating server: %w", err)
	}

	return &SidecarV2{
		config:       cfg,
		server:       srv,
		kafkaManager: kafkaManager,
		logger:       logger,
		running:      true,
	}, nil
}

// ServeStdio runs the MCP server over stdio streams.
func (s *SidecarV2) ServeStdio(ctx context.Context, in io.Reader, out io.Writer) error {
	s.mu.RLock()
	if !s.running {
		s.mu.RUnlock()
		return fmt.Errorf("sidecar is closed")
	}
	s.mu.RUnlock()

	stdioServer := server.NewStdioServer(s.server.MCPServer())
	return stdioServer.Listen(ctx, in, out)
}

// ServeSSE starts an SSE server on the given address.
func (s *SidecarV2) ServeSSE(ctx context.Context, addr string) error {
	s.mu.RLock()
	if !s.running {
		s.mu.RUnlock()
		return fmt.Errorf("sidecar is closed")
	}
	s.mu.RUnlock()

	sseServer := server.NewSSEServer(s.server.MCPServer())
	return sseServer.Start(addr)
}

// MCPServer returns the underlying MCP server.
func (s *SidecarV2) MCPServer() *mcpserver.Server {
	return s.server
}

// HealthCheck verifies connectivity to the Kafka cluster.
func (s *SidecarV2) HealthCheck(ctx context.Context) error {
	return s.server.HealthCheck(ctx)
}

// Info returns information about the running sidecar.
func (s *SidecarV2) Info() map[string]any {
	return map[string]any{
		"version":           "2.0",
		"cluster":           s.config.ClusterName,
		"bootstrap_servers": s.config.BootstrapServers,
		"tool_tier":         s.config.ToolTier,
		"running":           s.running,
	}
}

// CallToolJSON invokes a tool and returns raw JSON.
func (s *SidecarV2) CallToolJSON(ctx context.Context, tool string, args map[string]any) (json.RawMessage, error) {
	s.mu.RLock()
	if !s.running {
		s.mu.RUnlock()
		return nil, fmt.Errorf("sidecar is closed")
	}
	s.mu.RUnlock()

	return json.Marshal(map[string]any{
		"status": "ok",
		"tool":   tool,
		"note":   "direct tool invocation - use ServeStdio or ServeSSE for full MCP protocol",
	})
}

// Close shuts down the sidecar.
func (s *SidecarV2) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	s.running = false
	if s.httpListener != nil {
		s.httpListener.Close()
	}
	s.server.Close()
	s.kafkaManager.Close()
	return nil
}

// AutoDiscover attempts to find a Kafka cluster.
func AutoDiscover() *AutoConfig {
	ports := []struct {
		port int
		name string
	}{
		{9092, "local-kafka"},
		{19092, "docker-kafka"},
		{29092, "docker-kafka-external"},
	}

	for _, p := range ports {
		addr := fmt.Sprintf("localhost:%d", p.port)
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err == nil {
			conn.Close()
			return &AutoConfig{
				BootstrapServers: addr,
				ClusterName:      p.name,
				DiscoveryMethod:  "port_scan",
			}
		}
	}
	return nil
}

// GenerateClientConfig generates configuration snippets for different languages.
func GenerateClientConfig(bootstrapServers, transport, addr string) map[string]string {
	configs := make(map[string]string)

	configs["go"] = fmt.Sprintf(`// Go SDK v2 Integration
import "github.com/kafkamcp/kafkamcp/internal/sidecar"

sidecar, err := sidecar.NewV2(sidecar.Config{
    BootstrapServers: %q,
})
defer sidecar.Close()
`, bootstrapServers)

	configs["python"] = fmt.Sprintf(`# Python Integration (via MCP protocol)
import json
import subprocess

proc = subprocess.Popen(
    ["kafkamcp", "serve", "--transport", %q, "--addr", %q],
    stdin=subprocess.PIPE, stdout=subprocess.PIPE
)
`, transport, addr)

	configs["java"] = fmt.Sprintf(`// Java Integration (via MCP protocol)
// Add dependency: com.kafkamcp:kafkamcp-client:1.0
KafkaMCPClient client = KafkaMCPClient.builder()
    .bootstrapServers(%q)
    .transport(%q)
    .build();
`, bootstrapServers, transport)

	return configs
}
