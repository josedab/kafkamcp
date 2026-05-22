package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/config"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	mcpserver "github.com/kafkamcp/kafkamcp/internal/server"
	"github.com/mark3labs/mcp-go/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
)

var (
	version  = "0.1.0"
	cfgFile  string
	logLevel string
	debug    bool
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "kafkamcp",
		Short: "KafkaMCP — MCP server for Apache Kafka",
		Long: `KafkaMCP is an open source MCP (Model Context Protocol) server that makes
Apache Kafka topics fully readable, writable, and discoverable by AI agents.

It exposes Kafka topics, consumer groups, schemas, and cluster metadata as
MCP tools and resources that any MCP-compatible AI agent can discover, invoke,
and compose — with zero custom integration code.`,
		RunE: runServer,
	}

	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "kafkamcp.yaml", "Path to configuration file")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "", "Override log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().BoolVar(&debug, "debug", false, "Enable debug logging (shorthand for --log-level debug)")

	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print the version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("kafkamcp %s\n", version)
		},
	})

	rootCmd.AddCommand(&cobra.Command{
		Use:   "validate",
		Short: "Validate the configuration file",
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := config.Load(cfgFile)
			if err != nil {
				return fmt.Errorf("configuration validation failed: %w", err)
			}
			fmt.Println("Configuration is valid.")
			return nil
		},
	})

	rootCmd.AddCommand(&cobra.Command{
		Use:   "validate-config [file]",
		Short: "Validate a configuration file (accepts path as argument or --config flag)",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := cfgFile
			if len(args) > 0 {
				path = args[0]
			}
			cfg, err := config.Load(path)
			if err != nil {
				return fmt.Errorf("configuration validation failed: %w", err)
			}
			fmt.Printf("Configuration %s is valid.\n", path)
			fmt.Printf("  Transport: %s\n", cfg.Server.Transport)
			fmt.Printf("  Clusters:  %d\n", len(cfg.Clusters))
			for i, c := range cfg.Clusters {
				fmt.Printf("    [%d] %s (%s)\n", i, c.Name, c.BootstrapServers)
			}
			if cfg.Policies.DefaultDeny {
				fmt.Printf("  Policy:    default_deny=true (%d agent rules)\n", len(cfg.Policies.Agents))
			} else {
				fmt.Printf("  Policy:    default_deny=false (open access)\n")
			}
			if cfg.Masking.Enabled {
				fmt.Printf("  Masking:   enabled (%d rules)\n", len(cfg.Masking.Rules))
			}
			return nil
		},
	})

	initCmd := &cobra.Command{
		Use:   "init",
		Short: "Generate a starter configuration file",
		Long:  `Generate a minimal kafkamcp.yaml to stdout (or to a file with --output).`,
		RunE:  runConfigInit,
	}
	initCmd.Flags().StringP("output", "o", "", "Write config to file instead of stdout")
	initCmd.Flags().String("transport", "stdio", "Transport type (stdio, sse, streamable-http)")
	initCmd.Flags().String("bootstrap-servers", "localhost:9092", "Kafka bootstrap servers")
	initCmd.Flags().String("schema-registry", "", "Schema Registry URL (optional)")
	initCmd.Flags().Bool("claude-desktop", false, "Output a claude_desktop_config.json snippet for Claude Desktop integration")
	rootCmd.AddCommand(initCmd)

	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the MCP server (alias for root command)",
		Long:  `Start the KafkaMCP server. This is equivalent to running kafkamcp without a subcommand.`,
		RunE:  runServer,
	}
	rootCmd.AddCommand(serveCmd)

	devCmd := &cobra.Command{
		Use:   "dev",
		Short: "Start a local development environment with sample data",
		Long: `Start KafkaMCP in local development mode with pre-populated sample topics
and guided tutorials. Requires a local Kafka broker (or use 'make docker-dev').

This mode creates sample topics (orders, user-events, iot-sensors, click-stream,
payments) and provides interactive tutorials for exploring KafkaMCP capabilities.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("🚀 KafkaMCP Development Mode")
			fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━")
			fmt.Println()
			fmt.Println("Quick Start:")
			fmt.Println("  1. Ensure a local Kafka broker is running (make docker-dev)")
			fmt.Println("  2. Connect with your MCP client")
			fmt.Println("  3. Call kafka_dev_environment for setup info")
			fmt.Println("  4. Call kafka_dev_tutorial action=list to see tutorials")
			fmt.Println()
			fmt.Println("Sample Topics: orders, user-events, iot-sensors, click-stream, payments")
			fmt.Println("Tutorials:     getting-started, anomaly-detection, schema-management")
			fmt.Println()

			// Run the normal server
			return runServer(cmd, args)
		},
	}
	rootCmd.AddCommand(devCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func runConfigInit(cmd *cobra.Command, args []string) error {
	transport, err := cmd.Flags().GetString("transport")
	if err != nil {
		return fmt.Errorf("reading flag: %w", err)
	}
	bootstrap, err := cmd.Flags().GetString("bootstrap-servers")
	if err != nil {
		return fmt.Errorf("reading flag: %w", err)
	}
	schemaReg, err := cmd.Flags().GetString("schema-registry")
	if err != nil {
		return fmt.Errorf("reading flag: %w", err)
	}
	output, err := cmd.Flags().GetString("output")
	if err != nil {
		return fmt.Errorf("reading flag: %w", err)
	}
	claudeDesktop, err := cmd.Flags().GetBool("claude-desktop")
	if err != nil {
		return fmt.Errorf("reading flag: %w", err)
	}

	// Handle --claude-desktop: output Claude Desktop config JSON
	if claudeDesktop {
		return runClaudeDesktopInit(cmd, bootstrap, output)
	}

	switch transport {
	case "stdio", "sse", "streamable-http":
	default:
		return fmt.Errorf("unsupported transport %q; must be: stdio, sse, streamable-http", transport)
	}

	var schemaCfg string
	if schemaReg != "" {
		schemaCfg = fmt.Sprintf("\n    schema_registry:\n      url: %s", schemaReg)
	}

	cfg := fmt.Sprintf(`# KafkaMCP configuration — generated by kafkamcp init
# Validate with: kafkamcp validate --config <file>
server:
  transport: %s
  host: 0.0.0.0
  port: 3100
  log_level: info

clusters:
  - name: local
    bootstrap_servers: %s%s
`, transport, bootstrap, schemaCfg)

	if output != "" {
		if err := os.WriteFile(output, []byte(cfg), 0o644); err != nil {
			return fmt.Errorf("writing config to %s: %w", output, err)
		}
		fmt.Fprintf(os.Stderr, "Config written to %s\n", output)
		return nil
	}

	fmt.Print(cfg)
	return nil
}

func runClaudeDesktopInit(cmd *cobra.Command, bootstrap, output string) error {
	// Find the kafkamcp binary path
	binaryPath, err := os.Executable()
	if err != nil {
		binaryPath = "kafkamcp"
	}

	claudeConfig := map[string]any{
		"mcpServers": map[string]any{
			"kafkamcp": map[string]any{
				"command": binaryPath,
				"args":    []string{"--config", "kafkamcp.yaml"},
				"env": map[string]string{
					"KAFKA_BROKERS": bootstrap,
				},
			},
		},
	}

	data, err := json.MarshalIndent(claudeConfig, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling config: %w", err)
	}

	if output != "" {
		if err := os.WriteFile(output, data, 0o644); err != nil {
			return fmt.Errorf("writing config to %s: %w", output, err)
		}
		fmt.Fprintf(os.Stderr, "Claude Desktop config written to %s\n", output)
		return nil
	}

	fmt.Println(string(data))
	return nil
}

func runServer(cmd *cobra.Command, args []string) error {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("loading configuration: %w", err)
	}

	// CLI flag overrides config file log level
	if debug && logLevel == "" {
		logLevel = "debug"
	}
	if logLevel != "" {
		cfg.Server.LogLevel = logLevel
	}

	// Environment variable overrides config file transport
	if t := os.Getenv("KAFKAMCP_TRANSPORT"); t != "" {
		cfg.Server.Transport = t
	}

	// Environment variable overrides config file log level
	if l := os.Getenv("KAFKAMCP_LOG_LEVEL"); l != "" && logLevel == "" {
		cfg.Server.LogLevel = l
	}

	level := slog.LevelInfo
	switch cfg.Server.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	logger.Info("starting KafkaMCP", "version", version, "transport", cfg.Server.Transport)

	kafkaManager, err := kafka.NewManager(cfg, logger)
	if err != nil {
		if isConnectionRefused(err) {
			return fmt.Errorf("cannot connect to Kafka at %s: %w\n\n"+
				"Hint: Run 'make docker-dev' to start a local Kafka cluster, "+
				"or verify that bootstrap_servers in your config points to a running broker.",
				cfg.Clusters[0].BootstrapServers, err)
		}
		return fmt.Errorf("creating kafka manager: %w", err)
	}

	srv, err := mcpserver.New(cfg, kafkaManager, logger, version)
	if err != nil {
		return fmt.Errorf("creating server: %w", err)
	}
	defer srv.Close()

	logger.Info("tool packs configured",
		"packs", cfg.Server.ToolPacks,
		"registered_tools", srv.RegisteredToolCount(),
		"skipped_tools", srv.SkippedToolCount())

	if !cfg.Policies.DefaultDeny {
		logger.Warn("running with default_deny=false — all agents have unrestricted access; enable policies.default_deny for production use")

		hasRateLimits := false
		for _, agent := range cfg.Policies.Agents {
			if agent.RateLimit.RequestsPerMinute > 0 {
				hasRateLimits = true
				break
			}
		}
		if !hasRateLimits {
			logger.Warn("INSECURE DEFAULTS: default_deny is false and no rate limits are configured — this installation has zero access control; configure policies for production use")
		}
	}

	if cfg.Experimental.Chaos {
		logger.Warn("EXPERIMENTAL: chaos engineering tool enabled — current implementation records metadata only; it does NOT actually disrupt Kafka traffic")
	}
	if cfg.Experimental.WASMRuntime {
		logger.Warn("EXPERIMENTAL: WASM runtime enabled — current implementation does NOT execute WebAssembly modules; outputs are simulated")
	}
	if cfg.Experimental.KafkaProxy {
		logger.Warn("EXPERIMENTAL: Kafka proxy enabled — without a wired MCP translation callback the proxy returns hardcoded responses; do NOT point real Kafka clients at it in production")
	}
	if cfg.Experimental.Migration {
		logger.Warn("EXPERIMENTAL: migration execute/rollback enabled — current build's migration.ExecutePlan does NOT actually apply changes to the target cluster; results are simulated success counters")
	}
	if cfg.Experimental.ControlPlane {
		logger.Warn("EXPERIMENTAL: control plane enabled — kafka_controlplane_* tools fabricate org/instance/billing data; there is no real SaaS layer behind them")
	}
	if cfg.Experimental.Marketplace {
		logger.Warn("EXPERIMENTAL: plugin marketplace enabled — kafka_plugin_install only flips an in-memory boolean; it does NOT download, verify, or sandbox a real plugin")
	}

	// Persistence reality check (F15): all subsystems below keep state in
	// process memory only. Surface this once at startup so operators are not
	// surprised when restart wipes encryption keys, audit ring-buffers,
	// rate-limit buckets, sessions, etc.
	logger.Warn("VOLATILE STATE: keys, audit ring-buffer, rate-limit buckets, sessions, plugin/digital-twin state, etc. are in-memory only and will be lost on restart — see docs/PERSISTENCE.md")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("received signal, shutting down", "signal", sig)
		cancel()
	}()

	// Start Prometheus metrics server on a separate port if using HTTP transports
	if cfg.Server.Transport != "stdio" {
		metricsAddr := fmt.Sprintf("127.0.0.1:%d", cfg.Server.Port+1)
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.HandlerFor(srv.Metrics().Registry, promhttp.HandlerOpts{}))
		metricsMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			checkCtx, checkCancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer checkCancel()
			if err := srv.HealthCheck(checkCtx); err != nil {
				logger.Error("health check failed", "error", err)
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("unhealthy"))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})
		metricsLimiter := newSimpleRateLimiter(10, 20) // 10 req/s, burst of 20
		metricsServer := &http.Server{
			Addr:              metricsAddr,
			Handler:           rateLimitMiddleware(securityHeadersMiddleware(metricsMux), metricsLimiter),
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      60 * time.Second,
			IdleTimeout:       120 * time.Second,
		}
		go func() {
			logger.Info("metrics server listening", "address", metricsAddr)
			if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Error("metrics server error", "error", err)
			}
		}()
		go func() {
			<-ctx.Done()
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer shutdownCancel()
			metricsServer.Shutdown(shutdownCtx)
		}()
	}

	switch cfg.Server.Transport {
	case "stdio":
		logger.Info("serving via stdio transport")
		stdioServer := server.NewStdioServer(srv.MCPServer())
		return stdioServer.Listen(ctx, os.Stdin, os.Stdout)

	case "sse", "streamable-http":
		addr := cfg.Server.Address()
		logger.Info("serving via HTTP transport", "type", cfg.Server.Transport, "address", addr)

		var handler http.Handler
		if cfg.Server.Transport == "sse" {
			handler = server.NewSSEServer(srv.MCPServer())
		} else {
			handler = server.NewStreamableHTTPServer(srv.MCPServer())
		}

		// Wrap handler with security headers and body size limit (10 MB)
		secureHandler := http.MaxBytesHandler(securityHeadersMiddleware(handler), 10<<20)

		// When the deployment configures the mTLS identity provider,
		// the middleware extracts the verified peer cert and attaches
		// an identity.AgentIdentity to the request context so the MCP
		// auth middleware sees the verified identity instead of a raw
		// X-Agent-ID header. No-op when the provider is something else.
		secureHandler = srv.MTLSIdentityMiddleware(secureHandler)

		httpServer := &http.Server{
			Addr:              addr,
			Handler:           secureHandler,
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      60 * time.Second,
			IdleTimeout:       120 * time.Second,
		}

		// TLS support
		if cfg.Server.TLSCert != "" && cfg.Server.TLSKey != "" {
			tlsCfg := &tls.Config{MinVersion: tls.VersionTLS13}
			// If mTLS is the configured identity provider, layer on
			// RequireAndVerifyClientCert + the configured CA bundle.
			if mtlsCfg, err := srv.BuildMTLSTLSConfig(cfg.Policies.Identity.MTLS.CABundle); err != nil {
				return fmt.Errorf("configuring mTLS: %w", err)
			} else if mtlsCfg != nil {
				tlsCfg = mtlsCfg
			}
			httpServer.TLSConfig = tlsCfg
			go func() {
				<-ctx.Done()
				logger.Info("shutting down HTTPS server")
				shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer shutdownCancel()
				httpServer.Shutdown(shutdownCtx)
			}()
			logger.Info("TLS enabled", "cert", cfg.Server.TLSCert, "key", cfg.Server.TLSKey)
			if err := httpServer.ListenAndServeTLS(cfg.Server.TLSCert, cfg.Server.TLSKey); err != nil && err != http.ErrServerClosed {
				return fmt.Errorf("HTTPS server error: %w", err)
			}
		} else {
			go func() {
				<-ctx.Done()
				logger.Info("shutting down HTTP server")
				shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer shutdownCancel()
				httpServer.Shutdown(shutdownCtx)
			}()
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				return fmt.Errorf("HTTP server error: %w", err)
			}
		}
		return nil

	default:
		return fmt.Errorf("unsupported transport: %s", cfg.Server.Transport)
	}
}

// isConnectionRefused checks if an error indicates a Kafka connection failure.
func isConnectionRefused(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "no such host") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "dial tcp")
}

// securityHeadersMiddleware adds standard security headers to HTTP responses.
func securityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Security-Policy", "default-src 'none'")
		w.Header().Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		// Restrict cross-origin requests — MCP servers are not browser-facing
		w.Header().Set("Access-Control-Allow-Origin", "")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Agent-ID, X-Client-ID")
		next.ServeHTTP(w, r)
	})
}

// simpleRateLimiter implements a token-bucket rate limiter for the metrics endpoint.
type simpleRateLimiter struct {
	mu       sync.Mutex
	tokens   float64
	max      float64
	rate     float64 // tokens per second
	lastTime time.Time
}

func newSimpleRateLimiter(ratePerSec, burst float64) *simpleRateLimiter {
	return &simpleRateLimiter{
		tokens:   burst,
		max:      burst,
		rate:     ratePerSec,
		lastTime: time.Now(),
	}
}

func (rl *simpleRateLimiter) Allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(rl.lastTime).Seconds()
	rl.lastTime = now
	rl.tokens += elapsed * rl.rate
	if rl.tokens > rl.max {
		rl.tokens = rl.max
	}
	if rl.tokens < 1 {
		return false
	}
	rl.tokens--
	return true
}

func rateLimitMiddleware(next http.Handler, limiter *simpleRateLimiter) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !limiter.Allow() {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}
