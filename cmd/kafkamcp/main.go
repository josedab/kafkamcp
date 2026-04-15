package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

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

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func runServer(cmd *cobra.Command, args []string) error {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("loading configuration: %w", err)
	}

	// CLI flag overrides config file log level
	if logLevel != "" {
		cfg.Server.LogLevel = logLevel
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
		return fmt.Errorf("creating kafka manager: %w", err)
	}

	srv, err := mcpserver.New(cfg, kafkaManager, logger, version)
	if err != nil {
		return fmt.Errorf("creating server: %w", err)
	}
	defer srv.Close()

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
		metricsAddr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port+1)
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.Handler())
		metricsMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
		})
		metricsServer := &http.Server{Addr: metricsAddr, Handler: metricsMux}
		go func() {
			logger.Info("metrics server listening", "address", metricsAddr)
			if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Error("metrics server error", "error", err)
			}
		}()
		go func() {
			<-ctx.Done()
			metricsServer.Close()
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

		httpServer := &http.Server{Addr: addr, Handler: handler}

		// TLS support
		if cfg.Server.TLSCert != "" && cfg.Server.TLSKey != "" {
			httpServer.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS13}
			go func() {
				<-ctx.Done()
				logger.Info("shutting down HTTPS server")
				httpServer.Close()
			}()
			logger.Info("TLS enabled", "cert", cfg.Server.TLSCert, "key", cfg.Server.TLSKey)
			if err := httpServer.ListenAndServeTLS(cfg.Server.TLSCert, cfg.Server.TLSKey); err != nil && err != http.ErrServerClosed {
				return fmt.Errorf("HTTPS server error: %w", err)
			}
		} else {
			go func() {
				<-ctx.Done()
				logger.Info("shutting down HTTP server")
				httpServer.Close()
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
