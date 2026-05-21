package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/kafkaproxy"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleKafkaProxy(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_proxy", resourceType: auth.ResourceCluster, action: auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		if action == "" {
			return mcp.NewToolResultError("action is required"), nil
		}

		switch action {
		case "start":
			if s.kafkaProxy != nil {
				return mcp.NewToolResultError("proxy is already running"), nil
			}
			cfg := kafkaproxy.DefaultProxyConfig()
			if addr := request.GetString("listen_addr", ""); addr != "" {
				cfg.ListenAddr = addr
			}
			if broker := request.GetString("broker_addr", ""); broker != "" {
				cfg.BrokerAddr = broker
			}
			if maxConn := request.GetInt("max_connections", 0); maxConn > 0 {
				cfg.MaxConnections = int(clampInt64(int64(maxConn), 1, 10000))
			}

			proxy := kafkaproxy.NewProxy(cfg, s.logger)
			if err := proxy.Start(ctx); err != nil {
				return s.toolError("Failed to start proxy", err), nil
			}
			s.kafkaProxy = proxy

			s.logAudit(ctx, agentID, "kafka_proxy", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{
				"status":      "started",
				"listen_addr": proxy.ListenAddr(),
				"broker_addr": cfg.BrokerAddr,
			})

		case "stop":
			if s.kafkaProxy == nil {
				return mcp.NewToolResultError("proxy is not running"), nil
			}
			if err := s.kafkaProxy.Stop(); err != nil {
				return s.toolError("Failed to stop proxy", err), nil
			}
			s.kafkaProxy = nil
			s.logAudit(ctx, agentID, "kafka_proxy", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "stopped"})

		case "status":
			if s.kafkaProxy == nil {
				return marshalResult(map[string]any{"status": "stopped"})
			}
			stats := s.kafkaProxy.Stats()
			return marshalResult(map[string]any{
				"status":      "running",
				"listen_addr": s.kafkaProxy.ListenAddr(),
				"stats":       stats,
			})

		case "connections":
			if s.kafkaProxy == nil {
				return mcp.NewToolResultError("proxy is not running"), nil
			}
			conns := s.kafkaProxy.Connections()
			return marshalResult(map[string]any{
				"connections": conns,
				"total":       len(conns),
			})

		case "stats":
			if s.kafkaProxy == nil {
				return marshalResult(map[string]any{"status": "stopped"})
			}
			return marshalResult(s.kafkaProxy.Stats())

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action: %s (valid: start, stop, status, connections, stats)", action)), nil
		}
	})(ctx, request)
}
