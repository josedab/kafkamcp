package kafkaproxy

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ProxyConfig configures the TCP proxy listener.
type ProxyConfig struct {
	ListenAddr     string           `json:"listen_addr" yaml:"listen_addr"`
	BrokerAddr     string           `json:"broker_addr" yaml:"broker_addr"`
	MaxConnections int              `json:"max_connections" yaml:"max_connections"`
	ReadTimeout    time.Duration    `json:"read_timeout" yaml:"read_timeout"`
	WriteTimeout   time.Duration    `json:"write_timeout" yaml:"write_timeout"`
	Translator     TranslatorConfig `json:"translator" yaml:"translator"`
}

// DefaultProxyConfig returns sensible defaults.
func DefaultProxyConfig() ProxyConfig {
	return ProxyConfig{
		ListenAddr:     ":19092",
		BrokerAddr:     "localhost:9092",
		MaxConnections: 1000,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		Translator:     DefaultTranslatorConfig(),
	}
}

// ConnectionInfo tracks a single proxy connection.
type ConnectionInfo struct {
	ID        string    `json:"id"`
	ClientID  string    `json:"client_id"`
	RemoteAddr string   `json:"remote_addr"`
	ConnectedAt time.Time `json:"connected_at"`
	Requests  int64     `json:"requests"`
	BytesIn   int64     `json:"bytes_in"`
	BytesOut  int64     `json:"bytes_out"`
}

// ProxyStats tracks aggregate proxy metrics.
type ProxyStats struct {
	ActiveConnections int64              `json:"active_connections"`
	TotalConnections  int64              `json:"total_connections"`
	TotalRequests     int64              `json:"total_requests"`
	TotalErrors       int64              `json:"total_errors"`
	Translation       TranslationStats   `json:"translation"`
	Uptime            string             `json:"uptime"`
}

// Proxy is a transparent TCP proxy that intercepts Kafka protocol traffic.
type Proxy struct {
	config     ProxyConfig
	translator *Translator
	listener   net.Listener
	logger     *slog.Logger

	mu          sync.RWMutex
	connections map[string]*ConnectionInfo
	connCount   atomic.Int64
	totalConn   atomic.Int64
	totalReq    atomic.Int64
	totalErr    atomic.Int64
	startedAt   time.Time

	// Handler callback for translated requests (pluggable for MCP integration)
	OnTranslated func(result *TranslationResult) ([]byte, error)

	cancel context.CancelFunc
	done   chan struct{}
}

// NewProxy creates a new Kafka MCP proxy.
func NewProxy(cfg ProxyConfig, logger *slog.Logger) *Proxy {
	return &Proxy{
		config:      cfg,
		translator:  NewTranslator(cfg.Translator, logger),
		logger:      logger,
		connections: make(map[string]*ConnectionInfo),
		done:        make(chan struct{}),
	}
}

// Start begins listening for Kafka client connections.
func (p *Proxy) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", p.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", p.config.ListenAddr, err)
	}
	p.listener = ln
	p.startedAt = time.Now()

	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	p.logger.Info("kafka proxy started",
		"listen_addr", p.config.ListenAddr,
		"broker_addr", p.config.BrokerAddr,
		"max_connections", p.config.MaxConnections)

	go p.acceptLoop(ctx)
	return nil
}

// Stop gracefully shuts down the proxy.
func (p *Proxy) Stop() error {
	if p.cancel != nil {
		p.cancel()
	}
	if p.listener != nil {
		if err := p.listener.Close(); err != nil {
			return err
		}
	}
	<-p.done
	return nil
}

// Stats returns current proxy metrics.
func (p *Proxy) Stats() ProxyStats {
	return ProxyStats{
		ActiveConnections: p.connCount.Load(),
		TotalConnections:  p.totalConn.Load(),
		TotalRequests:     p.totalReq.Load(),
		TotalErrors:       p.totalErr.Load(),
		Translation:       p.translator.Stats(),
		Uptime:            time.Since(p.startedAt).Round(time.Second).String(),
	}
}

// Connections returns info about active connections.
func (p *Proxy) Connections() []ConnectionInfo {
	p.mu.RLock()
	defer p.mu.RUnlock()
	conns := make([]ConnectionInfo, 0, len(p.connections))
	for _, c := range p.connections {
		conns = append(conns, *c)
	}
	return conns
}

// ListenAddr returns the actual listener address (useful when port is 0).
func (p *Proxy) ListenAddr() string {
	if p.listener != nil {
		return p.listener.Addr().String()
	}
	return p.config.ListenAddr
}

func (p *Proxy) acceptLoop(ctx context.Context) {
	defer close(p.done)

	for {
		conn, err := p.listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				p.logger.Error("accept error", "error", err)
				continue
			}
		}

		if p.connCount.Load() >= int64(p.config.MaxConnections) {
			p.logger.Warn("max connections reached, rejecting", "remote", conn.RemoteAddr())
			conn.Close()
			continue
		}

		p.totalConn.Add(1)
		p.connCount.Add(1)
		go p.handleConnection(ctx, conn)
	}
}

func (p *Proxy) handleConnection(ctx context.Context, conn net.Conn) {
	defer func() {
		conn.Close()
		p.connCount.Add(-1)
	}()

	connID := fmt.Sprintf("conn-%d", p.totalConn.Load())
	info := &ConnectionInfo{
		ID:          connID,
		RemoteAddr:  conn.RemoteAddr().String(),
		ConnectedAt: time.Now(),
	}

	p.mu.Lock()
	p.connections[connID] = info
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		delete(p.connections, connID)
		p.mu.Unlock()
	}()

	p.logger.Debug("new connection", "id", connID, "remote", conn.RemoteAddr())

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if p.config.ReadTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(p.config.ReadTimeout))
		}

		hdr, body, err := ReadRequestHeader(conn)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Client disconnected
			return
		}

		info.Requests++
		info.BytesIn += int64(hdr.Size)
		if hdr.ClientID != "" {
			info.ClientID = hdr.ClientID
		}
		p.totalReq.Add(1)

		var responsePayload []byte

		// Handle APIVersions directly
		if hdr.APIKey == APIKeyAPIVersions {
			responsePayload = EncodeAPIVersionsResponse()
		} else {
			result, err := p.translator.Translate(hdr, body)
			if err != nil {
				p.totalErr.Add(1)
				p.logger.Warn("translation denied", "api", APIKeyName(hdr.APIKey), "client", hdr.ClientID, "error", err)
				responsePayload = encodeErrorResponse(hdr.APIKey)
				if writeErr := WriteResponse(conn, hdr.CorrelationID, responsePayload); writeErr != nil {
					return
				}
				continue
			}

			if result.Translated && p.OnTranslated != nil {
				resp, err := p.OnTranslated(result)
				if err != nil {
					p.totalErr.Add(1)
					p.logger.Error("MCP handler error", "tool", result.Tool, "error", err)
					responsePayload = encodeErrorResponse(hdr.APIKey)
				} else {
					responsePayload = resp
				}
			} else {
				// For unhandled or passthrough requests, return a minimal success response
				responsePayload = encodeMinimalResponse(hdr.APIKey)
			}
		}

		info.BytesOut += int64(len(responsePayload) + 8)

		if p.config.WriteTimeout > 0 {
			conn.SetWriteDeadline(time.Now().Add(p.config.WriteTimeout))
		}

		if err := WriteResponse(conn, hdr.CorrelationID, responsePayload); err != nil {
			p.totalErr.Add(1)
			return
		}
	}
}

// encodeErrorResponse returns a minimal Kafka error response for the given API.
func encodeErrorResponse(apiKey int16) []byte {
	// Generic error code: UNKNOWN_SERVER_ERROR = 1
	buf := make([]byte, 0, 16)
	buf = append(buf, 0, 0) // throttle time placeholder
	buf = append(buf, 0, 1) // error code = 1 (UNKNOWN_SERVER_ERROR)
	return buf
}

// encodeMinimalResponse returns a minimal success response.
func encodeMinimalResponse(apiKey int16) []byte {
	buf := make([]byte, 0, 16)
	buf = append(buf, 0, 0, 0, 0) // throttle time = 0
	buf = append(buf, 0, 0)       // error code = 0
	return buf
}
