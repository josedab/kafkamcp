package dashboard

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/audit"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/kafkamcp/kafkamcp/internal/ratelimit"
)

//go:embed static/*
var staticFS embed.FS

// Dashboard serves the KafkaMCP web UI and REST API.
type Dashboard struct {
	kafkaManager kafka.KafkaManager
	auditLogger  *audit.Logger
	rateLimiter  *ratelimit.Limiter
	logger       *slog.Logger

	mu         sync.RWMutex
	sseClients map[string]chan []byte
	clientID   int
}

// New creates a new dashboard server.
func New(km kafka.KafkaManager, al *audit.Logger, rl *ratelimit.Limiter, logger *slog.Logger) *Dashboard {
	return &Dashboard{
		kafkaManager: km,
		auditLogger:  al,
		rateLimiter:  rl,
		logger:       logger,
		sseClients:   make(map[string]chan []byte),
	}
}

// Handler returns the HTTP handler for the dashboard.
func (d *Dashboard) Handler() http.Handler {
	mux := http.NewServeMux()

	// REST API endpoints
	mux.HandleFunc("/api/v1/activity", d.handleActivity)
	mux.HandleFunc("/api/v1/audit", d.handleAuditTrail)
	mux.HandleFunc("/api/v1/topics", d.handleTopics)
	mux.HandleFunc("/api/v1/topology", d.handleTopology)
	mux.HandleFunc("/api/v1/ratelimits", d.handleRateLimits)
	mux.HandleFunc("/api/v1/schemas", d.handleSchemas)
	mux.HandleFunc("/api/v1/health", d.handleHealth)

	// SSE endpoint for live updates
	mux.HandleFunc("/api/v1/events", d.handleSSE)

	// Embedded SPA
	staticContent, err := fs.Sub(staticFS, "static")
	if err != nil {
		d.logger.Error("failed to load static files", "error", err)
	} else {
		mux.Handle("/", http.FileServer(http.FS(staticContent)))
	}

	return mux
}

func (d *Dashboard) handleActivity(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	entries := d.auditLogger.Recent(100)
	type activityItem struct {
		Timestamp string `json:"timestamp"`
		AgentID   string `json:"agent_id"`
		Tool      string `json:"tool"`
		Status    string `json:"status"`
		LatencyMs int64  `json:"latency_ms"`
	}

	var items []activityItem
	for _, e := range entries {
		items = append(items, activityItem{
			Timestamp: e.Timestamp,
			AgentID:   e.AgentID,
			Tool:      e.Tool,
			Status:    e.Result.Status,
			LatencyMs: e.LatencyMs,
		})
	}

	writeJSON(w, map[string]any{"activity": items, "total": len(items)})
}

func (d *Dashboard) handleAuditTrail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	entries := d.auditLogger.Recent(1000)
	writeJSON(w, map[string]any{"entries": entries, "total": len(entries)})
}

func (d *Dashboard) handleTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	topics, err := d.kafkaManager.ListTopics(ctx, "", false)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to list topics: %v", err), http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]any{"topics": topics, "total": len(topics)})
}

func (d *Dashboard) handleTopology(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	graph, err := d.kafkaManager.GetTopicGraph(ctx, "")
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to get topology: %v", err), http.StatusInternalServerError)
		return
	}

	writeJSON(w, graph)
}

func (d *Dashboard) handleRateLimits(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := d.rateLimiter.Status()
	writeJSON(w, map[string]any{"rate_limits": status})
}

func (d *Dashboard) handleSchemas(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, map[string]any{"message": "Schema browser requires Schema Registry configuration"})
}

func (d *Dashboard) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	err := d.kafkaManager.Ping(ctx)
	if err != nil {
		writeJSON(w, map[string]any{"status": "unhealthy", "error": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"status": "healthy"})
}

func (d *Dashboard) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	d.mu.Lock()
	d.clientID++
	clientID := fmt.Sprintf("client-%d", d.clientID)
	ch := make(chan []byte, 100)
	d.sseClients[clientID] = ch
	d.mu.Unlock()

	defer func() {
		d.mu.Lock()
		delete(d.sseClients, clientID)
		d.mu.Unlock()
	}()

	// Send initial connection event
	fmt.Fprintf(w, "event: connected\ndata: {\"client_id\":%q}\n\n", clientID)
	flusher.Flush()

	ctx := r.Context()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case data := <-ch:
			fmt.Fprintf(w, "event: update\ndata: %s\n\n", data)
			flusher.Flush()
		case <-ticker.C:
			fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}

// BroadcastEvent sends an event to all connected SSE clients.
func (d *Dashboard) BroadcastEvent(eventType string, data any) {
	payload, err := json.Marshal(map[string]any{
		"type":      eventType,
		"data":      data,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return
	}

	d.mu.RLock()
	defer d.mu.RUnlock()

	for _, ch := range d.sseClients {
		select {
		case ch <- payload:
		default:
			// Drop if channel is full
		}
	}
}

// ConnectedClients returns the number of connected SSE clients.
func (d *Dashboard) ConnectedClients() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.sseClients)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}
