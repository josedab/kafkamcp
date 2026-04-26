// Package ide provides the server-side API for IDE integrations (IntelliJ,
// VS Code, etc.), offering schema-aware editing support, topic browsing,
// and inline message inspection.
package ide

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/kafkamcp/kafkamcp/internal/schema"
)

// Server provides the IDE integration API.
type Server struct {
	kafkaManager   kafka.KafkaManager
	schemaClients  map[string]*schema.Client
	logger         *slog.Logger
	mu             sync.RWMutex
	sessions       map[string]*IDESession
	allowedOrigins map[string]bool
}

const maxIDERequestSize = 1 << 20 // 1 MB
const maxIDESessions = 1000
const ideSessionTTL = 30 * time.Minute
const ideSessionCleanupInterval = 5 * time.Minute
const maxIDEInputLength = 1024

// IDESession tracks a connected IDE instance.
type IDESession struct {
	ID        string    `json:"id"`
	IDE       string    `json:"ide"` // intellij, vscode, vim, emacs
	Version   string    `json:"version"`
	ConnectAt time.Time `json:"connected_at"`
	LastPing  time.Time `json:"last_ping"`
}

// TopicBrowseResult provides topic data for IDE tree views.
type TopicBrowseResult struct {
	Topics []TopicNode `json:"topics"`
	Total  int         `json:"total"`
}

// TopicNode is a tree node representing a topic for IDE display.
type TopicNode struct {
	Name             string           `json:"name"`
	Partitions       int              `json:"partitions"`
	ReplicationFactor int             `json:"replication_factor"`
	MessageCount     int64            `json:"message_count"`
	IsInternal       bool             `json:"is_internal"`
	SchemaSubject    string           `json:"schema_subject,omitempty"`
	Children         []PartitionNode  `json:"children,omitempty"`
}

// PartitionNode represents a partition in the IDE tree.
type PartitionNode struct {
	ID             int32 `json:"id"`
	Leader         int32 `json:"leader"`
	Messages       int64 `json:"messages"`
	EarliestOffset int64 `json:"earliest_offset"`
	LatestOffset   int64 `json:"latest_offset"`
}

// SchemaInfo provides schema details for IDE autocompletion.
type SchemaInfo struct {
	Subject    string   `json:"subject"`
	Version    int      `json:"version"`
	SchemaType string   `json:"schema_type"`
	Schema     string   `json:"schema"`
	Fields     []Field  `json:"fields,omitempty"`
}

// Field represents a schema field for IDE autocompletion.
type Field struct {
	Name     string  `json:"name"`
	Type     string  `json:"type"`
	Doc      string  `json:"doc,omitempty"`
	Required bool    `json:"required"`
	Children []Field `json:"children,omitempty"`
}

// MessagePreview provides a message preview for inline inspection.
type MessagePreview struct {
	Topic     string            `json:"topic"`
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Key       string            `json:"key"`
	Value     string            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp string            `json:"timestamp"`
	SchemaID  int               `json:"schema_id,omitempty"`
}

// CompletionItem represents an autocompletion suggestion for IDEs.
type CompletionItem struct {
	Label      string `json:"label"`
	Kind       string `json:"kind"` // topic, field, schema, group
	Detail     string `json:"detail"`
	InsertText string `json:"insert_text"`
	Doc        string `json:"documentation,omitempty"`
}

// Default allowed origins for IDE CORS.
var defaultAllowedOrigins = map[string]bool{
	"http://localhost":       true,
	"http://127.0.0.1":      true,
	"https://localhost":      true,
	"https://127.0.0.1":     true,
	"vscode-webview://":     true,
}

// NewServer creates a new IDE integration server.
func NewServer(km kafka.KafkaManager, schemaClients map[string]*schema.Client, logger *slog.Logger) *Server {
	s := &Server{
		kafkaManager:   km,
		schemaClients:  schemaClients,
		logger:         logger,
		sessions:       make(map[string]*IDESession),
		allowedOrigins: defaultAllowedOrigins,
	}
	return s
}

// SetAllowedOrigins replaces the default CORS origin allowlist.
func (s *Server) SetAllowedOrigins(origins []string) {
	m := make(map[string]bool, len(origins))
	for _, o := range origins {
		m[o] = true
	}
	s.allowedOrigins = m
}

// StartSessionCleanup starts a background goroutine that evicts expired IDE sessions.
// It stops when the provided context is cancelled.
func (s *Server) StartSessionCleanup(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(ideSessionCleanupInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.cleanupExpiredSessions()
			}
		}
	}()
}

func (s *Server) cleanupExpiredSessions() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for id, sess := range s.sessions {
		if now.Sub(sess.LastPing) > ideSessionTTL {
			delete(s.sessions, id)
			s.logger.Info("evicted expired IDE session", "session_id", id, "last_ping", sess.LastPing)
		}
	}
}

// RegisterRoutes adds IDE API routes to the given mux.
func (s *Server) RegisterRoutes(mux *http.ServeMux, authMiddleware func(http.HandlerFunc) http.HandlerFunc) {
	wrap := authMiddleware
	if wrap == nil {
		wrap = func(h http.HandlerFunc) http.HandlerFunc { return h }
	}

	mux.HandleFunc("/api/ide/v1/connect", wrap(s.handleConnect))
	mux.HandleFunc("/api/ide/v1/topics", wrap(s.handleTopicBrowse))
	mux.HandleFunc("/api/ide/v1/topics/", wrap(s.handleTopicDetail))
	mux.HandleFunc("/api/ide/v1/schemas", wrap(s.handleSchemas))
	mux.HandleFunc("/api/ide/v1/schemas/", wrap(s.handleSchemaDetail))
	mux.HandleFunc("/api/ide/v1/messages", wrap(s.handleMessagePreview))
	mux.HandleFunc("/api/ide/v1/completions", wrap(s.handleCompletions))
	mux.HandleFunc("/api/ide/v1/health", wrap(s.handleHealth))
}

func (s *Server) handleConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		IDE     string `json:"ide"`
		Version string `json:"version"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxIDERequestSize)).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	if len(s.sessions) >= maxIDESessions {
		s.mu.Unlock()
		http.Error(w, "maximum IDE session limit reached", http.StatusServiceUnavailable)
		return
	}
	sessionID := generateIDESessionID()
	s.sessions[sessionID] = &IDESession{
		ID:        sessionID,
		IDE:       req.IDE,
		Version:   req.Version,
		ConnectAt: time.Now(),
		LastPing:  time.Now(),
	}
	s.mu.Unlock()

	s.writeIDEJSON(w, r, http.StatusOK, map[string]any{
		"session_id": sessionID,
		"status":     "connected",
		"capabilities": []string{
			"topic_browsing",
			"schema_completion",
			"message_inspection",
			"inline_preview",
		},
	})
}

func (s *Server) handleTopicBrowse(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	cluster := r.URL.Query().Get("cluster")
	if !validateIDEParam(w, "cluster", cluster) {
		return
	}
	includeInternal := r.URL.Query().Get("internal") == "true"

	topics, err := s.kafkaManager.ListTopics(ctx, cluster, includeInternal)
	if err != nil {
		s.logger.Error("IDE: failed to list topics", "error", err)
		http.Error(w, "failed to list topics", http.StatusInternalServerError)
		return
	}

	var nodes []TopicNode
	for _, t := range topics {
		nodes = append(nodes, TopicNode{
			Name:              t.Name,
			Partitions:        t.Partitions,
			ReplicationFactor: t.ReplicationFactor,
			MessageCount:      t.MessageCount,
			IsInternal:        t.IsInternal,
			SchemaSubject:     t.SchemaSubject,
		})
	}

	s.writeIDEJSON(w, r, http.StatusOK, TopicBrowseResult{
		Topics: nodes,
		Total:  len(nodes),
	})
}

func (s *Server) handleTopicDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topicName := strings.TrimPrefix(r.URL.Path, "/api/ide/v1/topics/")
	if topicName == "" {
		http.Error(w, "topic name required", http.StatusBadRequest)
		return
	}
	if !validateIDEParam(w, "topic", topicName) {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	cluster := r.URL.Query().Get("cluster")
	if !validateIDEParam(w, "cluster", cluster) {
		return
	}
	detail, err := s.kafkaManager.DescribeTopic(ctx, topicName, cluster)
	if err != nil {
		http.Error(w, "topic not found", http.StatusNotFound)
		return
	}

	node := TopicNode{
		Name:              detail.Name,
		Partitions:        len(detail.Partitions),
		ReplicationFactor: detail.ReplicationFactor,
		MessageCount:      detail.MessageCount,
		IsInternal:        detail.IsInternal,
	}

	for _, p := range detail.Partitions {
		node.Children = append(node.Children, PartitionNode{
			ID:             p.ID,
			Leader:         p.Leader,
			Messages:       p.LatestOffset - p.EarliestOffset,
			EarliestOffset: p.EarliestOffset,
			LatestOffset:   p.LatestOffset,
		})
	}

	s.writeIDEJSON(w, r, http.StatusOK, node)
}

func (s *Server) handleSchemas(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.writeIDEJSON(w, r, http.StatusOK, map[string]any{
		"schemas":        []any{},
		"total":          0,
		"schema_registry": len(s.schemaClients) > 0,
	})
}

func (s *Server) handleSchemaDetail(w http.ResponseWriter, r *http.Request) {
	subject := strings.TrimPrefix(r.URL.Path, "/api/ide/v1/schemas/")
	if subject == "" {
		http.Error(w, "subject required", http.StatusBadRequest)
		return
	}
	if !validateIDEParam(w, "subject", subject) {
		return
	}

	s.writeIDEJSON(w, r, http.StatusOK, SchemaInfo{
		Subject:    subject,
		SchemaType: "AVRO",
	})
}

func (s *Server) handleMessagePreview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "topic parameter required", http.StatusBadRequest)
		return
	}
	if !validateIDEParam(w, "topic", topic) {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	cluster := r.URL.Query().Get("cluster")
	if !validateIDEParam(w, "cluster", cluster) {
		return
	}
	messages, err := s.kafkaManager.Consume(ctx, cluster, topic, -1, "latest", 5, 5000, "")
	if err != nil {
		http.Error(w, "failed to preview messages", http.StatusInternalServerError)
		return
	}

	var previews []MessagePreview
	for _, m := range messages {
		previews = append(previews, MessagePreview{
			Topic:     topic,
			Partition: m.Partition,
			Offset:    m.Offset,
			Key:       fmt.Sprintf("%v", m.Key),
			Value:     fmt.Sprintf("%v", m.Value),
			Headers:   m.Headers,
			Timestamp: m.Timestamp,
		})
	}

	s.writeIDEJSON(w, r, http.StatusOK, map[string]any{
		"messages": previews,
		"count":    len(previews),
	})
}

func (s *Server) handleCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	prefix := r.URL.Query().Get("prefix")
	kind := r.URL.Query().Get("kind")
	if !validateIDEParam(w, "prefix", prefix) || !validateIDEParam(w, "kind", kind) {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	var items []CompletionItem

	if kind == "" || kind == "topic" {
		topics, err := s.kafkaManager.ListTopics(ctx, "", false)
		if err == nil {
			for _, t := range topics {
				if prefix == "" || strings.HasPrefix(t.Name, prefix) {
					items = append(items, CompletionItem{
						Label:      t.Name,
						Kind:       "topic",
						Detail:     fmt.Sprintf("%d partitions, %d msgs", t.Partitions, t.MessageCount),
						InsertText: t.Name,
					})
				}
			}
		}
	}

	if kind == "" || kind == "group" {
		groups, err := s.kafkaManager.ListConsumerGroups(ctx, "")
		if err == nil {
			for _, g := range groups {
				if prefix == "" || strings.HasPrefix(g.GroupID, prefix) {
					items = append(items, CompletionItem{
						Label:      g.GroupID,
						Kind:       "group",
						Detail:     fmt.Sprintf("state: %s, lag: %d", g.State, g.TotalLag),
						InsertText: g.GroupID,
					})
				}
			}
		}
	}

	s.writeIDEJSON(w, r, http.StatusOK, map[string]any{
		"completions": items,
		"total":       len(items),
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	sessionCount := len(s.sessions)
	s.mu.RUnlock()

	s.writeIDEJSON(w, r, http.StatusOK, map[string]any{
		"status":       "healthy",
		"sessions":     sessionCount,
		"capabilities": []string{"topics", "schemas", "messages", "completions"},
	})
}

func generateIDESessionID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("ide_%d", time.Now().UnixNano())
	}
	return "ide_" + hex.EncodeToString(b)
}

func (s *Server) writeIDEJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	origin := r.Header.Get("Origin")
	if origin != "" && s.isAllowedOrigin(origin) {
		w.Header().Set("Access-Control-Allow-Origin", origin)
	}
	w.Header().Set("Access-Control-Allow-Credentials", "false")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (s *Server) isAllowedOrigin(origin string) bool {
	if s.allowedOrigins[origin] {
		return true
	}
	// Allow any localhost port
	for _, prefix := range []string{"http://localhost:", "http://127.0.0.1:", "https://localhost:", "https://127.0.0.1:"} {
		if strings.HasPrefix(origin, prefix) {
			return true
		}
	}
	return false
}

// validateIDEParam checks that a query parameter does not exceed the max allowed length.
func validateIDEParam(w http.ResponseWriter, name, value string) bool {
	if len(value) > maxIDEInputLength {
		http.Error(w, fmt.Sprintf("%s exceeds maximum length of %d characters", name, maxIDEInputLength), http.StatusBadRequest)
		return false
	}
	return true
}
