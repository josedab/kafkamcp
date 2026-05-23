package vectorsync

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// VectorDBType represents the target vector database.
type VectorDBType string

// DBPinecone and related values define constants for this package.
const (
	DBPinecone VectorDBType = "pinecone"
	DBWeaviate VectorDBType = "weaviate"
	DBQdrant   VectorDBType = "qdrant"
	DBChroma   VectorDBType = "chroma"
	DBMilvus   VectorDBType = "milvus"
)

// EmbeddingModel represents the embedding model to use.
type EmbeddingModel string

// ModelSimpleHash and related values define constants for this package.
const (
	ModelSimpleHash EmbeddingModel = "simple-hash"           // built-in hash-based
	ModelTFIDF      EmbeddingModel = "tfidf"                 // built-in TF-IDF
	ModelOpenAI     EmbeddingModel = "openai"                // external API
	ModelCohere     EmbeddingModel = "cohere"                // external API
	ModelSentence   EmbeddingModel = "sentence-transformers" // external
)

// SyncBridge defines a Kafka-to-VectorDB sync configuration.
type SyncBridge struct {
	ID             string          `json:"id"`
	Name           string          `json:"name"`
	Description    string          `json:"description,omitempty"`
	SourceTopic    string          `json:"source_topic"`
	SourceCluster  string          `json:"source_cluster,omitempty"`
	VectorDB       VectorDBConfig  `json:"vector_db"`
	Embedding      EmbeddingConfig `json:"embedding"`
	ContentField   string          `json:"content_field,omitempty"`
	MetadataFields []string        `json:"metadata_fields,omitempty"`
	Status         string          `json:"status"` // active, paused, stopped, error
	CreatedBy      string          `json:"created_by"`
	CreatedAt      time.Time       `json:"created_at"`
	Stats          SyncStats       `json:"stats"`
}

// VectorDBConfig configures the target vector database.
type VectorDBConfig struct {
	Type       VectorDBType      `json:"type"`
	URL        string            `json:"url,omitempty"`
	Collection string            `json:"collection"`
	Namespace  string            `json:"namespace,omitempty"`
	Dimension  int               `json:"dimension"`
	Properties map[string]string `json:"properties,omitempty"`
}

// EmbeddingConfig configures embedding generation.
type EmbeddingConfig struct {
	Model     EmbeddingModel `json:"model"`
	Dimension int            `json:"dimension"`
	BatchSize int            `json:"batch_size,omitempty"`
}

// SyncStats tracks sync bridge performance.
type SyncStats struct {
	DocumentsSynced     int64     `json:"documents_synced"`
	DocumentsFailed     int64     `json:"documents_failed"`
	EmbeddingsGenerated int64     `json:"embeddings_generated"`
	BytesProcessed      int64     `json:"bytes_processed"`
	LastSyncTime        time.Time `json:"last_sync_time,omitempty"`
	AvgEmbeddingMs      float64   `json:"avg_embedding_ms"`
}

// VectorDocument represents a document prepared for vector DB insertion.
type VectorDocument struct {
	ID        string         `json:"id"`
	Content   string         `json:"content"`
	Embedding []float64      `json:"embedding"`
	Metadata  map[string]any `json:"metadata,omitempty"`
	Source    DocumentSource `json:"source"`
}

// DocumentSource tracks where a document came from.
type DocumentSource struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Timestamp string `json:"timestamp"`
}

// Engine manages vector DB sync bridges.
type Engine struct {
	mu      sync.RWMutex
	bridges map[string]*SyncBridge
	docs    map[string][]VectorDocument // bridgeID -> recent docs
	logger  *slog.Logger
	nextID  int64
}

// NewEngine creates a new vector sync engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		bridges: make(map[string]*SyncBridge),
		docs:    make(map[string][]VectorDocument),
		logger:  logger,
	}
}

// CreateBridge creates a new sync bridge.
func (e *Engine) CreateBridge(name, sourceTopic, createdBy string, vectorDB VectorDBConfig, embedding EmbeddingConfig) (*SyncBridge, error) {
	if name == "" {
		return nil, fmt.Errorf("bridge name is required")
	}
	if sourceTopic == "" {
		return nil, fmt.Errorf("source_topic is required")
	}
	if vectorDB.Collection == "" {
		return nil, fmt.Errorf("vector_db collection is required")
	}
	if embedding.Dimension <= 0 {
		embedding.Dimension = 384
	}
	if embedding.Model == "" {
		embedding.Model = ModelSimpleHash
	}
	vectorDB.Dimension = embedding.Dimension

	e.mu.Lock()
	defer e.mu.Unlock()

	e.nextID++
	id := fmt.Sprintf("vsync_%d", e.nextID)

	bridge := &SyncBridge{
		ID:          id,
		Name:        name,
		SourceTopic: sourceTopic,
		VectorDB:    vectorDB,
		Embedding:   embedding,
		Status:      "active",
		CreatedBy:   createdBy,
		CreatedAt:   time.Now(),
	}

	e.bridges[id] = bridge
	e.docs[id] = make([]VectorDocument, 0)
	e.logger.Info("vector sync bridge created", "id", id, "name", name, "db", vectorDB.Type)
	return bridge, nil
}

// ProcessMessage processes a Kafka message into a vector document.
func (e *Engine) ProcessMessage(bridgeID, key, value string, partition int32, offset int64, topic string) (*VectorDocument, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	bridge, ok := e.bridges[bridgeID]
	if !ok {
		return nil, fmt.Errorf("bridge %q not found", bridgeID)
	}
	if bridge.Status != "active" {
		return nil, fmt.Errorf("bridge %q is %s", bridgeID, bridge.Status)
	}

	content := value
	if bridge.ContentField != "" {
		content = extractFieldValue(value, bridge.ContentField)
	}

	// Generate document ID
	docID := generateDocID(topic, partition, offset)

	// Generate embedding
	start := time.Now()
	embedding := generateEmbedding(content, bridge.Embedding.Model, bridge.Embedding.Dimension)
	embeddingMs := float64(time.Since(start).Milliseconds())

	doc := &VectorDocument{
		ID:        docID,
		Content:   content,
		Embedding: embedding,
		Metadata: map[string]any{
			"key":       key,
			"topic":     topic,
			"partition": partition,
			"offset":    offset,
		},
		Source: DocumentSource{
			Topic:     topic,
			Partition: partition,
			Offset:    offset,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		},
	}

	// Update stats
	bridge.Stats.DocumentsSynced++
	bridge.Stats.EmbeddingsGenerated++
	bridge.Stats.BytesProcessed += int64(len(value))
	bridge.Stats.LastSyncTime = time.Now()
	if bridge.Stats.AvgEmbeddingMs == 0 {
		bridge.Stats.AvgEmbeddingMs = embeddingMs
	} else {
		bridge.Stats.AvgEmbeddingMs = (bridge.Stats.AvgEmbeddingMs + embeddingMs) / 2
	}

	// Store recent docs (max 100)
	docs := e.docs[bridgeID]
	docs = append(docs, *doc)
	if len(docs) > 100 {
		docs = docs[len(docs)-100:]
	}
	e.docs[bridgeID] = docs

	return doc, nil
}

// SearchSimilar performs a similarity search against synced documents.
func (e *Engine) SearchSimilar(bridgeID, query string, topK int) ([]SimilarityResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	bridge, ok := e.bridges[bridgeID]
	if !ok {
		return nil, fmt.Errorf("bridge %q not found", bridgeID)
	}

	docs := e.docs[bridgeID]
	if len(docs) == 0 {
		return nil, nil
	}

	queryEmbedding := generateEmbedding(query, bridge.Embedding.Model, bridge.Embedding.Dimension)

	if topK <= 0 {
		topK = 10
	}

	type scored struct {
		doc   VectorDocument
		score float64
	}

	var results []scored
	for _, doc := range docs {
		score := cosineSimilarity(queryEmbedding, doc.Embedding)
		results = append(results, scored{doc: doc, score: score})
	}

	sort.Slice(results, func(i, j int) bool { return results[i].score > results[j].score })
	if len(results) > topK {
		results = results[:topK]
	}

	var simResults []SimilarityResult
	for _, r := range results {
		simResults = append(simResults, SimilarityResult{
			Document: r.doc,
			Score:    r.score,
		})
	}

	return simResults, nil
}

// SimilarityResult is a document with its similarity score.
type SimilarityResult struct {
	Document VectorDocument `json:"document"`
	Score    float64        `json:"score"`
}

// GetBridge returns a bridge by ID.
func (e *Engine) GetBridge(id string) (*SyncBridge, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	b, ok := e.bridges[id]
	if !ok {
		return nil, fmt.Errorf("bridge %q not found", id)
	}
	return b, nil
}

// ListBridges returns all bridges.
func (e *Engine) ListBridges() []SyncBridge {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]SyncBridge, 0, len(e.bridges))
	for _, b := range e.bridges {
		result = append(result, *b)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.After(result[j].CreatedAt) })
	return result
}

// UpdateBridgeStatus updates a bridge's status.
func (e *Engine) UpdateBridgeStatus(id, status string) error {
	validStatuses := map[string]bool{"active": true, "paused": true, "stopped": true}
	if !validStatuses[status] {
		return fmt.Errorf("invalid status %q", status)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	b, ok := e.bridges[id]
	if !ok {
		return fmt.Errorf("bridge %q not found", id)
	}
	b.Status = status
	return nil
}

// DeleteBridge removes a bridge.
func (e *Engine) DeleteBridge(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.bridges[id]; !ok {
		return fmt.Errorf("bridge %q not found", id)
	}
	delete(e.bridges, id)
	delete(e.docs, id)
	return nil
}

func generateDocID(topic string, partition int32, offset int64) string {
	raw := fmt.Sprintf("%s-%d-%d", topic, partition, offset)
	hash := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(hash[:16])
}

// generateEmbedding creates a simple hash-based embedding for demonstration.
func generateEmbedding(text string, model EmbeddingModel, dimension int) []float64 {
	embedding := make([]float64, dimension)

	words := strings.Fields(strings.ToLower(text))
	for i, word := range words {
		hash := sha256.Sum256([]byte(word))
		for j := 0; j < dimension && j < len(hash); j++ {
			idx := (i*7 + j) % dimension
			embedding[idx] += float64(hash[j]) / 255.0
		}
	}

	// Normalize
	var norm float64
	for _, v := range embedding {
		norm += v * v
	}
	norm = math.Sqrt(norm)
	if norm > 0 {
		for i := range embedding {
			embedding[i] /= norm
		}
	}

	return embedding
}

func cosineSimilarity(a, b []float64) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}

	var dotProduct, normA, normB float64
	for i := range a {
		dotProduct += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 0
	}
	return dotProduct / (math.Sqrt(normA) * math.Sqrt(normB))
}

func extractFieldValue(jsonStr, field string) string {
	// Simple field extraction without full JSON parsing
	search := fmt.Sprintf(`"%s"`, field)
	idx := strings.Index(jsonStr, search)
	if idx < 0 {
		return jsonStr
	}
	rest := jsonStr[idx+len(search):]
	colonIdx := strings.Index(rest, ":")
	if colonIdx < 0 {
		return jsonStr
	}
	rest = strings.TrimSpace(rest[colonIdx+1:])
	if len(rest) > 0 && rest[0] == '"' {
		end := strings.Index(rest[1:], `"`)
		if end >= 0 {
			return rest[1 : end+1]
		}
	}
	return jsonStr
}
