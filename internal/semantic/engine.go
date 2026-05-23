package semantic

import (
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// Engine provides semantic search and RAG capabilities over indexed messages.
type Engine struct {
	mu         sync.RWMutex
	index      map[string]*IndexedDocument
	topics     map[string][]string // topic -> document IDs
	topicIndex *TopicIndex
	logger     *slog.Logger
	maxDocs    int
	nextID     int64
}

// IndexedDocument represents a semantically indexed Kafka message.
type IndexedDocument struct {
	ID        string             `json:"id"`
	Topic     string             `json:"topic"`
	Partition int32              `json:"partition"`
	Offset    int64              `json:"offset"`
	Key       string             `json:"key,omitempty"`
	Content   string             `json:"content"`
	Tokens    []string           `json:"-"`
	TF        map[string]float64 `json:"-"`
	Vector    []float64          `json:"vector,omitempty"`
	Timestamp time.Time          `json:"timestamp"`
	Metadata  map[string]string  `json:"metadata,omitempty"`
}

// SearchResult represents a single search result with relevance score.
type SearchResult struct {
	Document  *IndexedDocument `json:"document"`
	Score     float64          `json:"score"`
	Highlight string           `json:"highlight,omitempty"`
}

// RAGContext provides context fragments for Retrieval-Augmented Generation.
type RAGContext struct {
	Query      string         `json:"query"`
	Results    []SearchResult `json:"results"`
	Context    string         `json:"context"`
	TokenCount int            `json:"token_count_estimate"`
	Topics     []string       `json:"source_topics"`
}

// IndexStats provides statistics about the semantic index.
type IndexStats struct {
	TotalDocuments int            `json:"total_documents"`
	TopicCounts    map[string]int `json:"topic_counts"`
	VocabSize      int            `json:"vocab_size"`
}

// NewEngine creates a new semantic search engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		index:      make(map[string]*IndexedDocument),
		topics:     make(map[string][]string),
		topicIndex: NewTopicIndex(),
		logger:     logger,
		maxDocs:    100000,
	}
}

// IndexMessage adds a Kafka message to the semantic index.
func (e *Engine) IndexMessage(topic string, partition int32, offset int64, key, content string, metadata map[string]string) string {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.nextID++
	id := fmt.Sprintf("doc_%d", e.nextID)

	tokens := tokenize(content)
	tf := computeTF(tokens)

	doc := &IndexedDocument{
		ID:        id,
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Key:       key,
		Content:   content,
		Tokens:    tokens,
		TF:        tf,
		Timestamp: time.Now(),
		Metadata:  metadata,
	}

	e.index[id] = doc
	e.topics[topic] = append(e.topics[topic], id)

	// Evict oldest if over capacity
	if len(e.index) > e.maxDocs {
		e.evictOldest()
	}

	return id
}

// Search performs semantic search using TF-IDF similarity.
func (e *Engine) Search(query string, topicFilter string, limit int) []SearchResult {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}

	queryTokens := tokenize(query)
	queryTF := computeTF(queryTokens)

	// Compute IDF across all documents
	idf := e.computeIDF()

	// Score each document
	var results []SearchResult
	for _, doc := range e.index {
		if topicFilter != "" && doc.Topic != topicFilter {
			continue
		}

		score := cosineSimilarity(queryTF, doc.TF, idf)
		if score > 0.01 {
			highlight := generateHighlight(doc.Content, queryTokens, 200)
			results = append(results, SearchResult{
				Document:  doc,
				Score:     score,
				Highlight: highlight,
			})
		}
	}

	sort.Slice(results, func(i, j int) bool { return results[i].Score > results[j].Score })

	if len(results) > limit {
		results = results[:limit]
	}

	return results
}

// BuildRAGContext performs search and constructs a RAG context string.
func (e *Engine) BuildRAGContext(query string, topicFilter string, maxTokens int) *RAGContext {
	if maxTokens <= 0 {
		maxTokens = 4000
	}

	results := e.Search(query, topicFilter, 20)

	var contextParts []string
	var sourceTopics = make(map[string]bool)
	totalTokens := 0

	for _, r := range results {
		content := r.Document.Content
		tokens := len(tokenize(content))

		if totalTokens+tokens > maxTokens {
			remaining := maxTokens - totalTokens
			if remaining > 50 {
				words := strings.Fields(content)
				if len(words) > remaining {
					content = strings.Join(words[:remaining], " ") + "..."
				}
			} else {
				break
			}
		}

		contextParts = append(contextParts, fmt.Sprintf("[Source: %s (offset %d, score %.2f)]\n%s",
			r.Document.Topic, r.Document.Offset, r.Score, content))
		sourceTopics[r.Document.Topic] = true
		totalTokens += len(tokenize(content))
	}

	var topicList []string
	for t := range sourceTopics {
		topicList = append(topicList, t)
	}

	return &RAGContext{
		Query:      query,
		Results:    results,
		Context:    strings.Join(contextParts, "\n\n---\n\n"),
		TokenCount: totalTokens,
		Topics:     topicList,
	}
}

// GetStats returns index statistics.
func (e *Engine) GetStats() IndexStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	topicCounts := make(map[string]int)
	vocab := make(map[string]bool)
	for _, doc := range e.index {
		topicCounts[doc.Topic]++
		for _, t := range doc.Tokens {
			vocab[t] = true
		}
	}

	return IndexStats{
		TotalDocuments: len(e.index),
		TopicCounts:    topicCounts,
		VocabSize:      len(vocab),
	}
}

// TopicIndex returns the NL-based topic search index.
func (e *Engine) TopicIndex() *TopicIndex {
	return e.topicIndex
}

// DeleteByTopic removes all documents for a topic from the index.
func (e *Engine) DeleteByTopic(topic string) int {
	e.mu.Lock()
	defer e.mu.Unlock()

	ids, ok := e.topics[topic]
	if !ok {
		return 0
	}

	for _, id := range ids {
		delete(e.index, id)
	}
	delete(e.topics, topic)
	return len(ids)
}

func (e *Engine) computeIDF() map[string]float64 {
	df := make(map[string]int)
	for _, doc := range e.index {
		seen := make(map[string]bool)
		for _, t := range doc.Tokens {
			if !seen[t] {
				df[t]++
				seen[t] = true
			}
		}
	}

	n := float64(len(e.index))
	if n == 0 {
		n = 1
	}

	idf := make(map[string]float64)
	for term, count := range df {
		idf[term] = math.Log(n / float64(count))
	}
	return idf
}

func (e *Engine) evictOldest() {
	var oldest *IndexedDocument
	for _, doc := range e.index {
		if oldest == nil || doc.Timestamp.Before(oldest.Timestamp) {
			oldest = doc
		}
	}
	if oldest != nil {
		delete(e.index, oldest.ID)
		if ids, ok := e.topics[oldest.Topic]; ok {
			for i, id := range ids {
				if id == oldest.ID {
					e.topics[oldest.Topic] = append(ids[:i], ids[i+1:]...)
					break
				}
			}
		}
	}
}

// tokenize splits text into lowercase tokens, filtering stopwords.
func tokenize(text string) []string {
	text = strings.ToLower(text)
	words := strings.FieldsFunc(text, func(c rune) bool {
		return !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-')
	})

	var tokens []string
	for _, w := range words {
		if len(w) < 2 || stopwords[w] {
			continue
		}
		tokens = append(tokens, w)
	}
	return tokens
}

func computeTF(tokens []string) map[string]float64 {
	counts := make(map[string]int)
	for _, t := range tokens {
		counts[t]++
	}
	tf := make(map[string]float64)
	total := float64(len(tokens))
	if total == 0 {
		return tf
	}
	for t, c := range counts {
		tf[t] = float64(c) / total
	}
	return tf
}

func cosineSimilarity(a, b map[string]float64, idf map[string]float64) float64 {
	dotProduct := 0.0
	normA := 0.0
	normB := 0.0

	allTerms := make(map[string]bool)
	for t := range a {
		allTerms[t] = true
	}
	for t := range b {
		allTerms[t] = true
	}

	for t := range allTerms {
		weight := idf[t]
		if weight == 0 {
			weight = 1
		}
		va := a[t] * weight
		vb := b[t] * weight
		dotProduct += va * vb
		normA += va * va
		normB += vb * vb
	}

	if normA == 0 || normB == 0 {
		return 0
	}
	return dotProduct / (math.Sqrt(normA) * math.Sqrt(normB))
}

func generateHighlight(content string, queryTokens []string, maxLen int) string {
	if len(content) <= maxLen {
		return content
	}

	lower := strings.ToLower(content)
	bestPos := 0
	bestScore := 0

	for i := 0; i < len(lower)-50; i += 20 {
		score := 0
		window := lower[i:min(i+maxLen, len(lower))]
		for _, qt := range queryTokens {
			score += strings.Count(window, qt)
		}
		if score > bestScore {
			bestScore = score
			bestPos = i
		}
	}

	end := bestPos + maxLen
	if end > len(content) {
		end = len(content)
	}

	result := content[bestPos:end]
	if bestPos > 0 {
		result = "..." + result
	}
	if end < len(content) {
		result += "..."
	}
	return result
}

var stopwords = map[string]bool{
	"the": true, "a": true, "an": true, "and": true, "or": true, "but": true,
	"in": true, "on": true, "at": true, "to": true, "for": true, "of": true,
	"is": true, "it": true, "be": true, "as": true, "was": true, "with": true,
	"that": true, "this": true, "by": true, "from": true, "not": true,
	"are": true, "were": true, "been": true, "have": true, "has": true,
}
