package msgsampling

import (
	"log/slog"
	"math"
	"sort"
	"sync"
)

// MessageSample represents a sampled message for profiling.
type MessageSample struct {
	KeySize    int    `json:"key_size"`
	ValueSize  int    `json:"value_size"`
	HasKey     bool   `json:"has_key"`
	KeyHash    string `json:"key_hash,omitempty"`
	HeaderCount int   `json:"header_count"`
	SchemaType  string `json:"schema_type,omitempty"` // json, avro, protobuf, string, binary
}

// ProfileResult contains the statistical profile of sampled messages.
type ProfileResult struct {
	Topic           string                  `json:"topic"`
	SampleCount     int                     `json:"sample_count"`
	SizeStats       SizeStatistics          `json:"size_statistics"`
	KeyAnalysis     KeyAnalysis             `json:"key_analysis"`
	SchemaDistribution map[string]int       `json:"schema_distribution"`
	SizeHistogram   []HistogramBucket       `json:"size_histogram"`
	Recommendations []string               `json:"recommendations,omitempty"`
}

// SizeStatistics provides message size metrics.
type SizeStatistics struct {
	MinBytes    int     `json:"min_bytes"`
	MaxBytes    int     `json:"max_bytes"`
	AvgBytes    float64 `json:"avg_bytes"`
	MedianBytes int    `json:"median_bytes"`
	P95Bytes    int     `json:"p95_bytes"`
	P99Bytes    int     `json:"p99_bytes"`
	TotalBytes  int64   `json:"total_bytes"`
}

// KeyAnalysis provides key distribution metrics.
type KeyAnalysis struct {
	UniqueKeys     int     `json:"unique_keys"`
	NullKeyPercent float64 `json:"null_key_percent"`
	Cardinality    string  `json:"cardinality"` // low, medium, high, very_high
	TopKeys        []KeyCount `json:"top_keys,omitempty"`
}

// KeyCount tracks key frequency.
type KeyCount struct {
	Key   string `json:"key"`
	Count int    `json:"count"`
}

// HistogramBucket represents a bucket in a size histogram.
type HistogramBucket struct {
	RangeLabel string `json:"range"`
	MinBytes   int    `json:"min_bytes"`
	MaxBytes   int    `json:"max_bytes"`
	Count      int    `json:"count"`
	Percent    float64 `json:"percent"`
}

// Engine performs message sampling and profiling.
type Engine struct {
	mu     sync.RWMutex
	cache  map[string]*ProfileResult
	logger *slog.Logger
}

// NewEngine creates a new message sampling engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		cache:  make(map[string]*ProfileResult),
		logger: logger,
	}
}

// Profile analyzes a set of message samples and produces a profile.
func (e *Engine) Profile(topic string, samples []MessageSample) *ProfileResult {
	result := &ProfileResult{
		Topic:              topic,
		SampleCount:        len(samples),
		SchemaDistribution: make(map[string]int),
	}

	if len(samples) == 0 {
		return result
	}

	// Size statistics
	var sizes []int
	var totalSize int64
	keyCounts := make(map[string]int)
	var nullKeys int

	for _, s := range samples {
		totalSize := s.KeySize + s.ValueSize
		sizes = append(sizes, totalSize)

		if s.HasKey {
			keyCounts[s.KeyHash]++
		} else {
			nullKeys++
		}

		schema := s.SchemaType
		if schema == "" {
			schema = "unknown"
		}
		result.SchemaDistribution[schema]++
	}
	_ = totalSize

	sort.Ints(sizes)

	var sumSizes int64
	for _, sz := range sizes {
		sumSizes += int64(sz)
	}

	result.SizeStats = SizeStatistics{
		MinBytes:    sizes[0],
		MaxBytes:    sizes[len(sizes)-1],
		AvgBytes:    float64(sumSizes) / float64(len(sizes)),
		MedianBytes: sizes[len(sizes)/2],
		P95Bytes:    sizes[int(float64(len(sizes))*0.95)],
		P99Bytes:    sizes[int(math.Min(float64(len(sizes))*0.99, float64(len(sizes)-1)))],
		TotalBytes:  sumSizes,
	}

	// Key analysis
	result.KeyAnalysis = KeyAnalysis{
		UniqueKeys:     len(keyCounts),
		NullKeyPercent: float64(nullKeys) / float64(len(samples)) * 100,
	}

	if len(samples) > 0 {
		ratio := float64(len(keyCounts)) / float64(len(samples))
		switch {
		case ratio > 0.8:
			result.KeyAnalysis.Cardinality = "very_high"
		case ratio > 0.5:
			result.KeyAnalysis.Cardinality = "high"
		case ratio > 0.1:
			result.KeyAnalysis.Cardinality = "medium"
		default:
			result.KeyAnalysis.Cardinality = "low"
		}
	}

	// Top keys
	type kv struct {
		key   string
		count int
	}
	var topKeys []kv
	for k, v := range keyCounts {
		topKeys = append(topKeys, kv{k, v})
	}
	sort.Slice(topKeys, func(i, j int) bool { return topKeys[i].count > topKeys[j].count })
	limit := 10
	if len(topKeys) < limit {
		limit = len(topKeys)
	}
	for _, kv := range topKeys[:limit] {
		result.KeyAnalysis.TopKeys = append(result.KeyAnalysis.TopKeys, KeyCount{Key: kv.key, Count: kv.count})
	}

	// Size histogram
	result.SizeHistogram = e.computeHistogram(sizes)

	// Recommendations
	result.Recommendations = e.generateRecommendations(result)

	// Cache result
	e.mu.Lock()
	e.cache[topic] = result
	e.mu.Unlock()

	return result
}

func (e *Engine) computeHistogram(sizes []int) []HistogramBucket {
	buckets := []struct {
		label string
		min   int
		max   int
	}{
		{"0-100B", 0, 100},
		{"100B-1KB", 100, 1024},
		{"1KB-10KB", 1024, 10240},
		{"10KB-100KB", 10240, 102400},
		{"100KB-1MB", 102400, 1048576},
		{"1MB+", 1048576, math.MaxInt64},
	}

	var result []HistogramBucket
	for _, b := range buckets {
		count := 0
		for _, sz := range sizes {
			if sz >= b.min && sz < b.max {
				count++
			}
		}
		pct := 0.0
		if len(sizes) > 0 {
			pct = float64(count) / float64(len(sizes)) * 100
		}
		result = append(result, HistogramBucket{
			RangeLabel: b.label,
			MinBytes:   b.min,
			MaxBytes:   b.max,
			Count:      count,
			Percent:    math.Round(pct*100) / 100,
		})
	}
	return result
}

func (e *Engine) generateRecommendations(result *ProfileResult) []string {
	var recs []string

	if result.SizeStats.P99Bytes > 1048576 {
		recs = append(recs, "Large messages detected (>1MB at P99). Consider compression or chunking.")
	}
	if result.KeyAnalysis.NullKeyPercent > 50 {
		recs = append(recs, "Over 50% of messages have null keys. Consider adding keys for partition ordering.")
	}
	if result.KeyAnalysis.Cardinality == "low" && result.SampleCount > 10 {
		recs = append(recs, "Low key cardinality may cause partition hotspots. Consider a different partitioning strategy.")
	}

	return recs
}

// GetCachedProfile returns the most recent profile for a topic.
func (e *Engine) GetCachedProfile(topic string) *ProfileResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.cache[topic]
}
