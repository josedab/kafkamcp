package semantic

import (
	"math"
	"sort"
	"strings"
	"sync"
)

// TopicProfile stores metadata and semantic embeddings for a topic.
type TopicProfile struct {
	Name         string            `json:"name"`
	Description  string            `json:"description"`
	Tags         []string          `json:"tags,omitempty"`
	SampleKeys   []string          `json:"sample_keys,omitempty"`
	MessageCount int64             `json:"message_count"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	tokens       []string
	tf           map[string]float64
}

// TopicSearchResult represents a topic found via NL search.
type TopicSearchResult struct {
	Topic       string  `json:"topic"`
	Score       float64 `json:"score"`
	Description string  `json:"description,omitempty"`
	Reason      string  `json:"reason"`
}

// TopicIndex provides NL-based topic discovery.
type TopicIndex struct {
	mu       sync.RWMutex
	profiles map[string]*TopicProfile
}

// NewTopicIndex creates a new topic search index.
func NewTopicIndex() *TopicIndex {
	return &TopicIndex{
		profiles: make(map[string]*TopicProfile),
	}
}

// IndexTopic adds or updates a topic profile in the index.
func (ti *TopicIndex) IndexTopic(name, description string, tags []string, sampleKeys []string, messageCount int64, metadata map[string]string) {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	// Build searchable text from all available signals
	var textParts []string
	textParts = append(textParts, name)
	if description != "" {
		textParts = append(textParts, description)
	}
	textParts = append(textParts, tags...)
	textParts = append(textParts, sampleKeys...)
	for k, v := range metadata {
		textParts = append(textParts, k, v)
	}

	// Expand topic name (e.g. "user-events" -> "user events")
	expandedName := strings.NewReplacer("-", " ", "_", " ", ".", " ").Replace(name)
	textParts = append(textParts, expandedName)

	fullText := strings.Join(textParts, " ")
	tokens := tokenize(fullText)
	tf := computeTF(tokens)

	ti.profiles[name] = &TopicProfile{
		Name:         name,
		Description:  description,
		Tags:         tags,
		SampleKeys:   sampleKeys,
		MessageCount: messageCount,
		Metadata:     metadata,
		tokens:       tokens,
		tf:           tf,
	}
}

// SearchTopics finds topics matching a natural language query.
func (ti *TopicIndex) SearchTopics(query string, limit int) []TopicSearchResult {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}

	queryTokens := tokenize(query)
	if len(queryTokens) == 0 {
		return nil
	}
	queryTF := computeTF(queryTokens)
	idf := ti.computeIDF()

	var results []TopicSearchResult
	for _, profile := range ti.profiles {
		score := topicCosineSimilarity(queryTF, profile.tf, idf)

		// Boost for direct name match
		lowerQuery := strings.ToLower(query)
		lowerName := strings.ToLower(profile.Name)
		if strings.Contains(lowerName, lowerQuery) || strings.Contains(lowerQuery, lowerName) {
			score += 0.5
		}

		// Boost for tag matches
		for _, tag := range profile.Tags {
			if strings.Contains(lowerQuery, strings.ToLower(tag)) {
				score += 0.2
			}
		}

		if score > 0.01 {
			reason := generateMatchReason(query, profile)
			results = append(results, TopicSearchResult{
				Topic:       profile.Name,
				Score:       score,
				Description: profile.Description,
				Reason:      reason,
			})
		}
	}

	sort.Slice(results, func(i, j int) bool { return results[i].Score > results[j].Score })

	if len(results) > limit {
		results = results[:limit]
	}

	return results
}

// GetProfile returns a topic's profile.
func (ti *TopicIndex) GetProfile(name string) (*TopicProfile, bool) {
	ti.mu.RLock()
	defer ti.mu.RUnlock()
	p, ok := ti.profiles[name]
	return p, ok
}

// ListProfiles returns all indexed topic profiles.
func (ti *TopicIndex) ListProfiles() []TopicProfile {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

	result := make([]TopicProfile, 0, len(ti.profiles))
	for _, p := range ti.profiles {
		result = append(result, *p)
	}
	return result
}

// RemoveTopic removes a topic from the index.
func (ti *TopicIndex) RemoveTopic(name string) bool {
	ti.mu.Lock()
	defer ti.mu.Unlock()
	_, ok := ti.profiles[name]
	delete(ti.profiles, name)
	return ok
}

// Count returns the number of indexed topics.
func (ti *TopicIndex) Count() int {
	ti.mu.RLock()
	defer ti.mu.RUnlock()
	return len(ti.profiles)
}

func (ti *TopicIndex) computeIDF() map[string]float64 {
	df := make(map[string]int)
	for _, p := range ti.profiles {
		seen := make(map[string]bool)
		for _, t := range p.tokens {
			if !seen[t] {
				df[t]++
				seen[t] = true
			}
		}
	}

	n := float64(len(ti.profiles))
	if n == 0 {
		n = 1
	}

	idf := make(map[string]float64)
	for term, count := range df {
		idf[term] = math.Log(n / float64(count))
	}
	return idf
}

func topicCosineSimilarity(a, b map[string]float64, idf map[string]float64) float64 {
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

func generateMatchReason(query string, profile *TopicProfile) string {
	queryTokens := tokenize(query)
	var matchedTokens []string
	profileText := strings.ToLower(strings.Join(profile.tokens, " "))
	for _, qt := range queryTokens {
		if strings.Contains(profileText, qt) {
			matchedTokens = append(matchedTokens, qt)
		}
	}
	if len(matchedTokens) > 0 {
		return "Matched on: " + strings.Join(matchedTokens, ", ")
	}
	return "Partial semantic match"
}
