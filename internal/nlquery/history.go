package nlquery

import (
	"fmt"
	"strings"
	"time"
)

// RecordHistory records a query execution in history.
func (e *Engine) RecordHistory(query string, plan QueryPlan, resultCount int, durationMs int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	entry := QueryHistoryEntry{
		Query:       query,
		Plan:        plan,
		Timestamp:   time.Now(),
		ResultCount: resultCount,
		DurationMs:  durationMs,
	}

	e.queryHistory = append(e.queryHistory, entry)
	if len(e.queryHistory) > e.maxHistory {
		e.queryHistory = e.queryHistory[len(e.queryHistory)-e.maxHistory:]
	}
}

// GetHistory returns recent query history.
func (e *Engine) GetHistory(limit int) []QueryHistoryEntry {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if limit <= 0 || limit > len(e.queryHistory) {
		limit = len(e.queryHistory)
	}
	// Return most recent first
	result := make([]QueryHistoryEntry, limit)
	for i := 0; i < limit; i++ {
		result[i] = e.queryHistory[len(e.queryHistory)-1-i]
	}
	return result
}

// Suggest returns query suggestions based on history and available topics.
func (e *Engine) Suggest(prefix string, availableTopics []string) []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	prefixLower := strings.ToLower(prefix)
	seen := make(map[string]bool)
	var suggestions []string

	// Suggest from history
	for i := len(e.queryHistory) - 1; i >= 0; i-- {
		q := e.queryHistory[i].Query
		if strings.HasPrefix(strings.ToLower(q), prefixLower) && !seen[q] {
			suggestions = append(suggestions, q)
			seen[q] = true
		}
		if len(suggestions) >= 5 {
			break
		}
	}

	// Suggest template queries based on topics
	templates := []string{
		"show me all messages from %s in the last hour",
		"count messages in %s",
		"show errors in %s",
		"average latency in %s",
	}
	for _, topic := range availableTopics {
		if len(suggestions) >= 10 {
			break
		}
		for _, tmpl := range templates {
			s := fmt.Sprintf(tmpl, topic)
			if strings.HasPrefix(strings.ToLower(s), prefixLower) && !seen[s] {
				suggestions = append(suggestions, s)
				seen[s] = true
			}
		}
	}

	return suggestions
}
