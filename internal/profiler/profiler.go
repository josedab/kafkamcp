package profiler

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// ToolCall records a single tool invocation.
type ToolCall struct {
	Tool      string         `json:"tool"`
	AgentID   string         `json:"agent_id"`
	Args      map[string]any `json:"args,omitempty"`
	LatencyMs int64          `json:"latency_ms"`
	Status    string         `json:"status"` // success, error
	Timestamp time.Time      `json:"timestamp"`
}

// AgentProfile contains performance metrics for a single agent.
type AgentProfile struct {
	AgentID        string             `json:"agent_id"`
	TotalCalls     int                `json:"total_calls"`
	UniqueTtools   int                `json:"unique_tools"`
	AvgLatencyMs   float64            `json:"avg_latency_ms"`
	P50LatencyMs   float64            `json:"p50_latency_ms"`
	P95LatencyMs   float64            `json:"p95_latency_ms"`
	P99LatencyMs   float64            `json:"p99_latency_ms"`
	ErrorRate      float64            `json:"error_rate"`
	ToolBreakdown  []ToolStats        `json:"tool_breakdown"`
	RedundantCalls []RedundantCall    `json:"redundant_calls,omitempty"`
	Optimizations  []Optimization     `json:"optimizations,omitempty"`
	EfficiencyScore int               `json:"efficiency_score"` // 0-100
	ProfiledSince  string             `json:"profiled_since"`
	ProfiledUntil  string             `json:"profiled_until"`
}

// ToolStats has per-tool statistics.
type ToolStats struct {
	Tool         string  `json:"tool"`
	CallCount    int     `json:"call_count"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
	MaxLatencyMs int64   `json:"max_latency_ms"`
	ErrorCount   int     `json:"error_count"`
	Percentage   float64 `json:"percentage_of_total"`
}

// RedundantCall identifies duplicate tool calls that could be optimized.
type RedundantCall struct {
	Tool        string `json:"tool"`
	Count       int    `json:"count"`
	Description string `json:"description"`
	Suggestion  string `json:"suggestion"`
}

// Optimization suggests how to improve agent efficiency.
type Optimization struct {
	Type        string `json:"type"`
	Priority    string `json:"priority"` // high, medium, low
	Description string `json:"description"`
	Impact      string `json:"impact"`
}

// SlowQuery identifies slow tool calls.
type SlowQuery struct {
	Tool        string  `json:"tool"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
	MaxLatencyMs int64   `json:"max_latency_ms"`
	CallCount   int     `json:"call_count"`
	Threshold   float64 `json:"threshold_ms"`
}

// Profiler tracks and analyzes agent performance.
type Profiler struct {
	calls         []ToolCall
	maxCalls      int
	slowThreshold float64 // ms
	mu            sync.RWMutex
}

// NewProfiler creates a new performance profiler.
func NewProfiler() *Profiler {
	return &Profiler{
		maxCalls:      100000,
		slowThreshold: 1000, // 1 second
	}
}

// RecordCall records a tool invocation for profiling.
func (p *Profiler) RecordCall(tool, agentID string, args map[string]any, latencyMs int64, status string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.calls = append(p.calls, ToolCall{
		Tool:      tool,
		AgentID:   agentID,
		Args:      args,
		LatencyMs: latencyMs,
		Status:    status,
		Timestamp: time.Now(),
	})

	if len(p.calls) > p.maxCalls {
		p.calls = p.calls[len(p.calls)-p.maxCalls:]
	}
}

// ProfileAgent generates a performance profile for a specific agent.
func (p *Profiler) ProfileAgent(agentID string, sinceHours float64) *AgentProfile {
	p.mu.RLock()
	defer p.mu.RUnlock()

	since := time.Now().Add(-time.Duration(sinceHours) * time.Hour)
	var agentCalls []ToolCall
	for _, c := range p.calls {
		if c.AgentID == agentID && c.Timestamp.After(since) {
			agentCalls = append(agentCalls, c)
		}
	}

	if len(agentCalls) == 0 {
		return &AgentProfile{
			AgentID:         agentID,
			EfficiencyScore: 100,
			ProfiledSince:   since.Format(time.RFC3339),
			ProfiledUntil:   time.Now().Format(time.RFC3339),
		}
	}

	profile := &AgentProfile{
		AgentID:       agentID,
		TotalCalls:    len(agentCalls),
		ProfiledSince: since.Format(time.RFC3339),
		ProfiledUntil: time.Now().Format(time.RFC3339),
	}

	// Calculate latency stats
	var latencies []float64
	toolCounts := make(map[string]*ToolStats)
	errorCount := 0

	for _, c := range agentCalls {
		latencies = append(latencies, float64(c.LatencyMs))
		if c.Status == "error" {
			errorCount++
		}

		ts, ok := toolCounts[c.Tool]
		if !ok {
			ts = &ToolStats{Tool: c.Tool}
			toolCounts[c.Tool] = ts
		}
		ts.CallCount++
		ts.AvgLatencyMs += float64(c.LatencyMs)
		if c.LatencyMs > ts.MaxLatencyMs {
			ts.MaxLatencyMs = c.LatencyMs
		}
		if c.Status == "error" {
			ts.ErrorCount++
		}
	}

	sort.Float64s(latencies)
	profile.AvgLatencyMs = average(latencies)
	profile.P50LatencyMs = percentile(latencies, 50)
	profile.P95LatencyMs = percentile(latencies, 95)
	profile.P99LatencyMs = percentile(latencies, 99)
	profile.ErrorRate = float64(errorCount) / float64(len(agentCalls))
	profile.UniqueTtools = len(toolCounts)

	// Build tool breakdown
	for _, ts := range toolCounts {
		ts.AvgLatencyMs /= float64(ts.CallCount)
		ts.Percentage = float64(ts.CallCount) / float64(len(agentCalls)) * 100
		profile.ToolBreakdown = append(profile.ToolBreakdown, *ts)
	}
	sort.Slice(profile.ToolBreakdown, func(i, j int) bool {
		return profile.ToolBreakdown[i].CallCount > profile.ToolBreakdown[j].CallCount
	})

	// Detect redundant calls
	profile.RedundantCalls = p.detectRedundantCalls(agentCalls)

	// Generate optimizations
	profile.Optimizations = p.generateOptimizations(profile)

	// Calculate efficiency score
	profile.EfficiencyScore = p.calculateEfficiency(profile)

	return profile
}

// GetSlowQueries returns tool calls exceeding the slow threshold.
func (p *Profiler) GetSlowQueries(agentID string, thresholdMs float64) []SlowQuery {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if thresholdMs <= 0 {
		thresholdMs = p.slowThreshold
	}

	type toolAgg struct {
		totalMs  float64
		maxMs    int64
		count    int
	}
	aggs := make(map[string]*toolAgg)

	for _, c := range p.calls {
		if agentID != "" && c.AgentID != agentID {
			continue
		}
		if float64(c.LatencyMs) >= thresholdMs {
			ta, ok := aggs[c.Tool]
			if !ok {
				ta = &toolAgg{}
				aggs[c.Tool] = ta
			}
			ta.totalMs += float64(c.LatencyMs)
			if c.LatencyMs > ta.maxMs {
				ta.maxMs = c.LatencyMs
			}
			ta.count++
		}
	}

	var result []SlowQuery
	for tool, ta := range aggs {
		result = append(result, SlowQuery{
			Tool:         tool,
			AvgLatencyMs: ta.totalMs / float64(ta.count),
			MaxLatencyMs: ta.maxMs,
			CallCount:    ta.count,
			Threshold:    thresholdMs,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].AvgLatencyMs > result[j].AvgLatencyMs
	})

	return result
}

// ListAgents returns all agents that have been profiled.
func (p *Profiler) ListAgents() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	seen := make(map[string]bool)
	for _, c := range p.calls {
		seen[c.AgentID] = true
	}

	var agents []string
	for a := range seen {
		agents = append(agents, a)
	}
	sort.Strings(agents)
	return agents
}

// GetCallCount returns total call count.
func (p *Profiler) GetCallCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.calls)
}

func (p *Profiler) detectRedundantCalls(calls []ToolCall) []RedundantCall {
	var redundant []RedundantCall

	// Detect consecutive identical calls within a short window
	type callKey struct {
		tool string
		args string
	}
	windowMs := int64(5000) // 5 second window

	for i := 0; i < len(calls); i++ {
		count := 1
		for j := i + 1; j < len(calls); j++ {
			if calls[j].Tool == calls[i].Tool &&
				calls[j].Timestamp.Sub(calls[i].Timestamp).Milliseconds() < windowMs {
				count++
			} else {
				break
			}
		}
		if count >= 3 {
			redundant = append(redundant, RedundantCall{
				Tool:        calls[i].Tool,
				Count:       count,
				Description: fmt.Sprintf("%s called %d times within 5 seconds", calls[i].Tool, count),
				Suggestion:  "Cache results or batch requests to reduce repeated calls",
			})
			i += count - 1
		}
	}

	return redundant
}

func (p *Profiler) generateOptimizations(profile *AgentProfile) []Optimization {
	var opts []Optimization

	// High error rate
	if profile.ErrorRate > 0.1 {
		opts = append(opts, Optimization{
			Type:        "error_handling",
			Priority:    "high",
			Description: fmt.Sprintf("Error rate is %.1f%% - investigate common failure patterns", profile.ErrorRate*100),
			Impact:      "Reduce failed calls and improve agent reliability",
		})
	}

	// Too many unique tools
	if profile.UniqueTtools > 15 {
		opts = append(opts, Optimization{
			Type:        "tool_consolidation",
			Priority:    "medium",
			Description: fmt.Sprintf("Agent uses %d different tools - consider using workflow templates", profile.UniqueTtools),
			Impact:      "Simplify agent logic and reduce cognitive load",
		})
	}

	// Redundant calls
	if len(profile.RedundantCalls) > 0 {
		totalRedundant := 0
		for _, r := range profile.RedundantCalls {
			totalRedundant += r.Count
		}
		opts = append(opts, Optimization{
			Type:        "caching",
			Priority:    "high",
			Description: fmt.Sprintf("%d redundant tool calls detected - implement result caching", totalRedundant),
			Impact:      "Reduce latency and server load",
		})
	}

	// High P95 latency
	if profile.P95LatencyMs > 5000 {
		opts = append(opts, Optimization{
			Type:        "performance",
			Priority:    "medium",
			Description: fmt.Sprintf("P95 latency is %.0fms - consider using async operations", profile.P95LatencyMs),
			Impact:      "Improve agent responsiveness",
		})
	}

	return opts
}

func (p *Profiler) calculateEfficiency(profile *AgentProfile) int {
	score := 100

	// Deduct for high error rate
	score -= int(profile.ErrorRate * 30)

	// Deduct for redundant calls
	score -= len(profile.RedundantCalls) * 5

	// Deduct for high latency
	if profile.P95LatencyMs > 5000 {
		score -= 10
	} else if profile.P95LatencyMs > 2000 {
		score -= 5
	}

	// Deduct for each optimization suggestion
	score -= len(profile.Optimizations) * 3

	if score < 0 {
		score = 0
	}
	return score
}

func average(sorted []float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range sorted {
		sum += v
	}
	return sum / float64(len(sorted))
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100.0*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
