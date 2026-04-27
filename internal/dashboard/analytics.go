package dashboard

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"
)

func (d *DashboardV2) handleHeatmap(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	hoursStr := r.URL.Query().Get("hours")
	hours := 24
	if hoursStr != "" {
		if h, err := strconv.Atoi(hoursStr); err == nil && h > 0 && h <= 168 {
			hours = h
		}
	}

	bucketMin := 60
	if hours <= 6 {
		bucketMin = 15
	} else if hours <= 12 {
		bucketMin = 30
	}

	entries := d.auditLogger.Recent(10000)

	cutoff := time.Now().Add(-time.Duration(hours) * time.Hour)

	type agentData struct {
		buckets map[string]int64
		errors  map[string]int64
		tools   map[string]int64
		total   int64
		errTot  int64
	}

	agents := make(map[string]*agentData)

	for _, e := range entries {
		ts, err := time.Parse(time.RFC3339, e.Timestamp)
		if err != nil || ts.Before(cutoff) {
			continue
		}

		ad, ok := agents[e.AgentID]
		if !ok {
			ad = &agentData{
				buckets: make(map[string]int64),
				errors:  make(map[string]int64),
				tools:   make(map[string]int64),
			}
			agents[e.AgentID] = ad
		}

		bucketTime := ts.Truncate(time.Duration(bucketMin) * time.Minute)
		bucketKey := bucketTime.UTC().Format(time.RFC3339)
		ad.buckets[bucketKey]++
		ad.tools[e.Tool]++
		ad.total++
		if e.Result.Status == "error" {
			ad.errors[bucketKey]++
			ad.errTot++
		}
	}

	resp := HeatmapResponse{
		TimeRange: fmt.Sprintf("%dh", hours),
		BucketMin: bucketMin,
	}

	for agentID, ad := range agents {
		entry := AgentHeatmapEntry{
			AgentID:    agentID,
			TotalCalls: ad.total,
		}
		if ad.total > 0 {
			entry.ErrorRate = float64(ad.errTot) / float64(ad.total)
		}

		// Sorted buckets
		var bucketKeys []string
		for k := range ad.buckets {
			bucketKeys = append(bucketKeys, k)
		}
		sort.Strings(bucketKeys)
		for _, k := range bucketKeys {
			entry.Buckets = append(entry.Buckets, HeatmapBucket{
				Time:   k,
				Count:  ad.buckets[k],
				Errors: ad.errors[k],
			})
		}

		// Top tools
		type kv struct {
			k string
			v int64
		}
		var toolList []kv
		for t, c := range ad.tools {
			toolList = append(toolList, kv{t, c})
		}
		sort.Slice(toolList, func(i, j int) bool { return toolList[i].v > toolList[j].v })
		if len(toolList) > 10 {
			toolList = toolList[:10]
		}
		for _, t := range toolList {
			entry.TopTools = append(entry.TopTools, ToolHeatEntry{Tool: t.k, Count: t.v})
		}

		resp.Agents = append(resp.Agents, entry)
	}

	sort.Slice(resp.Agents, func(i, j int) bool { return resp.Agents[i].TotalCalls > resp.Agents[j].TotalCalls })

	writeJSON(w, resp)
}

// RecordAgentCall updates the heatmap with agent activity data.
func (d *DashboardV2) RecordAgentCall(agentID, tool string, isError bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	hd, ok := d.heatmap[agentID]
	if !ok {
		hd = &AgentHeatmapData{
			AgentID:   agentID,
			Buckets:   make(map[string]int64),
			ToolCalls: make(map[string]int64),
		}
		d.heatmap[agentID] = hd
	}

	bucket := time.Now().Truncate(15 * time.Minute).UTC().Format(time.RFC3339)
	hd.Buckets[bucket]++
	hd.ToolCalls[tool]++
	hd.LastSeen = time.Now()
}
