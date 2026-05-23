package plugin

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Review represents a user review of a marketplace item.
type Review struct {
	ID        string  `json:"id"`
	ItemName  string  `json:"item_name"`
	ItemType  string  `json:"item_type"` // plugin, agent_config, tool_bundle
	Author    string  `json:"author"`
	Rating    float64 `json:"rating"` // 1.0-5.0
	Title     string  `json:"title"`
	Body      string  `json:"body"`
	Helpful   int     `json:"helpful_count"`
	CreatedAt string  `json:"created_at"`
}

// CategoryInfo describes a marketplace category with stats.
type CategoryInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	PluginCount int    `json:"plugin_count"`
	AgentCount  int    `json:"agent_count"`
	BundleCount int    `json:"bundle_count"`
}

// FeaturedItem is a curated marketplace recommendation.
type FeaturedItem struct {
	Name     string `json:"name"`
	Type     string `json:"type"` // plugin, agent_config, tool_bundle
	Reason   string `json:"reason"`
	Featured bool   `json:"featured"`
}

// DiscoveryResult aggregates marketplace search across all item types.
type DiscoveryResult struct {
	Plugins      []MarketplaceEntry `json:"plugins,omitempty"`
	AgentConfigs []AgentConfig      `json:"agent_configs,omitempty"`
	ToolBundles  []ToolBundle       `json:"tool_bundles,omitempty"`
	TotalResults int                `json:"total_results"`
	Query        string             `json:"query,omitempty"`
	Category     string             `json:"category,omitempty"`
}

// MarketplaceStats provides aggregate marketplace statistics.
type MarketplaceStats struct {
	TotalPlugins      int                `json:"total_plugins"`
	TotalAgentConfigs int                `json:"total_agent_configs"`
	TotalToolBundles  int                `json:"total_tool_bundles"`
	TotalReviews      int                `json:"total_reviews"`
	TotalDownloads    int                `json:"total_downloads"`
	Categories        []CategoryInfo     `json:"categories"`
	FeaturedItems     []FeaturedItem     `json:"featured_items"`
	TopRated          []MarketplaceEntry `json:"top_rated"`
}

// ReviewStore manages marketplace reviews.
type ReviewStore struct {
	mu      sync.RWMutex
	reviews map[string][]Review // item_name -> reviews
	counter int64
}

// NewReviewStore creates a review store.
func NewReviewStore() *ReviewStore {
	return &ReviewStore{
		reviews: make(map[string][]Review),
	}
}

// AddReview adds a review for a marketplace item.
func (rs *ReviewStore) AddReview(itemName, itemType, author string, rating float64, title, body string) (*Review, error) {
	if itemName == "" {
		return nil, fmt.Errorf("item name is required")
	}
	if rating < 1.0 || rating > 5.0 {
		return nil, fmt.Errorf("rating must be between 1.0 and 5.0")
	}
	if author == "" {
		return nil, fmt.Errorf("author is required")
	}

	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.counter++
	review := Review{
		ID:        fmt.Sprintf("review-%d", rs.counter),
		ItemName:  itemName,
		ItemType:  itemType,
		Author:    author,
		Rating:    rating,
		Title:     title,
		Body:      body,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	rs.reviews[itemName] = append(rs.reviews[itemName], review)
	return &review, nil
}

// GetReviews returns all reviews for an item.
func (rs *ReviewStore) GetReviews(itemName string) []Review {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	reviews := rs.reviews[itemName]
	result := make([]Review, len(reviews))
	copy(result, reviews)

	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt > result[j].CreatedAt
	})
	return result
}

// AverageRating returns the average rating for an item.
func (rs *ReviewStore) AverageRating(itemName string) float64 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	reviews := rs.reviews[itemName]
	if len(reviews) == 0 {
		return 0
	}
	var sum float64
	for _, r := range reviews {
		sum += r.Rating
	}
	return sum / float64(len(reviews))
}

// TotalReviews returns the total count of reviews across all items.
func (rs *ReviewStore) TotalReviews() int {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	total := 0
	for _, reviews := range rs.reviews {
		total += len(reviews)
	}
	return total
}

// Discover performs a unified search across plugins, agent configs, and tool bundles.
func (m *Marketplace) Discover(query, category string, store *AgentConfigStore) *DiscoveryResult {
	result := &DiscoveryResult{
		Query:    query,
		Category: category,
	}

	result.Plugins = m.Search(query, category, false)
	if store != nil {
		result.AgentConfigs = store.SearchConfigs(query, category, false)
		result.ToolBundles = store.SearchBundles(query, category)
	}

	result.TotalResults = len(result.Plugins) + len(result.AgentConfigs) + len(result.ToolBundles)
	return result
}

// GetStats returns aggregate marketplace statistics.
func (m *Marketplace) GetStats(store *AgentConfigStore, reviewStore *ReviewStore) *MarketplaceStats {
	stats := &MarketplaceStats{}

	// Collect marketplace data under marketplace lock
	m.mu.RLock()
	stats.TotalPlugins = len(m.catalog)
	catPlugins := make(map[string]int)
	totalDownloads := 0
	var allPlugins []MarketplaceEntry
	for _, p := range m.catalog {
		catPlugins[p.Category]++
		totalDownloads += p.Downloads
		allPlugins = append(allPlugins, *p)
	}
	var featured []FeaturedItem
	for _, p := range m.catalog {
		if p.Verified && p.Rating >= 4.5 {
			featured = append(featured, FeaturedItem{
				Name:     p.Name,
				Type:     "plugin",
				Reason:   "Verified & highly rated",
				Featured: true,
			})
		}
	}
	m.mu.RUnlock()

	stats.TotalDownloads = totalDownloads

	// Collect store data under store lock (separate from marketplace lock)
	catAgents := make(map[string]int)
	catBundles := make(map[string]int)
	if store != nil {
		store.mu.RLock()
		stats.TotalAgentConfigs = len(store.configs)
		stats.TotalToolBundles = len(store.bundles)
		for _, c := range store.configs {
			catAgents[c.Category]++
		}
		for _, b := range store.bundles {
			catBundles[b.Category]++
		}
		store.mu.RUnlock()
	}

	if reviewStore != nil {
		stats.TotalReviews = reviewStore.TotalReviews()
	}

	// Build category info
	allCats := make(map[string]bool)
	for c := range catPlugins {
		allCats[c] = true
	}
	for c := range catAgents {
		allCats[c] = true
	}
	for c := range catBundles {
		allCats[c] = true
	}

	catDescs := map[string]string{
		"monitoring":  "Monitoring, alerting, and observability tools",
		"security":    "Security, compliance, and governance tools",
		"analytics":   "Analytics, cost analysis, and ML tools",
		"integration": "Integration and notification tools",
		"utility":     "Utility and helper tools",
		"operations":  "Operational and automation tools",
		"pipeline":    "Data pipeline and streaming tools",
		"development": "Developer productivity tools",
	}
	for c := range allCats {
		desc := catDescs[c]
		if desc == "" {
			desc = titleCase(c) + " tools"
		}
		stats.Categories = append(stats.Categories, CategoryInfo{
			Name:        c,
			Description: desc,
			PluginCount: catPlugins[c],
			AgentCount:  catAgents[c],
			BundleCount: catBundles[c],
		})
	}
	sort.Slice(stats.Categories, func(i, j int) bool {
		return stats.Categories[i].Name < stats.Categories[j].Name
	})

	// Top rated plugins (already collected above)
	sort.Slice(allPlugins, func(i, j int) bool { return allPlugins[i].Rating > allPlugins[j].Rating })
	if len(allPlugins) > 5 {
		allPlugins = allPlugins[:5]
	}
	stats.TopRated = allPlugins
	stats.FeaturedItems = featured

	return stats
}

// titleCase capitalizes the first letter of a string.
func titleCase(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
