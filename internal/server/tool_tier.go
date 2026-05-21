package server

import "github.com/mark3labs/mcp-go/mcp"

// Tool tiers classify each MCP tool by how essential it is for the average
// AI-agent workflow. They drive the adaptive tool-pack registration
// implemented in tool_packs.go so installations can ship a small, focused
// surface (≤50 tools) by default while still allowing power users to opt
// into the full ≈300-tool catalog.
//
// LLM tool-routing quality degrades sharply past 40–60 advertised tools, so
// every new tool MUST declare a tier — implicitly via the per-pack lookup
// table or explicitly via withTier. Tools without an explicit tier default
// to TierAdvanced.
const (
	// TierEssential is the small set of tools an agent needs to do real
	// Kafka work: admin (list/describe/create topics, consumer groups,
	// ACLs read), schema-aware produce/consume, search, audit query and
	// cluster info. Always registered.
	TierEssential = "essential"

	// TierAdvanced is the default tier for everything that is real,
	// production-ready and useful but not strictly required for the
	// minimum viable agent workflow.
	TierAdvanced = "advanced"

	// TierExperimental marks tools backed by metadata-only simulations or
	// stub implementations. They sit behind both the experimental config
	// flags AND this tier so the default pack never exposes them.
	TierExperimental = "experimental"
)

// withTier returns an mcp.ToolOption that tags the tool with a tier in its
// MCP metadata. The pack filter uses this tag (read via toolTier) to decide
// whether a tool gets registered.
func withTier(tier string) mcp.ToolOption {
	return func(t *mcp.Tool) {
		if t.Meta == nil {
			t.Meta = &mcp.Meta{}
		}
		if t.Meta.AdditionalFields == nil {
			t.Meta.AdditionalFields = make(map[string]any)
		}
		t.Meta.AdditionalFields["tier"] = tier
	}
}

// toolTier returns the tier annotation for a tool, defaulting to
// TierAdvanced when the metadata has not set one explicitly.
func toolTier(t *mcp.Tool) string {
	if t == nil || t.Meta == nil || t.Meta.AdditionalFields == nil {
		return TierAdvanced
	}
	if v, ok := t.Meta.AdditionalFields["tier"]; ok {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	return TierAdvanced
}

// toolCategoryOf returns the category annotation for a tool, defaulting to
// the empty string when it is not set. Categories are author-supplied
// groupings (streaming, governance, analytics, …) used for pack lookups.
func toolCategoryOf(t *mcp.Tool) string {
	if t == nil || t.Meta == nil || t.Meta.AdditionalFields == nil {
		return ""
	}
	if v, ok := t.Meta.AdditionalFields["category"]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
