package server

import "github.com/mark3labs/mcp-go/mcp"

// withCategory returns a ToolOption that sets the category metadata on a tool.
// AI agents can use this to filter and discover tools by domain.
func withCategory(category string) mcp.ToolOption {
	return func(t *mcp.Tool) {
		if t.Meta == nil {
			t.Meta = &mcp.Meta{}
		}
		if t.Meta.AdditionalFields == nil {
			t.Meta.AdditionalFields = make(map[string]any)
		}
		t.Meta.AdditionalFields["category"] = category
	}
}

func toolCategoryOf(t *mcp.Tool) string {
	if t == nil || t.Meta == nil || t.Meta.AdditionalFields == nil {
		return ""
	}
	if value, ok := t.Meta.AdditionalFields["category"].(string); ok {
		return value
	}
	return ""
}
