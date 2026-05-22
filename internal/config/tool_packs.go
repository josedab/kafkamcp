package config

// ValidToolPacks is the canonical set of tool pack names accepted by
// ServerConfig.ToolPacks. The values mirror the constants in
// internal/server/tool_packs.go; keeping this set in the config package
// avoids an import cycle while preserving a single source of truth.
//
// When adding a new pack, update both this set and the corresponding
// constant in internal/server/tool_packs.go.
var ValidToolPacks = map[string]bool{
	"essential":    true,
	"ops":          true,
	"analytics":    true,
	"governance":   true,
	"streaming":    true,
	"experimental": true,
	"all":          true,
}
