package config

import "strings"

// ValidToolPacks is the canonical set of tool pack names accepted by
// ServerConfig.ToolPacks. The values mirror the constants in
// internal/server/tool_packs.go; keeping this set in the config package
// avoids an import cycle while preserving a single source of truth.
//
// When adding a new pack, update both this set and the corresponding
// constant in internal/server/tool_packs.go.
//
// The curated, secure-by-default surface is:
//
//   - core-read     : the default; <=15 strictly read-only tools.
//   - ops-write     : mutating/destructive operations (require approval grants).
//   - governance    : policy, audit, masking and approval controls.
//
// The pre-reset names "essential" and "ops" remain valid as deprecated aliases
// that emit migration guidance at startup.
var ValidToolPacks = map[string]bool{
	// Curated packs.
	"core-read":  true,
	"ops-write":  true,
	"governance": true,
	// Deprecated aliases (still accepted; emit migration guidance).
	"essential": true,
	"ops":       true,
}

// DefaultToolPack is the pack applied when ServerConfig.ToolPacks is empty. It
// is the curated read-only core.
const (
	PackCoreRead    = "core-read"
	PackOpsWrite    = "ops-write"
	PackGovernance  = "governance"
	DefaultToolPack = PackCoreRead
)

// DeprecatedToolPacks maps deprecated pack names to their guidance message.
// Used to emit a one-time migration warning at startup.
var DeprecatedToolPacks = map[string]string{
	"essential": "the 'essential' pack is deprecated and now maps to the read-only 'core-read' core; " +
		"add 'ops-write' to restore mutating tools (which now require approval grants)",
	"ops": "the 'ops' pack is deprecated; use 'core-read' plus 'ops-write' for mutating operations",
}

// HasToolPack reports whether the configuration explicitly enables pack. The
// deprecated "ops" alias is treated as enabling both core-read and ops-write.
func HasToolPack(packs []string, pack string) bool {
	for _, configured := range packs {
		configured = strings.ToLower(strings.TrimSpace(configured))
		switch {
		case configured == pack:
			return true
		case configured == "ops" && (pack == "core-read" || pack == "ops-write"):
			return true
		case configured == "essential" && pack == "core-read":
			return true
		}
	}
	return len(packs) == 0 && pack == DefaultToolPack
}
