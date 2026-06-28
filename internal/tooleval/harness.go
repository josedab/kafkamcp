// Package tooleval is a deterministic, offline evaluation harness for KafkaMCP's
// tool surface. It validates — without any paid external LLM APIs — that a
// realistic set of Kafka operator tasks route to the correct tools and that the
// product's safety invariants hold:
//
//   - the expected tool exists and is uniquely named,
//   - read-only tasks never route to a write/destructive tool,
//   - write tasks route to a tool that is NOT in the default read-only pack,
//   - adversarial prompts (e.g. "delete everything") do not smuggle an unsafe
//     tool into the read-only surface,
//   - each task's expected tool is selected by a deterministic keyword router
//     over a bounded candidate catalog.
//
// The fixtures live in fixtures.go and are exercised by harness_test.go, which
// CI runs to enforce the invariants on every change.
package tooleval

import (
	"sort"
	"strings"
)

// Task is a single evaluation fixture.
type Task struct {
	// Name is a short identifier for the task.
	Name string
	// Prompt is the natural-language request an agent might receive.
	Prompt string
	// ExpectedTool is the tool that should handle the prompt.
	ExpectedTool string
	// Write indicates the task requires a mutating/destructive tool.
	Write bool
	// Adversarial marks prompts crafted to try to escalate to an unsafe tool.
	Adversarial bool
}

// candidate is a routable tool with matching keywords.
type candidate struct {
	tool     string
	keywords []string
	write    bool
}

// catalog is the bounded set of tools the router chooses among. It intentionally
// covers the read core, the write set, and the governance tools so routing and
// safety can both be validated. Keywords are lowercase.
var catalog = []candidate{
	// --- core-read (read-only) ---
	{"kafka_list_topics", []string{"list", "topics", "show", "enumerate"}, false},
	{"kafka_describe_topic", []string{"describe", "topic", "details", "configuration", "partitions"}, false},
	{"kafka_list_schemas", []string{"schemas", "registry", "subjects"}, false},
	{"kafka_get_schema", []string{"schema", "fetch", "definition"}, false},
	{"kafka_check_schema_compatibility", []string{"compatibility", "compatible", "evolve", "breaking"}, false},
	{"kafka_consume", []string{"consume", "read", "peek", "tail"}, false},
	{"kafka_search", []string{"search", "find", "grep"}, false},
	{"kafka_list_consumer_groups", []string{"consumer", "groups"}, false},
	{"kafka_describe_consumer_group", []string{"members", "group", "membership"}, false},
	{"kafka_diagnose_lag", []string{"lag", "behind", "delay", "backlog"}, false},
	{"kafka_analyze_dlq", []string{"dlq", "dead", "letter", "poison", "failed"}, false},
	{"kafka_health", []string{"health", "healthy", "alive", "broker"}, false},
	{"kafka_metrics", []string{"metrics", "throughput", "stats"}, false},
	{"kafka_discover_clusters", []string{"clusters", "available"}, false},
	{"kafka_discover_tools", []string{"tools", "capabilities"}, false},
	// --- ops-write (mutating) ---
	{"kafka_produce", []string{"produce", "publish", "send", "emit"}, true},
	{"kafka_create_topic", []string{"create", "new", "provision"}, true},
	{"kafka_delete_topic", []string{"delete", "remove", "drop", "destroy", "wipe"}, true},
	{"kafka_alter_topic", []string{"alter", "modify", "change", "increase"}, true},
	{"kafka_reset_offsets", []string{"reset", "offsets", "rewind", "seek"}, true},
	{"kafka_acl_manage", []string{"acl", "permission", "grant", "revoke"}, true},
	{"kafka_create_connector", []string{"connector", "sink", "source"}, true},
	{"kafka_delete_connector", []string{"delete", "connector", "deregister"}, true},
	{"kafka_restart_connector", []string{"restart", "bounce", "connector"}, true},
	// --- governance ---
	{"kafka_policy_evaluate", []string{"policy", "authorization", "rbac", "evaluate"}, false},
	{"kafka_policy_simulate", []string{"policy", "simulate", "what", "if"}, false},
	{"kafka_approval", []string{"approval", "approve", "authorize"}, false},
}

// Route returns the highest-scoring tool for a prompt using deterministic
// keyword overlap. Ties are broken by tool name for stability.
func Route(prompt string) string {
	tokens := tokenize(prompt)
	tokenSet := make(map[string]struct{}, len(tokens))
	for _, t := range tokens {
		tokenSet[t] = struct{}{}
	}

	sorted := make([]candidate, len(catalog))
	copy(sorted, catalog)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].tool < sorted[j].tool })

	bestTool := ""
	bestScore := 0
	for _, c := range sorted {
		score := 0
		for _, kw := range c.keywords {
			if _, ok := tokenSet[kw]; ok {
				score++
			}
		}
		if score > bestScore {
			bestScore = score
			bestTool = c.tool
		}
	}
	return bestTool
}

// CatalogTools returns the sorted set of tool names in the eval catalog.
func CatalogTools() []string {
	out := make([]string, 0, len(catalog))
	for _, c := range catalog {
		out = append(out, c.tool)
	}
	sort.Strings(out)
	return out
}

// IsCatalogWriteTool reports whether the catalog marks a tool as mutating.
func IsCatalogWriteTool(tool string) bool {
	for _, c := range catalog {
		if c.tool == tool {
			return c.write
		}
	}
	return false
}

func tokenize(s string) []string {
	s = strings.ToLower(s)
	return strings.FieldsFunc(s, func(r rune) bool {
		return !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9')
	})
}
