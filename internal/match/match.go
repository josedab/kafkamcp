// Package match provides glob-style pattern matching for topic and subject names.
package match

import (
	"path"
	"strings"
)

// Glob matches a name against a glob-style pattern.
// Supports "*" wildcard, exact match, and dotted segment matching (e.g., "orders.*").
func Glob(pattern, name string) bool {
	if pattern == "*" {
		return true
	}

	if pattern == name {
		return true
	}

	if matched, err := path.Match(pattern, name); err == nil && matched {
		return true
	}

	// Support "prefix.*" patterns by also trying with segment matching
	if strings.Contains(pattern, ".") {
		parts := strings.Split(pattern, ".")
		nameParts := strings.Split(name, ".")
		if len(parts) <= len(nameParts) {
			allMatch := true
			for i, p := range parts {
				m, err := path.Match(p, nameParts[i])
				if err != nil || !m {
					allMatch = false
					break
				}
			}
			if allMatch {
				return true
			}
		}
	}

	return false
}
