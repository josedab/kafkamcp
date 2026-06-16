package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// updateFlag: run 'go test ./internal/config/ -run TestSchemaContract -update' to refresh.
var updateContract = flag.Bool("update", false, "update testdata/schema_contract.json from the current docs/kafkamcp.schema.json")

// TestSchemaContract verifies that docs/kafkamcp.schema.json has not changed its
// stable contract without a deliberate, reviewed update to the snapshot file.
//
// The snapshot captures the structural contract (property names, types, enums,
// required lists) but not volatile fields (descriptions, examples). A mismatch
// means the schema was changed; the test output shows exactly what changed so
// the reviewer can decide whether to update the snapshot.
//
// To update after an intentional change:
//
//	go test ./internal/config/ -run TestSchemaContract -update
func TestSchemaContract(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")

	schemaPath := filepath.Join(repoRoot, "docs", "kafkamcp.schema.json")
	snapshotPath := filepath.Join(filepath.Dir(thisFile), "testdata", "schema_contract.json")

	schemaData, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("cannot read %s: %v\n\nEnsure you are running tests from the repository root.", schemaPath, err)
	}

	var raw map[string]any
	if err := json.Unmarshal(schemaData, &raw); err != nil {
		t.Fatalf("docs/kafkamcp.schema.json is not valid JSON: %v", err)
	}

	current := extractSchemaContract(raw)

	if *updateContract {
		out := map[string]any{
			"$schema_version": "1",
			"$source":         "docs/kafkamcp.schema.json",
			"$note":           "Stable config-schema contract. Any change to this file means a breaking or additive schema change. Run: go test ./internal/config/ -run TestSchemaContract -update",
			"schema":          current,
		}
		data, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			t.Fatalf("cannot marshal updated contract: %v", err)
		}
		if err := os.WriteFile(snapshotPath, data, 0o644); err != nil {
			t.Fatalf("cannot write %s: %v", snapshotPath, err)
		}
		t.Logf("updated %s", snapshotPath)
		return
	}

	snapshotData, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("snapshot not found at %s: %v\n\nRun 'go test ./internal/config/ -run TestSchemaContract -update' to create it.", snapshotPath, err)
	}

	var snapshotFile map[string]any
	if err := json.Unmarshal(snapshotData, &snapshotFile); err != nil {
		t.Fatalf("cannot parse %s: %v", snapshotPath, err)
	}

	snapshotSchema, ok := snapshotFile["schema"]
	if !ok {
		t.Fatalf("%s is missing 'schema' key", snapshotPath)
	}

	diffs := diffContracts("", snapshotSchema, current)
	if len(diffs) > 0 {
		t.Errorf("docs/kafkamcp.schema.json contract has changed from snapshot.\n"+
			"If this is intentional, run: go test ./internal/config/ -run TestSchemaContract -update\n\n"+
			"Changes detected (%d):\n%s", len(diffs), strings.Join(diffs, "\n"))
	}
}

// extractSchemaContract walks the JSON-Schema object tree and extracts the
// machine-stable contract: property names, types, enums, required lists,
// defaults, and numeric bounds. Descriptions, examples, and $id/$schema meta
// are excluded because they change frequently and are not structural.
func extractSchemaContract(obj map[string]any) map[string]any {
	result := map[string]any{}

	if t, ok := obj["type"]; ok {
		result["type"] = t
	}
	if stability, ok := obj["x-stability"]; ok {
		result["x-stability"] = stability
	}
	if e, ok := obj["enum"]; ok {
		if arr, ok := e.([]any); ok {
			result["enum"] = sortedStringSlice(arr)
		}
	}
	if req, ok := obj["required"]; ok {
		if arr, ok := req.([]any); ok {
			result["required"] = sortedStringSlice(arr)
		}
	}
	if def, ok := obj["default"]; ok {
		result["default"] = def
	}
	if min, ok := obj["minimum"]; ok {
		result["minimum"] = min
	}
	if max, ok := obj["maximum"]; ok {
		result["maximum"] = max
	}

	if props, ok := obj["properties"]; ok {
		if propsMap, ok := props.(map[string]any); ok {
			contracted := map[string]any{}
			for k, v := range propsMap {
				if vMap, ok := v.(map[string]any); ok {
					contracted[k] = extractSchemaContract(vMap)
				}
			}
			result["properties"] = contracted
		}
	}

	if items, ok := obj["items"]; ok {
		if itemsMap, ok := items.(map[string]any); ok {
			result["items"] = extractSchemaContract(itemsMap)
		}
	}

	return result
}

// sortedStringSlice converts a []any to a sorted []any of strings.
// This ensures deterministic ordering and JSON round-trip compatibility.
func sortedStringSlice(arr []any) []any {
	strs := make([]string, 0, len(arr))
	for _, v := range arr {
		strs = append(strs, fmt.Sprintf("%v", v))
	}
	sort.Strings(strs)
	out := make([]any, len(strs))
	for i, s := range strs {
		out[i] = s
	}
	return out
}

// diffContracts returns a list of human-readable diff lines between expected and actual.
func diffContracts(path string, expected, actual any) []string {
	var diffs []string

	switch e := expected.(type) {
	case map[string]any:
		a, ok := actual.(map[string]any)
		if !ok {
			diffs = append(diffs, fmt.Sprintf("  %s: type changed from object to %T", path, actual))
			return diffs
		}
		// Check for removed or changed keys
		for k, ev := range e {
			fullPath := joinPath(path, k)
			av, exists := a[k]
			if !exists {
				diffs = append(diffs, fmt.Sprintf("  REMOVED  %s", fullPath))
			} else {
				diffs = append(diffs, diffContracts(fullPath, ev, av)...)
			}
		}
		// Check for added keys
		for k := range a {
			if _, exists := e[k]; !exists {
				diffs = append(diffs, fmt.Sprintf("  ADDED    %s", joinPath(path, k)))
			}
		}
	case []any:
		a, ok := actual.([]any)
		if !ok {
			diffs = append(diffs, fmt.Sprintf("  %s: type changed from array to %T", path, actual))
			return diffs
		}
		if !reflect.DeepEqual(normalizeSlice(e), normalizeSlice(a)) {
			diffs = append(diffs, fmt.Sprintf("  CHANGED  %s: was %v, now %v", path, e, a))
		}
	default:
		if !reflect.DeepEqual(expected, actual) {
			diffs = append(diffs, fmt.Sprintf("  CHANGED  %s: was %v, now %v", path, expected, actual))
		}
	}
	return diffs
}

func joinPath(base, key string) string {
	if base == "" {
		return key
	}
	return base + "." + key
}

func normalizeSlice(s []any) []string {
	strs := make([]string, 0, len(s))
	for _, v := range s {
		strs = append(strs, fmt.Sprintf("%v", v))
	}
	sort.Strings(strs)
	return strs
}
