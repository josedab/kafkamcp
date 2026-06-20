package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// TestStableCoreReadSnapshot enforces the compatibility contract for the stable
// read-only core. The core-read pack is the STABLE, semver-protected surface;
// removing or renaming any tool in it is a breaking change. This test compares
// the live coreReadTools set against the committed snapshot at
// testdata/stable_core_read.json and fails if they diverge, so any breaking
// change to stable tools must be a deliberate, reviewed update to the snapshot.
func TestStableCoreReadSnapshot(t *testing.T) {
	path := filepath.Join("testdata", "stable_core_read.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading snapshot: %v", err)
	}
	var snap struct {
		Tools     []string `json:"tools"`
		Resources []string `json:"resources"`
	}
	if err := json.Unmarshal(data, &snap); err != nil {
		t.Fatalf("parsing snapshot: %v", err)
	}

	want := append([]string(nil), snap.Tools...)
	sort.Strings(want)

	got := make([]string, 0, len(coreReadTools))
	for name := range coreReadTools {
		got = append(got, name)
	}
	sort.Strings(got)

	if len(got) != len(want) {
		t.Fatalf("stable core-read tool count changed: got %d, snapshot has %d.\n"+
			"If intentional, update testdata/stable_core_read.json and treat as a compatibility change.\ngot=%v\nwant=%v",
			len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("stable core-read tools diverge from snapshot at index %d: got %q, want %q.\n"+
				"Removing/renaming a stable tool is a BREAKING change; update the snapshot deliberately.\ngot=%v\nwant=%v",
				i, got[i], want[i], got, want)
		}
	}

	resourceWant := append([]string(nil), snap.Resources...)
	resourceGot := append([]string(nil), stableResourceURIs...)
	sort.Strings(resourceWant)
	sort.Strings(resourceGot)
	if len(resourceGot) != len(resourceWant) {
		t.Fatalf("stable resource count changed: got %d, snapshot has %d\ngot=%v\nwant=%v",
			len(resourceGot), len(resourceWant), resourceGot, resourceWant)
	}
	for i := range resourceWant {
		if resourceGot[i] != resourceWant[i] {
			t.Fatalf("stable resources diverge at index %d: got %q, want %q\ngot=%v\nwant=%v",
				i, resourceGot[i], resourceWant[i], resourceGot, resourceWant)
		}
	}
}
