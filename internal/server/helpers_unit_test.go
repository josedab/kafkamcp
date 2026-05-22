package server

import (
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
)

func TestSplitComma(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"empty", "", []string{""}},
		{"single", "a", []string{"a"}},
		{"two", "a,b", []string{"a", "b"}},
		{"trims whitespace", "a, b , c", []string{"a", "b", "c"}},
		{"trailing comma", "a,", []string{"a", ""}},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := splitComma(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("len mismatch: got %v want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("index %d: got %q want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestSplitString(t *testing.T) {
	t.Parallel()
	got := splitString("a;b;c", ';')
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("len mismatch: got %v want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("index %d: got %q want %q", i, got[i], want[i])
		}
	}
	// no separator
	if got := splitString("abc", ','); len(got) != 1 || got[0] != "abc" {
		t.Errorf("expected [abc], got %v", got)
	}
}

func TestTrimSpace(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in, want string
	}{
		{"", ""},
		{"abc", "abc"},
		{"  abc  ", "abc"},
		{"\t abc \t", "abc"},
		{"   ", ""},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			if got := trimSpace(tc.in); got != tc.want {
				t.Errorf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestArgString(t *testing.T) {
	t.Parallel()
	args := map[string]any{"a": "value", "b": 42, "c": nil}
	if got := argString(args, "a"); got != "value" {
		t.Errorf("string key: got %q", got)
	}
	if got := argString(args, "b"); got != "" {
		t.Errorf("non-string key should be empty, got %q", got)
	}
	if got := argString(args, "missing"); got != "" {
		t.Errorf("missing key should be empty, got %q", got)
	}
}

func TestBoolFromMap(t *testing.T) {
	t.Parallel()
	m := map[string]any{"yes": true, "no": false, "str": "x"}
	if !boolFromMap(m, "yes") {
		t.Error("expected true")
	}
	if boolFromMap(m, "no") {
		t.Error("expected false")
	}
	if boolFromMap(m, "str") {
		t.Error("non-bool should return false")
	}
	if boolFromMap(m, "missing") {
		t.Error("missing key should return false")
	}
}

func TestExtractStrings(t *testing.T) {
	t.Parallel()
	args := map[string]any{
		"list":  []any{"a", "b", "c"},
		"mixed": []any{"a", 1, "b"},
		"empty": []any{},
		"wrong": "not-a-list",
	}
	if got := extractStrings(args, "list"); len(got) != 3 || got[0] != "a" || got[2] != "c" {
		t.Errorf("got %v", got)
	}
	if got := extractStrings(args, "mixed"); len(got) != 2 {
		t.Errorf("expected only string elements, got %v", got)
	}
	if got := extractStrings(args, "empty"); len(got) != 0 {
		t.Errorf("expected empty, got %v", got)
	}
	if got := extractStrings(args, "wrong"); got != nil {
		t.Errorf("non-list arg should return nil, got %v", got)
	}
	if got := extractStrings(args, "missing"); got != nil {
		t.Errorf("missing key should return nil, got %v", got)
	}
}

func TestStringFromMap(t *testing.T) {
	t.Parallel()
	m := map[string]any{"a": "value", "b": 5}
	if got := stringFromMap(m, "a"); got != "value" {
		t.Errorf("got %q", got)
	}
	if got := stringFromMap(m, "b"); got != "" {
		t.Errorf("non-string should be empty, got %q", got)
	}
	if got := stringFromMap(m, "missing"); got != "" {
		t.Errorf("missing should be empty, got %q", got)
	}
}

func TestExtractAnyMapArg(t *testing.T) {
	t.Parallel()
	t.Run("returns nested map", func(t *testing.T) {
		t.Parallel()
		nested := map[string]any{"x": 1}
		req := mcp.CallToolRequest{}
		req.Params.Arguments = map[string]any{"cfg": nested}
		got := extractAnyMapArg(req, "cfg")
		if got == nil || got["x"] != 1 {
			t.Errorf("got %v", got)
		}
	})
	t.Run("missing key returns nil", func(t *testing.T) {
		t.Parallel()
		req := mcp.CallToolRequest{}
		req.Params.Arguments = map[string]any{"other": 1}
		if got := extractAnyMapArg(req, "cfg"); got != nil {
			t.Errorf("expected nil, got %v", got)
		}
	})
	t.Run("non-map arguments returns nil", func(t *testing.T) {
		t.Parallel()
		req := mcp.CallToolRequest{}
		req.Params.Arguments = "not a map"
		if got := extractAnyMapArg(req, "cfg"); got != nil {
			t.Errorf("expected nil, got %v", got)
		}
	})
}
