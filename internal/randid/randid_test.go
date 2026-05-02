package randid

import (
	"strings"
	"testing"
)

func TestNew(t *testing.T) {
	tests := []struct {
		prefix string
	}{
		{prefix: "run"},
		{prefix: "exec"},
		{prefix: ""},
		{prefix: "long-prefix-value"},
	}

	for _, tt := range tests {
		t.Run("prefix="+tt.prefix, func(t *testing.T) {
			id := New(tt.prefix)

			wantPrefix := tt.prefix + "-"
			if !strings.HasPrefix(id, wantPrefix) {
				t.Errorf("New(%q) = %q, want prefix %q", tt.prefix, id, wantPrefix)
			}

			// 16 hex chars after the prefix and dash
			hexPart := strings.TrimPrefix(id, wantPrefix)
			if len(hexPart) != 16 {
				t.Errorf("hex portion of New(%q) has length %d, want 16", tt.prefix, len(hexPart))
			}

			// Ensure uniqueness across calls
			id2 := New(tt.prefix)
			if id == id2 {
				t.Errorf("New(%q) returned duplicate IDs: %q", tt.prefix, id)
			}
		})
	}
}
