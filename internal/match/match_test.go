package match

import "testing"

func TestGlob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		input   string
		want    bool
	}{
		// Wildcard
		{"star matches anything", "*", "orders.created", true},
		{"star matches empty-like name", "*", "x", true},

		// Exact match
		{"exact match", "orders.created", "orders.created", true},
		{"exact mismatch", "orders.created", "orders.deleted", false},

		// path.Match glob patterns
		{"glob star prefix", "orders.*", "orders.created", true},
		{"glob star prefix mismatch", "orders.*", "payments.created", false},
		{"glob question mark", "order?", "orders", true},
		{"glob question mark mismatch", "order?", "or", false},

		// Dotted segment matching
		{"dotted segment wildcard", "orders.*", "orders.created", true},
		{"dotted segment exact", "orders.created", "orders.created", true},
		{"dotted segment mismatch prefix", "orders.*", "payments.created", false},
		{"dotted two segments", "a.b.*", "a.b.c", true},
		{"dotted two segments mismatch", "a.b.*", "a.x.c", false},
		{"dotted pattern shorter than name", "a.*", "a.b.c", true},
		{"dotted pattern longer than name", "a.b.c", "a.b", false},

		// No match cases
		{"completely different", "foo", "bar", false},
		{"empty pattern", "", "", true},
		{"empty pattern non-empty name", "", "something", false},
		{"substring not glob", "order", "orders", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := Glob(tt.pattern, tt.input)
			if got != tt.want {
				t.Errorf("Glob(%q, %q) = %v, want %v", tt.pattern, tt.input, got, tt.want)
			}
		})
	}
}
