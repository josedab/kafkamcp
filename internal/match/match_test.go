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

func TestGlob_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		input   string
		want    bool
	}{
		// Nested wildcards
		{"nested wildcards", "*.*", "a.b", true},
		{"nested wildcards three segments", "*.*.*", "a.b.c", true},
		{"nested wildcards mismatch depth", "*.*.*", "a.b", false},
		{"wildcard in middle", "a.*.c", "a.b.c", true},
		{"wildcard in middle mismatch", "a.*.c", "a.b.d", false},
		{"multiple wildcards", "*.*.events", "prod.orders.events", true},

		// Empty strings
		{"both empty", "", "", true},
		{"empty name star pattern", "*", "", true},
		{"empty pattern non-empty name", "", "x", false},

		// Special characters
		{"hyphen in name", "my-topic.*", "my-topic.created", true},
		{"underscore in name", "my_topic.*", "my_topic.events", true},
		{"numeric segments", "topic.123.*", "topic.123.data", true},
		{"dots only in name", "*", "...", true},
		{"pattern with brackets", "[abc]", "[abc]", true},

		// Deep nesting
		{"four-level exact", "a.b.c.d", "a.b.c.d", true},
		{"four-level wildcard tail", "a.b.c.*", "a.b.c.d", true},
		{"four-level mismatch", "a.b.c.d", "a.b.c.e", false},

		// Single character names
		{"single char match", "a", "a", true},
		{"single char mismatch", "a", "b", false},
		{"question mark on single char", "?", "x", true},

		// Repeated segments
		{"repeated segments", "a.a.a", "a.a.a", true},
		{"repeated with wildcard", "a.*.a", "a.b.a", true},
		{"repeated mismatch", "a.a.a", "a.a.b", false},

		// Long names
		{"long topic name", "production.region-us-east.service-orders.*", "production.region-us-east.service-orders.events", true},
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
