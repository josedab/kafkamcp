package cost

import (
	"testing"
)

func TestCompressionFactor(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  float64
	}{
		{"gzip", 0.3},
		{"snappy", 0.5},
		{"lz4", 0.5},
		{"zstd", 0.25},
		{"none", 1.0},
		{"", 1.0},
		{"unknown", 1.0},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := compressionFactor(tt.input)
			if got != tt.want {
				t.Errorf("compressionFactor(%q) = %f, want %f", tt.input, got, tt.want)
			}
		})
	}
}
