package masking

import (
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/josedab/kafkamcp/internal/config"
)

func TestRuleCompilationCharacterization(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tests := []struct {
		name                 string
		ruleType             string
		pattern              string
		wantConstructorRules int
		wantAddRuleError     bool
		wantPolicyError      bool
	}{
		{
			name:                 "built in rule accepted everywhere",
			ruleType:             "email",
			wantConstructorRules: 1,
		},
		{
			name:                 "safe leading case flag accepted everywhere",
			ruleType:             "regex",
			pattern:              `(?i)secret`,
			wantConstructorRules: 1,
		},
		{
			name:                 "mid expression case flag rejected only by AddRule",
			ruleType:             "regex",
			pattern:              `prefix(?i)suffix`,
			wantConstructorRules: 1,
			wantAddRuleError:     true,
		},
		{
			name:                 "empty regex rejected only by AddRule",
			ruleType:             "regex",
			wantConstructorRules: 1,
			wantAddRuleError:     true,
		},
		{
			name:                 "unknown type rejected only by AddRule",
			ruleType:             "custom",
			wantConstructorRules: 1,
			wantAddRuleError:     true,
		},
		{
			name:             "other regexp flags rejected everywhere",
			ruleType:         "regex",
			pattern:          `prefix(?s)suffix`,
			wantAddRuleError: true,
			wantPolicyError:  true,
		},
		{
			name:             "long regexp rejected everywhere",
			ruleType:         "regex",
			pattern:          strings.Repeat("a", maxRegexPatternLength+1),
			wantAddRuleError: true,
			wantPolicyError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			engine := NewEngine(config.MaskingConfig{
				Enabled: true,
				Rules: []config.MaskingRule{{
					Name: "rule", Type: tt.ruleType, Pattern: tt.pattern,
				}},
			}, logger)
			if got := engine.RuleCount(); got != tt.wantConstructorRules {
				t.Fatalf("NewEngine rule count = %d, want %d", got, tt.wantConstructorRules)
			}

			dynamicEngine := NewEngine(config.MaskingConfig{}, logger)
			addErr := dynamicEngine.AddRule("rule", tt.ruleType, "", tt.pattern, "", nil)
			if (addErr != nil) != tt.wantAddRuleError {
				t.Fatalf("AddRule error = %v, wantError=%t", addErr, tt.wantAddRuleError)
			}

			policyManager := NewPolicyManager()
			policyErr := policyManager.Apply(&MaskingPolicy{
				Rules: []PolicyRule{{
					Name: "rule", Type: tt.ruleType, Pattern: tt.pattern,
				}},
			})
			if (policyErr != nil) != tt.wantPolicyError {
				t.Fatalf("PolicyManager.Apply error = %v, wantError=%t", policyErr, tt.wantPolicyError)
			}
		})
	}
}
