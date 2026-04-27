package masking

import (
	"io"
	"log/slog"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/config"
)

func FuzzMaskValue(f *testing.F) {
	f.Add("user@example.com")
	f.Add("123-45-6789")
	f.Add("+1 (555) 123-4567")
	f.Add("4111 1111 1111 1111")
	f.Add("no pii here")
	f.Add("")
	f.Add("{\"email\": \"test@test.com\"}")
	f.Add("call me at 555-0100 or email foo@bar.com")
	f.Add("SSN: 000-00-0000, CC: 0000-0000-0000-0000")

	cfg := config.MaskingConfig{
		Enabled: true,
		Rules: []config.MaskingRule{
			{Name: "email", Type: "email"},
			{Name: "ssn", Type: "ssn"},
			{Name: "phone", Type: "phone"},
			{Name: "cc", Type: "credit_card"},
			{Name: "ner", Type: "ner"},
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	f.Fuzz(func(t *testing.T, input string) {
		e := NewEngine(cfg, logger)
		// Should never panic
		result := e.MaskValue("agent-1", input)
		if result == nil {
			t.Error("nil result from MaskValue")
		}
	})
}

func FuzzAddRuleRegex(f *testing.F) {
	f.Add(`\d+`)
	f.Add(`[a-z]+`)
	f.Add(`(?i)secret`)
	f.Add(`(`)
	f.Add(`[`)
	f.Add(`***`)
	f.Add("")
	f.Add(`(?P<name>test)`)
	f.Add(`a{1000}`)

	f.Fuzz(func(t *testing.T, pattern string) {
		cfg := config.MaskingConfig{Enabled: true}
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		e := NewEngine(cfg, logger)
		// Should never panic, may return error for invalid patterns
		_ = e.AddRule("fuzz-rule", "regex", "", pattern, "***", nil)
	})
}

func FuzzMatchFieldPath(f *testing.F) {
	f.Add("user.email", "user.email")
	f.Add("**.email", "user.profile.email")
	f.Add("**email**", "nested.user_email.field")
	f.Add("*", "anything")
	f.Add("a.*.c", "a.b.c")
	f.Add("", "")

	f.Fuzz(func(t *testing.T, pattern, path string) {
		// Should never panic
		_ = matchFieldPath(pattern, path)
	})
}

func FuzzLuhnCheck(f *testing.F) {
	f.Add("4111111111111111")
	f.Add("1234567890123")
	f.Add("0000000000000")
	f.Add("9999999999999999999")
	f.Add("not-a-number")
	f.Add("")

	f.Fuzz(func(t *testing.T, number string) {
		// Should never panic
		_ = luhnCheck(number)
	})
}
