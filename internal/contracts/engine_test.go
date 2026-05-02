package contracts

import (
	"testing"
)

func TestNewEngine(t *testing.T) {
	t.Parallel()
	e := NewEngine()
	if e == nil {
		t.Fatal("expected non-nil engine")
	}
	if len(e.contracts) != 0 {
		t.Error("expected empty contracts map")
	}
	if len(e.stats) != 0 {
		t.Error("expected empty stats map")
	}
}

func TestRegisterContract(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		contract Contract
		wantErr bool
	}{
		{
			name: "valid contract",
			contract: Contract{
				Name:    "user-events",
				Version: "1.0.0",
				Topic:   "user.events",
				Owner:   "platform-team",
			},
			wantErr: false,
		},
		{
			name: "missing name",
			contract: Contract{
				Version: "1.0.0",
				Topic:   "user.events",
				Owner:   "platform-team",
			},
			wantErr: true,
		},
		{
			name: "missing version",
			contract: Contract{
				Name:  "user-events",
				Topic: "user.events",
				Owner: "platform-team",
			},
			wantErr: true,
		},
		{
			name: "missing topic",
			contract: Contract{
				Name:    "user-events",
				Version: "1.0.0",
				Owner:   "platform-team",
			},
			wantErr: true,
		},
		{
			name: "missing owner",
			contract: Contract{
				Name:    "user-events",
				Version: "1.0.0",
				Topic:   "user.events",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewEngine()
			err := e.RegisterContract(tt.contract)
			if (err != nil) != tt.wantErr {
				t.Errorf("RegisterContract() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRegisterContractSetsTimestamps(t *testing.T) {
	t.Parallel()
	e := NewEngine()
	c := Contract{
		Name:    "test",
		Version: "1.0.0",
		Topic:   "test.topic",
		Owner:   "team",
	}
	if err := e.RegisterContract(c); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, _ := e.GetContract("test.topic")
	if got.CreatedAt == "" {
		t.Error("expected CreatedAt to be set")
	}
	if got.UpdatedAt == "" {
		t.Error("expected UpdatedAt to be set")
	}
}

func TestGetContract(t *testing.T) {
	t.Parallel()
	e := NewEngine()
	_ = e.RegisterContract(Contract{
		Name:    "orders",
		Version: "2.0.0",
		Topic:   "order.created",
		Owner:   "commerce-team",
	})

	t.Run("existing topic", func(t *testing.T) {
		c, err := e.GetContract("order.created")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.Name != "orders" {
			t.Errorf("expected name 'orders', got %q", c.Name)
		}
		if c.Version != "2.0.0" {
			t.Errorf("expected version '2.0.0', got %q", c.Version)
		}
	})

	t.Run("missing topic", func(t *testing.T) {
		_, err := e.GetContract("nonexistent")
		if err == nil {
			t.Error("expected error for missing topic")
		}
	})
}

func TestListContracts(t *testing.T) {
	t.Parallel()
	e := NewEngine()

	t.Run("empty", func(t *testing.T) {
		list := e.ListContracts()
		if len(list) != 0 {
			t.Errorf("expected 0 contracts, got %d", len(list))
		}
	})

	_ = e.RegisterContract(Contract{Name: "c1", Version: "1.0", Topic: "t1", Owner: "o1"})
	_ = e.RegisterContract(Contract{Name: "c2", Version: "1.0", Topic: "t2", Owner: "o2"})

	t.Run("two contracts", func(t *testing.T) {
		list := e.ListContracts()
		if len(list) != 2 {
			t.Errorf("expected 2 contracts, got %d", len(list))
		}
	})
}

func TestValidateMessage_NoContract(t *testing.T) {
	t.Parallel()
	e := NewEngine()
	result := e.ValidateMessage("unknown.topic", map[string]any{"key": "value"})
	if result != nil {
		t.Error("expected nil result for unknown topic")
	}
}

func TestValidateMessage_NotNull(t *testing.T) {
	t.Parallel()
	e := NewEngine()
	_ = e.RegisterContract(Contract{
		Name:    "test",
		Version: "1.0",
		Topic:   "t",
		Owner:   "o",
		Quality: []QualityRule{
			{Field: "email", Rule: "not_null", Severity: "error"},
		},
	})

	tests := []struct {
		name  string
		value map[string]any
		valid bool
	}{
		{"present", map[string]any{"email": "a@b.com"}, true},
		{"missing", map[string]any{"name": "test"}, false},
		{"nil value", map[string]any{"email": nil}, false},
		{"empty string", map[string]any{"email": ""}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := e.ValidateMessage("t", tt.value)
			if r.Valid != tt.valid {
				t.Errorf("Valid = %v, want %v", r.Valid, tt.valid)
			}
		})
	}
}

func TestValidateMessage_Enum(t *testing.T) {
	t.Parallel()
	e := NewEngine()
	_ = e.RegisterContract(Contract{
		Name:    "test",
		Version: "1.0",
		Topic:   "t",
		Owner:   "o",
		Quality: []QualityRule{
			{Field: "status", Rule: "enum", Value: "active, inactive, pending", Severity: "error"},
		},
	})

	tests := []struct {
		name  string
		value map[string]any
		valid bool
	}{
		{"allowed value", map[string]any{"status": "active"}, true},
		{"another allowed", map[string]any{"status": "pending"}, true},
		{"disallowed", map[string]any{"status": "deleted"}, false},
		{"missing field", map[string]any{"other": "x"}, false},
		{"empty string", map[string]any{"status": ""}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := e.ValidateMessage("t", tt.value)
			if r.Valid != tt.valid {
				t.Errorf("Valid = %v, want %v", r.Valid, tt.valid)
			}
		})
	}
}

func TestValidateMessage_Regex(t *testing.T) {
	t.Parallel()
	e := NewEngine()
	_ = e.RegisterContract(Contract{
		Name:    "test",
		Version: "1.0",
		Topic:   "t",
		Owner:   "o",
		Quality: []QualityRule{
			{Field: "email", Rule: "regex", Value: `^[^@]+@[^@]+\.[^@]+$`, Severity: "warning"},
		},
	})

	tests := []struct {
		name  string
		value map[string]any
		valid bool
	}{
		{"valid email", map[string]any{"email": "user@example.com"}, true},
		{"invalid email", map[string]any{"email": "notanemail"}, false},
		{"missing field", map[string]any{"other": "x"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := e.ValidateMessage("t", tt.value)
			if r.Valid != tt.valid {
				t.Errorf("Valid = %v, want %v", r.Valid, tt.valid)
			}
		})
	}
}

func TestValidateMessage_MinLength(t *testing.T) {
	t.Parallel()
	e := NewEngine()
	_ = e.RegisterContract(Contract{
		Name:    "test",
		Version: "1.0",
		Topic:   "t",
		Owner:   "o",
		Quality: []QualityRule{
			{Field: "name", Rule: "min_length", Value: "3", Severity: "error"},
		},
	})

	tests := []struct {
		name  string
		value map[string]any
		valid bool
	}{
		{"long enough", map[string]any{"name": "Alice"}, true},
		{"exact min", map[string]any{"name": "Bob"}, true},
		{"too short", map[string]any{"name": "Al"}, false},
		{"missing field", map[string]any{"other": "x"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := e.ValidateMessage("t", tt.value)
			if r.Valid != tt.valid {
				t.Errorf("Valid = %v, want %v", r.Valid, tt.valid)
			}
		})
	}
}

func TestValidateMessage_MaxLength(t *testing.T) {
	t.Parallel()
	e := NewEngine()
	_ = e.RegisterContract(Contract{
		Name:    "test",
		Version: "1.0",
		Topic:   "t",
		Owner:   "o",
		Quality: []QualityRule{
			{Field: "code", Rule: "max_length", Value: "5", Severity: "warning"},
		},
	})

	tests := []struct {
		name  string
		value map[string]any
		valid bool
	}{
		{"within limit", map[string]any{"code": "ABC"}, true},
		{"exact max", map[string]any{"code": "ABCDE"}, true},
		{"exceeds max", map[string]any{"code": "ABCDEF"}, false},
		{"missing field ok", map[string]any{"other": "x"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := e.ValidateMessage("t", tt.value)
			if r.Valid != tt.valid {
				t.Errorf("Valid = %v, want %v", r.Valid, tt.valid)
			}
		})
	}
}

func TestValidateMessage_MultipleRules(t *testing.T) {
	t.Parallel()
	e := NewEngine()
	_ = e.RegisterContract(Contract{
		Name:    "multi",
		Version: "1.0",
		Topic:   "t",
		Owner:   "o",
		Quality: []QualityRule{
			{Field: "name", Rule: "not_null", Severity: "error"},
			{Field: "name", Rule: "min_length", Value: "2", Severity: "warning"},
			{Field: "status", Rule: "enum", Value: "on,off", Severity: "error"},
		},
	})

	r := e.ValidateMessage("t", map[string]any{"name": "A", "status": "maybe"})
	if r.Valid {
		t.Error("expected invalid result")
	}
	if len(r.Violations) != 2 {
		t.Errorf("expected 2 violations, got %d", len(r.Violations))
	}
}

func TestValidateMessage_ResultFields(t *testing.T) {
	t.Parallel()
	e := NewEngine()
	_ = e.RegisterContract(Contract{
		Name:    "events",
		Version: "1.0",
		Topic:   "my.topic",
		Owner:   "team",
		Quality: []QualityRule{
			{Field: "id", Rule: "not_null", Severity: "error"},
		},
	})

	r := e.ValidateMessage("my.topic", map[string]any{"id": "123"})
	if r.ContractName != "events" {
		t.Errorf("ContractName = %q, want 'events'", r.ContractName)
	}
	if r.Topic != "my.topic" {
		t.Errorf("Topic = %q, want 'my.topic'", r.Topic)
	}
	if r.Timestamp == "" {
		t.Error("expected non-empty Timestamp")
	}
	if len(r.Violations) != 0 {
		t.Errorf("expected 0 violations, got %d", len(r.Violations))
	}
}

func TestGetComplianceReport(t *testing.T) {
	t.Parallel()
	e := NewEngine()
	_ = e.RegisterContract(Contract{
		Name:    "test",
		Version: "1.0",
		Topic:   "t",
		Owner:   "o",
		Quality: []QualityRule{
			{Field: "id", Rule: "not_null", Severity: "error"},
		},
	})

	t.Run("no report yet", func(t *testing.T) {
		_, err := e.GetComplianceReport("t")
		if err == nil {
			t.Error("expected error for topic with no validations")
		}
	})

	// Generate some validations.
	e.ValidateMessage("t", map[string]any{"id": "1"})
	e.ValidateMessage("t", map[string]any{"id": "2"})
	e.ValidateMessage("t", map[string]any{"other": "x"}) // violation

	t.Run("report after validations", func(t *testing.T) {
		report, err := e.GetComplianceReport("t")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if report.TotalChecked != 3 {
			t.Errorf("TotalChecked = %d, want 3", report.TotalChecked)
		}
		if report.PassedCount != 2 {
			t.Errorf("PassedCount = %d, want 2", report.PassedCount)
		}
		if report.FailedCount != 1 {
			t.Errorf("FailedCount = %d, want 1", report.FailedCount)
		}
		expectedRate := float64(2) / float64(3) * 100.0
		if report.ComplianceRate != expectedRate {
			t.Errorf("ComplianceRate = %f, want %f", report.ComplianceRate, expectedRate)
		}
		if report.ViolationsByRule["not_null"] != 1 {
			t.Errorf("ViolationsByRule[not_null] = %d, want 1", report.ViolationsByRule["not_null"])
		}
		if report.ContractName != "test" {
			t.Errorf("ContractName = %q, want 'test'", report.ContractName)
		}
	})

	t.Run("unknown topic", func(t *testing.T) {
		_, err := e.GetComplianceReport("nonexistent")
		if err == nil {
			t.Error("expected error for unknown topic")
		}
	})
}

func TestValidateMessage_InvalidRegex(t *testing.T) {
	t.Parallel()
	e := NewEngine()
	_ = e.RegisterContract(Contract{
		Name:    "test",
		Version: "1.0",
		Topic:   "t",
		Owner:   "o",
		Quality: []QualityRule{
			{Field: "val", Rule: "regex", Value: "[invalid", Severity: "error"},
		},
	})

	r := e.ValidateMessage("t", map[string]any{"val": "test"})
	if r.Valid {
		t.Error("expected invalid result for bad regex")
	}
}

func TestValidateMessage_InvalidMinLengthValue(t *testing.T) {
	t.Parallel()
	e := NewEngine()
	_ = e.RegisterContract(Contract{
		Name:    "test",
		Version: "1.0",
		Topic:   "t",
		Owner:   "o",
		Quality: []QualityRule{
			{Field: "val", Rule: "min_length", Value: "notanumber", Severity: "error"},
		},
	})

	r := e.ValidateMessage("t", map[string]any{"val": "test"})
	if r.Valid {
		t.Error("expected invalid result for bad min_length value")
	}
}

func TestValidateMessage_InvalidMaxLengthValue(t *testing.T) {
	t.Parallel()
	e := NewEngine()
	_ = e.RegisterContract(Contract{
		Name:    "test",
		Version: "1.0",
		Topic:   "t",
		Owner:   "o",
		Quality: []QualityRule{
			{Field: "val", Rule: "max_length", Value: "notanumber", Severity: "error"},
		},
	})

	r := e.ValidateMessage("t", map[string]any{"val": "test"})
	if r.Valid {
		t.Error("expected invalid result for bad max_length value")
	}
}

func TestRegisterContractOverwrite(t *testing.T) {
	t.Parallel()
	e := NewEngine()
	_ = e.RegisterContract(Contract{Name: "v1", Version: "1.0", Topic: "t", Owner: "o"})
	_ = e.RegisterContract(Contract{Name: "v2", Version: "2.0", Topic: "t", Owner: "o"})

	c, _ := e.GetContract("t")
	if c.Name != "v2" {
		t.Errorf("expected overwritten contract name 'v2', got %q", c.Name)
	}
}

func TestSplitCSV(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  int
	}{
		{"a,b,c", 3},
		{"a, b, c", 3},
		{"single", 1},
		{"", 0},
	}

	for _, tt := range tests {
		got := splitCSV(tt.input)
		if len(got) != tt.want {
			t.Errorf("splitCSV(%q) = %d items, want %d", tt.input, len(got), tt.want)
		}
	}
}
