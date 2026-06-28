package doctor

import (
	"context"
	"errors"
	"testing"

	"github.com/josedab/kafkamcp/internal/config"
)

func findCheck(r Report, name string) (Check, bool) {
	for _, c := range r.Checks {
		if c.Name == name {
			return c, true
		}
	}
	return Check{}, false
}

func baseStdioCfg() *config.Config {
	c := &config.Config{}
	c.Server.Transport = "stdio"
	return c
}

func TestRun_StdioMinimalPasses(t *testing.T) {
	r := Run(context.Background(), baseStdioCfg(), Options{})
	if !r.OK {
		t.Fatalf("stdio minimal config should pass (warnings allowed): %s", r.Summary())
	}
	if c, _ := findCheck(r, "policy"); c.Status != StatusWarn {
		t.Errorf("expected policy warning for default_deny=false, got %s", c.Status)
	}
}

func TestRun_NetworkWithoutSecurityFails(t *testing.T) {
	c := &config.Config{}
	c.Server.Transport = "streamable-http"
	r := Run(context.Background(), c, Options{})
	if r.OK {
		t.Fatal("network transport without security must fail the doctor")
	}
	chk, ok := findCheck(r, "transport-security")
	if !ok || chk.Status != StatusFail {
		t.Fatalf("expected transport-security failure, got %+v", chk)
	}
}

func TestRun_MaskingKeyChecks(t *testing.T) {
	c := baseStdioCfg()
	c.Masking.Enabled = true
	// No key -> warn (fail closed).
	r := Run(context.Background(), c, Options{})
	if chk, _ := findCheck(r, "masking-key"); chk.Status != StatusWarn {
		t.Errorf("expected masking-key warn without key, got %s", chk.Status)
	}
	// Valid key -> ok.
	t.Setenv("KAFKAMCP_MASKING_KEY", "0123456789abcdef0123456789abcdef")
	r = Run(context.Background(), c, Options{})
	if chk, _ := findCheck(r, "masking-key"); chk.Status != StatusOK {
		t.Errorf("expected masking-key ok with valid key, got %s", chk.Status)
	}
}

func TestRun_SqliteWithoutPathFails(t *testing.T) {
	c := baseStdioCfg()
	c.Server.Persistence.Backend = "sqlite"
	r := Run(context.Background(), c, Options{})
	if r.OK {
		t.Fatal("sqlite backend without path must fail")
	}
	if chk, _ := findCheck(r, "persistence"); chk.Status != StatusFail {
		t.Errorf("expected persistence failure, got %s", chk.Status)
	}
}

func TestRun_KafkaProbeFailurePropagates(t *testing.T) {
	c := baseStdioCfg()
	r := Run(context.Background(), c, Options{
		KafkaProbe: func(context.Context) error { return errors.New("connection refused") },
	})
	if r.OK {
		t.Fatal("failing kafka probe must fail the doctor")
	}
	chk, _ := findCheck(r, "kafka-connectivity")
	if chk.Status != StatusFail {
		t.Errorf("expected kafka-connectivity failure, got %s", chk.Status)
	}
}

func TestRun_DeprecatedPackWarns(t *testing.T) {
	c := baseStdioCfg()
	c.Server.ToolPacks = []string{"essential"}
	r := Run(context.Background(), c, Options{})
	if chk, _ := findCheck(r, "tool-packs"); chk.Status != StatusWarn {
		t.Errorf("expected deprecated pack warning, got %s", chk.Status)
	}
}

func TestReport_JSONShape(t *testing.T) {
	r := Run(context.Background(), baseStdioCfg(), Options{})
	if len(r.Checks) < 8 {
		t.Fatalf("expected a comprehensive set of checks, got %d", len(r.Checks))
	}
}
