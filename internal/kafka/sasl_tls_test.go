package kafka

import (
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/josedab/kafkamcp/internal/config"
)

// TestSASL_RequiresTLS verifies that PLAIN and SCRAM mechanisms refuse to
// send credentials over plaintext unless KAFKAMCP_ALLOW_INSECURE is set
// (F10 regression).
func TestSASL_RequiresTLS(t *testing.T) {
	t.Parallel()
	mgr := &Manager{logger: slog.Default()}

	for _, mech := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"} {
		mech := mech
		t.Run(mech+"/plaintext-rejected", func(t *testing.T) {
			cl := &config.ClusterConfig{
				Name:             "test",
				BootstrapServers: "localhost:9092",
				SecurityProtocol: "PLAINTEXT",
				SASLMechanism:    mech,
				SASLUsername:     "u",
				SASLPassword:     "p",
			}
			_, err := mgr.clientOpts(cl)
			if err == nil {
				t.Fatalf("expected error for %s over plaintext", mech)
			}
			if !strings.Contains(err.Error(), "requires TLS") {
				t.Errorf("expected TLS-required error, got %v", err)
			}
		})

		t.Run(mech+"/ssl-accepted", func(t *testing.T) {
			cl := &config.ClusterConfig{
				Name:             "test",
				BootstrapServers: "localhost:9092",
				SecurityProtocol: "SASL_SSL",
				SASLMechanism:    mech,
				SASLUsername:     "u",
				SASLPassword:     "p",
			}
			if _, err := mgr.clientOpts(cl); err != nil {
				t.Errorf("unexpected error for %s over SSL: %v", mech, err)
			}
		})
	}
}

// TestSASL_AllowInsecureOverride verifies the documented escape hatch works.
func TestSASL_AllowInsecureOverride(t *testing.T) {
	t.Setenv("KAFKAMCP_ALLOW_INSECURE", "true")
	mgr := &Manager{logger: slog.Default()}
	cl := &config.ClusterConfig{
		Name:             "test",
		BootstrapServers: "localhost:9092",
		SecurityProtocol: "PLAINTEXT",
		SASLMechanism:    "SCRAM-SHA-256",
		SASLUsername:     "u",
		SASLPassword:     "p",
	}
	if _, err := mgr.clientOpts(cl); err != nil {
		t.Errorf("with override, expected acceptance, got %v", err)
	}
	_ = os.Unsetenv("KAFKAMCP_ALLOW_INSECURE")
}
