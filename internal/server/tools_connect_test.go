package server

import (
	"strings"
	"testing"
)

func TestValidateConnectorConfig_NormalPassthrough(t *testing.T) {
	config := map[string]string{
		"connector.class": "io.debezium.connector.mysql.MySqlConnector",
		"tasks.max":       "1",
	}
	deny, redacted := validateConnectorConfig(config)
	if deny != nil {
		t.Fatalf("unexpected denial: %v", deny)
	}
	if redacted["connector.class"] != "io.debezium.connector.mysql.MySqlConnector" {
		t.Errorf("expected passthrough, got %s", redacted["connector.class"])
	}
	if redacted["tasks.max"] != "1" {
		t.Errorf("expected passthrough, got %s", redacted["tasks.max"])
	}
}

func TestValidateConnectorConfig_RedactsSensitiveKeys(t *testing.T) {
	sensitiveKeys := []string{
		"database.password",
		"aws.secret.access.key",
		"api.key.value",
		"sasl.jaas.config",
		"ssl.keystore.password",
		"ssl.truststore.password",
		"ssl.key.password",
		"my.secret.setting",
		"auth.token.value",
		"consumer.override.sasl.jaas.config",
		"producer.override.sasl.jaas.config",
	}

	for _, key := range sensitiveKeys {
		config := map[string]string{key: "supersecret123"}
		deny, redacted := validateConnectorConfig(config)
		if deny != nil {
			t.Fatalf("unexpected denial for key %s", key)
		}
		if redacted[key] != "***REDACTED***" {
			t.Errorf("key %q should be redacted, got %q", key, redacted[key])
		}
	}
}

func TestValidateConnectorConfig_CaseInsensitiveRedaction(t *testing.T) {
	config := map[string]string{
		"Database.PASSWORD": "secret",
		"MY.SECRET.KEY":     "hidden",
	}
	deny, redacted := validateConnectorConfig(config)
	if deny != nil {
		t.Fatalf("unexpected denial")
	}
	for k, v := range redacted {
		if v != "***REDACTED***" {
			t.Errorf("key %q should be redacted (case insensitive), got %q", k, v)
		}
	}
}

func TestValidateConnectorConfig_OversizedKeyRejected(t *testing.T) {
	longKey := strings.Repeat("x", maxInputLength+1)
	config := map[string]string{longKey: "value"}
	deny, _ := validateConnectorConfig(config)
	if deny == nil {
		t.Fatal("expected denial for oversized key")
	}
}

func TestValidateConnectorConfig_OversizedValueRejected(t *testing.T) {
	longVal := strings.Repeat("v", maxInputLength+1)
	config := map[string]string{"key": longVal}
	deny, _ := validateConnectorConfig(config)
	if deny == nil {
		t.Fatal("expected denial for oversized value")
	}
}

func TestValidateConnectorConfig_EmptyConfig(t *testing.T) {
	deny, redacted := validateConnectorConfig(map[string]string{})
	if deny != nil {
		t.Fatalf("unexpected denial for empty config")
	}
	if len(redacted) != 0 {
		t.Errorf("expected empty redacted map, got %v", redacted)
	}
}

func TestValidateConnectorConfig_MultipleSensitiveKeys(t *testing.T) {
	config := map[string]string{
		"normal.key":       "visible",
		"database.password": "secret1",
		"api.key":          "secret2",
	}
	deny, redacted := validateConnectorConfig(config)
	if deny != nil {
		t.Fatalf("unexpected denial")
	}
	if redacted["normal.key"] != "visible" {
		t.Errorf("normal key should not be redacted, got %s", redacted["normal.key"])
	}
	if redacted["database.password"] != "***REDACTED***" {
		t.Errorf("password key should be redacted")
	}
	if redacted["api.key"] != "***REDACTED***" {
		t.Errorf("api.key should be redacted")
	}
}
