package encryption

import (
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

// --- GetAgentPolicy ---

func TestGetAgentPolicy_Existing(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.SetAgentPolicy("agent-1", []string{"key-1"}, []string{"orders"}, true, true)

	policy, err := e.GetAgentPolicy("agent-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if policy.AgentID != "agent-1" {
		t.Errorf("expected agent-1, got %s", policy.AgentID)
	}
	if !policy.CanEncrypt || !policy.CanDecrypt {
		t.Error("expected both encrypt and decrypt permissions")
	}
}

func TestGetAgentPolicy_NonExistent(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	_, err := e.GetAgentPolicy("no-such-agent")
	if err == nil {
		t.Error("expected error for non-existent agent")
	}
}

func TestGetAgentPolicy_AfterUpdate(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	e.SetAgentPolicy("agent-1", []string{"key-1"}, []string{"orders"}, true, false)
	e.SetAgentPolicy("agent-1", []string{"key-1", "key-2"}, []string{"orders", "payments"}, true, true)

	policy, err := e.GetAgentPolicy("agent-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(policy.AllowedKeyIDs) != 2 {
		t.Errorf("expected 2 key IDs, got %d", len(policy.AllowedKeyIDs))
	}
	if !policy.CanDecrypt {
		t.Error("expected decrypt permission after update")
	}
}

// --- GetKey ---

func TestGetKey_Existing(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	key, _ := e.CreateKey("test-key", ProviderLocal, "AES-256-GCM")

	got, err := e.GetKey(key.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Name != "test-key" {
		t.Errorf("expected test-key, got %s", got.Name)
	}
	if got.Status != "active" {
		t.Errorf("expected active, got %s", got.Status)
	}
	// Should not expose key material
	if got.keyMaterial != nil {
		t.Error("GetKey should not expose key material")
	}
}

func TestGetKey_NonExistent(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	_, err := e.GetKey("nonexistent-key")
	if err == nil {
		t.Error("expected error for non-existent key")
	}
}

func TestGetKey_Revoked(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	key, _ := e.CreateKey("revoke-test", ProviderLocal, "")
	e.RevokeKey(key.ID)

	got, err := e.GetKey(key.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != "revoked" {
		t.Errorf("expected revoked, got %s", got.Status)
	}
}

// --- DecryptMessage ---

func TestDecryptMessage_NonJSONWithENCPrefix(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	key, _ := e.CreateKey("dec-key", ProviderLocal, "AES-256-GCM")
	e.ConfigureFieldEncryption("topic", key.ID, []string{"data"})

	// Encrypt a non-JSON message: returns base64 without ENC: prefix
	encResult, err := e.EncryptMessage("", "topic", "plain text")
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	// DecryptMessage with "ENC:" prefix manually added
	prefixed := "ENC:" + encResult.EncryptedMessage
	decResult, err := e.DecryptMessage("", "topic", prefixed)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if decResult.DecryptedFields != 1 {
		t.Errorf("expected 1 decrypted field, got %d", decResult.DecryptedFields)
	}
	if decResult.DecryptedMessage != "plain text" {
		t.Errorf("expected original text, got %q", decResult.DecryptedMessage)
	}
}

func TestDecryptMessage_JSONMultipleEncryptedFields(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	key, _ := e.CreateKey("multi-key", ProviderLocal, "AES-256-GCM")
	e.ConfigureFieldEncryption("multi-topic", key.ID, []string{"email", "ssn", "phone"})

	msg := `{"email":"user@test.com","ssn":"123-45-6789","phone":"555-0100","name":"John"}`
	encResult, err := e.EncryptMessage("", "multi-topic", msg)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if encResult.EncryptedFields != 3 {
		t.Errorf("expected 3 encrypted, got %d", encResult.EncryptedFields)
	}

	decResult, err := e.DecryptMessage("", "multi-topic", encResult.EncryptedMessage)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if decResult.DecryptedFields != 3 {
		t.Errorf("expected 3 decrypted, got %d", decResult.DecryptedFields)
	}

	// Verify name was preserved
	var data map[string]any
	json.Unmarshal([]byte(decResult.DecryptedMessage), &data)
	if data["name"] != "John" {
		t.Errorf("non-encrypted field 'name' was altered")
	}
}

func TestDecryptMessage_FieldNotPrefixedWithENC(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	key, _ := e.CreateKey("pass-key", ProviderLocal, "AES-256-GCM")
	e.ConfigureFieldEncryption("pass-topic", key.ID, []string{"data"})

	// Message with field configured but not ENC-prefixed
	msg := `{"data":"plain value"}`
	decResult, err := e.DecryptMessage("", "pass-topic", msg)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if decResult.DecryptedFields != 0 {
		t.Errorf("expected 0 decrypted for non-ENC field, got %d", decResult.DecryptedFields)
	}
}

func TestDecryptMessage_NestedJSONField(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	key, _ := e.CreateKey("nested-key", ProviderLocal, "AES-256-GCM")
	e.ConfigureFieldEncryption("nested-topic", key.ID, []string{"user.email"})

	msg := `{"user":{"email":"secret@test.com","name":"John"}}`
	encResult, err := e.EncryptMessage("", "nested-topic", msg)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	decResult, err := e.DecryptMessage("", "nested-topic", encResult.EncryptedMessage)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if decResult.DecryptedFields != 1 {
		t.Errorf("expected 1, got %d", decResult.DecryptedFields)
	}
}

func TestDecryptMessage_AgentWithoutDecryptPermission(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	key, _ := e.CreateKey("policy-key", ProviderLocal, "")
	e.ConfigureFieldEncryption("sec-topic", key.ID, []string{"data"})
	e.SetAgentPolicy("agent-no-dec", nil, nil, true, false)

	_, err := e.DecryptMessage("agent-no-dec", "sec-topic", `{"data":"ENC:fake"}`)
	if err == nil {
		t.Error("expected error for agent without decrypt permission")
	}
}

func TestDecryptMessage_WrongTopicAccess(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	key, _ := e.CreateKey("topic-key", ProviderLocal, "")
	e.ConfigureFieldEncryption("allowed-topic", key.ID, []string{"data"})
	e.SetAgentPolicy("agent-wrong", nil, []string{"other-topic"}, true, true)

	_, err := e.DecryptMessage("agent-wrong", "allowed-topic", `{"data":"ENC:fake"}`)
	if err == nil {
		t.Error("expected error for agent without topic access")
	}
	if !strings.Contains(err.Error(), "not permitted") {
		t.Errorf("expected permission error, got: %v", err)
	}
}

func TestDecryptMessage_NoEncryptedFieldsPassthrough(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	key, _ := e.CreateKey("pt-key", ProviderLocal, "")
	e.ConfigureFieldEncryption("pt-topic", key.ID, []string{"secret"})

	msg := `{"name":"John","age":30}`
	decResult, err := e.DecryptMessage("", "pt-topic", msg)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if decResult.DecryptedFields != 0 {
		t.Errorf("expected 0, got %d", decResult.DecryptedFields)
	}
	// Message should pass through
	var data map[string]any
	json.Unmarshal([]byte(decResult.DecryptedMessage), &data)
	if data["name"] != "John" {
		t.Error("passthrough message was altered")
	}
}

func TestDecryptMessage_PartiallyEncryptedFields(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	key, _ := e.CreateKey("partial-key", ProviderLocal, "AES-256-GCM")
	e.ConfigureFieldEncryption("partial-topic", key.ID, []string{"email", "name"})

	// Encrypt only email, leave name as plain
	encResult, err := e.EncryptMessage("", "partial-topic", `{"email":"secret@test.com","name":"John"}`)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	decResult, err := e.DecryptMessage("", "partial-topic", encResult.EncryptedMessage)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if decResult.DecryptedFields != 2 {
		t.Errorf("expected 2 decrypted, got %d", decResult.DecryptedFields)
	}
}

func TestDecryptMessage_NoConfig(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	msg := `{"data":"value"}`
	decResult, err := e.DecryptMessage("", "unconfigured-topic", msg)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if decResult.DecryptedMessage != msg {
		t.Error("message should pass through when no config")
	}
}
