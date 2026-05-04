package encryption

import (
	"fmt"
	"log/slog"
	"testing"
)

func TestCreateKeyAndEncryptDecrypt(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	key, err := e.CreateKey("test-key", ProviderLocal, "AES-256-GCM")
	if err != nil {
		t.Fatalf("CreateKey: %v", err)
	}
	if key.Status != "active" {
		t.Errorf("expected active, got %s", key.Status)
	}

	_, err = e.ConfigureFieldEncryption("orders", key.ID, []string{"email", "payment.card"})
	if err != nil {
		t.Fatalf("ConfigureFieldEncryption: %v", err)
	}

	msg := `{"id":"order-1","email":"user@test.com","payment":{"card":"4111111111111111"}}`

	encResult, err := e.EncryptMessage("", "orders", msg)
	if err != nil {
		t.Fatalf("EncryptMessage: %v", err)
	}
	if encResult.EncryptedFields != 2 {
		t.Errorf("expected 2 encrypted fields, got %d", encResult.EncryptedFields)
	}

	decResult, err := e.DecryptMessage("", "orders", encResult.EncryptedMessage)
	if err != nil {
		t.Fatalf("DecryptMessage: %v", err)
	}
	if decResult.DecryptedFields != 2 {
		t.Errorf("expected 2 decrypted fields, got %d", decResult.DecryptedFields)
	}
}

func TestAgentPolicy(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	key, _ := e.CreateKey("policy-key", ProviderLocal, "")
	e.ConfigureFieldEncryption("sensitive", key.ID, []string{"ssn"})

	// Agent without decrypt permission
	e.SetAgentPolicy("agent-1", []string{key.ID}, []string{"sensitive"}, true, false)

	_, err := e.DecryptMessage("agent-1", "sensitive", `{"ssn":"ENC:fake"}`)
	if err == nil {
		t.Error("expected error for agent without decrypt permission")
	}

	// Agent without topic access
	e.SetAgentPolicy("agent-2", nil, []string{"other-topic"}, true, true)
	_, err = e.EncryptMessage("agent-2", "sensitive", `{"ssn":"123-45-6789"}`)
	if err == nil {
		t.Error("expected error for agent without topic access")
	}
}

func TestKeyRotation(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	key, _ := e.CreateKey("rotate-me", ProviderLocal, "AES-256-GCM")

	result, err := e.RotateKey(key.ID)
	if err != nil {
		t.Fatalf("RotateKey: %v", err)
	}
	if result.NewVersion != 2 {
		t.Errorf("expected version 2, got %d", result.NewVersion)
	}

	// Verify key still works
	e.ConfigureFieldEncryption("test-topic", key.ID, []string{"data"})
	msg := `{"data":"secret"}`
	enc, err := e.EncryptMessage("", "test-topic", msg)
	if err != nil {
		t.Fatalf("EncryptMessage after rotation: %v", err)
	}
	dec, err := e.DecryptMessage("", "test-topic", enc.EncryptedMessage)
	if err != nil {
		t.Fatalf("DecryptMessage after rotation: %v", err)
	}
	if dec.DecryptedFields != 1 {
		t.Errorf("expected 1 decrypted field, got %d", dec.DecryptedFields)
	}
}

func TestKeyRevoke(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	key, _ := e.CreateKey("revoke-me", ProviderLocal, "")
	e.ConfigureFieldEncryption("topic", key.ID, []string{"x"})

	if err := e.RevokeKey(key.ID); err != nil {
		t.Fatalf("RevokeKey: %v", err)
	}

	_, err := e.EncryptMessage("", "topic", `{"x":"val"}`)
	if err == nil {
		t.Error("expected error after key revocation")
	}
}

func TestListKeys(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	e.CreateKey("k1", ProviderLocal, "")
	e.CreateKey("k2", ProviderVault, "")

	keys := e.ListKeys()
	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}
}

func TestEncryptionConcurrent(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	key, _ := e.CreateKey("concurrent-key", ProviderLocal, "AES-256-GCM")
	e.ConfigureFieldEncryption("concurrent-topic", key.ID, []string{"data"})

	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			msg := fmt.Sprintf(`{"data":"value-%d"}`, idx)
			enc, err := e.EncryptMessage("", "concurrent-topic", msg)
			if err != nil {
				t.Errorf("concurrent encrypt %d: %v", idx, err)
				done <- false
				return
			}
			_, err = e.DecryptMessage("", "concurrent-topic", enc.EncryptedMessage)
			if err != nil {
				t.Errorf("concurrent decrypt %d: %v", idx, err)
				done <- false
				return
			}
			done <- true
		}(i)
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestEncryptNonJSONMessage(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	key, _ := e.CreateKey("plain-key", ProviderLocal, "AES-256-GCM")
	e.ConfigureFieldEncryption("plain-topic", key.ID, []string{"data"})

	result, err := e.EncryptMessage("", "plain-topic", "plain text not JSON")
	if err != nil {
		t.Fatalf("EncryptMessage: %v", err)
	}
	if result.EncryptedFields != 1 {
		t.Errorf("expected 1 encrypted field for non-JSON, got %d", result.EncryptedFields)
	}
}

func TestEncryptNoConfig(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	result, err := e.EncryptMessage("", "unconfigured-topic", `{"data":"value"}`)
	if err != nil {
		t.Fatalf("EncryptMessage: %v", err)
	}
	if result.EncryptedMessage != `{"data":"value"}` {
		t.Error("message should pass through unchanged without config")
	}
}

func TestCreateKeyValidation(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	_, err := e.CreateKey("", ProviderLocal, "")
	if err == nil {
		t.Error("expected error for empty name")
	}
}

func TestRevokeKeyPreventsEncryption(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	key, _ := e.CreateKey("revoke-test", ProviderLocal, "")
	e.ConfigureFieldEncryption("secure-topic", key.ID, []string{"ssn"})

	// Works before revoke
	_, err := e.EncryptMessage("", "secure-topic", `{"ssn":"123-45-6789"}`)
	if err != nil {
		t.Fatalf("EncryptMessage before revoke: %v", err)
	}

	e.RevokeKey(key.ID)

	// Fails after revoke
	_, err = e.EncryptMessage("", "secure-topic", `{"ssn":"123-45-6789"}`)
	if err == nil {
		t.Error("expected error after key revocation")
	}
}
