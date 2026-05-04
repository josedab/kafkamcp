package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/randid"
)

// KeyProvider identifies the key management backend.
type KeyProvider string

const (
	ProviderLocal    KeyProvider = "local"
	ProviderVault    KeyProvider = "vault"
	ProviderAWSKMS   KeyProvider = "aws_kms"
	ProviderGCPKMS   KeyProvider = "gcp_kms"
	ProviderAzureKV  KeyProvider = "azure_keyvault"
)

// EncryptionKey represents a managed encryption key.
type EncryptionKey struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	Provider    KeyProvider `json:"provider"`
	Algorithm   string      `json:"algorithm"` // AES-256-GCM, AES-128-GCM
	Status      string      `json:"status"`    // active, rotated, revoked
	Version     int         `json:"version"`
	CreatedAt   string      `json:"created_at"`
	RotatedAt   string      `json:"rotated_at,omitempty"`
	ExpiresAt   string      `json:"expires_at,omitempty"`
	keyMaterial []byte      // never serialized
}

// AgentKeyPolicy defines which keys an agent can use for which topics.
type AgentKeyPolicy struct {
	AgentID       string   `json:"agent_id"`
	AllowedKeyIDs []string `json:"allowed_key_ids"`
	AllowedTopics []string `json:"allowed_topics"`
	CanEncrypt    bool     `json:"can_encrypt"`
	CanDecrypt    bool     `json:"can_decrypt"`
	CreatedAt     string   `json:"created_at"`
}

// FieldEncryptionConfig defines which fields to encrypt in a message.
type FieldEncryptionConfig struct {
	Topic      string   `json:"topic"`
	KeyID      string   `json:"key_id"`
	FieldPaths []string `json:"field_paths"` // e.g., "user.email", "payment.card_number"
	Algorithm  string   `json:"algorithm"`
}

// EncryptionResult captures the outcome of an encryption operation.
type EncryptionResult struct {
	OriginalFields   int      `json:"original_fields"`
	EncryptedFields  int      `json:"encrypted_fields"`
	SkippedFields    int      `json:"skipped_fields"`
	Errors           []string `json:"errors,omitempty"`
	KeyID            string   `json:"key_id"`
	EncryptedMessage string   `json:"encrypted_message,omitempty"`
}

// DecryptionResult captures the outcome of a decryption operation.
type DecryptionResult struct {
	DecryptedFields int      `json:"decrypted_fields"`
	FailedFields    int      `json:"failed_fields"`
	Errors          []string `json:"errors,omitempty"`
	DecryptedMessage string  `json:"decrypted_message,omitempty"`
}

// RotationResult captures the outcome of a key rotation.
type RotationResult struct {
	OldKeyID    string `json:"old_key_id"`
	NewKeyID    string `json:"new_key_id"`
	OldVersion  int    `json:"old_version"`
	NewVersion  int    `json:"new_version"`
	RotatedAt   string `json:"rotated_at"`
}

// Engine manages encryption keys, policies, and field-level crypto operations.
type Engine struct {
	mu       sync.RWMutex
	keys     map[string]*EncryptionKey
	policies map[string]*AgentKeyPolicy // agentID -> policy
	configs  map[string]*FieldEncryptionConfig // topic -> config
	logger   *slog.Logger
}

// NewEngine creates a new encryption engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		keys:     make(map[string]*EncryptionKey),
		policies: make(map[string]*AgentKeyPolicy),
		configs:  make(map[string]*FieldEncryptionConfig),
		logger:   logger,
	}
}

// CreateKey generates a new encryption key.
func (e *Engine) CreateKey(name string, provider KeyProvider, algorithm string) (*EncryptionKey, error) {
	if name == "" {
		return nil, fmt.Errorf("key name is required")
	}
	if algorithm == "" {
		algorithm = "AES-256-GCM"
	}

	keySize := 32 // AES-256
	if algorithm == "AES-128-GCM" {
		keySize = 16
	}

	keyMaterial := make([]byte, keySize)
	if _, err := io.ReadFull(rand.Reader, keyMaterial); err != nil {
		return nil, fmt.Errorf("failed to generate key material: %w", err)
	}

	if provider == "" {
		provider = ProviderLocal
	}

	key := &EncryptionKey{
		ID:          randid.New("key"),
		Name:        name,
		Provider:    provider,
		Algorithm:   algorithm,
		Status:      "active",
		Version:     1,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		keyMaterial: keyMaterial,
	}

	e.mu.Lock()
	e.keys[key.ID] = key
	e.mu.Unlock()

	e.logger.Info("encryption key created", "key_id", key.ID, "name", name, "algorithm", algorithm)
	return key, nil
}

// RotateKey creates a new version of an existing key.
func (e *Engine) RotateKey(keyID string) (*RotationResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	key, ok := e.keys[keyID]
	if !ok {
		return nil, fmt.Errorf("key %q not found", keyID)
	}
	if key.Status != "active" {
		return nil, fmt.Errorf("key %q is %s; only active keys can be rotated", keyID, key.Status)
	}

	oldVersion := key.Version
	keySize := len(key.keyMaterial)
	newMaterial := make([]byte, keySize)
	if _, err := io.ReadFull(rand.Reader, newMaterial); err != nil {
		return nil, fmt.Errorf("failed to generate new key material: %w", err)
	}

	key.keyMaterial = newMaterial
	key.Version++
	key.RotatedAt = time.Now().UTC().Format(time.RFC3339)

	return &RotationResult{
		OldKeyID:   keyID,
		NewKeyID:   keyID,
		OldVersion: oldVersion,
		NewVersion: key.Version,
		RotatedAt:  key.RotatedAt,
	}, nil
}

// RevokeKey marks a key as revoked.
func (e *Engine) RevokeKey(keyID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	key, ok := e.keys[keyID]
	if !ok {
		return fmt.Errorf("key %q not found", keyID)
	}
	key.Status = "revoked"
	key.keyMaterial = nil // clear material
	return nil
}

// SetAgentPolicy sets the encryption policy for an agent.
func (e *Engine) SetAgentPolicy(agentID string, keyIDs, topics []string, canEncrypt, canDecrypt bool) (*AgentKeyPolicy, error) {
	if agentID == "" {
		return nil, fmt.Errorf("agent ID is required")
	}

	policy := &AgentKeyPolicy{
		AgentID:       agentID,
		AllowedKeyIDs: keyIDs,
		AllowedTopics: topics,
		CanEncrypt:    canEncrypt,
		CanDecrypt:    canDecrypt,
		CreatedAt:     time.Now().UTC().Format(time.RFC3339),
	}

	e.mu.Lock()
	e.policies[agentID] = policy
	e.mu.Unlock()
	return policy, nil
}

// GetAgentPolicy returns the policy for an agent.
func (e *Engine) GetAgentPolicy(agentID string) (*AgentKeyPolicy, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	policy, ok := e.policies[agentID]
	if !ok {
		return nil, fmt.Errorf("no policy for agent %q", agentID)
	}
	return policy, nil
}

// ConfigureFieldEncryption sets which fields to encrypt for a topic.
func (e *Engine) ConfigureFieldEncryption(topic, keyID string, fieldPaths []string) (*FieldEncryptionConfig, error) {
	if topic == "" {
		return nil, fmt.Errorf("topic is required")
	}
	if keyID == "" {
		return nil, fmt.Errorf("key ID is required")
	}

	e.mu.RLock()
	key, ok := e.keys[keyID]
	e.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("key %q not found", keyID)
	}

	config := &FieldEncryptionConfig{
		Topic:      topic,
		KeyID:      keyID,
		FieldPaths: fieldPaths,
		Algorithm:  key.Algorithm,
	}

	e.mu.Lock()
	e.configs[topic] = config
	e.mu.Unlock()
	return config, nil
}

// EncryptMessage encrypts specified fields in a JSON message.
func (e *Engine) EncryptMessage(agentID, topic, message string) (*EncryptionResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Check agent policy
	if err := e.checkPolicy(agentID, topic, true); err != nil {
		return nil, err
	}

	config, ok := e.configs[topic]
	if !ok {
		return &EncryptionResult{
			OriginalFields: 0,
			SkippedFields:  0,
			EncryptedMessage: message,
		}, nil
	}

	key, ok := e.keys[config.KeyID]
	if !ok || key.Status != "active" {
		return nil, fmt.Errorf("encryption key %q not available", config.KeyID)
	}

	var data map[string]any
	if err := json.Unmarshal([]byte(message), &data); err != nil {
		// Encrypt entire message if not JSON
		encrypted, err := encrypt(key.keyMaterial, []byte(message))
		if err != nil {
			return nil, fmt.Errorf("encryption failed: %w", err)
		}
		return &EncryptionResult{
			OriginalFields:   1,
			EncryptedFields:  1,
			KeyID:            config.KeyID,
			EncryptedMessage: encrypted,
		}, nil
	}

	result := &EncryptionResult{KeyID: config.KeyID}
	for _, fieldPath := range config.FieldPaths {
		val := getNestedField(data, fieldPath)
		if val == nil {
			result.SkippedFields++
			continue
		}
		result.OriginalFields++

		plaintext, _ := json.Marshal(val)
		encrypted, err := encrypt(key.keyMaterial, plaintext)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("failed to encrypt %s: %v", fieldPath, err))
			continue
		}
		setNestedField(data, fieldPath, "ENC:"+encrypted)
		result.EncryptedFields++
	}

	out, _ := json.Marshal(data)
	result.EncryptedMessage = string(out)
	return result, nil
}

// DecryptMessage decrypts encrypted fields in a JSON message.
func (e *Engine) DecryptMessage(agentID, topic, message string) (*DecryptionResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if err := e.checkPolicy(agentID, topic, false); err != nil {
		return nil, err
	}

	config, ok := e.configs[topic]
	if !ok {
		return &DecryptionResult{DecryptedMessage: message}, nil
	}

	key, ok := e.keys[config.KeyID]
	if !ok {
		return nil, fmt.Errorf("decryption key %q not found", config.KeyID)
	}

	var data map[string]any
	if err := json.Unmarshal([]byte(message), &data); err != nil {
		if strings.HasPrefix(message, "ENC:") {
			decrypted, err := decrypt(key.keyMaterial, message[4:])
			if err != nil {
				return nil, fmt.Errorf("decryption failed: %w", err)
			}
			return &DecryptionResult{DecryptedFields: 1, DecryptedMessage: string(decrypted)}, nil
		}
		return &DecryptionResult{DecryptedMessage: message}, nil
	}

	result := &DecryptionResult{}
	for _, fieldPath := range config.FieldPaths {
		val := getNestedField(data, fieldPath)
		if val == nil {
			continue
		}
		strVal, ok := val.(string)
		if !ok || !strings.HasPrefix(strVal, "ENC:") {
			continue
		}

		decrypted, err := decrypt(key.keyMaterial, strVal[4:])
		if err != nil {
			result.FailedFields++
			result.Errors = append(result.Errors, fmt.Sprintf("failed to decrypt %s: %v", fieldPath, err))
			continue
		}

		var parsed any
		if json.Unmarshal(decrypted, &parsed) == nil {
			setNestedField(data, fieldPath, parsed)
		} else {
			setNestedField(data, fieldPath, string(decrypted))
		}
		result.DecryptedFields++
	}

	out, _ := json.Marshal(data)
	result.DecryptedMessage = string(out)
	return result, nil
}

// GetKey returns a key by ID (without key material).
func (e *Engine) GetKey(keyID string) (*EncryptionKey, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	key, ok := e.keys[keyID]
	if !ok {
		return nil, fmt.Errorf("key %q not found", keyID)
	}
	// Return copy without material
	result := *key
	result.keyMaterial = nil
	return &result, nil
}

// ListKeys returns all keys (without key material).
func (e *Engine) ListKeys() []EncryptionKey {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]EncryptionKey, 0, len(e.keys))
	for _, k := range e.keys {
		copy := *k
		copy.keyMaterial = nil
		result = append(result, copy)
	}
	return result
}

func (e *Engine) checkPolicy(agentID, topic string, isEncrypt bool) error {
	if agentID == "" {
		return nil // no policy enforcement without agent ID
	}
	policy, ok := e.policies[agentID]
	if !ok {
		return nil // no policy = allow
	}

	if isEncrypt && !policy.CanEncrypt {
		return fmt.Errorf("agent %q is not permitted to encrypt", agentID)
	}
	if !isEncrypt && !policy.CanDecrypt {
		return fmt.Errorf("agent %q is not permitted to decrypt", agentID)
	}

	if len(policy.AllowedTopics) > 0 {
		allowed := false
		for _, t := range policy.AllowedTopics {
			if t == topic || t == "*" {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("agent %q is not permitted to access topic %q", agentID, topic)
		}
	}

	return nil
}

func encrypt(key, plaintext []byte) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	ciphertext := aead.Seal(nonce, nonce, plaintext, nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

func decrypt(key []byte, encoded string) ([]byte, error) {
	ciphertext, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonceSize := aead.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}
	nonce, ct := ciphertext[:nonceSize], ciphertext[nonceSize:]
	return aead.Open(nil, nonce, ct, nil)
}

func getNestedField(data map[string]any, path string) any {
	parts := strings.Split(path, ".")
	current := any(data)
	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current, ok = m[part]
		if !ok {
			return nil
		}
	}
	return current
}

func setNestedField(data map[string]any, path string, value any) {
	parts := strings.Split(path, ".")
	current := data
	for i, part := range parts {
		if i == len(parts)-1 {
			current[part] = value
			return
		}
		next, ok := current[part].(map[string]any)
		if !ok {
			return
		}
		current = next
	}
}
