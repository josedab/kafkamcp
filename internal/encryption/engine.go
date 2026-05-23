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

// ProviderLocal and related values define constants for this package.
const (
	ProviderLocal   KeyProvider = "local"
	ProviderVault   KeyProvider = "vault"
	ProviderAWSKMS  KeyProvider = "aws_kms"
	ProviderGCPKMS  KeyProvider = "gcp_kms"
	ProviderAzureKV KeyProvider = "azure_keyvault"
)

// keyVersion holds the bytes and metadata for one version of a key. Older
// versions are retained so messages encrypted under prior versions can still
// be decrypted after RotateKey.
type keyVersion struct {
	Version   int    `json:"version"`
	CreatedAt string `json:"created_at"`
	material  []byte // never serialized
}

// EncryptionKey represents a managed encryption key.
type EncryptionKey struct {
	ID        string       `json:"id"`
	Name      string       `json:"name"`
	Provider  KeyProvider  `json:"provider"`
	Algorithm string       `json:"algorithm"` // AES-256-GCM, AES-128-GCM
	Status    string       `json:"status"`    // active, rotated, revoked
	Version   int          `json:"version"`   // current version pointer
	Versions  []keyVersion `json:"versions"`  // all versions (current + historical)
	CreatedAt string       `json:"created_at"`
	RotatedAt string       `json:"rotated_at,omitempty"`
	ExpiresAt string       `json:"expires_at,omitempty"`
}

// currentMaterial returns the current version's key bytes (or nil for revoked).
func (k *EncryptionKey) currentMaterial() []byte {
	for i := range k.Versions {
		if k.Versions[i].Version == k.Version {
			return k.Versions[i].material
		}
	}
	return nil
}

// materialForVersion returns the bytes of a specific version, or nil.
func (k *EncryptionKey) materialForVersion(v int) []byte {
	for i := range k.Versions {
		if k.Versions[i].Version == v {
			return k.Versions[i].material
		}
	}
	return nil
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
	DecryptedFields  int      `json:"decrypted_fields"`
	FailedFields     int      `json:"failed_fields"`
	Errors           []string `json:"errors,omitempty"`
	DecryptedMessage string   `json:"decrypted_message,omitempty"`
}

// RotationResult captures the outcome of a key rotation.
type RotationResult struct {
	OldKeyID   string `json:"old_key_id"`
	NewKeyID   string `json:"new_key_id"`
	OldVersion int    `json:"old_version"`
	NewVersion int    `json:"new_version"`
	RotatedAt  string `json:"rotated_at"`
}

// Engine manages encryption keys, policies, and field-level crypto operations.
type Engine struct {
	mu       sync.RWMutex
	keys     map[string]*EncryptionKey
	policies map[string]*AgentKeyPolicy        // agentID -> policy
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
//
// Only the "local" provider is implemented in this build; the API accepts the
// Vault/AWS/GCP/Azure values for forward compatibility but rejects them so
// agents are not misled into believing the integration exists.
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

	if provider == "" {
		provider = ProviderLocal
	}
	if provider != ProviderLocal {
		return nil, fmt.Errorf("provider %q is not implemented in this build; only %q is currently supported", provider, ProviderLocal)
	}

	keyMaterial := make([]byte, keySize)
	if _, err := io.ReadFull(rand.Reader, keyMaterial); err != nil {
		return nil, fmt.Errorf("failed to generate key material: %w", err)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	key := &EncryptionKey{
		ID:        randid.New("key"),
		Name:      name,
		Provider:  provider,
		Algorithm: algorithm,
		Status:    "active",
		Version:   1,
		Versions: []keyVersion{
			{Version: 1, CreatedAt: now, material: keyMaterial},
		},
		CreatedAt: now,
	}

	e.mu.Lock()
	e.keys[key.ID] = key
	e.mu.Unlock()

	e.logger.Info("encryption key created", "key_id", key.ID, "name", name, "algorithm", algorithm)
	return key, nil
}

// RotateKey appends a new key version, keeping prior versions so data
// encrypted under them remains decryptable.
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
	keySize := len(key.currentMaterial())
	if keySize == 0 {
		return nil, fmt.Errorf("key %q has no active material", keyID)
	}
	newMaterial := make([]byte, keySize)
	if _, err := io.ReadFull(rand.Reader, newMaterial); err != nil {
		return nil, fmt.Errorf("failed to generate new key material: %w", err)
	}

	newVersion := key.Version + 1
	key.Versions = append(key.Versions, keyVersion{
		Version:   newVersion,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		material:  newMaterial,
	})
	key.Version = newVersion
	key.RotatedAt = time.Now().UTC().Format(time.RFC3339)

	return &RotationResult{
		OldKeyID:   keyID,
		NewKeyID:   keyID,
		OldVersion: oldVersion,
		NewVersion: newVersion,
		RotatedAt:  key.RotatedAt,
	}, nil
}

// RevokeKey marks a key as revoked and zeros every stored version's material
// so it cannot be reconstructed even from memory dumps after revocation.
func (e *Engine) RevokeKey(keyID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	key, ok := e.keys[keyID]
	if !ok {
		return fmt.Errorf("key %q not found", keyID)
	}
	key.Status = "revoked"
	for i := range key.Versions {
		for j := range key.Versions[i].material {
			key.Versions[i].material[j] = 0
		}
		key.Versions[i].material = nil
	}
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

// EncryptMessage encrypts specified fields in a JSON message. The wire
// envelope is `ENC:v{version}:base64(ciphertext)` so DecryptMessage can pick
// the right key version after rotation.
func (e *Engine) EncryptMessage(agentID, topic, message string) (*EncryptionResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	config, ok := e.configs[topic]
	if !ok {
		// No encryption configured for this topic; agent policy may still
		// require explicit permission, but absent a config there is nothing
		// to encrypt and we return the original message.
		if err := e.checkPolicy(agentID, topic, "", true); err != nil {
			return nil, err
		}
		return &EncryptionResult{
			OriginalFields:   0,
			SkippedFields:    0,
			EncryptedMessage: message,
		}, nil
	}

	if err := e.checkPolicy(agentID, topic, config.KeyID, true); err != nil {
		return nil, err
	}

	key, ok := e.keys[config.KeyID]
	if !ok || key.Status != "active" {
		return nil, fmt.Errorf("encryption key %q not available", config.KeyID)
	}
	material := key.currentMaterial()
	if material == nil {
		return nil, fmt.Errorf("encryption key %q has no active material", config.KeyID)
	}

	var data map[string]any
	if err := json.Unmarshal([]byte(message), &data); err != nil {
		// Encrypt entire message if not JSON
		encrypted, err := encrypt(material, []byte(message))
		if err != nil {
			return nil, fmt.Errorf("encryption failed: %w", err)
		}
		return &EncryptionResult{
			OriginalFields:   1,
			EncryptedFields:  1,
			KeyID:            config.KeyID,
			EncryptedMessage: encEnvelope(key.Version, encrypted),
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
		encrypted, err := encrypt(material, plaintext)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("failed to encrypt %s: %v", fieldPath, err))
			continue
		}
		if err := setNestedField(data, fieldPath, encEnvelope(key.Version, encrypted)); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("failed to set %s: %v", fieldPath, err))
			result.SkippedFields++
			continue
		}
		result.EncryptedFields++
	}

	out, _ := json.Marshal(data)
	result.EncryptedMessage = string(out)
	return result, nil
}

// DecryptMessage decrypts encrypted fields in a JSON message. Envelopes that
// declare a previous key version are decrypted with that version's bytes.
func (e *Engine) DecryptMessage(agentID, topic, message string) (*DecryptionResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	config, ok := e.configs[topic]
	if !ok {
		if err := e.checkPolicy(agentID, topic, "", false); err != nil {
			return nil, err
		}
		return &DecryptionResult{DecryptedMessage: message}, nil
	}

	if err := e.checkPolicy(agentID, topic, config.KeyID, false); err != nil {
		return nil, err
	}

	key, ok := e.keys[config.KeyID]
	if !ok {
		return nil, fmt.Errorf("decryption key %q not found", config.KeyID)
	}

	pickMaterial := func(envelope string) ([]byte, string, error) {
		v, payload, err := parseEnvelope(envelope)
		if err != nil {
			return nil, "", err
		}
		// Version 0 (legacy or double-wrapped envelopes) means "use current".
		if v == 0 {
			v = key.Version
		}
		m := key.materialForVersion(v)
		if m == nil {
			return nil, "", fmt.Errorf("key %q has no material for version %d", config.KeyID, v)
		}
		return m, payload, nil
	}

	var data map[string]any
	if err := json.Unmarshal([]byte(message), &data); err != nil {
		if strings.HasPrefix(message, encPrefix) {
			material, payload, err := pickMaterial(message)
			if err != nil {
				return nil, fmt.Errorf("decryption failed: %w", err)
			}
			decrypted, err := decrypt(material, payload)
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
		if !ok || !strings.HasPrefix(strVal, encPrefix) {
			continue
		}

		material, payload, err := pickMaterial(strVal)
		if err != nil {
			result.FailedFields++
			result.Errors = append(result.Errors, fmt.Sprintf("failed to resolve key for %s: %v", fieldPath, err))
			continue
		}
		decrypted, err := decrypt(material, payload)
		if err != nil {
			result.FailedFields++
			result.Errors = append(result.Errors, fmt.Sprintf("failed to decrypt %s: %v", fieldPath, err))
			continue
		}

		var parsed any
		if json.Unmarshal(decrypted, &parsed) == nil {
			_ = setNestedField(data, fieldPath, parsed)
		} else {
			_ = setNestedField(data, fieldPath, string(decrypted))
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
	return key.publicCopy(), nil
}

// ListKeys returns all keys (without key material).
func (e *Engine) ListKeys() []EncryptionKey {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]EncryptionKey, 0, len(e.keys))
	for _, k := range e.keys {
		result = append(result, *k.publicCopy())
	}
	return result
}

// publicCopy returns a copy of the key suitable for serialization: versions
// keep their metadata but the raw material is stripped.
func (k *EncryptionKey) publicCopy() *EncryptionKey {
	c := *k
	c.Versions = make([]keyVersion, len(k.Versions))
	for i, v := range k.Versions {
		c.Versions[i] = keyVersion{
			Version:   v.Version,
			CreatedAt: v.CreatedAt,
			// material intentionally omitted
		}
	}
	return &c
}

// checkPolicy enforces per-agent encryption/decryption permissions. When the
// agent has an explicit policy, it must:
//   - have the appropriate CanEncrypt / CanDecrypt flag set, AND
//   - have AllowedKeyIDs that includes keyID (if non-empty and keyID is supplied), AND
//   - have AllowedTopics that includes topic (or "*").
//
// Operations without a configured agentID skip enforcement entirely (matches
// the legacy "anonymous" behavior for backwards compatibility).
func (e *Engine) checkPolicy(agentID, topic, keyID string, isEncrypt bool) error {
	if agentID == "" {
		return nil
	}
	policy, ok := e.policies[agentID]
	if !ok {
		return nil
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

	if keyID != "" && len(policy.AllowedKeyIDs) > 0 {
		allowed := false
		for _, k := range policy.AllowedKeyIDs {
			if k == keyID || k == "*" {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("agent %q is not permitted to use key %q", agentID, keyID)
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

func setNestedField(data map[string]any, path string, value any) error {
	parts := strings.Split(path, ".")
	current := data
	for i, part := range parts {
		if i == len(parts)-1 {
			current[part] = value
			return nil
		}
		next, ok := current[part].(map[string]any)
		if !ok {
			return fmt.Errorf("path segment %q at %d not a map", part, i)
		}
		current = next
	}
	return nil
}

// Envelope format used to wrap encrypted field values:
//
//	ENC:v{version}:{base64-ciphertext}
//
// Legacy values without the version segment (`ENC:{base64}`) are still
// accepted and decoded with the current key version, preserving backward
// compatibility with payloads produced before key-versioning was introduced.
const encPrefix = "ENC:"

func encEnvelope(version int, ciphertext string) string {
	return fmt.Sprintf("%sv%d:%s", encPrefix, version, ciphertext)
}

// parseEnvelope returns the (version, base64Payload) pair encoded by
// encEnvelope. When the envelope lacks a version segment it returns (0, raw)
// — callers must treat version 0 as "use current".
func parseEnvelope(envelope string) (int, string, error) {
	if !strings.HasPrefix(envelope, encPrefix) {
		return 0, "", fmt.Errorf("not an ENC envelope")
	}
	rest := envelope[len(encPrefix):]
	if len(rest) >= 2 && rest[0] == 'v' {
		// Find the ':' separating version from payload.
		colon := strings.IndexByte(rest, ':')
		if colon < 2 {
			return 0, "", fmt.Errorf("malformed ENC envelope: missing version separator")
		}
		var v int
		if _, err := fmt.Sscanf(rest[1:colon], "%d", &v); err != nil {
			return 0, "", fmt.Errorf("malformed ENC envelope version: %w", err)
		}
		return v, rest[colon+1:], nil
	}
	return 0, rest, nil
}
