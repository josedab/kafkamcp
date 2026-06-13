package masking

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"os"
)

const (
	// DefaultKeyEnv is the environment variable consulted for the masking /
	// tokenization HMAC key when a config does not name a different variable.
	DefaultKeyEnv = "KAFKAMCP_MASKING_KEY"

	// MinKeyLength is the minimum acceptable HMAC key length in bytes. Keys
	// shorter than this are rejected so that tokenization cannot be enabled with
	// trivially guessable secrets.
	MinKeyLength = 16

	tokenPrefix = "tok_"

	// tokenBytes controls how many bytes of the HMAC-SHA-256 digest are rendered
	// into the token. 16 bytes (128 bits) keeps tokens compact while retaining a
	// negligible collision probability.
	tokenBytes = 16
)

// Errors returned when validating a tokenization key.
var (
	// ErrNoKey indicates no tokenization key is configured.
	ErrNoKey = errors.New("masking: no tokenization key configured")
	// ErrKeyTooShort indicates the configured key is shorter than MinKeyLength.
	ErrKeyTooShort = errors.New("masking: tokenization key too short")
)

// Tokenizer produces deterministic, non-reversible tokens using keyed
// HMAC-SHA-256. It replaces the previous unsalted FNV construction, which was
// trivially reversible for low-cardinality fields. A Tokenizer with no key is
// "unconfigured" and always fails closed (Token returns ok=false), so callers
// must redact rather than emit a weak token.
type Tokenizer struct {
	key []byte
}

// NewTokenizer builds a Tokenizer from a raw key. A nil or empty key yields an
// unconfigured tokenizer that fails closed. A non-empty but too-short key is
// rejected with ErrKeyTooShort so misconfiguration surfaces loudly rather than
// silently weakening tokenization.
func NewTokenizer(key []byte) (*Tokenizer, error) {
	if len(key) == 0 {
		return &Tokenizer{}, nil
	}
	if len(key) < MinKeyLength {
		return nil, ErrKeyTooShort
	}
	k := make([]byte, len(key))
	copy(k, key)
	return &Tokenizer{key: k}, nil
}

// TokenizerFromEnv reads the key from the named environment variable. When
// envName is empty, DefaultKeyEnv is used. The secret is never stored in
// serialized configuration; only the environment variable name is.
func TokenizerFromEnv(envName string) (*Tokenizer, error) {
	if envName == "" {
		envName = DefaultKeyEnv
	}
	return NewTokenizer([]byte(os.Getenv(envName)))
}

// Configured reports whether a valid key is present.
func (t *Tokenizer) Configured() bool {
	return t != nil && len(t.key) > 0
}

// Token returns a deterministic token for value within the given domain.
//
// domain provides cryptographic domain separation: identical values under
// different domains (for example the "hash" versus "tokenize" actions, or two
// different field paths) produce unrelated tokens, preventing cross-field
// correlation. The domain is length-prefixed before the value so that no pair
// of (domain, value) inputs can collide by ambiguous concatenation.
//
// The second return value is false when no key is configured; callers MUST fail
// closed (redact) in that case rather than emitting an unkeyed token.
func (t *Tokenizer) Token(domain, value string) (string, bool) {
	if !t.Configured() {
		return "", false
	}
	mac := hmac.New(sha256.New, t.key)
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(domain)))
	_, _ = mac.Write(lenBuf[:])
	_, _ = mac.Write([]byte(domain))
	_, _ = mac.Write([]byte(value))
	sum := mac.Sum(nil)
	return tokenPrefix + hex.EncodeToString(sum[:tokenBytes]), true
}
