package masking

import (
	"strings"
	"testing"
)

func TestNewTokenizer_EmptyKeyUnconfigured(t *testing.T) {
	t.Parallel()
	tok, err := NewTokenizer(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tok.Configured() {
		t.Error("empty key should yield an unconfigured tokenizer")
	}
	if _, ok := tok.Token("hash", "value"); ok {
		t.Error("unconfigured tokenizer must fail closed")
	}
}

func TestNewTokenizer_ShortKeyRejected(t *testing.T) {
	t.Parallel()
	if _, err := NewTokenizer([]byte("tooshort")); err != ErrKeyTooShort {
		t.Fatalf("expected ErrKeyTooShort, got %v", err)
	}
}

func TestTokenizer_DeterministicAndPrefixed(t *testing.T) {
	t.Parallel()
	tok, err := NewTokenizer([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	a, ok := tok.Token("hash", "user@example.com")
	if !ok {
		t.Fatal("expected configured tokenizer to produce a token")
	}
	if !strings.HasPrefix(a, tokenPrefix) {
		t.Errorf("expected %q prefix, got %q", tokenPrefix, a)
	}
	b, _ := tok.Token("hash", "user@example.com")
	if a != b {
		t.Errorf("tokens must be deterministic: %q != %q", a, b)
	}
}

func TestTokenizer_DomainSeparation(t *testing.T) {
	t.Parallel()
	tok, _ := NewTokenizer([]byte("0123456789abcdef0123456789abcdef"))
	hashTok, _ := tok.Token("hash", "same-value")
	tokenizeTok, _ := tok.Token("tokenize", "same-value")
	if hashTok == tokenizeTok {
		t.Error("expected different tokens across domains for the same value")
	}
	// Domain length-prefixing must prevent boundary collisions:
	// ("ab", "cd") and ("a", "bcd") must not collide.
	x, _ := tok.Token("ab", "cd")
	y, _ := tok.Token("a", "bcd")
	if x == y {
		t.Error("domain/value boundary collision detected")
	}
}

func TestTokenizer_KeySeparation(t *testing.T) {
	t.Parallel()
	tok1, _ := NewTokenizer([]byte("0123456789abcdef0123456789abcdef"))
	tok2, _ := NewTokenizer([]byte("ffffffffffffffff0123456789abcdef"))
	a, _ := tok1.Token("hash", "value")
	b, _ := tok2.Token("hash", "value")
	if a == b {
		t.Error("different keys must produce different tokens (keyed HMAC)")
	}
}

func TestTokenizerFromEnv(t *testing.T) {
	t.Setenv("CUSTOM_MASK_KEY", "0123456789abcdef0123456789abcdef")
	tok, err := TokenizerFromEnv("CUSTOM_MASK_KEY")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !tok.Configured() {
		t.Error("expected configured tokenizer from env var")
	}
}

func TestTokenizer_NilReceiverFailsClosed(t *testing.T) {
	t.Parallel()
	var tok *Tokenizer
	if tok.Configured() {
		t.Error("nil tokenizer must report unconfigured")
	}
	if _, ok := tok.Token("hash", "value"); ok {
		t.Error("nil tokenizer must fail closed")
	}
}
