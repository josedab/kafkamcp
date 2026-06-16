package identity

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"testing"
)

func TestNoneProvider(t *testing.T) {
	p := NewNone()
	if p.Name() != "none" {
		t.Fatalf("Name=%s", p.Name())
	}
	_, err := p.Verify(context.Background(), http.Header{"Authorization": {"Bearer abc"}})
	if !errors.Is(err, ErrNoCredentials) {
		t.Fatalf("Verify want ErrNoCredentials, got %v", err)
	}
}

func TestStaticProvider(t *testing.T) {
	p, err := NewStatic([]StaticToken{
		{Token: "tok-alice", AgentID: "alice", Groups: []string{"readers"}},
		{Token: "tok-bob", AgentID: "bob"},
	})
	if err != nil {
		t.Fatalf("NewStatic: %v", err)
	}
	if p.Name() != "static" {
		t.Fatalf("Name=%s", p.Name())
	}
	if len(p.tokens) != 2 {
		t.Fatalf("stored token fingerprints = %d, want 2", len(p.tokens))
	}
	if p.tokens[0].fingerprint != sha256.Sum256([]byte("tok-alice")) {
		t.Fatal("static provider did not store the expected token fingerprint")
	}

	// No header → ErrNoCredentials.
	if _, err := p.Verify(context.Background(), http.Header{}); !errors.Is(err, ErrNoCredentials) {
		t.Fatalf("no header want ErrNoCredentials, got %v", err)
	}
	// Bogus header → ErrInvalidToken.
	if _, err := p.Verify(context.Background(), http.Header{"Authorization": {"Bearer nope"}}); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("bogus token want ErrInvalidToken, got %v", err)
	}

	// Happy path.
	id, err := p.Verify(context.Background(), http.Header{"Authorization": {"Bearer tok-alice"}})
	if err != nil {
		t.Fatalf("Verify alice: %v", err)
	}
	if id.ID != "alice" || id.Method != "static" || len(id.Groups) != 1 || id.Groups[0] != "readers" {
		t.Fatalf("unexpected identity %+v", id)
	}
	// Mutating returned groups must not affect the provider's internal state.
	id.Groups[0] = "x"
	id2, _ := p.Verify(context.Background(), http.Header{"Authorization": {"Bearer tok-alice"}})
	if id2.Groups[0] != "readers" {
		t.Fatalf("internal state mutated; groups=%v", id2.Groups)
	}

	// Case-insensitive scheme.
	if _, err := p.Verify(context.Background(), http.Header{"Authorization": {"bearer tok-bob"}}); err != nil {
		t.Fatalf("lowercase bearer: %v", err)
	}
}

func TestStaticProvider_InvalidConfig(t *testing.T) {
	if _, err := NewStatic([]StaticToken{{Token: "", AgentID: "a"}}); err == nil {
		t.Fatal("empty token: expected error")
	}
	if _, err := NewStatic([]StaticToken{{Token: "t", AgentID: ""}}); err == nil {
		t.Fatal("empty agent_id: expected error")
	}
	if _, err := NewStatic([]StaticToken{{Token: "t", AgentID: "a"}, {Token: "t", AgentID: "b"}}); err == nil {
		t.Fatal("duplicate token: expected error")
	}
}

func TestBearerToken(t *testing.T) {
	cases := []struct {
		hv   string
		want string
	}{
		{"", ""},
		{"Basic xxx", ""},
		{"Bearer", ""},
		{"Bearer ", ""},
		{"Bearer foo", "foo"},
		{"bearer foo", "foo"},
		{"BEARER  foo  ", "foo"},
	}
	for _, c := range cases {
		got := bearerToken(http.Header{"Authorization": {c.hv}})
		if got != c.want {
			t.Errorf("bearerToken(%q)=%q want %q", c.hv, got, c.want)
		}
	}
	if got := bearerToken(nil); got != "" {
		t.Errorf("nil headers: %q", got)
	}
}

func TestExtractStringSlice(t *testing.T) {
	if got := extractStringSlice(nil); got != nil {
		t.Errorf("nil: %v", got)
	}
	if got := extractStringSlice([]any{"a", "b", "", 42}); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("[]any: %v", got)
	}
	if got := extractStringSlice("solo"); len(got) != 1 || got[0] != "solo" {
		t.Errorf("string: %v", got)
	}
	if got := extractStringSlice([]string{"x", "y"}); len(got) != 2 || got[0] != "x" {
		t.Errorf("[]string: %v", got)
	}
	if got := extractStringSlice(42); got != nil {
		t.Errorf("other: %v", got)
	}
}

func TestAgentIdentity_LogAttrs(t *testing.T) {
	var id *AgentIdentity
	if got := id.LogAttrs(); len(got) != 2 || got[0] != "identity" {
		t.Errorf("nil identity: %v", got)
	}
	id = &AgentIdentity{ID: "alice", Method: "oidc", Issuer: "https://x", Groups: []string{"r"}}
	attrs := id.LogAttrs()
	// agent_id, alice, identity_method, oidc, identity_issuer, https://x, identity_groups, [r]
	if len(attrs) != 8 {
		t.Errorf("unexpected len: %v", attrs)
	}
}

func TestBuild(t *testing.T) {
	ctx := context.Background()
	p, err := Build(ctx, Config{})
	if err != nil || p.Name() != "none" {
		t.Fatalf("default: %v %s", err, p.Name())
	}
	p, err = Build(ctx, Config{Provider: "static", Static: StaticConfig{Tokens: []StaticToken{{Token: "t", AgentID: "a"}}}})
	if err != nil || p.Name() != "static" {
		t.Fatalf("static: %v %s", err, p.Name())
	}
	if _, err := Build(ctx, Config{Provider: "unknown"}); err == nil {
		t.Fatal("unknown provider expected error")
	}
}

func TestContextHelpers(t *testing.T) {
	ctx := context.Background()
	if got, ok := FromContext(ctx); got != nil || ok {
		t.Errorf("empty context: got %v, %v", got, ok)
	}

	// WithIdentity(nil, ...) is a no-op.
	if c := WithIdentity(ctx, nil); c != ctx {
		t.Error("WithIdentity(ctx, nil) must return ctx unchanged")
	}

	ident := &AgentIdentity{ID: "alice", Method: "static"}
	ctxWith := WithIdentity(ctx, ident)
	got, ok := FromContext(ctxWith)
	if !ok || got == nil {
		t.Fatalf("expected identity, got %v ok=%v", got, ok)
	}
	if got.ID != "alice" || got.Method != "static" {
		t.Errorf("identity round-trip lost data: %+v", got)
	}

	// nil context is tolerated.
	if got, ok := FromContext(context.TODO()); got != nil || ok {
		t.Errorf("nil context: got %v ok=%v", got, ok)
	}
}
