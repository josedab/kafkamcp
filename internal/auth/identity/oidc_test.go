package identity

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestOIDCProvider_VerifySignedJWT spins up a fake OIDC issuer
// (discovery + JWKS endpoint) and asserts the provider verifies tokens
// signed by the matching private key while rejecting tampered, wrong-
// audience, or expired tokens.
func TestOIDCProvider_VerifySignedJWT(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	const keyID = "test-key-1"

	mux := http.NewServeMux()
	var issuerURL string
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
  "issuer": "` + issuerURL + `",
  "jwks_uri": "` + issuerURL + `/jwks",
  "authorization_endpoint": "` + issuerURL + `/authorize",
  "token_endpoint": "` + issuerURL + `/token",
  "id_token_signing_alg_values_supported": ["RS256"]
}`))
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
  "keys": [
    {
      "kty": "RSA",
      "use": "sig",
      "alg": "RS256",
      "kid": "` + keyID + `",
      "n": "` + base64URLUint(priv.PublicKey.N) + `",
      "e": "` + base64URLUint(big.NewInt(int64(priv.PublicKey.E))) + `"
    }
  ]
}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	issuerURL = srv.URL

	prov, err := NewOIDC(context.Background(), OIDCConfig{
		IssuerURL:    issuerURL,
		ClientID:     "kafkamcp",
		AgentIDClaim: "preferred_username",
		GroupsClaim:  "groups",
	})
	if err != nil {
		t.Fatalf("NewOIDC: %v", err)
	}
	if prov.Name() != "oidc" {
		t.Fatalf("name=%s", prov.Name())
	}

	now := time.Now()
	baseClaims := map[string]any{
		"iss":                issuerURL,
		"sub":                "user-123",
		"aud":                "kafkamcp",
		"exp":                now.Add(1 * time.Hour).Unix(),
		"iat":                now.Unix(),
		"preferred_username": "alice",
		"groups":             []string{"readers", "writers"},
		"email":              "alice@example.com",
	}
	token := signRS256(t, priv, keyID, baseClaims)

	// Happy path: AgentID comes from preferred_username.
	id, err := prov.Verify(context.Background(), http.Header{"Authorization": {"Bearer " + token}})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if id.ID != "alice" || id.Method != "oidc" || id.Email != "alice@example.com" || id.Subject != "user-123" {
		t.Fatalf("identity=%+v", id)
	}
	if len(id.Groups) != 2 || id.Groups[0] != "readers" {
		t.Fatalf("groups=%v", id.Groups)
	}
	if id.Issuer != issuerURL {
		t.Fatalf("issuer=%s want %s", id.Issuer, issuerURL)
	}

	// No header → ErrNoCredentials.
	if _, err := prov.Verify(context.Background(), nil); !errors.Is(err, ErrNoCredentials) {
		t.Fatalf("nil headers: %v", err)
	}

	// Tampered signature → ErrInvalidToken.
	tampered := token[:len(token)-3] + "AAA"
	if _, err := prov.Verify(context.Background(), http.Header{"Authorization": {"Bearer " + tampered}}); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("tampered: %v", err)
	}

	// Wrong audience → ErrInvalidToken.
	badAud := cloneClaims(baseClaims)
	badAud["aud"] = "other-app"
	badTok := signRS256(t, priv, keyID, badAud)
	if _, err := prov.Verify(context.Background(), http.Header{"Authorization": {"Bearer " + badTok}}); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("bad aud: %v", err)
	}

	// Expired → ErrInvalidToken.
	expired := cloneClaims(baseClaims)
	expired["exp"] = now.Add(-1 * time.Hour).Unix()
	expTok := signRS256(t, priv, keyID, expired)
	if _, err := prov.Verify(context.Background(), http.Header{"Authorization": {"Bearer " + expTok}}); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expired: %v", err)
	}

	// Missing custom claim → falls back to JWT sub.
	noUser := cloneClaims(baseClaims)
	delete(noUser, "preferred_username")
	noUserTok := signRS256(t, priv, keyID, noUser)
	id, err = prov.Verify(context.Background(), http.Header{"Authorization": {"Bearer " + noUserTok}})
	if err != nil {
		t.Fatalf("noUser: %v", err)
	}
	if id.ID != "user-123" {
		t.Fatalf("fallback to sub failed: %+v", id)
	}
}

func TestNewOIDC_ValidatesConfig(t *testing.T) {
	if _, err := NewOIDC(context.Background(), OIDCConfig{}); err == nil {
		t.Fatal("missing issuer expected error")
	}
	if _, err := NewOIDC(context.Background(), OIDCConfig{IssuerURL: "https://x"}); err == nil {
		t.Fatal("missing client_id expected error")
	}
}

// --- helpers ---

func cloneClaims(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func base64URLUint(b *big.Int) string {
	return base64.RawURLEncoding.EncodeToString(b.Bytes())
}

func signRS256(t *testing.T, key *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	hdr := map[string]any{"alg": "RS256", "typ": "JWT", "kid": kid}
	hb, _ := json.Marshal(hdr)
	cb, _ := json.Marshal(claims)
	signingInput := base64.RawURLEncoding.EncodeToString(hb) + "." + base64.RawURLEncoding.EncodeToString(cb)
	hashed := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, hashed[:])
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
}
