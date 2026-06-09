// Package approval implements short-lived, single-use approval grants that gate
// mutating/destructive KafkaMCP tools. A grant is scoped to a verified agent
// identity, a specific tool, a cluster/resource, and a digest of the normalized
// call arguments. Grants carry a cryptographically unguessable token, a bounded
// TTL, an explicit blast-radius preview, and a fully auditable lifecycle
// (request -> approve -> use -> expire/revoke).
//
// The store is backed by the shared engine backend, so grants persist across
// restarts when the sqlite backend is configured and stay in memory otherwise.
// All operations are safe for concurrent use.
package approval

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"
)

// Status is the lifecycle state of a grant.
type Status string

const (
	// StatusRequested means the grant has been requested but not yet approved.
	// It carries no usable token.
	StatusRequested Status = "requested"
	// StatusApproved means the grant is approved and may be used until it
	// expires or is used (when single-use) or revoked.
	StatusApproved Status = "approved"
	// StatusUsed means a single-use grant has been consumed.
	StatusUsed Status = "used"
	// StatusExpired means the grant passed its expiry without being used.
	StatusExpired Status = "expired"
	// StatusRevoked means the grant was explicitly revoked.
	StatusRevoked Status = "revoked"
)

// Scope identifies exactly what a grant authorizes. Every field must match at
// use time; a mismatch on any field denies the operation.
type Scope struct {
	AgentID        string `json:"agent_id"`
	IdentityMethod string `json:"identity_method"`
	Tool           string `json:"tool"`
	Cluster        string `json:"cluster"`
	Resource       string `json:"resource"`
	ArgsDigest     string `json:"args_digest"`
}

// Equal reports whether two scopes are identical.
func (s Scope) Equal(o Scope) bool {
	return s.AgentID == o.AgentID &&
		s.IdentityMethod == o.IdentityMethod &&
		s.Tool == o.Tool &&
		s.Cluster == o.Cluster &&
		s.Resource == o.Resource &&
		s.ArgsDigest == o.ArgsDigest
}

// Grant is a persisted approval record. The raw token is never stored; only its
// hash is kept, so a leaked store cannot be replayed against the enforcement
// path.
type Grant struct {
	ID           string           `json:"id"`
	Scope        Scope            `json:"scope"`
	Status       Status           `json:"status"`
	Preview      string           `json:"preview"`
	SingleUse    bool             `json:"single_use"`
	TokenHash    string           `json:"token_hash,omitempty"`
	Requester    string           `json:"requester"`
	Approver     string           `json:"approver,omitempty"`
	CreatedAt    time.Time        `json:"created_at"`
	ApprovedAt   *time.Time       `json:"approved_at,omitempty"`
	ExpiresAt    *time.Time       `json:"expires_at,omitempty"`
	UsedAt       *time.Time       `json:"used_at,omitempty"`
	RevokedAt    *time.Time       `json:"revoked_at,omitempty"`
	RevokeReason string           `json:"revoke_reason,omitempty"`
	Decisions    []DecisionRecord `json:"decisions,omitempty"`
	StateMAC     string           `json:"state_mac,omitempty"`
}

// IsActive reports whether the grant is approved and not yet expired at t.
func (g *Grant) IsActive(t time.Time) bool {
	if g.Status != StatusApproved {
		return false
	}
	if g.ExpiresAt != nil && !t.Before(*g.ExpiresAt) {
		return false
	}
	return true
}

// Redacted returns a copy of the grant with the token hash cleared, suitable
// for returning to clients and audit logs.
func (g *Grant) Redacted() Grant {
	c := *g
	c.TokenHash = ""
	return c
}

// DigestArgs returns a stable digest of normalized call arguments. Maps are
// serialized with sorted keys so that logically-identical argument sets produce
// the same digest regardless of ordering. Volatile/irrelevant keys (such as the
// approval token itself) should be removed by the caller before digesting.
func DigestArgs(args map[string]any) string {
	if len(args) == 0 {
		return hashString("{}")
	}
	canonical := canonicalize(args)
	b, err := json.Marshal(canonical)
	if err != nil {
		// Fall back to a best-effort stable representation.
		return hashString(sortedKV(args))
	}
	return hashString(string(b))
}

// hashToken returns the hex SHA-256 of a raw token, domain-separated from arg
// digests.
func hashToken(raw string) string {
	return hashString("approval-token\x00" + raw)
}

func hashString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// canonicalize recursively converts maps into sorted key/value slices so JSON
// marshaling is deterministic.
func canonicalize(v any) any {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][2]any, 0, len(keys))
		for _, k := range keys {
			out = append(out, [2]any{k, canonicalize(t[k])})
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i := range t {
			out[i] = canonicalize(t[i])
		}
		return out
	default:
		return t
	}
}

func sortedKV(args map[string]any) string {
	keys := make([]string, 0, len(args))
	for k := range args {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b []byte
	for _, k := range keys {
		b = append(b, k...)
		b = append(b, '=')
		b = append(b, []byte(toString(args[k]))...)
		b = append(b, ';')
	}
	return string(b)
}

func toString(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}
