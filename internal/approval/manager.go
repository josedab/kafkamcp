package approval

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/josedab/kafkamcp/internal/engine"
	"github.com/josedab/kafkamcp/internal/engine/backend"
)

// Namespace constants for the engine backend.
const (
	grantsNamespace   = "approval_grants"
	tokenIdxNamespace = "approval_token_index"
)

// Default TTL parameters.
const (
	DefaultTTL = 5 * time.Minute
	// MaxTTLCap is the hard upper bound on a grant TTL regardless of config.
	MaxTTLCap = 1 * time.Hour
	// DefaultTerminalRetention bounds persisted terminal approval history.
	DefaultTerminalRetention = 7 * 24 * time.Hour
)

// Errors returned by the Manager.
var (
	ErrNotFound         = errors.New("approval: grant not found")
	ErrInvalidToken     = errors.New("approval: invalid or unknown token")
	ErrNotApproved      = errors.New("approval: grant is not approved")
	ErrExpired          = errors.New("approval: grant has expired")
	ErrAlreadyUsed      = errors.New("approval: single-use grant already used")
	ErrRevoked          = errors.New("approval: grant has been revoked")
	ErrScopeMismatch    = errors.New("approval: request does not match the approved scope")
	ErrWrongState       = errors.New("approval: grant is not in a state that allows this operation")
	ErrMissingRequired  = errors.New("approval: agent_id and tool are required")
	ErrSelfApproval     = errors.New("approval: requester cannot approve its own grant")
	ErrConcurrentUpdate = errors.New("approval: concurrent state update")
)

const maxCASAttempts = 8

// Config controls Manager behavior.
type Config struct {
	// DefaultTTL is applied when a request does not specify a TTL.
	DefaultTTL time.Duration
	// MaxTTL caps the effective TTL. It is itself capped by MaxTTLCap.
	MaxTTL time.Duration
	// SingleUse, when true (the default), consumes a grant on first use.
	SingleUse bool
	// AllowSelfApproval permits a requester to approve its own grant. The safe
	// default is false.
	AllowSelfApproval bool
	// DecisionSigningKey signs the immutable decision chain stored with every
	// grant. Production ops-write configurations require at least 32 bytes.
	DecisionSigningKey []byte
	// RequireSignedDecisions makes a missing/invalid signing key a startup
	// error, and rejects persisted unsigned/tampered decision records.
	RequireSignedDecisions bool
	// VerificationKeys are previous signing keys accepted during rotation.
	VerificationKeys [][]byte
	// TerminalRetention controls pruning of used/revoked/expired/abandoned
	// grants. Zero uses DefaultTerminalRetention.
	TerminalRetention time.Duration
}

// DefaultConfig returns a safe default configuration: 5-minute TTL, 1-hour cap,
// single-use.
func DefaultConfig() Config {
	return Config{DefaultTTL: DefaultTTL, MaxTTL: MaxTTLCap, SingleUse: true}
}

func (c Config) normalized() Config {
	if c.DefaultTTL <= 0 {
		c.DefaultTTL = DefaultTTL
	}
	if c.MaxTTL <= 0 || c.MaxTTL > MaxTTLCap {
		c.MaxTTL = MaxTTLCap
	}
	if c.DefaultTTL > c.MaxTTL {
		c.DefaultTTL = c.MaxTTL
	}
	if c.TerminalRetention <= 0 {
		c.TerminalRetention = DefaultTerminalRetention
	} else if c.TerminalRetention > 30*24*time.Hour {
		c.TerminalRetention = 30 * 24 * time.Hour
	}
	return c
}

// AuditFunc receives lifecycle events for external audit logging.
type AuditFunc func(event, grantID string, scope Scope, detail map[string]any)

// Manager issues and enforces approval grants. It is safe for concurrent use.
type Manager struct {
	mu                     sync.Mutex
	grants                 *engine.Store[Grant]
	tokenIdx               *engine.Store[string]
	cfg                    Config
	now                    func() time.Time
	audit                  AuditFunc
	signingKey             []byte
	verificationKeys       [][]byte
	requireSignedDecisions bool
	allowSelfApproval      bool
	terminalRetention      time.Duration
	lastPrune              time.Time
}

// NewManager builds a Manager backed by be. A nil backend uses an in-memory
// backend. Persisted decision chains are verified before the manager is
// returned, so tampering fails startup rather than being silently ignored.
func NewManager(ctx context.Context, be backend.Backend, cfg Config) (*Manager, error) {
	if be == nil {
		be = backend.NewInMemory()
	}
	cfg = cfg.normalized()
	if cfg.RequireSignedDecisions && len(cfg.DecisionSigningKey) < minDecisionSigningKeyBytes {
		return nil, ErrDecisionSigningKey
	}
	m := &Manager{
		grants:                 engine.NewStore[Grant](be, grantsNamespace),
		tokenIdx:               engine.NewStore[string](be, tokenIdxNamespace),
		cfg:                    cfg,
		now:                    time.Now,
		signingKey:             append([]byte(nil), cfg.DecisionSigningKey...),
		requireSignedDecisions: cfg.RequireSignedDecisions,
		allowSelfApproval:      cfg.AllowSelfApproval,
		terminalRetention:      cfg.TerminalRetention,
	}
	m.verificationKeys = appendVerificationKey(m.verificationKeys, cfg.DecisionSigningKey)
	for _, key := range cfg.VerificationKeys {
		m.verificationKeys = appendVerificationKey(m.verificationKeys, key)
	}
	all, err := m.grants.List(ctx, "")
	if err != nil {
		return nil, err
	}
	for _, grant := range all {
		if err := m.verifyGrantAny(grant); err != nil {
			return nil, err
		}
	}
	if err := m.reconcileTokenIndex(ctx, all); err != nil {
		return nil, err
	}
	if _, err := m.pruneLocked(ctx, m.now()); err != nil {
		return nil, err
	}
	return m, nil
}

// SetClock overrides the time source (for tests).
func (m *Manager) SetClock(now func() time.Time) { m.now = now }

// SetAudit installs a lifecycle audit hook.
func (m *Manager) SetAudit(fn AuditFunc) { m.audit = fn }

// SignedDecisionsRequired reports whether unsigned grant records are rejected.
func (m *Manager) SignedDecisionsRequired() bool { return m.requireSignedDecisions }

func (m *Manager) emit(event, id string, scope Scope, detail map[string]any) {
	if m.audit != nil {
		m.audit(event, id, scope, detail)
	}
}

// RequestSpec describes a grant request.
type RequestSpec struct {
	Scope     Scope
	Preview   string
	Requester string
	Reason    string
	// TTL overrides the configured default; it is capped by MaxTTL.
	TTL time.Duration
	// SingleUse overrides the configured default when non-nil.
	SingleUse *bool
}

// Request creates a new grant in the "requested" state. It returns the redacted
// grant (no token, since none is issued until approval).
func (m *Manager) Request(ctx context.Context, spec RequestSpec) (Grant, error) {
	if spec.Scope.AgentID == "" || spec.Scope.Tool == "" {
		return Grant{}, ErrMissingRequired
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.now().Sub(m.lastPrune) >= time.Hour {
		if _, err := m.pruneLocked(ctx, m.now()); err != nil {
			return Grant{}, err
		}
	}

	singleUse := m.cfg.SingleUse
	if spec.SingleUse != nil {
		singleUse = *spec.SingleUse
	}
	now := m.now()
	for range maxCASAttempts {
		id, err := randomID(16)
		if err != nil {
			return Grant{}, err
		}
		g := Grant{
			ID:        id,
			Scope:     spec.Scope,
			Status:    StatusRequested,
			Preview:   spec.Preview,
			SingleUse: singleUse,
			Requester: spec.Requester,
			CreatedAt: now,
		}
		if err := appendDecision(&g, m.signingKey, m.requireSignedDecisions, "request", spec.Requester,
			map[string]any{"preview": g.Preview, "reason": spec.Reason}, now); err != nil {
			return Grant{}, err
		}
		created, err := m.grants.Create(ctx, id, g)
		if err != nil {
			return Grant{}, err
		}
		if !created {
			continue
		}
		m.emit("request", id, g.Scope, map[string]any{"preview": g.Preview, "actor": g.Requester})
		return g.Redacted(), nil
	}
	return Grant{}, ErrConcurrentUpdate
}

// Approve moves a requested grant to approved, issues a one-time token, and sets
// the expiry. The raw token is returned exactly once and never persisted.
func (m *Manager) Approve(ctx context.Context, id, approver string, ttl time.Duration) (Grant, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for range maxCASAttempts {
		current, ok, err := m.grants.GetVersioned(ctx, id)
		if err != nil {
			return Grant{}, "", err
		}
		if !ok {
			return Grant{}, "", ErrNotFound
		}
		g := current.Value
		if err := m.verifyGrantCurrent(g); err != nil {
			return Grant{}, "", err
		}
		if g.Status != StatusRequested {
			return Grant{}, "", fmt.Errorf("%w: status=%s", ErrWrongState, g.Status)
		}
		if !m.allowSelfApproval && (approver == g.Requester || approver == g.Scope.AgentID) {
			return Grant{}, "", ErrSelfApproval
		}

		effTTL := m.effectiveTTL(ttl)
		now := m.now()
		exp := now.Add(effTTL)
		rawToken, err := randomToken(32)
		if err != nil {
			return Grant{}, "", err
		}
		g.Status = StatusApproved
		g.Approver = approver
		g.ApprovedAt = &now
		g.ExpiresAt = &exp
		g.TokenHash = hashToken(rawToken)
		if err := appendDecision(&g, m.signingKey, m.requireSignedDecisions, "approve", approver,
			map[string]any{"expires_at": exp, "single_use": g.SingleUse}, now); err != nil {
			return Grant{}, "", err
		}

		indexed, err := m.tokenIdx.Create(ctx, g.TokenHash, id)
		if err != nil {
			return Grant{}, "", err
		}
		if !indexed {
			continue
		}
		swapped, err := m.grants.CompareAndSwap(ctx, id, current.Raw, g)
		if err != nil {
			_ = m.tokenIdx.Delete(ctx, g.TokenHash)
			return Grant{}, "", err
		}
		if !swapped {
			_ = m.tokenIdx.Delete(ctx, g.TokenHash)
			continue
		}
		m.emit("approve", id, g.Scope, map[string]any{"actor": approver, "expires_at": exp})
		return g.Redacted(), rawToken, nil
	}
	return Grant{}, "", ErrConcurrentUpdate
}

// effectiveTTL applies defaults and the cap.
func (m *Manager) effectiveTTL(requested time.Duration) time.Duration {
	ttl := requested
	if ttl <= 0 {
		ttl = m.cfg.DefaultTTL
	}
	if ttl > m.cfg.MaxTTL {
		ttl = m.cfg.MaxTTL
	}
	return ttl
}

// Consume validates and (for single-use grants) consumes an approval. The
// presented token and scope must match an approved, unexpired grant. This is
// the only method the enforcement path calls at tool-execution time.
func (m *Manager) Consume(ctx context.Context, rawToken string, scope Scope) (Grant, error) {
	if rawToken == "" {
		return Grant{}, ErrInvalidToken
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	th := hashToken(rawToken)
	id, ok, err := m.tokenIdx.Get(ctx, th)
	if err != nil {
		return Grant{}, err
	}
	if !ok {
		return Grant{}, ErrInvalidToken
	}
	for range maxCASAttempts {
		current, exists, err := m.grants.GetVersioned(ctx, id)
		if err != nil {
			return Grant{}, err
		}
		if !exists {
			return Grant{}, ErrInvalidToken
		}
		g := current.Value
		if err := m.verifyGrantCurrent(g); err != nil {
			return Grant{}, err
		}

		switch g.Status {
		case StatusUsed:
			return Grant{}, ErrAlreadyUsed
		case StatusRevoked:
			return Grant{}, ErrRevoked
		case StatusRequested:
			return Grant{}, ErrNotApproved
		case StatusExpired:
			return Grant{}, ErrExpired
		}

		now := m.now()
		if g.ExpiresAt != nil && !now.Before(*g.ExpiresAt) {
			g.Status = StatusExpired
			if err := appendDecision(&g, m.signingKey, m.requireSignedDecisions, "expire", "system", nil, now); err != nil {
				return Grant{}, err
			}
			swapped, err := m.grants.CompareAndSwap(ctx, id, current.Raw, g)
			if err != nil {
				return Grant{}, err
			}
			if !swapped {
				continue
			}
			_ = m.tokenIdx.Delete(ctx, th)
			m.emit("expire", id, g.Scope, map[string]any{"actor": "system"})
			return Grant{}, ErrExpired
		}

		if !g.Scope.Equal(scope) {
			if err := appendDecision(&g, m.signingKey, m.requireSignedDecisions, "deny", scope.AgentID,
				map[string]any{"reason": "scope_mismatch"}, now); err != nil {
				return Grant{}, err
			}
			swapped, err := m.grants.CompareAndSwap(ctx, id, current.Raw, g)
			if err != nil {
				return Grant{}, err
			}
			if !swapped {
				continue
			}
			m.emit("deny", id, g.Scope, map[string]any{"actor": scope.AgentID, "reason": "scope_mismatch"})
			return Grant{}, ErrScopeMismatch
		}

		if g.SingleUse {
			g.Status = StatusUsed
			g.UsedAt = &now
		}
		if err := appendDecision(&g, m.signingKey, m.requireSignedDecisions, "use", scope.AgentID,
			map[string]any{"single_use": g.SingleUse}, now); err != nil {
			return Grant{}, err
		}
		swapped, err := m.grants.CompareAndSwap(ctx, id, current.Raw, g)
		if err != nil {
			return Grant{}, err
		}
		if !swapped {
			continue
		}
		if g.SingleUse {
			_ = m.tokenIdx.Delete(ctx, th)
		}
		m.emit("use", id, g.Scope, map[string]any{"actor": scope.AgentID, "single_use": g.SingleUse})
		return g.Redacted(), nil
	}
	return Grant{}, ErrConcurrentUpdate
}

// Revoke marks a grant revoked. Approved-but-unused and requested grants may be
// revoked. Already used/expired/revoked grants return ErrWrongState.
func (m *Manager) Revoke(ctx context.Context, id, revoker, reason string) (Grant, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for range maxCASAttempts {
		current, ok, err := m.grants.GetVersioned(ctx, id)
		if err != nil {
			return Grant{}, err
		}
		if !ok {
			return Grant{}, ErrNotFound
		}
		g := current.Value
		if err := m.verifyGrantCurrent(g); err != nil {
			return Grant{}, err
		}
		if g.Status != StatusApproved && g.Status != StatusRequested {
			return Grant{}, fmt.Errorf("%w: status=%s", ErrWrongState, g.Status)
		}
		now := m.now()
		g.Status = StatusRevoked
		g.RevokedAt = &now
		g.RevokeReason = reason
		if err := appendDecision(&g, m.signingKey, m.requireSignedDecisions, "revoke", revoker,
			map[string]any{"reason": reason}, now); err != nil {
			return Grant{}, err
		}
		swapped, err := m.grants.CompareAndSwap(ctx, id, current.Raw, g)
		if err != nil {
			return Grant{}, err
		}
		if !swapped {
			continue
		}
		if g.TokenHash != "" {
			_ = m.tokenIdx.Delete(ctx, g.TokenHash)
		}
		m.emit("revoke", id, g.Scope, map[string]any{"actor": revoker, "reason": reason})
		return g.Redacted(), nil
	}
	return Grant{}, ErrConcurrentUpdate
}

// Get returns a grant by ID (redacted).
func (m *Manager) Get(ctx context.Context, id string) (Grant, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok, err := m.grants.Get(ctx, id)
	if err != nil {
		return Grant{}, err
	}
	if !ok {
		return Grant{}, ErrNotFound
	}
	if err := m.verifyGrantAny(g); err != nil {
		return Grant{}, err
	}
	return g.Redacted(), nil
}

// Verify checks the persisted decision chain for a grant.
func (m *Manager) Verify(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok, err := m.grants.Get(ctx, id)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotFound
	}
	return m.verifyGrantAny(g)
}

// List returns all grants (redacted), optionally filtered by agent ID.
func (m *Manager) List(ctx context.Context, agentID string) ([]Grant, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	all, err := m.grants.List(ctx, "")
	if err != nil {
		return nil, err
	}
	out := make([]Grant, 0, len(all))
	for _, g := range all {
		if agentID != "" && g.Scope.AgentID != agentID {
			continue
		}
		if err := m.verifyGrantAny(g); err != nil {
			return nil, err
		}
		out = append(out, g.Redacted())
	}
	return out, nil
}

// Sweep transitions approved-but-expired grants to expired. Returns the number
// of grants expired. Safe to call periodically.
func (m *Manager) Sweep(ctx context.Context) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	all, err := m.grants.ListVersioned(ctx, "")
	if err != nil {
		return 0, err
	}
	now := m.now()
	n := 0
	for id, current := range all {
		g := current.Value
		if err := m.verifyGrantAny(g); err != nil {
			return n, err
		}
		if g.Status == StatusApproved && g.ExpiresAt != nil && !now.Before(*g.ExpiresAt) {
			if err := m.verifyGrantCurrent(g); err != nil {
				continue
			}
			g.Status = StatusExpired
			if err := appendDecision(&g, m.signingKey, m.requireSignedDecisions, "expire", "system", nil, now); err != nil {
				return n, err
			}
			swapped, err := m.grants.CompareAndSwap(ctx, id, current.Raw, g)
			if err != nil {
				return n, err
			}
			if !swapped {
				continue
			}
			if g.TokenHash != "" {
				_ = m.tokenIdx.Delete(ctx, g.TokenHash)
			}
			m.emit("expire", id, g.Scope, map[string]any{"actor": "system"})
			n++
		}
	}
	pruned, err := m.pruneLocked(ctx, now)
	return n + pruned, err
}

// Prune removes terminal or abandoned grants older than the configured
// retention period.
func (m *Manager) Prune(ctx context.Context) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pruneLocked(ctx, m.now())
}

func (m *Manager) pruneLocked(ctx context.Context, now time.Time) (int, error) {
	all, err := m.grants.ListVersioned(ctx, "")
	if err != nil {
		return 0, err
	}
	pruned := 0
	for id, current := range all {
		grant := current.Value
		if err := m.verifyGrantAny(grant); err != nil {
			return pruned, err
		}
		terminalAt, terminal := grantTerminalTime(grant, now)
		if !terminal || now.Sub(terminalAt) < m.terminalRetention {
			continue
		}
		deleted, err := m.grants.DeleteIfUnchanged(ctx, id, current.Raw)
		if err != nil {
			return pruned, err
		}
		if !deleted {
			continue
		}
		if grant.TokenHash != "" {
			if err := m.tokenIdx.Delete(ctx, grant.TokenHash); err != nil {
				return pruned, err
			}
		}
		pruned++
	}
	m.lastPrune = now
	return pruned, nil
}

func (m *Manager) reconcileTokenIndex(ctx context.Context, grants map[string]Grant) error {
	index, err := m.tokenIdx.List(ctx, "")
	if err != nil {
		return err
	}
	for tokenHash, grantID := range index {
		grant, ok := grants[grantID]
		if !ok || grant.TokenHash != tokenHash || grant.Status != StatusApproved {
			if err := m.tokenIdx.Delete(ctx, tokenHash); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Manager) verifyGrantAny(grant Grant) error {
	if len(m.verificationKeys) == 0 {
		return verifyGrant(grant, nil, m.requireSignedDecisions)
	}
	for _, key := range m.verificationKeys {
		if err := verifyGrant(grant, key, m.requireSignedDecisions); err == nil {
			return nil
		}
	}
	return fmt.Errorf("%w: grant=%s did not verify with any configured key", ErrDecisionTampered, grant.ID)
}

func (m *Manager) verifyGrantCurrent(grant Grant) error {
	currentErr := verifyGrant(grant, m.signingKey, m.requireSignedDecisions)
	if currentErr == nil {
		return nil
	}
	for _, key := range m.verificationKeys {
		if bytes.Equal(key, m.signingKey) {
			continue
		}
		if err := verifyGrant(grant, key, m.requireSignedDecisions); err == nil {
			return fmt.Errorf("approval: grant %s was signed with a previous key; request a new approval", grant.ID)
		}
	}
	return currentErr
}

func appendVerificationKey(keys [][]byte, candidate []byte) [][]byte {
	if len(candidate) < minDecisionSigningKeyBytes {
		return keys
	}
	for _, existing := range keys {
		if bytes.Equal(existing, candidate) {
			return keys
		}
	}
	return append(keys, append([]byte(nil), candidate...))
}

func grantTerminalTime(grant Grant, now time.Time) (time.Time, bool) {
	switch grant.Status {
	case StatusUsed:
		if grant.UsedAt != nil {
			return *grant.UsedAt, true
		}
	case StatusRevoked:
		if grant.RevokedAt != nil {
			return *grant.RevokedAt, true
		}
	case StatusExpired:
		if grant.ExpiresAt != nil {
			return *grant.ExpiresAt, true
		}
	case StatusRequested:
		return grant.CreatedAt, true
	case StatusApproved:
		if grant.ExpiresAt != nil && !now.Before(*grant.ExpiresAt) {
			return *grant.ExpiresAt, true
		}
	}
	return time.Time{}, false
}

// randomToken returns a URL-safe base64 token from n random bytes.
func randomToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// randomID returns a short random hex identifier.
func randomID(n int) (string, error) {
	return randomToken(n)
}
