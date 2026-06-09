package approval

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const minDecisionSigningKeyBytes = 32

var (
	// ErrDecisionSigningKey indicates that signed decision records are required
	// but the configured key is missing or too short.
	ErrDecisionSigningKey = errors.New("approval: decision signing key must be at least 32 bytes")
	// ErrDecisionTampered indicates a persisted decision chain failed
	// verification and must not be trusted.
	ErrDecisionTampered = errors.New("approval: decision record verification failed")
)

// DecisionRecord is an immutable, tamper-evident approval lifecycle event. Each
// grant has its own HMAC-linked chain, stored atomically with the grant.
type DecisionRecord struct {
	Sequence    int            `json:"sequence"`
	Event       string         `json:"event"`
	Actor       string         `json:"actor"`
	Detail      map[string]any `json:"detail,omitempty"`
	Timestamp   time.Time      `json:"timestamp"`
	PreviousMAC string         `json:"previous_mac,omitempty"`
	MAC         string         `json:"mac,omitempty"`
}

type decisionPayload struct {
	Sequence    int            `json:"sequence"`
	Event       string         `json:"event"`
	Actor       string         `json:"actor"`
	Detail      map[string]any `json:"detail,omitempty"`
	Timestamp   time.Time      `json:"timestamp"`
	PreviousMAC string         `json:"previous_mac,omitempty"`
}

type grantStatePayload struct {
	ID           string     `json:"id"`
	Scope        Scope      `json:"scope"`
	Status       Status     `json:"status"`
	Preview      string     `json:"preview"`
	SingleUse    bool       `json:"single_use"`
	TokenHash    string     `json:"token_hash,omitempty"`
	Requester    string     `json:"requester"`
	Approver     string     `json:"approver,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	ApprovedAt   *time.Time `json:"approved_at,omitempty"`
	ExpiresAt    *time.Time `json:"expires_at,omitempty"`
	UsedAt       *time.Time `json:"used_at,omitempty"`
	RevokedAt    *time.Time `json:"revoked_at,omitempty"`
	RevokeReason string     `json:"revoke_reason,omitempty"`
	LastDecision string     `json:"last_decision,omitempty"`
}

func appendDecision(g *Grant, key []byte, required bool, event, actor string, detail map[string]any, at time.Time) error {
	if required && len(key) < minDecisionSigningKeyBytes {
		return ErrDecisionSigningKey
	}
	previous := ""
	if n := len(g.Decisions); n > 0 {
		previous = g.Decisions[n-1].MAC
	}
	record := DecisionRecord{
		Sequence:    len(g.Decisions) + 1,
		Event:       event,
		Actor:       actor,
		Detail:      cloneDetail(detail),
		Timestamp:   at.UTC(),
		PreviousMAC: previous,
	}
	if len(key) >= minDecisionSigningKeyBytes {
		mac, err := decisionMAC(key, record)
		if err != nil {
			return err
		}
		record.MAC = mac
	}
	g.Decisions = append(g.Decisions, record)
	return sealGrant(g, key, required)
}

func verifyGrant(g Grant, key []byte, required bool) error {
	if err := verifyDecisionChain(g, key, required); err != nil {
		return err
	}
	if required && g.StateMAC == "" {
		return fmt.Errorf("%w: grant=%s has no state signature", ErrDecisionTampered, g.ID)
	}
	if len(key) < minDecisionSigningKeyBytes {
		if g.StateMAC != "" {
			return fmt.Errorf("%w: grant=%s has an unverifiable state signature", ErrDecisionTampered, g.ID)
		}
		return nil
	}
	expected, err := grantStateMAC(key, g)
	if err != nil {
		return err
	}
	expectedBytes, err := hex.DecodeString(expected)
	if err != nil {
		return err
	}
	actualBytes, err := hex.DecodeString(g.StateMAC)
	if err != nil || !hmac.Equal(expectedBytes, actualBytes) {
		return fmt.Errorf("%w: grant=%s state", ErrDecisionTampered, g.ID)
	}
	return nil
}

func verifyDecisionChain(g Grant, key []byte, required bool) error {
	if required && len(key) < minDecisionSigningKeyBytes {
		return ErrDecisionSigningKey
	}
	if required && len(g.Decisions) == 0 {
		return fmt.Errorf("%w: grant=%s has no signed decisions", ErrDecisionTampered, g.ID)
	}
	previous := ""
	for i, record := range g.Decisions {
		if record.Sequence != i+1 || record.PreviousMAC != previous {
			return fmt.Errorf("%w: grant=%s sequence=%d", ErrDecisionTampered, g.ID, i+1)
		}
		if len(key) >= minDecisionSigningKeyBytes {
			expected, err := decisionMAC(key, record)
			if err != nil {
				return err
			}
			decodedExpected, err := hex.DecodeString(expected)
			if err != nil {
				return err
			}
			decodedActual, err := hex.DecodeString(record.MAC)
			if err != nil || !hmac.Equal(decodedExpected, decodedActual) {
				return fmt.Errorf("%w: grant=%s sequence=%d", ErrDecisionTampered, g.ID, i+1)
			}
		} else if required || record.MAC != "" {
			return fmt.Errorf("%w: grant=%s sequence=%d", ErrDecisionTampered, g.ID, i+1)
		}
		previous = record.MAC
	}
	return nil
}

func sealGrant(g *Grant, key []byte, required bool) error {
	if required && len(key) < minDecisionSigningKeyBytes {
		return ErrDecisionSigningKey
	}
	if len(key) < minDecisionSigningKeyBytes {
		g.StateMAC = ""
		return nil
	}
	mac, err := grantStateMAC(key, *g)
	if err != nil {
		return err
	}
	g.StateMAC = mac
	return nil
}

func grantStateMAC(key []byte, grant Grant) (string, error) {
	lastDecision := ""
	if len(grant.Decisions) > 0 {
		lastDecision = grant.Decisions[len(grant.Decisions)-1].MAC
	}
	payload := grantStatePayload{
		ID:           grant.ID,
		Scope:        grant.Scope,
		Status:       grant.Status,
		Preview:      grant.Preview,
		SingleUse:    grant.SingleUse,
		TokenHash:    grant.TokenHash,
		Requester:    grant.Requester,
		Approver:     grant.Approver,
		CreatedAt:    grant.CreatedAt.UTC(),
		ApprovedAt:   utcTimePtr(grant.ApprovedAt),
		ExpiresAt:    utcTimePtr(grant.ExpiresAt),
		UsedAt:       utcTimePtr(grant.UsedAt),
		RevokedAt:    utcTimePtr(grant.RevokedAt),
		RevokeReason: grant.RevokeReason,
		LastDecision: lastDecision,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("approval: marshal grant state: %w", err)
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte("kafkamcp-approval-grant-v1\x00"))
	_, _ = mac.Write(data)
	return hex.EncodeToString(mac.Sum(nil)), nil
}

func utcTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	utc := value.UTC()
	return &utc
}

func decisionMAC(key []byte, record DecisionRecord) (string, error) {
	payload := decisionPayload{
		Sequence:    record.Sequence,
		Event:       record.Event,
		Actor:       record.Actor,
		Detail:      record.Detail,
		Timestamp:   record.Timestamp.UTC(),
		PreviousMAC: record.PreviousMAC,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("approval: marshal decision record: %w", err)
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte("kafkamcp-approval-decision-v1\x00"))
	_, _ = mac.Write(data)
	return hex.EncodeToString(mac.Sum(nil)), nil
}

func cloneDetail(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
