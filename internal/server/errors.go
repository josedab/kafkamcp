package server

import "fmt"

// AuthError represents an authentication failure.
type AuthError struct {
	AgentID string
	Reason  string
}

func (e *AuthError) Error() string {
	return fmt.Sprintf("authentication required for agent %q: %s", e.AgentID, e.Reason)
}

// AuthzError represents an authorization (permission) denial.
type AuthzError struct {
	AgentID      string
	Tool         string
	ResourceName string
	Action       string
}

func (e *AuthzError) Error() string {
	return fmt.Sprintf("agent %q not authorized: tool=%s resource=%s action=%s", e.AgentID, e.Tool, e.ResourceName, e.Action)
}

// RateLimitError represents a rate limit exceeded denial.
type RateLimitError struct {
	AgentID  string
	Resource string
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("rate limit exceeded for agent %q on %s", e.AgentID, e.Resource)
}
