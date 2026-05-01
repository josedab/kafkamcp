// Package randid provides cryptographically random ID generation.
package randid

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// New returns a random ID with the given prefix in the form "prefix-<16 hex chars>".
func New(prefix string) string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand should never fail on supported platforms;
		// panic rather than silently falling back to a predictable value.
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	return prefix + "-" + hex.EncodeToString(b)
}
