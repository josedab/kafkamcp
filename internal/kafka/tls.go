package kafka

import (
	"crypto/tls"
	"fmt"
	"strings"
)

func tlsMinVersion(version string) (uint16, error) {
	switch strings.TrimSpace(version) {
	case "", "1.2":
		return tls.VersionTLS12, nil
	case "1.3":
		return tls.VersionTLS13, nil
	default:
		return 0, fmt.Errorf("unsupported TLS minimum version %q", version)
	}
}
