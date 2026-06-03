package kafka

import (
	"crypto/tls"
	"net/http"
	"testing"
)

func TestTLSMinVersion(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		input string
		want  uint16
	}{
		{"", tls.VersionTLS12},
		{"1.2", tls.VersionTLS12},
		{"1.3", tls.VersionTLS13},
	} {
		got, err := tlsMinVersion(tc.input)
		if err != nil {
			t.Fatalf("tlsMinVersion(%q): %v", tc.input, err)
		}
		if got != tc.want {
			t.Errorf("tlsMinVersion(%q) = %d, want %d", tc.input, got, tc.want)
		}
	}
	if _, err := tlsMinVersion("1.1"); err == nil {
		t.Fatal("tlsMinVersion(1.1) should fail")
	}
}

func TestWithConnectMinVersion(t *testing.T) {
	t.Parallel()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if err := WithConnectMinVersion("1.3")(transport); err != nil {
		t.Fatal(err)
	}
	if transport.TLSClientConfig == nil || transport.TLSClientConfig.MinVersion != tls.VersionTLS13 {
		t.Fatalf("Connect TLS minimum = %#v, want TLS 1.3", transport.TLSClientConfig)
	}
}
