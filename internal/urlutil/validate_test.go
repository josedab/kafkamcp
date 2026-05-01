package urlutil

import (
	"net"
	"testing"
)

func TestValidateWebhookURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		// Valid URLs
		{"valid HTTPS URL", "https://example.com/webhook", false},
		{"valid HTTPS with port", "https://example.com:8443/webhook", false},
		{"valid HTTPS with path", "https://hooks.example.com/api/v1/notify", true},
		{"empty URL is allowed", "", false},

		// Scheme validation
		{"HTTP rejected", "http://example.com/webhook", true},
		{"FTP rejected", "ftp://example.com/file", true},
		{"no scheme rejected", "example.com/webhook", true},

		// Localhost / loopback
		{"localhost rejected", "https://localhost/webhook", true},
		{"localhost with port rejected", "https://localhost:8080/webhook", true},
		{"ip6-localhost rejected", "https://ip6-localhost/webhook", true},
		{"ip6-loopback rejected", "https://ip6-loopback/webhook", true},
		{"127.0.0.1 rejected", "https://127.0.0.1/webhook", true},
		{"127.0.0.2 rejected", "https://127.0.0.2/webhook", true},
		{"IPv6 loopback rejected", "https://[::1]/webhook", true},

		// Private IPs (RFC 1918)
		{"10.x.x.x rejected", "https://10.0.0.1/webhook", true},
		{"172.16.x.x rejected", "https://172.16.0.1/webhook", true},
		{"192.168.x.x rejected", "https://192.168.1.1/webhook", true},

		// Link-local
		{"169.254.x.x link-local rejected", "https://169.254.1.1/webhook", true},

		// Cloud metadata IP (SSRF target)
		{"metadata IP rejected", "https://169.254.169.254/latest/meta-data", true},

		// Missing hostname
		{"no hostname rejected", "https:///webhook", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateWebhookURL(tt.url)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateWebhookURL(%q) error = %v, wantErr %v", tt.url, err, tt.wantErr)
			}
		})
	}
}

func TestIsPrivateIP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ip   string
		want bool
	}{
		{"loopback v4", "127.0.0.1", true},
		{"loopback v6", "::1", true},
		{"private 10.x", "10.0.0.1", true},
		{"private 172.16.x", "172.16.0.1", true},
		{"private 192.168.x", "192.168.1.1", true},
		{"link-local", "169.254.1.1", true},
		{"metadata IP", "169.254.169.254", true},
		{"unspecified v4", "0.0.0.0", true},
		{"unspecified v6", "::", true},
		{"public IP", "8.8.8.8", false},
		{"public IP 2", "93.184.216.34", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ip := net.ParseIP(tt.ip)
			if ip == nil {
				t.Fatalf("failed to parse IP %q", tt.ip)
			}
			got := isPrivateIP(ip)
			if got != tt.want {
				t.Errorf("isPrivateIP(%q) = %v, want %v", tt.ip, got, tt.want)
			}
		})
	}
}

func TestIsMetadataIP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ip   string
		want bool
	}{
		{"metadata IP", "169.254.169.254", true},
		{"not metadata", "169.254.169.253", false},
		{"public IP", "8.8.8.8", false},
		{"loopback", "127.0.0.1", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ip := net.ParseIP(tt.ip)
			if ip == nil {
				t.Fatalf("failed to parse IP %q", tt.ip)
			}
			got := isMetadataIP(ip)
			if got != tt.want {
				t.Errorf("isMetadataIP(%q) = %v, want %v", tt.ip, got, tt.want)
			}
		})
	}
}
