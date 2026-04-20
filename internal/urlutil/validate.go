package urlutil

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// ValidateWebhookURL checks that a URL is safe to use as a webhook destination.
// It rejects private/internal IP ranges and non-HTTPS schemes.
func ValidateWebhookURL(rawURL string) error {
	if rawURL == "" {
		return nil
	}

	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid webhook URL: %w", err)
	}

	if parsed.Scheme != "https" {
		return fmt.Errorf("webhook URL must use HTTPS scheme, got %q", parsed.Scheme)
	}

	hostname := parsed.Hostname()
	if hostname == "" {
		return fmt.Errorf("webhook URL must include a hostname")
	}

	// Block localhost variants
	lower := strings.ToLower(hostname)
	if lower == "localhost" || lower == "ip6-localhost" || lower == "ip6-loopback" {
		return fmt.Errorf("webhook URL must not target localhost")
	}

	// Resolve and check IP ranges
	ips, err := net.LookupHost(hostname)
	if err != nil {
		// If DNS fails, also check if the hostname is a literal IP
		if ip := net.ParseIP(hostname); ip != nil {
			if isPrivateIP(ip) {
				return fmt.Errorf("webhook URL must not target private/internal IP ranges")
			}
		}
		// Allow URLs that can't be resolved at config time (may resolve later)
		return nil
	}

	for _, ipStr := range ips {
		ip := net.ParseIP(ipStr)
		if ip != nil && isPrivateIP(ip) {
			return fmt.Errorf("webhook URL must not target private/internal IP ranges")
		}
	}

	return nil
}

// isPrivateIP checks if an IP is in a private, loopback, link-local, or reserved range.
func isPrivateIP(ip net.IP) bool {
	return ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() ||
		isMetadataIP(ip)
}

// isMetadataIP checks for cloud metadata service IPs (169.254.169.254).
func isMetadataIP(ip net.IP) bool {
	metadata := net.ParseIP("169.254.169.254")
	return ip.Equal(metadata)
}
