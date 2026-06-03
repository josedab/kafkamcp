package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	nethttp "net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

// ConnectorSummary represents a Kafka Connect connector.
type ConnectorSummary struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	State  string `json:"state"`
	Worker string `json:"worker"`
}

// ConnectorDetail provides full details for a Kafka Connect connector.
type ConnectorDetail struct {
	Name   string            `json:"name"`
	Type   string            `json:"type"`
	State  string            `json:"state"`
	Worker string            `json:"worker"`
	Config map[string]string `json:"config"`
	Tasks  []ConnectorTask   `json:"tasks"`
}

// ConnectorTask describes a connector task.
type ConnectorTask struct {
	ID     int    `json:"id"`
	State  string `json:"state"`
	Worker string `json:"worker"`
	Trace  string `json:"trace,omitempty"`
}

// ConnectClient wraps HTTP calls to the Kafka Connect REST API.
type ConnectClient struct {
	baseURL    string
	httpClient *nethttp.Client
}

// NewConnectClient creates a Connect REST API client with optional TLS configuration.
func NewConnectClient(baseURL string, tlsOpts ...ConnectTLSOption) (*ConnectClient, error) {
	if baseURL == "" {
		return nil, nil
	}

	transport := nethttp.DefaultTransport.(*nethttp.Transport).Clone()
	for _, opt := range tlsOpts {
		if err := opt(transport); err != nil {
			return nil, fmt.Errorf("configuring Connect TLS: %w", err)
		}
	}

	return &ConnectClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &nethttp.Client{
			Timeout:   10 * time.Second,
			Transport: transport,
		},
	}, nil
}

// ConnectTLSOption configures TLS for the Connect client.
type ConnectTLSOption func(*nethttp.Transport) error

// WithConnectMinVersion sets the minimum TLS version for the Connect API.
func WithConnectMinVersion(version string) ConnectTLSOption {
	return func(t *nethttp.Transport) error {
		minVersion, err := tlsMinVersion(version)
		if err != nil {
			return err
		}
		if t.TLSClientConfig == nil {
			t.TLSClientConfig = &tls.Config{}
		}
		t.TLSClientConfig.MinVersion = minVersion
		return nil
	}
}

// WithConnectCA adds a custom CA certificate for the Connect API.
func WithConnectCA(caFile string) ConnectTLSOption {
	return func(t *nethttp.Transport) error {
		if caFile == "" {
			return nil
		}
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return fmt.Errorf("reading Connect CA file %q: %w", caFile, err)
		}
		if t.TLSClientConfig == nil {
			t.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		pool := t.TLSClientConfig.RootCAs
		if pool == nil {
			pool = x509.NewCertPool()
		}
		pool.AppendCertsFromPEM(caCert)
		t.TLSClientConfig.RootCAs = pool
		return nil
	}
}

// WithConnectInsecureSkipVerify disables TLS certificate verification (not recommended for production).
func WithConnectInsecureSkipVerify(skip bool) ConnectTLSOption {
	return func(t *nethttp.Transport) error {
		if !skip {
			return nil
		}
		slog.Warn("Kafka Connect TLS certificate verification disabled — connections are vulnerable to MITM attacks")
		if t.TLSClientConfig == nil {
			t.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		t.TLSClientConfig.InsecureSkipVerify = skip
		return nil
	}
}

// ListConnectors lists all connectors.
func (c *ConnectClient) ListConnectors(ctx context.Context) ([]ConnectorSummary, error) {
	if c == nil {
		return nil, fmt.Errorf("Kafka Connect URL not configured")
	}

	body, err := c.httpGet(ctx, c.baseURL+"/connectors?expand=status")
	if err != nil {
		return nil, fmt.Errorf("listing connectors: %w", err)
	}

	var expanded map[string]struct {
		Status struct {
			Name      string `json:"name"`
			Type      string `json:"type"`
			Connector struct {
				State  string `json:"state"`
				Worker string `json:"worker_id"`
			} `json:"connector"`
		} `json:"status"`
	}
	if err := json.Unmarshal(body, &expanded); err != nil {
		// Try simple list
		var names []string
		if err := json.Unmarshal(body, &names); err != nil {
			return nil, fmt.Errorf("parsing connectors: %w", err)
		}
		var result []ConnectorSummary
		for _, n := range names {
			result = append(result, ConnectorSummary{Name: n})
		}
		sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
		return result, nil
	}

	var result []ConnectorSummary
	for name, data := range expanded {
		result = append(result, ConnectorSummary{
			Name:   name,
			Type:   data.Status.Type,
			State:  data.Status.Connector.State,
			Worker: data.Status.Connector.Worker,
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

// DescribeConnector returns full details for a connector.
func (c *ConnectClient) DescribeConnector(ctx context.Context, name string) (*ConnectorDetail, error) {
	if c == nil {
		return nil, fmt.Errorf("Kafka Connect URL not configured")
	}

	// Get config
	configBody, err := c.httpGet(ctx, c.baseURL+"/connectors/"+url.PathEscape(name)+"/config")
	if err != nil {
		return nil, fmt.Errorf("getting connector config: %w", err)
	}
	var config map[string]string
	if err := json.Unmarshal(configBody, &config); err != nil {
		return nil, fmt.Errorf("parsing connector config JSON: %w", err)
	}

	// Get status
	statusBody, err := c.httpGet(ctx, c.baseURL+"/connectors/"+url.PathEscape(name)+"/status")
	if err != nil {
		return nil, fmt.Errorf("getting connector status: %w", err)
	}
	var status struct {
		Name      string `json:"name"`
		Type      string `json:"type"`
		Connector struct {
			State  string `json:"state"`
			Worker string `json:"worker_id"`
		} `json:"connector"`
		Tasks []struct {
			ID     int    `json:"id"`
			State  string `json:"state"`
			Worker string `json:"worker_id"`
			Trace  string `json:"trace"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal(statusBody, &status); err != nil {
		return nil, fmt.Errorf("parsing connector status JSON: %w", err)
	}

	detail := &ConnectorDetail{
		Name:   name,
		Type:   status.Type,
		State:  status.Connector.State,
		Worker: status.Connector.Worker,
		Config: redactSensitiveConfig(config),
	}
	for _, t := range status.Tasks {
		detail.Tasks = append(detail.Tasks, ConnectorTask{
			ID:     t.ID,
			State:  t.State,
			Worker: t.Worker,
			Trace:  t.Trace,
		})
	}

	return detail, nil
}

// sensitiveConfigKeys contains substrings that indicate a config key holds a secret.
var sensitiveConfigKeys = []string{
	"password", "secret", "credential", "token", "api.key", "apikey",
	"private.key", "ssl.key", "keystore", "truststore",
}

// redactSensitiveConfig returns a copy of the config map with sensitive values replaced.
func redactSensitiveConfig(cfg map[string]string) map[string]string {
	redacted := make(map[string]string, len(cfg))
	for k, v := range cfg {
		lower := strings.ToLower(k)
		isSensitive := false
		for _, s := range sensitiveConfigKeys {
			if strings.Contains(lower, s) {
				isSensitive = true
				break
			}
		}
		if isSensitive && v != "" {
			redacted[k] = "**REDACTED**"
		} else {
			redacted[k] = v
		}
	}
	return redacted
}

func (c *ConnectClient) httpGet(ctx context.Context, reqURL string) ([]byte, error) {
	// Enforce a deadline if the parent context doesn't have one
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}
	client := c.httpClient
	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("Connect API error (HTTP %d)", resp.StatusCode)
	}
	return body, nil
}
