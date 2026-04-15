package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Client is an HTTP client for the Confluent Schema Registry API.
type Client struct {
	baseURL    string
	username   string
	password   string
	httpClient *http.Client
	cache      *Cache
}

// Subject represents a schema subject in the Schema Registry.
type Subject struct {
	SubjectName       string `json:"subject_name"`
	LatestVersion     int    `json:"latest_version"`
	SchemaType        string `json:"schema_type"`
	CompatibilityLevel string `json:"compatibility_level,omitempty"`
}

// SchemaInfo contains a full schema definition.
type SchemaInfo struct {
	Subject           string `json:"subject"`
	Version           int    `json:"version"`
	SchemaID          int    `json:"schema_id"`
	SchemaType        string `json:"schema_type"`
	CompatibilityLevel string `json:"compatibility_level,omitempty"`
	Schema            any    `json:"schema"`
	FieldSummary      string `json:"field_summary"`
	References        []any  `json:"references"`
}

// NewClient creates a new Schema Registry client.
func NewClient(baseURL, username, password string, cacheMaxEntries, cacheTTLSeconds int) *Client {
	if username != "" && !strings.HasPrefix(baseURL, "https://") {
		slog.Warn("Schema Registry credentials configured over non-HTTPS connection — credentials will be sent in plaintext", "url", baseURL)
	}
	return &Client{
		baseURL:  strings.TrimRight(baseURL, "/"),
		username: username,
		password: password,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		cache: NewCache(cacheMaxEntries, time.Duration(cacheTTLSeconds)*time.Second),
	}
}

func (c *Client) doRequest(ctx context.Context, method, path string) ([]byte, error) {
	reqURL := c.baseURL + path

	req, err := http.NewRequestWithContext(ctx, method, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json")
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("schema registry request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("schema registry error (HTTP %d)", resp.StatusCode)
	}

	return body, nil
}

// ListSubjects returns all subjects in the Schema Registry.
func (c *Client) ListSubjects(ctx context.Context) ([]string, error) {
	body, err := c.doRequest(ctx, http.MethodGet, "/subjects")
	if err != nil {
		return nil, err
	}

	var subjects []string
	if err := json.Unmarshal(body, &subjects); err != nil {
		return nil, fmt.Errorf("parsing subjects: %w", err)
	}

	return subjects, nil
}

// GetSubjectVersions returns all versions for a subject.
func (c *Client) GetSubjectVersions(ctx context.Context, subject string) ([]int, error) {
	path := fmt.Sprintf("/subjects/%s/versions", url.PathEscape(subject))
	body, err := c.doRequest(ctx, http.MethodGet, path)
	if err != nil {
		return nil, err
	}

	var versions []int
	if err := json.Unmarshal(body, &versions); err != nil {
		return nil, fmt.Errorf("parsing versions: %w", err)
	}
	return versions, nil
}

// GetSchema returns a schema for a specific subject and version.
func (c *Client) GetSchema(ctx context.Context, subject, version string) (*SchemaInfo, error) {
	// Validate version to prevent path traversal
	if version != "latest" {
		if _, err := strconv.Atoi(version); err != nil {
			return nil, fmt.Errorf("invalid version %q: must be 'latest' or a numeric value", version)
		}
	}

	cacheKey := fmt.Sprintf("%s:%s", subject, version)

	// Check cache
	if cached, ok := c.cache.Get(cacheKey); ok {
		return cached, nil
	}

	path := fmt.Sprintf("/subjects/%s/versions/%s", url.PathEscape(subject), url.PathEscape(version))
	body, err := c.doRequest(ctx, http.MethodGet, path)
	if err != nil {
		return nil, err
	}

	var raw struct {
		Subject    string `json:"subject"`
		Version    int    `json:"version"`
		ID         int    `json:"id"`
		SchemaType string `json:"schemaType"`
		Schema     string `json:"schema"`
		References []any  `json:"references"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("parsing schema: %w", err)
	}

	// Parse the schema string as JSON
	var parsedSchema any
	if err := json.Unmarshal([]byte(raw.Schema), &parsedSchema); err != nil {
		parsedSchema = raw.Schema
	}

	schemaType := raw.SchemaType
	if schemaType == "" {
		schemaType = "AVRO"
	}

	// Get compatibility level
	compat := c.getCompatibility(ctx, subject)

	info := &SchemaInfo{
		Subject:           raw.Subject,
		Version:           raw.Version,
		SchemaID:          raw.ID,
		SchemaType:        schemaType,
		CompatibilityLevel: compat,
		Schema:            parsedSchema,
		FieldSummary:      generateFieldSummary(parsedSchema, schemaType),
		References:        raw.References,
	}

	// Cache the result
	c.cache.Set(cacheKey, info)

	return info, nil
}

func (c *Client) getCompatibility(ctx context.Context, subject string) string {
	path := fmt.Sprintf("/config/%s", url.PathEscape(subject))
	body, err := c.doRequest(ctx, http.MethodGet, path)
	if err != nil {
		return ""
	}

	var config struct {
		CompatibilityLevel string `json:"compatibilityLevel"`
	}
	if err := json.Unmarshal(body, &config); err != nil {
		return ""
	}
	return config.CompatibilityLevel
}

// GetLatestSchema returns the latest schema for a subject.
func (c *Client) GetLatestSchema(ctx context.Context, subject string) (*SchemaInfo, error) {
	return c.GetSchema(ctx, subject, "latest")
}

// ListSubjectsDetailed returns detailed info for all subjects.
func (c *Client) ListSubjectsDetailed(ctx context.Context) ([]Subject, error) {
	subjects, err := c.ListSubjects(ctx)
	if err != nil {
		return nil, err
	}

	type result struct {
		idx     int
		subject Subject
		err     error
	}

	results := make(chan result, len(subjects))
	var wg sync.WaitGroup

	// Limit concurrency
	sem := make(chan struct{}, 10)

	for i, subj := range subjects {
		wg.Add(1)
		go func(idx int, subj string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			versions, err := c.GetSubjectVersions(ctx, subj)
			if err != nil {
				results <- result{idx: idx, err: err}
				return
			}

			latestVersion := 0
			if len(versions) > 0 {
				latestVersion = versions[len(versions)-1]
			}

			schema, err := c.GetSchema(ctx, subj, "latest")
			var schemaType, compat string
			if err == nil && schema != nil {
				schemaType = schema.SchemaType
				compat = schema.CompatibilityLevel
			}

			results <- result{
				idx: idx,
				subject: Subject{
					SubjectName:        subj,
					LatestVersion:      latestVersion,
					SchemaType:         schemaType,
					CompatibilityLevel: compat,
				},
			}
		}(i, subj)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	detailed := make([]Subject, len(subjects))
	for r := range results {
		if r.err != nil {
			// Still include the subject with partial info
			detailed[r.idx] = Subject{SubjectName: subjects[r.idx]}
			continue
		}
		detailed[r.idx] = r.subject
	}

	return detailed, nil
}

// IsAvailable checks if the Schema Registry is reachable.
func (c *Client) IsAvailable() bool {
	if c.baseURL == "" {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := c.doRequest(ctx, http.MethodGet, "/subjects")
	return err == nil
}

// generateFieldSummary creates a human-readable field summary from a schema.
func generateFieldSummary(schema any, schemaType string) string {
	if schemaType != "AVRO" {
		return fmt.Sprintf("(%s schema)", schemaType)
	}

	schemaMap, ok := schema.(map[string]any)
	if !ok {
		return ""
	}

	name, ok := schemaMap["name"].(string)
	if !ok {
		name = ""
	}
	fields, ok := schemaMap["fields"].([]any)
	if !ok {
		return name
	}

	var fieldDescs []string
	for _, f := range fields {
		field, ok := f.(map[string]any)
		if !ok {
			continue
		}
		fieldName, ok := field["name"].(string)
		if !ok {
			continue
		}
		fieldType := formatAvroType(field["type"])

		desc := fieldName + ": " + fieldType
		if def, ok := field["default"]; ok {
			desc += fmt.Sprintf(" (default: %v)", def)
		}
		fieldDescs = append(fieldDescs, desc)
	}

	return fmt.Sprintf("%s { %s }", name, strings.Join(fieldDescs, ", "))
}

func formatAvroType(t any) string {
	switch v := t.(type) {
	case string:
		return v
	case map[string]any:
		if typ, ok := v["type"].(string); ok {
			if logicalType, ok := v["logicalType"].(string); ok {
				return logicalType
			}
			if typ == "array" {
				items := formatAvroType(v["items"])
				return "array<" + items + ">"
			}
			if typ == "map" {
				values := formatAvroType(v["values"])
				return "map<string, " + values + ">"
			}
			return typ
		}
		return "complex"
	case []any:
		if len(v) == 2 {
			for _, item := range v {
				if s, ok := item.(string); ok && s == "null" {
					continue
				}
				return formatAvroType(item) + "?"
			}
		}
		types := make([]string, len(v))
		for i, item := range v {
			types[i] = formatAvroType(item)
		}
		return "union<" + strings.Join(types, ", ") + ">"
	default:
		return fmt.Sprintf("%v", t)
	}
}

// CompatibilityResult represents the result of a schema compatibility check.
type CompatibilityResult struct {
	IsCompatible bool     `json:"is_compatible"`
	Messages     []string `json:"messages,omitempty"`
}

// CheckCompatibility tests a schema against the subject's compatibility policy.
func (c *Client) CheckCompatibility(ctx context.Context, subject, version, schemaStr, schemaType string) (*CompatibilityResult, error) {
	if version == "" {
		version = "latest"
	}
	if version != "latest" {
		if _, err := strconv.Atoi(version); err != nil {
			return nil, fmt.Errorf("invalid version %q: must be 'latest' or a numeric value", version)
		}
	}
	if schemaType == "" {
		schemaType = "AVRO"
	}

	reqBody := fmt.Sprintf(`{"schema": %s, "schemaType": %q}`,
		jsonEscapeString(schemaStr), schemaType)

	path := fmt.Sprintf("/compatibility/subjects/%s/versions/%s",
		url.PathEscape(subject), url.PathEscape(version))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path,
		strings.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json")
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing compatibility check: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("compatibility check failed (HTTP %d)", resp.StatusCode)
	}

	var result struct {
		IsCompatible bool     `json:"is_compatible"`
		Messages     []string `json:"messages"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("parsing compatibility result: %w", err)
	}

	return &CompatibilityResult{
		IsCompatible: result.IsCompatible,
		Messages:     result.Messages,
	}, nil
}

func jsonEscapeString(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		// Fallback: double-encode to guarantee valid JSON string
		return `""`
	}
	return string(b)
}
