package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	nethttp "net/http"
	"net/url"
	"time"
)

// CreateConnectorResult is the result of creating a connector.
type CreateConnectorResult struct {
	Name   string            `json:"name"`
	Config map[string]string `json:"config"`
	Status string            `json:"status"`
}

// CreateConnector creates a new connector via the Connect REST API.
func (c *ConnectClient) CreateConnector(ctx context.Context, name string, config map[string]string) (*CreateConnectorResult, error) {
	if c == nil {
		return nil, fmt.Errorf("Kafka Connect URL not configured")
	}

	config["name"] = name
	payload := map[string]any{
		"name":   name,
		"config": config,
	}

	body, err := c.httpPost(ctx, c.baseURL+"/connectors", payload)
	if err != nil {
		return nil, fmt.Errorf("creating connector: %w", err)
	}

	var result struct {
		Name   string            `json:"name"`
		Config map[string]string `json:"config"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("parsing response: %w", err)
	}

	return &CreateConnectorResult{
		Name:   result.Name,
		Config: redactSensitiveConfig(result.Config),
		Status: "created",
	}, nil
}

// UpdateConnector updates a connector's configuration.
func (c *ConnectClient) UpdateConnector(ctx context.Context, name string, config map[string]string) (*CreateConnectorResult, error) {
	if c == nil {
		return nil, fmt.Errorf("Kafka Connect URL not configured")
	}

	body, err := c.httpPut(ctx, c.baseURL+"/connectors/"+url.PathEscape(name)+"/config", config)
	if err != nil {
		return nil, fmt.Errorf("updating connector: %w", err)
	}

	var result map[string]string
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("parsing response: %w", err)
	}

	return &CreateConnectorResult{
		Name:   name,
		Config: redactSensitiveConfig(result),
		Status: "updated",
	}, nil
}

// DeleteConnector deletes a connector.
func (c *ConnectClient) DeleteConnector(ctx context.Context, name string) error {
	if c == nil {
		return fmt.Errorf("Kafka Connect URL not configured")
	}
	return c.httpDelete(ctx, c.baseURL+"/connectors/"+url.PathEscape(name))
}

// RestartResult is the result of a connector restart operation.
type RestartResult struct {
	Name           string `json:"name"`
	TasksRestarted int    `json:"tasks_restarted"`
	Status         string `json:"status"`
}

// RestartConnector restarts a connector or specific tasks.
func (c *ConnectClient) RestartConnector(ctx context.Context, name string, taskID int, failedOnly bool) (*RestartResult, error) {
	if c == nil {
		return nil, fmt.Errorf("Kafka Connect URL not configured")
	}

	if taskID >= 0 {
		// Restart specific task
		reqURL := fmt.Sprintf("%s/connectors/%s/tasks/%d/restart", c.baseURL, url.PathEscape(name), taskID)
		_, err := c.httpPostRaw(ctx, reqURL, nil)
		if err != nil {
			return nil, fmt.Errorf("restarting task %d: %w", taskID, err)
		}
		return &RestartResult{Name: name, TasksRestarted: 1, Status: "task_restarted"}, nil
	}

	if failedOnly {
		// Get status and restart only failed tasks
		detail, err := c.DescribeConnector(ctx, name)
		if err != nil {
			return nil, err
		}
		restarted := 0
		for _, task := range detail.Tasks {
			if task.State == "FAILED" {
				reqURL := fmt.Sprintf("%s/connectors/%s/tasks/%d/restart", c.baseURL, url.PathEscape(name), task.ID)
				if _, err := c.httpPostRaw(ctx, reqURL, nil); err == nil {
					restarted++
				}
			}
		}
		return &RestartResult{Name: name, TasksRestarted: restarted, Status: "failed_tasks_restarted"}, nil
	}

	// Restart entire connector
	reqURL := fmt.Sprintf("%s/connectors/%s/restart", c.baseURL, url.PathEscape(name))
	_, err := c.httpPostRaw(ctx, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("restarting connector: %w", err)
	}
	return &RestartResult{Name: name, TasksRestarted: -1, Status: "connector_restarted"}, nil
}

// ConnectorHealthResult provides health diagnostics for a connector.
type ConnectorHealthResult struct {
	Name            string   `json:"name"`
	HealthScore     float64  `json:"health_score"` // 0.0 - 1.0
	Status          string   `json:"status"`
	RunningTasks    int      `json:"running_tasks"`
	FailedTasks     int      `json:"failed_tasks"`
	TotalTasks      int      `json:"total_tasks"`
	Recommendations []string `json:"recommendations"`
}

// ConnectorHealth computes a health score for a connector.
func (c *ConnectClient) ConnectorHealth(ctx context.Context, name string) (*ConnectorHealthResult, error) {
	if c == nil {
		return nil, fmt.Errorf("Kafka Connect URL not configured")
	}

	detail, err := c.DescribeConnector(ctx, name)
	if err != nil {
		return nil, err
	}

	result := &ConnectorHealthResult{
		Name:       name,
		Status:     detail.State,
		TotalTasks: len(detail.Tasks),
	}

	for _, task := range detail.Tasks {
		switch task.State {
		case "RUNNING":
			result.RunningTasks++
		case "FAILED":
			result.FailedTasks++
		}
	}

	// Compute health score
	score := 1.0
	if detail.State != "RUNNING" {
		score -= 0.5
	}
	if result.TotalTasks > 0 {
		taskHealth := float64(result.RunningTasks) / float64(result.TotalTasks)
		score *= taskHealth
	}
	result.HealthScore = score

	// Generate recommendations
	if detail.State == "PAUSED" {
		result.Recommendations = append(result.Recommendations, "Connector is paused. Resume if it should be running.")
	}
	if result.FailedTasks > 0 {
		result.Recommendations = append(result.Recommendations,
			fmt.Sprintf("%d task(s) have failed. Use kafka_restart_connector with failed_only=true to restart them.", result.FailedTasks))
	}
	if result.TotalTasks == 0 {
		result.Recommendations = append(result.Recommendations, "No tasks configured. Check connector configuration.")
	}
	if score >= 0.9 && len(result.Recommendations) == 0 {
		result.Recommendations = append(result.Recommendations, "Connector is healthy. No action needed.")
	}

	return result, nil
}

// PauseConnector pauses a running connector via the Connect REST API.
func (c *ConnectClient) PauseConnector(ctx context.Context, name string) error {
	if c == nil {
		return fmt.Errorf("Kafka Connect URL not configured")
	}
	reqURL := c.baseURL + "/connectors/" + url.PathEscape(name) + "/pause"
	_, err := c.httpPut(ctx, reqURL, nil)
	if err != nil {
		return fmt.Errorf("pausing connector %q: %w", name, err)
	}
	return nil
}

// ResumeConnector resumes a paused connector via the Connect REST API.
func (c *ConnectClient) ResumeConnector(ctx context.Context, name string) error {
	if c == nil {
		return fmt.Errorf("Kafka Connect URL not configured")
	}
	reqURL := c.baseURL + "/connectors/" + url.PathEscape(name) + "/resume"
	_, err := c.httpPut(ctx, reqURL, nil)
	if err != nil {
		return fmt.Errorf("resuming connector %q: %w", name, err)
	}
	return nil
}

func (c *ConnectClient) httpPost(ctx context.Context, reqURL string, payload any) ([]byte, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return c.httpPostRaw(ctx, reqURL, data)
}

func (c *ConnectClient) httpPostRaw(ctx context.Context, reqURL string, data []byte) ([]byte, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}

	var body io.Reader
	if data != nil {
		body = bytes.NewReader(data)
	}

	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodPost, reqURL, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("Connect API error (HTTP %d): %s", resp.StatusCode, string(respBody))
	}
	return respBody, nil
}

func (c *ConnectClient) httpPut(ctx context.Context, reqURL string, payload any) ([]byte, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodPut, reqURL, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("Connect API error (HTTP %d): %s", resp.StatusCode, string(body))
	}
	return body, nil
}

func (c *ConnectClient) httpDelete(ctx context.Context, reqURL string) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}

	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodDelete, reqURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
		if readErr != nil {
			return fmt.Errorf("Connect API error (HTTP %d): failed to read response body: %w", resp.StatusCode, readErr)
		}
		return fmt.Errorf("Connect API error (HTTP %d): %s", resp.StatusCode, string(body))
	}
	return nil
}
