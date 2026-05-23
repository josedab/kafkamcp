package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/llmpipeline"
)

func (s *Server) registerLLMPipelineTools() {
	if s.llmPipelineEngine == nil {
		return
	}

	s.addTool(mcp.NewTool("llm_pipeline_create",
		withCategory("ml"),
		mcp.WithDescription("Create an LLM training data export pipeline from Kafka topics with sampling, PII redaction, and deduplication."),
		mcp.WithString("topics", mcp.Required(), mcp.Description("Comma-separated list of source topics")),
		mcp.WithString("format", mcp.Description("Output format: jsonl, csv, parquet. Default: jsonl")),
		mcp.WithString("sampling_strategy", mcp.Description("Sampling: random, time_based, key_based, stratified. Default: random")),
		mcp.WithNumber("sampling_rate", mcp.Description("Sampling rate 0.0-1.0. Default: 1.0")),
		mcp.WithBoolean("pii_redaction", mcp.Description("Enable automatic PII redaction. Default: true")),
		mcp.WithBoolean("deduplicate", mcp.Description("Enable deduplication. Default: true")),
		mcp.WithNumber("max_messages", mcp.Description("Max messages to export. Default: 100000")),
		mcp.WithString("destination", mcp.Description("Destination: local, s3, gcs. Default: local")),
		mcp.WithString("destination_path", mcp.Description("Output path")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "llm_pipeline_create",
		resourceType: "topic",
		action:       "read",
	}, s.handleLLMPipelineCreate))

	s.addTool(mcp.NewTool("llm_pipeline_list",
		withCategory("ml"),
		mcp.WithDescription("List all configured LLM training data export pipelines."),
	), s.withToolAuth(toolAuthConfig{
		tool:         "llm_pipeline_list",
		resourceType: "cluster",
		action:       "read",
	}, s.handleLLMPipelineList))

	s.addTool(mcp.NewTool("llm_pipeline_status",
		withCategory("ml"),
		mcp.WithDescription("Get the status and results of an LLM training data pipeline."),
		mcp.WithString("pipeline_id", mcp.Required(), mcp.Description("Pipeline ID")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "llm_pipeline_status",
		resourceType: "cluster",
		action:       "read",
	}, s.handleLLMPipelineStatus))
}

func (s *Server) handleLLMPipelineCreate(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	topicsStr, _ := args["topics"].(string)
	if topicsStr == "" {
		return mcp.NewToolResultError("topics is required"), nil
	}

	var topics []string
	for _, t := range splitComma(topicsStr) {
		if t != "" {
			topics = append(topics, t)
		}
	}

	cfg := llmpipeline.PipelineConfig{
		Topics:       topics,
		PIIRedaction: true,
		Deduplicate:  true,
	}

	if f, ok := args["format"].(string); ok && f != "" {
		cfg.Format = llmpipeline.ExportFormat(f)
	}
	if ss, ok := args["sampling_strategy"].(string); ok && ss != "" {
		cfg.Sampling.Strategy = llmpipeline.SamplingStrategy(ss)
	}
	if sr, ok := args["sampling_rate"].(float64); ok {
		cfg.Sampling.Rate = sr
	}
	if pii, ok := args["pii_redaction"].(bool); ok {
		cfg.PIIRedaction = pii
	}
	if dd, ok := args["deduplicate"].(bool); ok {
		cfg.Deduplicate = dd
	}
	if mm, ok := args["max_messages"].(float64); ok {
		cfg.MaxMessages = int64(mm)
	}
	if dest, ok := args["destination"].(string); ok && dest != "" {
		cfg.Destination = llmpipeline.ExportDestination(dest)
	}
	if path, ok := args["destination_path"].(string); ok {
		cfg.DestinationPath = path
	}

	result, err := s.llmPipelineEngine.CreatePipeline(cfg)
	if err != nil {
		s.logAudit(ctx, agentID, "llm_pipeline_create", args, "", "error", 0, err, start)
		return s.toolError("failed to create pipeline", err), nil
	}

	s.logAudit(ctx, agentID, "llm_pipeline_create", args, "", "success", 1, nil, start)
	return marshalResult(result)
}

func (s *Server) handleLLMPipelineList(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	pipelines := s.llmPipelineEngine.ListPipelines()
	s.logAudit(ctx, agentID, "llm_pipeline_list", args, "", "success", len(pipelines), nil, start)
	return marshalResult(map[string]any{
		"pipelines": pipelines,
		"total":     len(pipelines),
	})
}

func (s *Server) handleLLMPipelineStatus(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	pipelineID, _ := args["pipeline_id"].(string)
	if pipelineID == "" {
		return mcp.NewToolResultError("pipeline_id is required"), nil
	}

	result, ok := s.llmPipelineEngine.GetResult(pipelineID)
	if !ok {
		// Check if pipeline exists
		if _, exists := s.llmPipelineEngine.GetPipeline(pipelineID); !exists {
			return marshalResult(map[string]any{"pipeline_id": pipelineID, "status": "not_found"})
		}
		return marshalResult(map[string]any{"pipeline_id": pipelineID, "status": "not_started"})
	}

	s.logAudit(ctx, agentID, "llm_pipeline_status", args, "", "success", 1, nil, start)
	return marshalResult(result)
}

func splitComma(s string) []string {
	var result []string
	for _, part := range splitString(s, ',') {
		result = append(result, trimSpace(part))
	}
	return result
}

func splitString(s string, sep byte) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}
