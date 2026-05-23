package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/pipeline"
)

func (s *Server) registerPipelineTools() {
	if s.pipelineEngine == nil {
		return
	}

	s.addTool(mcp.NewTool("kafka_pipeline_create",
		withCategory("streaming"),
		mcp.WithDescription("Create a streaming function pipeline with filter/map/enrich/transform steps. Pipelines process Kafka messages through a chain of micro-functions."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Pipeline name")),
		mcp.WithString("source_topic", mcp.Required(), mcp.Description("Source topic to consume from")),
		mcp.WithString("sink_topic", mcp.Description("Output topic for processed messages")),
		mcp.WithObject("steps", mcp.Required(), mcp.Description("Array of processing steps: [{type: 'filter|map|enrich|transform', name: '...', config: {...}}]")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_pipeline_create",
		resourceType: "topic",
		action:       "write",
	}, s.handlePipelineCreate))

	s.addTool(mcp.NewTool("kafka_pipeline_execute",
		withCategory("streaming"),
		mcp.WithDescription("Execute a pipeline on messages from the source topic. Consumes, processes through the pipeline steps, and returns the results."),
		mcp.WithString("pipeline_id", mcp.Required(), mcp.Description("Pipeline ID to execute")),
		mcp.WithNumber("limit", mcp.Description("Max messages to process. Default: 50")),
		mcp.WithString("cluster", mcp.Description("Cluster name")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_pipeline_execute",
		resourceType: "topic",
		action:       "read",
	}, s.handlePipelineExecute))

	s.addTool(mcp.NewTool("kafka_pipeline_manage",
		withCategory("streaming"),
		mcp.WithDescription("Manage streaming function pipelines. Actions: list, get, delete, runs (execution history)."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: list, get, delete, runs")),
		mcp.WithString("pipeline_id", mcp.Description("Pipeline ID")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_pipeline_manage",
		resourceType: "topic",
		action:       "read",
	}, s.handlePipelineManage))
}

func (s *Server) handlePipelineCreate(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	name, _ := args["name"].(string)
	sourceTopic, _ := args["source_topic"].(string)
	sinkTopic, _ := args["sink_topic"].(string)

	if name == "" || sourceTopic == "" {
		return mcp.NewToolResultError("name and source_topic are required"), nil
	}

	var steps []pipeline.Step
	if stepsRaw, ok := args["steps"]; ok {
		stepsJSON, err := json.Marshal(stepsRaw)
		if err != nil {
			return mcp.NewToolResultError("invalid steps format"), nil
		}
		if err := json.Unmarshal(stepsJSON, &steps); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("invalid steps: %s", err)), nil
		}
	}

	pipe, err := s.pipelineEngine.CreatePipeline(name, sourceTopic, sinkTopic, agentID, steps)
	if err != nil {
		return s.toolError("failed to create pipeline", err), nil
	}

	s.logAudit(ctx, agentID, "kafka_pipeline_create", args, "", "success", 1, nil, start)
	return marshalResult(pipe)
}

func (s *Server) handlePipelineExecute(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	pipelineID, _ := args["pipeline_id"].(string)
	if pipelineID == "" {
		return mcp.NewToolResultError("pipeline_id is required"), nil
	}

	pipe, err := s.pipelineEngine.GetPipeline(pipelineID)
	if err != nil {
		return s.toolError("pipeline not found", err), nil
	}

	cluster, _ := args["cluster"].(string)
	cluster = s.resolveClusterName(cluster)

	limit := 50
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
		if limit > 1000 {
			limit = 1000
		}
	}

	// Consume from source topic
	messages, err := s.kafkaManager.Consume(ctx, cluster, pipe.SourceTopic, -1, "latest", limit, 10000, "")
	if err != nil {
		s.logAudit(ctx, agentID, "kafka_pipeline_execute", args, cluster, "error", 0, err, start)
		return s.toolError("failed to consume source messages", err), nil
	}

	// Convert to pipeline messages
	var pipeMessages []pipeline.PipelineMessage
	for _, m := range messages {
		pipeMessages = append(pipeMessages, pipeline.PipelineMessage{
			Key:       fmt.Sprintf("%v", m.Key),
			Value:     fmt.Sprintf("%v", m.Value),
			Headers:   m.Headers,
			Partition: m.Partition,
			Offset:    m.Offset,
		})
	}

	// Execute pipeline
	run, output, err := s.pipelineEngine.Execute(pipelineID, pipeMessages)
	if err != nil {
		s.logAudit(ctx, agentID, "kafka_pipeline_execute", args, cluster, "error", 0, err, start)
		return s.toolError("pipeline execution failed", err), nil
	}

	s.logAudit(ctx, agentID, "kafka_pipeline_execute", args, cluster, "success", run.OutputCount, nil, start)
	return marshalResult(map[string]any{
		"run":    run,
		"output": output,
	})
}

func (s *Server) handlePipelineManage(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	action, _ := args["action"].(string)
	pipelineID, _ := args["pipeline_id"].(string)

	switch action {
	case "list":
		pipelines := s.pipelineEngine.ListPipelines()
		s.logAudit(ctx, agentID, "kafka_pipeline_manage", args, "", "success", len(pipelines), nil, start)
		return marshalResult(map[string]any{"pipelines": pipelines, "total": len(pipelines)})

	case "get":
		if pipelineID == "" {
			return mcp.NewToolResultError("pipeline_id is required"), nil
		}
		pipe, err := s.pipelineEngine.GetPipeline(pipelineID)
		if err != nil {
			return s.toolError("pipeline not found", err), nil
		}
		s.logAudit(ctx, agentID, "kafka_pipeline_manage", args, "", "success", 1, nil, start)
		return marshalResult(pipe)

	case "delete":
		if pipelineID == "" {
			return mcp.NewToolResultError("pipeline_id is required"), nil
		}
		err := s.pipelineEngine.DeletePipeline(pipelineID)
		if err != nil {
			return s.toolError("failed to delete pipeline", err), nil
		}
		s.logAudit(ctx, agentID, "kafka_pipeline_manage", args, "", "success", 1, nil, start)
		return marshalResult(map[string]string{"status": "deleted"})

	case "runs":
		runs := s.pipelineEngine.ListRuns(pipelineID, 50)
		s.logAudit(ctx, agentID, "kafka_pipeline_manage", args, "", "success", len(runs), nil, start)
		return marshalResult(map[string]any{"runs": runs, "total": len(runs)})

	default:
		return mcp.NewToolResultError("action must be: list, get, delete, or runs"), nil
	}
}
