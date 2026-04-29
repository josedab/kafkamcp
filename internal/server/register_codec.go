package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/codec"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerCodecTools() {
	if s.codecEngine == nil {
		return
	}

	s.mcpServer.AddTool(mcp.NewTool("codec_decode",
		withCategory("codec"),
		mcp.WithDescription("Decode a wire-format message (Confluent Schema Registry format with Avro/Protobuf support, auto-detect with JSON fallback)."),
		mcp.WithString("data", mcp.Required(), mcp.Description("Base64-encoded message data or raw JSON string")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "codec_decode",
		resourceType: "topic",
		action:       "read",
	}, s.handleCodecDecode))

	s.mcpServer.AddTool(mcp.NewTool("codec_encode",
		withCategory("codec"),
		mcp.WithDescription("Encode a message using Confluent wire format (magic byte + schema ID + Avro/Protobuf/JSON payload)."),
		mcp.WithString("value", mcp.Required(), mcp.Description("JSON value to encode")),
		mcp.WithNumber("schema_id", mcp.Required(), mcp.Description("Schema Registry schema ID")),
		mcp.WithString("format", mcp.Description("Encoding format: avro, protobuf, json. Default: json")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "codec_encode",
		resourceType: "topic",
		action:       "write",
	}, s.handleCodecEncode))

	s.mcpServer.AddTool(mcp.NewTool("codec_detect_format",
		withCategory("codec"),
		mcp.WithDescription("Detect the serialization format of a message (Confluent wire format, JSON, or raw bytes)."),
		mcp.WithString("data", mcp.Required(), mcp.Description("Base64-encoded message data or raw string")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "codec_detect_format",
		resourceType: "topic",
		action:       "read",
	}, s.handleCodecDetectFormat))
}

func (s *Server) handleCodecDecode(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	data, _ := args["data"].(string)
	if data == "" {
		return mcp.NewToolResultError("data is required"), nil
	}

	result, err := s.codecEngine.Decode([]byte(data))
	if err != nil {
		s.logAudit(agentID, "codec_decode", args, "", "error", 0, err, start)
		return s.toolError("failed to decode message", err), nil
	}

	s.logAudit(agentID, "codec_decode", args, "", "success", 1, nil, start)
	return marshalResult(result)
}

func (s *Server) handleCodecEncode(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	value, _ := args["value"].(string)
	if value == "" {
		return mcp.NewToolResultError("value is required"), nil
	}

	schemaID := 0
	if id, ok := args["schema_id"].(float64); ok {
		schemaID = int(id)
	}

	format := codec.FormatJSON
	if f, ok := args["format"].(string); ok && f != "" {
		format = codec.Format(f)
	}

	result, err := s.codecEngine.Encode(value, schemaID, format)
	if err != nil {
		s.logAudit(agentID, "codec_encode", args, "", "error", 0, err, start)
		return s.toolError("failed to encode message", err), nil
	}

	s.logAudit(agentID, "codec_encode", args, "", "success", 1, nil, start)
	return marshalResult(map[string]any{
		"schema_id": result.SchemaID,
		"format":    result.Format,
		"size":      len(result.Data),
	})
}

func (s *Server) handleCodecDetectFormat(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	data, _ := args["data"].(string)
	if data == "" {
		return mcp.NewToolResultError("data is required"), nil
	}

	detected := codec.DetectFormat([]byte(data))
	s.logAudit(agentID, "codec_detect_format", args, "", "success", 1, nil, start)
	return marshalResult(map[string]any{
		"format": detected,
	})
}
