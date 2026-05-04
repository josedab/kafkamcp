package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerReplayStudioTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_replay_record",
		withCategory("testing"),
		mcp.WithDescription("Record production message sequences for replay. Start, stop, or manage recording sessions."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: start, stop, get, list")),
		mcp.WithString("topic", mcp.Description("Topic to record (required for start)")),
		mcp.WithString("name", mcp.Description("Recording name")),
		mcp.WithString("cluster", mcp.Description("Cluster name")),
		mcp.WithString("recording_id", mcp.Description("Recording ID (for stop/get)")),
		mcp.WithNumber("max_messages", mcp.Description("Max messages to record. Default: 1000")),
	), s.handleReplayRecord)

	s.mcpServer.AddTool(mcp.NewTool("kafka_replay_run",
		withCategory("testing"),
		mcp.WithDescription("Replay a recording in a sandboxed environment with time dilation and assertion checking."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: create, run, get, list")),
		mcp.WithString("recording_id", mcp.Description("Recording ID (for create)")),
		mcp.WithString("session_id", mcp.Description("Replay session ID (for run/get)")),
		mcp.WithString("name", mcp.Description("Replay session name")),
		mcp.WithNumber("time_dilation", mcp.Description("Time dilation factor: 0.1-10.0. Default: 1.0 (realtime)")),
		mcp.WithString("sandbox_id", mcp.Description("Sandbox ID for isolated replay")),
	), s.handleReplayRun)

	s.mcpServer.AddTool(mcp.NewTool("kafka_replay_regression",
		withCategory("testing"),
		mcp.WithDescription("Create and run regression test suites against recorded message sequences."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: create_suite, run_suite")),
		mcp.WithString("recording_id", mcp.Description("Recording ID (for create_suite)")),
		mcp.WithString("suite_id", mcp.Description("Suite ID (for run_suite)")),
		mcp.WithString("name", mcp.Description("Suite name")),
		mcp.WithString("description", mcp.Description("Suite description")),
	), s.handleReplayRegression)
}
