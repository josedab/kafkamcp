package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerFeatureTools() {
	// Anomaly Detection
	s.mcpServer.AddTool(mcp.NewTool("kafka_anomalies",
		mcp.WithDescription("Get AI-detected anomalies in consumer lag, throughput, and error rates. Uses z-score and IQR statistical models with configurable sensitivity."),
		mcp.WithString("severity", mcp.Description("Filter by severity: warning, critical")),
		mcp.WithString("metric", mcp.Description("Filter by metric type: consumer_lag, throughput, error_rate")),
		mcp.WithNumber("limit", mcp.Description("Max alerts to return. Default: 50")),
		mcp.WithNumber("since_hours", mcp.Description("Look back period in hours. Default: 24")),
	), s.handleAnomalies)

	s.mcpServer.AddTool(mcp.NewTool("kafka_anomaly_ingest",
		mcp.WithDescription("Manually ingest a data point for anomaly detection analysis."),
		mcp.WithString("metric", mcp.Required(), mcp.Description("Metric type: consumer_lag, throughput, error_rate")),
		mcp.WithString("source", mcp.Required(), mcp.Description("Source name (topic or consumer group)")),
		mcp.WithNumber("value", mcp.Required(), mcp.Description("Metric value")),
	), s.handleIngestAnomaly)

	// Natural Language Query
	s.mcpServer.AddTool(mcp.NewTool("kafka_nl_query",
		mcp.WithDescription("Query Kafka with plain English. Semantic parser handles complex boolean filters, aggregations, time ranges, multi-topic joins. Actions: execute (default), history, suggest. Examples: 'show me all failed orders in the last hour', 'count errors in logs group by service', 'average latency for payments'."),
		mcp.WithString("query", mcp.Description("Natural language query (required for execute/suggest)")),
		mcp.WithString("action", mcp.Description("Action: execute, history, suggest. Default: execute")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
		mcp.WithNumber("limit", mcp.Description("Max results. Default: 100")),
	), s.handleNLQuery)

	// Cost Analysis
	s.mcpServer.AddTool(mcp.NewTool("kafka_cost_analyze",
		mcp.WithDescription("Analyze storage and compute costs for a Kafka topic. Estimates monthly cost based on retention, partitions, throughput, and replication."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
		mcp.WithString("provider", mcp.Description("Cost model: aws-msk, confluent-cloud, self-hosted. Default: aws-msk")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleCostAnalyze)

	s.mcpServer.AddTool(mcp.NewTool("kafka_cost_waste",
		mcp.WithDescription("Detect cost waste across all topics: idle topics, over-replication, compression opportunities, excessive retention."),
		mcp.WithString("provider", mcp.Description("Cost model: aws-msk, confluent-cloud, self-hosted. Default: aws-msk")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleCostWaste)

	s.mcpServer.AddTool(mcp.NewTool("kafka_cost_whatif",
		mcp.WithDescription("Simulate configuration changes and see cost impact. What-if analysis for retention, compression, partitions, and replication changes."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
		mcp.WithNumber("new_retention_ms", mcp.Description("New retention period in ms")),
		mcp.WithString("new_compression", mcp.Description("New compression type: gzip, snappy, lz4, zstd, none")),
		mcp.WithNumber("new_partitions", mcp.Description("New partition count")),
		mcp.WithNumber("new_replication_factor", mcp.Description("New replication factor")),
		mcp.WithString("provider", mcp.Description("Cost model. Default: aws-msk")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleCostWhatIf)

	// Workflow Templates
	s.mcpServer.AddTool(mcp.NewTool("kafka_workflows",
		mcp.WithDescription("Manage pre-built workflow templates for common Kafka operations. Actions: list (browse templates), describe (get template details), execute (start a workflow)."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: list, describe, execute")),
		mcp.WithString("template", mcp.Description("Template name (required for describe/execute)")),
		mcp.WithString("category", mcp.Description("Filter by category: operations, planning, development, monitoring, discovery, quality, debugging")),
		mcp.WithObject("params", mcp.Description("Parameters for template execution")),
		mcp.WithString("topic", mcp.Description("Topic parameter for workflow")),
		mcp.WithString("group_id", mcp.Description("Consumer group parameter for workflow")),
		mcp.WithString("subject", mcp.Description("Schema subject parameter for workflow")),
	), s.handleWorkflows)

	// GitOps
	s.mcpServer.AddTool(mcp.NewTool("kafka_gitops_diff",
		mcp.WithDescription("Compare desired Kafka configuration (declarative YAML) against live cluster state. Generates a diff and migration plan with dry-run support."),
		mcp.WithObject("topics", mcp.Description("Array of desired topic specs: [{name, partitions, replication_factor, configs}]")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleGitOpsDiff)

	// Knowledge Graph
	s.mcpServer.AddTool(mcp.NewTool("kafka_knowledge_graph",
		mcp.WithDescription("Query the embedded knowledge graph of topic relationships, consumer group topologies, and schema dependencies. Supports: 'depends_on <node>', 'impacts <node>', 'path <from> to <to>', 'neighbors <node>', 'type <node_type>', or free-text search."),
		mcp.WithString("query", mcp.Required(), mcp.Description("Graph query (e.g., 'depends_on orders', 'impacts user-events', 'type consumer_group')")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleKnowledgeGraphQuery)

	// Platform Info
	s.mcpServer.AddTool(mcp.NewTool("kafka_platform_info",
		mcp.WithDescription("Get information about configured Kafka-compatible platforms (Apache Kafka, Redpanda, Confluent Cloud, MSK Serverless) and their capabilities."),
	), s.handlePlatformInfo)

	// Data Contracts
	s.mcpServer.AddTool(mcp.NewTool("kafka_validate_contract",
		mcp.WithDescription("Validate messages against data contracts, register contracts, or get compliance reports. Actions: validate, register, get, list, compliance."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: validate, register, get, list, compliance")),
		mcp.WithString("topic", mcp.Description("Topic name")),
		mcp.WithObject("contract", mcp.Description("Contract definition for register action")),
		mcp.WithObject("message", mcp.Description("Message value to validate")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleDataContract)

	// Consumer Lag Diagnosis
	s.mcpServer.AddTool(mcp.NewTool("kafka_diagnose_lag",
		mcp.WithDescription("Deep diagnostic analysis of consumer group lag. Traces root cause: slow consumers, producer spikes, partition imbalance, or GC pressure."),
		mcp.WithString("group_id", mcp.Required(), mcp.Description("Consumer group ID")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleDiagnoseLag)

	// Migration Planning
	s.mcpServer.AddTool(mcp.NewTool("kafka_migration_plan",
		mcp.WithDescription("Plan and validate Kafka migrations between clusters. Actions: plan (diff clusters), validate (check plan), list (show plans)."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: plan, validate, list")),
		mcp.WithString("source_cluster", mcp.Description("Source cluster name")),
		mcp.WithString("target_cluster", mcp.Description("Target cluster name")),
	), s.handleMigrationPlan)

	s.mcpServer.AddTool(mcp.NewTool("kafka_migrate_execute",
		mcp.WithDescription("Execute a migration plan. Supports dry-run mode, parallel execution, and progress tracking."),
		mcp.WithString("plan_id", mcp.Required(), mcp.Description("Migration plan ID to execute")),
		mcp.WithBoolean("dry_run", mcp.Description("If true, validate without making changes. Default: false")),
	), s.handleMigrationExecute)

	s.mcpServer.AddTool(mcp.NewTool("kafka_migrate_status",
		mcp.WithDescription("Get the status and progress of a migration execution."),
		mcp.WithString("plan_id", mcp.Required(), mcp.Description("Migration plan ID")),
		mcp.WithString("view", mcp.Description("View type: progress, report. Default: progress")),
	), s.handleMigrationStatus)

	s.mcpServer.AddTool(mcp.NewTool("kafka_migrate_rollback",
		mcp.WithDescription("Rollback a completed or failed migration."),
		mcp.WithString("plan_id", mcp.Required(), mcp.Description("Migration plan ID to rollback")),
	), s.handleMigrationRollback)

	// Data Lineage
	s.mcpServer.AddTool(mcp.NewTool("kafka_trace_lineage",
		mcp.WithDescription("Track data lineage through Kafka topics. Shows producer→topic→consumer relationships, PII annotations, and GDPR/CCPA compliance reports."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: trace, annotate_pii, compliance_report, detect_pii")),
		mcp.WithString("topic", mcp.Description("Topic name")),
		mcp.WithString("framework", mcp.Description("Compliance framework: GDPR, CCPA")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleTraceLineage)

	// Consumer Group Health
	s.mcpServer.AddTool(mcp.NewTool("kafka_group_health",
		mcp.WithDescription("Evaluate consumer group health with scoring (0-100), detect zombies, rebalance storms, stuck partitions, and suggest auto-remediation."),
		mcp.WithString("group_id", mcp.Required(), mcp.Description("Consumer group ID")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleGroupHealth)

	// Agent Personas
	s.mcpServer.AddTool(mcp.NewTool("kafka_persona",
		mcp.WithDescription("Manage declarative agent personas. Actions: list (all personas), get (persona details), activate (set active persona)."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: list, get, activate")),
		mcp.WithString("persona", mcp.Description("Persona name: sre, data-engineer, compliance, developer, admin")),
	), s.handlePersona)

	// Capacity Forecasting
	s.mcpServer.AddTool(mcp.NewTool("kafka_forecast",
		mcp.WithDescription("ML-based capacity forecasting using linear regression and seasonal decomposition. Predicts throughput growth and storage needs."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
		mcp.WithString("metric", mcp.Description("Metric to forecast: throughput, storage, lag. Default: throughput")),
		mcp.WithNumber("horizon_days", mcp.Description("Forecast horizon in days. Default: 7")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleForecast)
}
