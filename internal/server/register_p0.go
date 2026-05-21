package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerP0Tools() {
	s.addTool(mcp.NewTool("kafka_list_topics",
		withCategory("streaming"),
		mcp.WithDescription("List all Kafka topics in the cluster with metadata including partition count, replication factor, and estimated message count."),
		mcp.WithString("filter", mcp.Description("Glob pattern to filter topic names (e.g., 'orders.*', '*-dlq')")),
		mcp.WithBoolean("include_internal", mcp.Description("Include internal topics (e.g., __consumer_offsets). Default: false")),
		mcp.WithString("cluster", mcp.Description("Cluster name for multi-cluster setups.")),
	), s.handleListTopics)

	s.addTool(mcp.NewTool("kafka_describe_topic",
		withCategory("streaming"),
		mcp.WithDescription("Get detailed metadata for a specific topic including partition details, replicas, ISR, offsets, and configuration."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
		mcp.WithString("cluster", mcp.Description("Cluster name for multi-cluster setups.")),
	), s.handleDescribeTopic)

	s.addTool(mcp.NewTool("kafka_consume",
		withCategory("streaming"),
		mcp.WithDescription("Read messages from a Kafka topic with flexible positioning. Supports earliest, latest, numeric offset, or ISO 8601 timestamp."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
		mcp.WithNumber("partition", mcp.Description("Specific partition (-1 for all). Default: all")),
		mcp.WithString("offset", mcp.Description("Start position: 'earliest', 'latest', numeric offset, or ISO 8601 timestamp. Default: 'latest'")),
		mcp.WithNumber("limit", mcp.Description("Maximum messages to return (1-1000). Default: 10")),
		mcp.WithString("deserializer", mcp.Description("Deserialization format: auto, avro, protobuf, json, string, bytes. Default: auto")),
		mcp.WithNumber("timeout_ms", mcp.Description("Maximum wait time in ms. Default: 5000")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleConsume)

	s.addTool(mcp.NewTool("kafka_produce",
		withCategory("streaming"),
		mcp.WithDescription("Publish one or more messages to a Kafka topic. Returns delivery confirmation with partition, offset, and timestamp."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
		mcp.WithObject("messages", mcp.Required(), mcp.Description("Array of messages: [{key, value, headers, partition}]. Max 100.")),
		mcp.WithString("serializer", mcp.Description("Serialization format: auto, avro, protobuf, json, string. Default: auto")),
		mcp.WithNumber("schema_id", mcp.Description("Explicit Schema Registry schema ID for serialization")),
		mcp.WithString("acks", mcp.Description("Acknowledgment level: 0, 1, all. Default: all")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleProduce)

	if s.hasSchemaRegistry() {
		s.addTool(mcp.NewTool("kafka_list_schemas",
			withCategory("streaming"),
			mcp.WithDescription("List all subjects in the Schema Registry with latest version, type, and compatibility level."),
			mcp.WithString("filter", mcp.Description("Glob pattern to filter subject names")),
		), s.handleListSchemas)

		s.addTool(mcp.NewTool("kafka_get_schema",
			withCategory("streaming"),
			mcp.WithDescription("Get the full schema definition for a subject. Returns parsed schema, field summary, compatibility, and references."),
			mcp.WithString("subject", mcp.Required(), mcp.Description("Schema subject name (e.g., 'orders.created-value')")),
			mcp.WithString("version", mcp.Description("Version: 'latest' or number. Default: 'latest'")),
		), s.handleGetSchema)

		// P1: Schema compatibility check
		s.addTool(mcp.NewTool("kafka_check_schema_compatibility",
			withCategory("streaming"),
			mcp.WithDescription("Check if a proposed schema is compatible with the subject's compatibility policy."),
			mcp.WithString("subject", mcp.Required(), mcp.Description("Schema subject name")),
			mcp.WithString("schema", mcp.Required(), mcp.Description("The proposed schema definition as a JSON string")),
			mcp.WithString("schema_type", mcp.Description("Schema type: AVRO, PROTOBUF, JSON. Default: AVRO")),
			mcp.WithString("version", mcp.Description("Version to check against. Default: 'latest'")),
		), s.handleCheckSchemaCompatibility)
	}

	s.addTool(mcp.NewTool("kafka_describe_consumer_group",
		withCategory("streaming"),
		mcp.WithDescription("Get the state, members, partition assignments, and per-partition lag of a consumer group."),
		mcp.WithString("group_id", mcp.Required(), mcp.Description("Consumer group ID")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleDescribeConsumerGroup)

	s.addTool(mcp.NewTool("kafka_list_consumer_groups",
		withCategory("streaming"),
		mcp.WithDescription("List all consumer groups with summary state, member count, and total lag."),
		mcp.WithString("state_filter", mcp.Description("Filter by state (Stable, Empty, Dead)")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleListConsumerGroups)
}
