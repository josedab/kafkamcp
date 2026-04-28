// KafkaMCP TypeScript SDK
// Provides type-safe async/await access to KafkaMCP tools

/**
 * Transport interface for communicating with a KafkaMCP server.
 * Implement this interface to provide connectivity over stdio, SSE, or HTTP.
 */
export interface Transport {
  /** Invoke an MCP tool by name with the given arguments. */
  callTool(name: string, args: Record<string, unknown>): Promise<unknown>;
  /** Read an MCP resource by URI. */
  readResource(uri: string): Promise<unknown>;
  /** Close the transport connection and release resources. */
  close(): Promise<void>;
}

/**
 * Configuration options for creating a {@link KafkaMCPClient}.
 */
export interface ClientOptions {
  /** Agent identifier sent with each request for audit and access control. Defaults to `"sdk-ts-agent"`. */
  agentId?: string;
  /** Request timeout in milliseconds. Defaults to `30000`. */
  timeout?: number;
}

/**
 * Summary information for a Kafka topic.
 */
export interface TopicSummary {
  /** Topic name. */
  name: string;
  /** Number of partitions. */
  partitions: number;
  /** Replication factor. */
  replication_factor: number;
  /** Estimated total message count across all partitions. */
  message_count_estimate: number;
}

/**
 * A consumed or searched Kafka message.
 */
export interface Message {
  /** Partition the message was read from. */
  partition: number;
  /** Offset within the partition. */
  offset: number;
  /** Message timestamp (ISO 8601). */
  timestamp: string;
  /** Message key (may be null). */
  key: unknown;
  /** Message value (deserialized). */
  value: unknown;
  /** Optional message headers. */
  headers?: Record<string, string>;
}

/**
 * Result of producing a message to a Kafka topic.
 */
export interface DeliveryResult {
  /** Partition the message was written to. */
  partition: number;
  /** Assigned offset. */
  offset: number;
  /** Server-side timestamp (ISO 8601). */
  timestamp: string;
  /** Delivery status (e.g., `"success"`). */
  status: string;
}

/**
 * Summary information for a consumer group.
 */
export interface ConsumerGroupSummary {
  /** Consumer group identifier. */
  group_id: string;
  /** Group state (e.g., `"Stable"`, `"Empty"`). */
  state: string;
  /** Number of active members. */
  member_count: number;
  /** Total lag across all assigned partitions. */
  total_lag: number;
}

/**
 * Schema Registry subject information.
 */
export interface SchemaSubject {
  /** Subject name. */
  subject: string;
  /** Latest version number. */
  version: number;
  /** Schema type (e.g., `"AVRO"`, `"PROTOBUF"`, `"JSON"`). */
  schema_type: string;
}

/**
 * Type-safe client for interacting with a KafkaMCP server.
 *
 * Wraps an MCP {@link Transport} and provides async/await methods for all
 * Kafka operations: topic management, message consumption/production,
 * consumer group inspection, schema registry access, and AI analytics.
 *
 * @example
 * ```typescript
 * const client = new KafkaMCPClient(transport, { agentId: "my-agent" });
 * const topics = await client.listTopics("orders.*");
 * await client.close();
 * ```
 */
export class KafkaMCPClient {
  private transport: Transport;
  private agentId: string;
  private timeout: number;

  /**
   * Create a new KafkaMCP client.
   * @param transport - MCP transport implementation (stdio, SSE, or HTTP).
   * @param options - Optional client configuration.
   */
  constructor(transport: Transport, options: ClientOptions = {}) {
    this.transport = transport;
    this.agentId = options.agentId ?? "sdk-ts-agent";
    this.timeout = options.timeout ?? 30000;
  }

  // --- Topic Operations ---

  /** List topics, optionally filtered by a glob pattern. */
  async listTopics(filter?: string): Promise<TopicSummary[]> {
    const result = (await this.transport.callTool("kafka_list_topics", {
      filter: filter ?? "",
    })) as { topics: TopicSummary[] };
    return result.topics;
  }

  /** Get detailed metadata for a topic including partitions, configs, and offsets. */
  async describeTopic(topic: string): Promise<unknown> {
    return this.transport.callTool("kafka_describe_topic", { topic });
  }

  /**
   * Create a new Kafka topic.
   * @param topic - Topic name.
   * @param partitions - Number of partitions.
   * @param replicationFactor - Replication factor.
   * @param configs - Optional topic-level configuration overrides.
   */
  async createTopic(
    topic: string,
    partitions: number,
    replicationFactor: number,
    configs?: Record<string, string>
  ): Promise<void> {
    await this.transport.callTool("kafka_create_topic", {
      topic,
      partitions,
      replication_factor: replicationFactor,
      ...(configs ? { configs } : {}),
    });
  }

  /** Delete a Kafka topic. This is irreversible. */
  async deleteTopic(topic: string): Promise<void> {
    await this.transport.callTool("kafka_delete_topic", { topic });
  }

  // --- Message Operations ---

  /**
   * Consume messages from a topic.
   * @param topic - Topic to consume from.
   * @param limit - Maximum number of messages to return (default: 10).
   * @param offset - Starting position: `"earliest"`, `"latest"`, or a numeric offset (default: `"latest"`).
   */
  async consume(
    topic: string,
    limit = 10,
    offset = "latest"
  ): Promise<Message[]> {
    const result = (await this.transport.callTool("kafka_consume", {
      topic,
      limit,
      offset,
    })) as Message[];
    return result;
  }

  /**
   * Produce messages to a topic.
   * @param topic - Target topic.
   * @param messages - Array of messages with optional key, value, and headers.
   * @returns Delivery results with partition, offset, and timestamp for each message.
   */
  async produce(
    topic: string,
    messages: Array<{ key?: string; value: unknown; headers?: Record<string, string> }>
  ): Promise<DeliveryResult[]> {
    const result = (await this.transport.callTool("kafka_produce", {
      topic,
      messages,
    })) as { results: DeliveryResult[] };
    return result.results;
  }

  /**
   * Search messages in a topic by value content.
   * @param topic - Topic to search.
   * @param valueContains - Substring to match in message values.
   * @param limit - Maximum number of matches to return (default: 100).
   */
  async search(
    topic: string,
    valueContains: string,
    limit = 100
  ): Promise<Message[]> {
    const result = (await this.transport.callTool("kafka_search", {
      topic,
      value_contains: valueContains,
      limit,
    })) as { matches: Message[] };
    return result.matches;
  }

  // --- Consumer Group Operations ---

  /** List consumer groups, optionally filtered to a specific cluster. */
  async listConsumerGroups(
    cluster?: string
  ): Promise<ConsumerGroupSummary[]> {
    const result = (await this.transport.callTool(
      "kafka_list_consumer_groups",
      cluster ? { cluster } : {}
    )) as { groups: ConsumerGroupSummary[] };
    return result.groups;
  }

  /** Describe a consumer group including members, assignments, and lag. */
  async describeConsumerGroup(groupId: string): Promise<unknown> {
    return this.transport.callTool("kafka_describe_consumer_group", {
      group_id: groupId,
    });
  }

  /**
   * Reset consumer group offsets.
   * @param groupId - Consumer group ID (group must be inactive).
   * @param topic - Topic to reset offsets for.
   * @param strategy - Reset strategy: `"earliest"`, `"latest"`, or `"to-offset:<n>"`.
   */
  async resetOffsets(
    groupId: string,
    topic: string,
    strategy: string
  ): Promise<void> {
    await this.transport.callTool("kafka_reset_offsets", {
      group_id: groupId,
      topic,
      strategy,
    });
  }

  // --- Schema Registry ---

  /** List all Schema Registry subjects. */
  async listSchemas(): Promise<string[]> {
    const result = (await this.transport.callTool("kafka_list_schemas", {})) as {
      subjects: string[];
    };
    return result.subjects;
  }

  /** Get schema definition and metadata for a subject. */
  async describeSchema(subject: string): Promise<unknown> {
    return this.transport.callTool("kafka_describe_schema", { subject });
  }

  // --- AI & Analytics ---

  /** Execute a natural language query against Kafka data. */
  async naturalLanguageQuery(query: string): Promise<unknown> {
    return this.transport.callTool("kafka_nl_query", { query });
  }

  /**
   * Get detected anomalies in Kafka metrics.
   * @param severity - Filter by severity level (e.g., `"critical"`, `"warning"`).
   * @param limit - Maximum number of anomalies to return.
   */
  async getAnomalies(
    severity?: string,
    limit?: number
  ): Promise<unknown> {
    const args: Record<string, unknown> = {};
    if (severity) args.severity = severity;
    if (limit) args.limit = limit;
    return this.transport.callTool("kafka_anomalies", args);
  }

  /** Run an advisor scan to detect anti-patterns and generate recommendations. */
  async advisorScan(cluster?: string): Promise<unknown> {
    return this.transport.callTool("kafka_advisor_scan", cluster ? { cluster } : {});
  }

  /** Generate a comprehensive advisor report with prioritized findings. */
  async advisorReport(cluster?: string): Promise<unknown> {
    return this.transport.callTool("kafka_advisor_report", cluster ? { cluster } : {});
  }

  /** Diagnose consumer group lag and identify root causes. */
  async diagnoseLag(groupId: string): Promise<unknown> {
    return this.transport.callTool("kafka_diagnose_lag", {
      group_id: groupId,
    });
  }

  // --- Utility ---

  /** Close the underlying transport connection. */
  async close(): Promise<void> {
    await this.transport.close();
  }
}

export default KafkaMCPClient;
