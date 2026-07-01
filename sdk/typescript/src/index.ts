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

export interface MutationOptions {
  /** Short-lived token issued by kafka_approval for the exact mutation. */
  approvalToken: string;
  /** Optional retry key for mutations whose server schema supports it. */
  idempotencyKey?: string;
  /** Optional configured Kafka cluster name. */
  cluster?: string;
}

function mutationControls(options: MutationOptions, idempotent: boolean): Record<string, string> {
  if (!options.approvalToken.trim()) {
    throw new Error("approvalToken is required for guarded mutations");
  }
  return {
    approval_token: options.approvalToken,
    ...(idempotent && options.idempotencyKey
      ? { idempotency_key: options.idempotencyKey }
      : {}),
    ...(options.cluster ? { cluster: options.cluster } : {}),
  };
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
  /** Subject compatibility level when reported by Schema Registry. */
  compatibility_level?: string;
}

interface SchemaSubjectResponse {
  subject_name: string;
  latest_version: number;
  schema_type: string;
  compatibility_level?: string;
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
 * const client = new KafkaMCPClient(transport);
 * const topics = await client.listTopics("orders.*");
 * await client.close();
 * ```
 */
export class KafkaMCPClient {
  private transport: Transport;

  /**
   * Create a new KafkaMCP client.
   * @param transport - MCP transport implementation (stdio, SSE, or HTTP).
   */
  constructor(transport: Transport) {
    this.transport = transport;
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
    name: string,
    partitions: number,
    replicationFactor: number,
    configs: Record<string, string> | undefined,
    options: MutationOptions
  ): Promise<void> {
    await this.transport.callTool("kafka_create_topic", {
      name,
      partitions,
      replication_factor: replicationFactor,
      ...(configs ? { configs } : {}),
      ...mutationControls(options, true),
    });
  }

  /** Delete a Kafka topic. This is irreversible. */
  async deleteTopic(name: string, options: MutationOptions): Promise<void> {
    await this.transport.callTool("kafka_delete_topic", {
      name,
      ...mutationControls(options, true),
    });
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
    })) as { messages: Message[] };
    return result.messages;
  }

  /**
   * Produce messages to a topic.
   * @param topic - Target topic.
   * @param messages - Array of messages with optional key, value, and headers.
   * @returns Delivery results with partition, offset, and timestamp for each message.
   */
  async produce(
    topic: string,
    messages: Array<{ key?: string; value: unknown; headers?: Record<string, string> }>,
    options: MutationOptions
  ): Promise<DeliveryResult[]> {
    const result = (await this.transport.callTool("kafka_produce", {
      topic,
      messages,
      ...mutationControls(options, false),
    })) as { deliveries: DeliveryResult[] };
    return result.deliveries;
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
    })) as { messages: Message[] };
    return result.messages;
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
    strategy: "earliest" | "latest" | "timestamp",
    options: MutationOptions,
    value?: string
  ): Promise<void> {
    await this.transport.callTool("kafka_reset_offsets", {
      group_id: groupId,
      topic,
      strategy,
      ...(value ? { value } : {}),
      ...mutationControls(options, false),
    });
  }

  // --- Schema Registry ---

  /** List all Schema Registry subjects. */
  async listSchemas(): Promise<SchemaSubject[]> {
    const result = (await this.transport.callTool("kafka_list_schemas", {})) as {
      subjects: SchemaSubjectResponse[];
    };
    return result.subjects.map((subject) => ({
      subject: subject.subject_name,
      version: subject.latest_version,
      schema_type: subject.schema_type,
      ...(subject.compatibility_level
        ? { compatibility_level: subject.compatibility_level }
        : {}),
    }));
  }

  /** Get schema definition and metadata for a subject. */
  async getSchema(subject: string): Promise<unknown> {
    return this.transport.callTool("kafka_get_schema", { subject });
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
