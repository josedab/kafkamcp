// KafkaMCP TypeScript SDK
// Provides type-safe async/await access to KafkaMCP tools

export interface Transport {
  callTool(name: string, args: Record<string, unknown>): Promise<unknown>;
  readResource(uri: string): Promise<unknown>;
  close(): Promise<void>;
}

export interface ClientOptions {
  agentId?: string;
  timeout?: number;
}

export interface TopicSummary {
  name: string;
  partitions: number;
  replication_factor: number;
  message_count_estimate: number;
}

export interface Message {
  partition: number;
  offset: number;
  timestamp: string;
  key: unknown;
  value: unknown;
  headers?: Record<string, string>;
}

export interface DeliveryResult {
  partition: number;
  offset: number;
  timestamp: string;
  status: string;
}

export interface ConsumerGroupSummary {
  group_id: string;
  state: string;
  member_count: number;
  total_lag: number;
}

export interface SchemaSubject {
  subject: string;
  version: number;
  schema_type: string;
}

export class KafkaMCPClient {
  private transport: Transport;
  private agentId: string;
  private timeout: number;

  constructor(transport: Transport, options: ClientOptions = {}) {
    this.transport = transport;
    this.agentId = options.agentId ?? "sdk-ts-agent";
    this.timeout = options.timeout ?? 30000;
  }

  // --- Topic Operations ---

  async listTopics(filter?: string): Promise<TopicSummary[]> {
    const result = (await this.transport.callTool("kafka_list_topics", {
      filter: filter ?? "",
    })) as { topics: TopicSummary[] };
    return result.topics;
  }

  async describeTopic(topic: string): Promise<unknown> {
    return this.transport.callTool("kafka_describe_topic", { topic });
  }

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

  async deleteTopic(topic: string): Promise<void> {
    await this.transport.callTool("kafka_delete_topic", { topic });
  }

  // --- Message Operations ---

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

  async listConsumerGroups(
    cluster?: string
  ): Promise<ConsumerGroupSummary[]> {
    const result = (await this.transport.callTool(
      "kafka_list_consumer_groups",
      cluster ? { cluster } : {}
    )) as { groups: ConsumerGroupSummary[] };
    return result.groups;
  }

  async describeConsumerGroup(groupId: string): Promise<unknown> {
    return this.transport.callTool("kafka_describe_consumer_group", {
      group_id: groupId,
    });
  }

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

  async listSchemas(): Promise<string[]> {
    const result = (await this.transport.callTool("kafka_list_schemas", {})) as {
      subjects: string[];
    };
    return result.subjects;
  }

  async describeSchema(subject: string): Promise<unknown> {
    return this.transport.callTool("kafka_describe_schema", { subject });
  }

  // --- AI & Analytics ---

  async naturalLanguageQuery(query: string): Promise<unknown> {
    return this.transport.callTool("kafka_nl_query", { query });
  }

  async getAnomalies(
    severity?: string,
    limit?: number
  ): Promise<unknown> {
    const args: Record<string, unknown> = {};
    if (severity) args.severity = severity;
    if (limit) args.limit = limit;
    return this.transport.callTool("kafka_anomalies", args);
  }

  async advisorScan(cluster?: string): Promise<unknown> {
    return this.transport.callTool("kafka_advisor_scan", cluster ? { cluster } : {});
  }

  async advisorReport(cluster?: string): Promise<unknown> {
    return this.transport.callTool("kafka_advisor_report", cluster ? { cluster } : {});
  }

  async diagnoseLag(groupId: string): Promise<unknown> {
    return this.transport.callTool("kafka_diagnose_lag", {
      group_id: groupId,
    });
  }

  // --- Utility ---

  async close(): Promise<void> {
    await this.transport.close();
  }
}

export default KafkaMCPClient;
