# KafkaMCP — Product Requirements Document

> **Open Source MCP Server That Makes Kafka Topics Fully Readable, Writable, and Discoverable by AI Agents Without Human Intermediaries**

| Field              | Value                                      |
|--------------------|--------------------------------------------|
| **Product**        | KafkaMCP                                   |
| **Version**        | 1.0 (Initial Release)                      |
| **Author**         | Jose D.                                    |
| **Status**         | Draft                                      |
| **Last Updated**   | 2025-07                                    |
| **Category**       | Software for Agents                        |
| **Score**          | 8.8 / 10                                   |
| **Classification** | Internal — Confidential                    |

**Sub-scores:**

| Dimension        | Score  |
|-------------------|--------|
| RFS Alignment     | 8.5    |
| Skill Fit         | 10.0   |
| OSS Traction      | 9.0    |
| Solo Feasibility   | 8.5    |
| Market Size        | 7.8    |

---

## Table of Contents

1.  [Executive Summary](#1-executive-summary)
2.  [Problem Statement](#2-problem-statement)
3.  [Vision & Strategic Goals](#3-vision--strategic-goals)
4.  [Target Users & Personas](#4-target-users--personas)
5.  [User Stories & Use Cases](#5-user-stories--use-cases)
6.  [Functional Requirements](#6-functional-requirements)
7.  [Non-Functional Requirements](#7-non-functional-requirements)
8.  [Technical Architecture](#8-technical-architecture)
9.  [API Design](#9-api-design)
10. [Data Model](#10-data-model)
11. [Security Considerations](#11-security-considerations)
12. [Performance Requirements & SLAs](#12-performance-requirements--slas)
13. [Deployment & Infrastructure](#13-deployment--infrastructure)
14. [Metrics & KPIs](#14-metrics--kpis)
15. [Go-to-Market Strategy](#15-go-to-market-strategy)
16. [Competitive Analysis](#16-competitive-analysis)
17. [Risks & Mitigations](#17-risks--mitigations)
18. [Milestones & Roadmap](#18-milestones--roadmap)
19. [Open Questions & Future Considerations](#19-open-questions--future-considerations)
20. [Appendix](#20-appendix)

---

## 1. Executive Summary

### 1.1 Vision

KafkaMCP is an **open source MCP (Model Context Protocol) server** that transforms Apache Kafka — the dominant streaming platform powering data pipelines at virtually every major technology company — into a first-class, agent-accessible data layer. Today, AI agents cannot subscribe to Kafka topics, publish events, discover schemas, or reason about streaming data without purpose-built glue code maintained by human engineers. KafkaMCP eliminates this gap entirely. It exposes Kafka topics, consumer groups, schemas, and cluster metadata as **MCP tools and resources** that any MCP-compatible AI agent can discover, invoke, and compose — with zero custom integration code.

### 1.2 Why Now

Three converging forces make this the right moment:

1. **The MCP Standard Has Won:** Anthropic's Model Context Protocol has achieved critical mass as the universal interface for agent-tool interaction. Major AI providers (Anthropic, OpenAI via adapters, Google via adapters) and hundreds of agent frameworks (LangChain, CrewAI, AutoGen, OpenAI Agents SDK) support MCP. The protocol is stable enough to build production infrastructure on top of.

2. **Kafka Is Ubiquitous But Agent-Blind:** Apache Kafka processes trillions of messages daily across the Fortune 500. LinkedIn, Uber, Netflix, Airbnb, Goldman Sachs, and every major cloud provider run Kafka. Yet not a single production-grade bridge exists between Kafka and the agent ecosystem. Every team that wants agents to interact with Kafka writes bespoke, fragile wrapper code.

3. **AI Agent Adoption Is Crossing the Chasm:** Enterprise AI agent deployments are moving from experimental to production. Data engineering teams, ML platform teams, and LLMOps teams are the fastest adopters — and they are precisely the teams that live in Kafka every day. They need their agents to read from and write to the same streaming infrastructure they already operate.

### 1.3 One-Liner

Open source MCP server that makes Kafka topics fully readable, writable, and discoverable by AI agents without human intermediaries.

### 1.4 Business Model

KafkaMCP follows an **open-core model**:

| Tier            | License      | Features                                                                     | Pricing                    |
|-----------------|--------------|------------------------------------------------------------------------------|----------------------------|
| **Community**   | Apache 2.0   | Full MCP server, topic CRUD, produce/consume, schema discovery, ACL read     | Free                       |
| **Cloud**       | Proprietary  | Managed multi-tenant server, agent audit trail, usage analytics, hosted UI   | Per-agent-hour + data tier |
| **Enterprise**  | Proprietary  | On-prem, RBAC policies, SSO/SAML, data masking, compliance logging, SLA     | Annual contract             |

### 1.5 Key Differentiators

| Differentiator                     | Description                                                                                         |
|------------------------------------|-----------------------------------------------------------------------------------------------------|
| **MCP-native, not bolted on**      | Purpose-built MCP server, not a REST-to-MCP adapter; every Kafka operation is a first-class MCP tool |
| **Schema-aware intelligence**      | Automatically discovers Avro/Protobuf/JSON Schema from Schema Registry and surfaces them as typed MCP resources, so agents understand message structure without human annotation |
| **Streaming-native consumption**   | Supports long-lived MCP subscriptions (via SSE/streaming) so agents can react to Kafka events in real time, not just poll |
| **Zero-config discovery**          | Agents can list topics, describe schemas, inspect consumer group lag, and browse partitions with no prior knowledge of the cluster |
| **Battle-tested Kafka expertise**  | Built by a founder who has operated Kafka at GitHub scale — 47-broker clusters, controller failovers, ZooKeeper migration to KRaft |
| **OSS-first distribution**         | Apache 2.0 core ensures adoption by ML platform, data engineering, and LLMOps communities           |

---

## 2. Problem Statement

### 2.1 The Agent-Data Gap

Modern AI agents are increasingly capable of reasoning, planning, and executing multi-step workflows. However, they are fundamentally disconnected from the real-time data streams that power modern enterprises. Kafka is the central nervous system of data infrastructure — event sourcing, CDC pipelines, ML feature stores, real-time analytics, and inter-service communication all flow through Kafka topics. Yet agents have no native way to access this data.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    THE AGENT-KAFKA DISCONNECT                          │
│                                                                        │
│   ┌──────────────┐                              ┌──────────────────┐   │
│   │   AI Agent    │                              │  Kafka Cluster   │   │
│   │              │          ╳ NO PATH ╳          │                  │   │
│   │  • Can call  │◄─────────────────────────────▶│  • 500+ topics   │   │
│   │    MCP tools │                               │  • 10M msgs/sec  │   │
│   │  • Can reason│                               │  • Avro schemas  │   │
│   │  • Can plan  │                               │  • Consumer grps │   │
│   └──────────────┘                               └──────────────────┘   │
│                                                                        │
│   Today's workaround:                                                  │
│   1. Human writes Python/Go consumer → dumps to file/DB               │
│   2. Human builds REST API wrapper around consumer                     │
│   3. Human writes MCP tool that calls REST API                         │
│   4. Agent finally gets stale, batch-mode access                       │
│                                                                        │
│   Problems:                                                            │
│   • 3+ layers of bespoke code to maintain                              │
│   • No schema awareness — agent gets raw bytes                         │
│   • No real-time streaming — agent polls or reads dumps                │
│   • No discovery — agent must be hard-coded per topic                  │
│   • No publish capability — agent is read-only at best                 │
│   • Every team reinvents this stack independently                      │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Market Pain Points

| Pain Point | Impact | Who Suffers |
|---|---|---|
| **No standard agent-to-Kafka bridge** | Every team writes custom glue code; 2–4 weeks per integration | Data engineering, ML platform teams |
| **Schema blindness** | Agents receive raw bytes or untyped JSON; cannot reason about message structure, validate payloads, or generate correct producers | Any team using Avro/Protobuf schemas |
| **Batch-only access** | Agents can only access Kafka data via periodic dumps to S3/databases; real-time reactivity is impossible | LLMOps teams, incident response agents |
| **No topic discovery** | Agent must be told which topics exist and what they contain; cannot self-explore a Kafka cluster | Platform teams building autonomous agents |
| **Write exclusion** | Even when read access is hacked together, agents cannot publish events back to Kafka — breaking feedback loops | Automation agents, data correction bots |
| **Security theater** | Ad-hoc wrappers bypass Kafka ACLs, audit logging, and encryption; no agent-specific access control | Security and compliance teams |
| **Operational overhead** | Each bespoke integration requires monitoring, alerting, and on-call coverage independent of Kafka's own observability | SRE and infrastructure teams |

### 2.3 Current Alternatives and Why They Fail

| Alternative | What It Does | Why It Fails for Agents |
|---|---|---|
| **Kafka REST Proxy (Confluent)** | HTTP/REST interface to produce/consume | Not MCP-compatible; no schema introspection; no streaming; requires human integration per agent; not designed for discovery |
| **Custom Python/Go consumers** | Bespoke scripts that consume topics and expose data via APIs | Per-topic, per-team maintenance burden; no standardized interface; no schema awareness; no agent discovery |
| **Kafka Connect + sink to DB** | Pipes Kafka data to PostgreSQL/Elasticsearch | Adds latency (seconds to minutes); agents access a stale copy; no write-back path; schema translation lossy |
| **LangChain Kafka integration** | Community-contributed, minimal Kafka tool wrapper | Incomplete; no schema support; no consumer group management; no streaming; abandoned maintenance |
| **Direct Kafka client in agent** | Embed kafka-python or librdkafka in agent code | Requires Kafka client expertise in agent code; violates separation of concerns; connection management hell; no MCP protocol support |
| **ksqlDB / Flink SQL** | Stream processing engines with SQL interfaces | Designed for stream transformations, not agent interaction; no MCP bridge; overkill for read/write/discover; operational burden |

### 2.4 The Cost of Inaction

Organizations that delay adopting a standard agent-to-Kafka bridge face:

- **Fragmentation tax:** 5–15 bespoke integrations per company, each with independent maintenance, monitoring, and on-call rotations.
- **Stale-data risk:** Agents operating on batch exports make decisions on data that is minutes to hours old — unacceptable for fraud detection, incident response, and real-time personalization.
- **Security exposure:** Ad-hoc REST wrappers bypass Kafka's native ACL system, creating unaudited access paths to sensitive event streams.
- **Talent bottleneck:** Every agent-Kafka integration requires a developer who understands both Kafka internals and the agent framework — a rare and expensive combination.

---

## 3. Vision & Strategic Goals

### 3.1 Product Vision Statement

> **Any AI agent, using any MCP-compatible framework, should be able to discover, read from, write to, and reason about any Kafka topic — in real time, with schema awareness, and under the same access control policies as human operators — within five minutes of configuration.**

### 3.2 Strategic Goals

#### Short-Term (0–6 months): Establish the Standard

| Goal | Success Metric |
|---|---|
| Ship v0.1 OSS with core produce/consume/discover MCP tools | GitHub release with passing CI |
| Achieve 1,000 GitHub stars | Star count on GitHub |
| Get adopted by 3 production teams at recognizable companies | Case studies or public testimonials |
| Publish Schema Registry integration (Avro + JSON Schema) | Feature parity with schema-aware reads/writes |
| Build community: Discord server with 200+ members | Member count |

#### Mid-Term (6–18 months): Become the Default

| Goal | Success Metric |
|---|---|
| Support all major schema formats (Avro, Protobuf, JSON Schema) | Format coverage |
| Launch KafkaMCP Cloud (managed, multi-tenant) | Paying beta customers |
| Integration with 5+ agent frameworks (LangChain, CrewAI, AutoGen, Claude Desktop, OpenAI Agents SDK) | Published integrations + tutorials |
| Confluent Cloud, AWS MSK, Redpanda, and WarpStream compatibility | Tested and documented |
| 5,000 GitHub stars, 50+ contributors | Community metrics |

#### Long-Term (18–36 months): Own the Agent-Streaming Category

| Goal | Success Metric |
|---|---|
| Enterprise tier with $1M+ ARR | Revenue |
| Expand beyond Kafka: Pulsar, Kinesis, Redpanda native protocol | Multi-platform support |
| Become the reference MCP implementation for streaming infrastructure | Ecosystem recognition, MCP spec references |
| Agent-native streaming primitives: declarative subscriptions, event-driven triggers, stream joins via MCP | Feature set that defines a new product category |
| 15,000+ GitHub stars, contributor ecosystem | Community metrics |

---

## 4. Target Users & Personas

### 4.1 Persona: Maya — Senior Data Engineer

| Attribute | Detail |
|---|---|
| **Role** | Senior Data Engineer at a Series C fintech (200 engineers) |
| **Experience** | 6 years; built CDC pipelines with Debezium + Kafka; manages 120 topics in Confluent Cloud |
| **Daily tools** | Kafka, dbt, Airflow, Spark, Terraform, Python |
| **Agent exposure** | Uses GitHub Copilot; has experimented with Claude for SQL generation; wants agents to automate pipeline monitoring |
| **Pain points** | Spends 4 hours/week writing one-off scripts to let internal tools query Kafka topics; each new agent integration is a 2-week project; schema changes break downstream consumers silently |
| **Goal with KafkaMCP** | Drop in KafkaMCP so her team's AI agents can self-serve Kafka data without filing tickets to her team |
| **Success looks like** | Agent autonomously detects a schema drift on a payment events topic, reads the new schema, and updates the downstream consumer config — all without a human ticket |

### 4.2 Persona: Raj — ML Platform Lead

| Attribute | Detail |
|---|---|
| **Role** | ML Platform Lead at a FAANG-adjacent company (5,000+ engineers) |
| **Experience** | 8 years; built feature stores on top of Kafka Streams; operates 2,000+ topics across 5 Kafka clusters |
| **Daily tools** | Kafka, Flink, Feast, Kubernetes, Go, Terraform, internal ML platform |
| **Agent exposure** | His team is building autonomous ML pipeline agents that need to discover and read feature streams, detect training data drift, and trigger retraining |
| **Pain points** | Feature store data lives in Kafka but agents can only access it via a hand-built REST API with 200ms+ latency; no way for agents to discover new feature topics automatically; security team blocks direct Kafka access from agent pods |
| **Goal with KafkaMCP** | Standardize agent access to Kafka across all 5 clusters behind a single MCP interface with ACL enforcement |
| **Success looks like** | An ML pipeline agent discovers a new feature topic, reads its Avro schema, validates compatibility with the training pipeline's expected input, and subscribes — all autonomously, in < 30 seconds |

### 4.3 Persona: Sara — LLMOps / AI Infrastructure Engineer

| Attribute | Detail |
|---|---|
| **Role** | LLMOps Engineer at an AI-native startup (40 engineers) |
| **Experience** | 3 years; came from backend engineering; now runs the inference and evaluation pipelines |
| **Daily tools** | Kafka (Redpanda), LangChain, Claude API, Docker, Python, Prometheus |
| **Agent exposure** | Building customer-facing AI agents that need real-time context from event streams (user activity, product catalog updates, order events) |
| **Pain points** | Her agents are "blind" to real-time events; she wrote a Kafka-to-Redis bridge for one topic, but it doesn't scale to the 30+ topics her agents need; no schema awareness means agents hallucinate field names |
| **Goal with KafkaMCP** | Give her LangChain-based agents native, schema-aware access to all Kafka topics so they can provide real-time, context-rich responses to customers |
| **Success looks like** | Customer-facing agent receives a query about an order, fetches the latest order events from Kafka in real time via MCP, and responds with accurate, up-to-the-second information — no stale database reads |

### 4.4 Persona: Carlos — SRE / Incident Responder

| Attribute | Detail |
|---|---|
| **Role** | Senior SRE at an e-commerce platform (1,000+ engineers) |
| **Experience** | 7 years; on-call for Kafka clusters, Kubernetes, and the payment processing pipeline |
| **Daily tools** | Kafka, Kubernetes, Datadog, PagerDuty, Terraform, Go |
| **Agent exposure** | Using an internal incident response agent that triages alerts; wants it to have real-time visibility into Kafka consumer lag, dead-letter topics, and error event streams |
| **Pain points** | During incidents, he manually runs `kafka-console-consumer` and `kafka-consumer-groups` CLI commands to diagnose lag and message backlog; wants the incident agent to do this autonomously |
| **Goal with KafkaMCP** | Plug KafkaMCP into the incident response agent so it can autonomously inspect consumer group lag, sample error events from DLQ topics, and correlate event patterns during outages |
| **Success looks like** | Agent detects consumer lag spike via Datadog alert, uses KafkaMCP to inspect the lagging consumer group, samples recent messages from the dead-letter topic, identifies a schema incompatibility as root cause, and pages the owning team with a summary — all within 2 minutes of the alert |

---

## 5. User Stories & Use Cases

### 5.1 User Stories

| ID | As a... | I want to... | So that... | Priority |
|---|---|---|---|---|
| US-01 | AI agent | list all available Kafka topics in a cluster | I can discover what data streams exist without prior configuration | P0 |
| US-02 | AI agent | read the schema (Avro/Protobuf/JSON Schema) for a topic | I can understand message structure and generate correct deserializers | P0 |
| US-03 | AI agent | consume the latest N messages from a topic | I can inspect recent data for analysis, debugging, or context gathering | P0 |
| US-04 | AI agent | produce a message to a topic with schema validation | I can write events back to Kafka with guaranteed schema conformance | P0 |
| US-05 | AI agent | subscribe to a topic and receive messages in real time via streaming | I can react to events as they happen, not on a polling schedule | P1 |
| US-06 | AI agent | describe a consumer group (members, offsets, lag) | I can diagnose processing delays and consumer health issues | P0 |
| US-07 | AI agent | search/filter messages in a topic by key, header, or time range | I can find specific events without consuming the entire topic | P1 |
| US-08 | Data engineer | configure access policies per agent (which topics, read/write/admin) | I can enforce least-privilege access for different agents | P0 |
| US-09 | AI agent | create a new topic with specified partitions, replication, and retention | I can provision infrastructure as part of automated workflows | P1 |
| US-10 | AI agent | list and describe all schemas in the Schema Registry | I can understand the full data model of the organization's event streams | P0 |
| US-11 | AI agent | check schema compatibility before producing a message | I can avoid breaking downstream consumers with incompatible schema changes | P1 |
| US-12 | SRE agent | read from a dead-letter queue topic and categorize error types | I can automate incident triage for processing failures | P1 |
| US-13 | ML agent | consume a windowed aggregate (last 5 min of events) from a topic | I can compute real-time features for inference without a separate stream processor | P2 |
| US-14 | AI agent | reset consumer group offsets to a specific timestamp | I can replay events for reprocessing or recovery scenarios | P2 |
| US-15 | Platform team | view audit logs of all agent interactions with Kafka via KafkaMCP | I can track which agents accessed which topics and when for compliance | P1 |

### 5.2 Key Use Cases

#### UC-1: Autonomous Schema Drift Detection

```
Agent discovers topic "payments.completed" → reads current schema (v3) →
compares with expected schema stored in its context → detects new field
"fraud_score" (float) added → verifies backward compatibility →
notifies data team via Slack with diff summary → updates its own
deserialization logic to include the new field
```

#### UC-2: Real-Time Incident Triage

```
PagerDuty alert fires: "Consumer group order-processor lag > 100K" →
Incident agent receives alert → uses KafkaMCP to:
  1. Describe consumer group "order-processor" (lag, partition assignment)
  2. Sample 10 recent messages from DLQ topic "orders.dlq"
  3. Read schema for "orders.created" topic
  4. Identify pattern: all DLQ messages fail on field "shipping_address.zip"
     with "null not allowed" error
→ Agent posts root cause summary to incident channel:
  "Order processing failures caused by null zip codes in shipping_address.
   Introduced by producer change at 14:32 UTC. 127K messages affected."
```

#### UC-3: Feature Store Data Discovery

```
ML pipeline agent needs features for a new recommendation model →
uses KafkaMCP to list all topics matching "features.*" →
reads schemas for "features.user_activity", "features.product_views",
"features.purchase_history" → evaluates field types and freshness →
selects "features.user_activity" and "features.purchase_history" →
subscribes to both topics for real-time feature ingestion
```

#### UC-4: Automated Data Quality Monitoring

```
Data quality agent runs on a 5-minute schedule →
consumes last 5 minutes of messages from "events.page_views" →
validates each message against registered Avro schema →
detects 2.3% of messages have null "user_id" (policy requires non-null) →
publishes data quality alert event to "monitoring.data_quality" topic →
includes sample of violating messages and statistical summary
```

---

## 6. Functional Requirements

### 6.1 P0 — Must Have (v0.1 Launch)

#### FR-01: Topic Discovery

| Attribute | Detail |
|---|---|
| **MCP Tool** | `kafka_list_topics` |
| **Description** | List all topics in the connected Kafka cluster with metadata |
| **Input** | Optional filter pattern (glob or regex), include_internal flag |
| **Output** | Array of topic objects: name, partition count, replication factor, retention.ms, is_internal, message_count_estimate |
| **Constraints** | Must complete within 2 seconds for clusters with up to 10,000 topics |

#### FR-02: Topic Description

| Attribute | Detail |
|---|---|
| **MCP Tool** | `kafka_describe_topic` |
| **Description** | Get detailed metadata for a specific topic |
| **Input** | topic_name (required) |
| **Output** | Topic config (all Kafka topic-level configs), partition details (leader, replicas, ISR per partition), latest offsets per partition, estimated message count, associated schema subjects |
| **Constraints** | Must include partition-level detail for operational diagnosis |

#### FR-03: Consume Messages

| Attribute | Detail |
|---|---|
| **MCP Tool** | `kafka_consume` |
| **Description** | Read messages from a topic with flexible positioning |
| **Input** | topic_name, partition (optional, default all), offset ("earliest", "latest", specific offset, or ISO 8601 timestamp), limit (max messages, default 10, max 1000), deserializer ("auto", "avro", "protobuf", "json", "string", "bytes") |
| **Output** | Array of message objects: key (deserialized), value (deserialized), partition, offset, timestamp, headers (as key-value pairs) |
| **Constraints** | "auto" deserializer must detect format via Schema Registry or content sniffing; raw bytes returned as base64 when deserialization fails |

#### FR-04: Produce Messages

| Attribute | Detail |
|---|---|
| **MCP Tool** | `kafka_produce` |
| **Description** | Publish one or more messages to a topic |
| **Input** | topic_name, messages (array of {key, value, headers, partition}), serializer ("auto", "avro", "protobuf", "json", "string"), schema_id (optional, for explicit schema binding) |
| **Output** | Array of delivery confirmations: partition, offset, timestamp per message |
| **Constraints** | Must validate message against Schema Registry schema when serializer is "avro" or "protobuf"; reject and return validation errors on schema mismatch; support idempotent producer semantics |

#### FR-05: Schema Discovery

| Attribute | Detail |
|---|---|
| **MCP Tool** | `kafka_list_schemas` |
| **Description** | List all subjects in the connected Schema Registry |
| **Input** | Optional subject_filter (glob), include_deleted flag |
| **Output** | Array of schema subject objects: subject_name, latest_version, schema_type (AVRO, PROTOBUF, JSON), compatibility_level |

#### FR-06: Schema Retrieval

| Attribute | Detail |
|---|---|
| **MCP Tool** | `kafka_get_schema` |
| **Description** | Get the full schema definition for a subject at a specific version |
| **Input** | subject_name, version (optional, default "latest") |
| **Output** | Schema definition (parsed), schema_type, version, schema_id, compatibility_level, references (for schemas with imports), human-readable field summary |
| **Constraints** | Must return a human/agent-readable field summary alongside the raw schema definition |

#### FR-07: Consumer Group Inspection

| Attribute | Detail |
|---|---|
| **MCP Tool** | `kafka_describe_consumer_group` |
| **Description** | Get the state, members, and lag of a consumer group |
| **Input** | group_id (required) |
| **Output** | Group state (Stable, Rebalancing, Dead, Empty), protocol_type, members (client_id, host, assignment), per-partition current offset, log-end offset, and lag |
| **Constraints** | Lag calculation must be real-time (current offset vs. log-end offset at query time) |

#### FR-08: Consumer Group Listing

| Attribute | Detail |
|---|---|
| **MCP Tool** | `kafka_list_consumer_groups` |
| **Description** | List all consumer groups with summary state |
| **Input** | Optional state_filter (Stable, Empty, Dead, etc.), topic_filter |
| **Output** | Array of group summaries: group_id, state, member_count, topics_subscribed, total_lag |

#### FR-09: Cluster Metadata

| Attribute | Detail |
|---|---|
| **MCP Resource** | `kafka://cluster/metadata` |
| **Description** | Expose cluster-level information as an MCP resource |
| **Output** | Broker count, broker list (id, host, port, rack), controller broker, cluster_id, Kafka version, KRaft or ZooKeeper mode, total topic count, total partition count |

#### FR-10: Agent Access Control Configuration

| Attribute | Detail |
|---|---|
| **Configuration** | YAML/TOML policy file |
| **Description** | Define per-agent or per-role access policies |
| **Capabilities** | Topic-level read/write/admin permissions; schema registry read/write; consumer group describe/reset; glob patterns for topic matching; default-deny semantics |

### 6.2 P1 — Should Have (v0.2–v0.3)

| ID | Feature | Description |
|---|---|---|
| FR-11 | **Streaming Subscription** | MCP streaming tool (`kafka_subscribe`) that returns messages in real time via SSE/streaming response; supports consumer group semantics, auto-commit or manual ack |
| FR-12 | **Message Search** | `kafka_search` tool: filter messages by key pattern, header match, time range, or JSONPath expression within message values; bounded scan with configurable max-scan-messages |
| FR-13 | **Topic Management** | `kafka_create_topic`, `kafka_alter_topic`, `kafka_delete_topic` tools for agent-driven infrastructure provisioning |
| FR-14 | **Schema Compatibility Check** | `kafka_check_schema_compatibility` tool: validate a proposed schema against the subject's compatibility policy before registering |
| FR-15 | **Consumer Group Offset Reset** | `kafka_reset_offsets` tool: reset consumer group offsets to earliest, latest, timestamp, or specific offset — with dry-run mode |
| FR-16 | **Audit Log MCP Resource** | `kafka://audit/log` resource exposing all agent interactions with KafkaMCP (who, what, when, result) |
| FR-17 | **Multi-Cluster Support** | Configuration to connect to multiple Kafka clusters; tools accept optional `cluster` parameter; cluster discovery via MCP resource |
| FR-18 | **Dead-Letter Queue Analysis** | `kafka_analyze_dlq` tool: consume from DLQ topics, parse error headers, categorize failure types, return statistical summary |

### 6.3 P2 — Nice to Have (v0.4+)

| ID | Feature | Description |
|---|---|---|
| FR-19 | **Windowed Aggregation** | `kafka_aggregate` tool: compute count, sum, avg, min, max over a time window on a numeric field — lightweight stream processing without Flink/ksqlDB |
| FR-20 | **Topic Topology Mapping** | `kafka_topic_graph` resource: visualize producer → topic → consumer relationships as a dependency graph |
| FR-21 | **Schema Evolution Advisor** | `kafka_suggest_schema_evolution` tool: given a current schema and a desired change, suggest compatible evolution strategies |
| FR-22 | **Kafka Connect Integration** | `kafka_list_connectors`, `kafka_describe_connector` tools for discovering and inspecting Kafka Connect deployments |
| FR-23 | **Cross-Topic Join** | `kafka_join` tool: time-windowed join across two topics on a key field — agent-accessible stream join without deploying a stream processor |
| FR-24 | **Natural Language Query** | `kafka_query` tool: accept a natural language description of desired data (e.g., "last 10 failed payments") and translate to topic + filter + consume call |

---

## 7. Non-Functional Requirements

### 7.1 Performance

| Metric | Requirement | Rationale |
|---|---|---|
| Topic list latency | < 2s for 10,000 topics | Agents need fast discovery for interactive loops |
| Single message produce latency (p99) | < 100ms (ack=1), < 500ms (ack=all) | Agent tool calls should feel interactive |
| Consume N messages latency (p95) | < 500ms for N ≤ 100 | Sampling use cases need sub-second response |
| Schema retrieval latency (p95) | < 200ms | Schema is read on nearly every produce/consume call |
| Concurrent agent connections | 100+ simultaneous agents | Multi-agent deployments at enterprise scale |
| Message throughput (produce) | 10,000 msgs/sec sustained | Support high-frequency automation agents |
| Message throughput (consume) | 50,000 msgs/sec sustained | Support bulk data access patterns |

### 7.2 Scalability

- **Horizontal scaling:** Multiple KafkaMCP instances behind a load balancer; stateless design (all state lives in Kafka + Schema Registry).
- **Connection pooling:** Internal Kafka client connection pool; configurable pool size per cluster.
- **Topic partition awareness:** Consume operations distribute across partitions automatically; no single-partition bottleneck.
- **Multi-cluster:** Support up to 10 Kafka clusters per KafkaMCP deployment; configurable per-cluster resource limits.

### 7.3 Reliability

| Requirement | Detail |
|---|---|
| Availability target | 99.9% uptime (< 8.76 hours/year downtime) |
| Crash recovery | Stateless process; restart and reconnect within 5 seconds |
| Kafka client resilience | Automatic reconnection with exponential backoff on broker failures; metadata refresh on leader changes |
| Produce guarantees | Idempotent producer enabled by default; optional transactional semantics |
| Consume guarantees | At-least-once for streaming subscriptions; exactly-once via consumer group commit semantics |
| Schema Registry failover | Cache schemas locally with configurable TTL; serve cached schemas when registry is unavailable |

### 7.4 Security

| Requirement | Detail |
|---|---|
| Transport encryption | TLS 1.3 for all Kafka broker connections; TLS for MCP transport (stdio is local, SSE/HTTP requires TLS) |
| Authentication to Kafka | SASL/PLAIN, SASL/SCRAM-SHA-256/512, SASL/OAUTHBEARER, mTLS |
| Agent authentication | API key per agent, or mTLS client certificates for MCP connections |
| Authorization | Policy-based access control (see FR-10); default-deny; topic-level granularity |
| Secrets management | Integration with environment variables, HashiCorp Vault, AWS Secrets Manager, Kubernetes secrets |
| Data masking | P1: configurable field-level masking for sensitive data (PII, payment info) before messages are returned to agents |

---

## 8. Technical Architecture

### 8.1 High-Level System Design

```
┌──────────────────────────────────────────────────────────────────────┐
│                        KAFKAMCP ARCHITECTURE                        │
│                                                                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                  │
│  │ AI Agent A  │  │ AI Agent B  │  │ AI Agent C  │   MCP Clients    │
│  │ (Claude)    │  │ (LangChain) │  │ (AutoGen)   │                  │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                  │
│         │                │                │                          │
│         ▼                ▼                ▼                          │
│  ┌──────────────────────────────────────────────────────┐            │
│  │              MCP Transport Layer                     │            │
│  │  ┌──────────┐  ┌──────────────┐  ┌───────────────┐  │            │
│  │  │  stdio   │  │  SSE/HTTP    │  │  Streamable   │  │            │
│  │  │  (local) │  │  (remote)    │  │  HTTP (future)│  │            │
│  │  └──────────┘  └──────────────┘  └───────────────┘  │            │
│  └──────────────────────┬───────────────────────────────┘            │
│                         │                                            │
│  ┌──────────────────────▼───────────────────────────────┐            │
│  │              KafkaMCP Server Core                     │            │
│  │                                                       │            │
│  │  ┌─────────────────┐  ┌──────────────────────────┐   │            │
│  │  │  Tool Registry  │  │  Resource Registry       │   │            │
│  │  │                 │  │                          │   │            │
│  │  │  kafka_consume  │  │  kafka://cluster/meta    │   │            │
│  │  │  kafka_produce  │  │  kafka://topics/{name}   │   │            │
│  │  │  kafka_list_*   │  │  kafka://schemas/{subj}  │   │            │
│  │  │  kafka_describe*│  │  kafka://groups/{id}     │   │            │
│  │  │  kafka_search   │  │  kafka://audit/log       │   │            │
│  │  └────────┬────────┘  └────────────┬─────────────┘   │            │
│  │           │                        │                  │            │
│  │  ┌────────▼────────────────────────▼─────────────┐   │            │
│  │  │           Request Handler Layer                │   │            │
│  │  │                                                │   │            │
│  │  │  ┌──────────┐ ┌──────────┐ ┌───────────────┐  │   │            │
│  │  │  │ AuthZ    │ │ Rate     │ │ Audit Logger  │  │   │            │
│  │  │  │ Engine   │ │ Limiter  │ │               │  │   │            │
│  │  │  └──────────┘ └──────────┘ └───────────────┘  │   │            │
│  │  └────────────────────┬───────────────────────────┘   │            │
│  │                       │                               │            │
│  │  ┌────────────────────▼───────────────────────────┐   │            │
│  │  │         Kafka Client Abstraction Layer          │   │            │
│  │  │                                                 │   │            │
│  │  │  ┌──────────┐  ┌──────────┐  ┌──────────────┐ │   │            │
│  │  │  │ Producer │  │ Consumer │  │ Admin Client  │ │   │            │
│  │  │  │ Pool     │  │ Pool     │  │              │ │   │            │
│  │  │  └──────────┘  └──────────┘  └──────────────┘ │   │            │
│  │  │                                                 │   │            │
│  │  │  ┌──────────────────┐  ┌─────────────────────┐ │   │            │
│  │  │  │ Schema Registry  │  │ Connection Manager  │ │   │            │
│  │  │  │ Client + Cache   │  │ (Multi-Cluster)     │ │   │            │
│  │  │  └──────────────────┘  └─────────────────────┘ │   │            │
│  │  └─────────────────────────────────────────────────┘   │            │
│  └────────────────────────────────────────────────────────┘            │
│                         │                                              │
│         ┌───────────────┼───────────────────────┐                     │
│         ▼               ▼                       ▼                     │
│  ┌──────────┐   ┌──────────────┐   ┌─────────────────┐               │
│  │  Kafka   │   │   Schema     │   │  Kafka Cluster  │               │
│  │  Broker  │   │   Registry   │   │  (KRaft/ZK)     │               │
│  │  Cluster │   │  (Confluent) │   │  Admin APIs     │               │
│  └──────────┘   └──────────────┘   └─────────────────┘               │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
```

### 8.2 Component Breakdown

| Component | Responsibility | Implementation Notes |
|---|---|---|
| **MCP Transport Layer** | Accept MCP connections via stdio (local), SSE/HTTP (remote), and Streamable HTTP (future) | Use the official MCP Go SDK or a well-maintained Go MCP implementation; stdio for CLI/desktop agents; `net/http` or `chi`/`echo` for remote transports; single-binary delivery simplifies sidecar installs |
| **Tool Registry** | Register and dispatch MCP tool calls to handler functions | Each tool is a Go handler backed by shared service interfaces; tool metadata is declared in Go structs; `cobra` can expose debug/admin commands that reuse the same service layer |
| **Resource Registry** | Expose Kafka metadata as browsable MCP resources | Resources support `list` and `read` operations; URIs follow `kafka://` scheme; handlers share the same internal Go packages as tools |
| **AuthZ Engine** | Evaluate per-request authorization against the policy file | Load policies at startup; hot-reload on file change; deny by default; concurrency-safe caches guarded with `sync.RWMutex` |
| **Rate Limiter** | Per-agent and global rate limiting | Token bucket per agent ID; configurable limits in policy file; implemented with goroutines, channels, and ticker-based refill loops |
| **Audit Logger** | Structured logging of every agent interaction | JSON-structured logs via `slog` or `zap`; includes agent_id, tool, parameters, result, latency, timestamp |
| **Kafka Client Abstraction** | Pooled, reusable Kafka producer/consumer/admin clients | `confluent-kafka-go` producer/consumer/admin clients backed by librdkafka; goroutines manage delivery reports, background polling, and reconnection |
| **Schema Registry Client** | Fetch and cache schemas; validate messages; resolve references | HTTP client built with Go primitives; uses `protobuf`/`encoding/json` plus schema-specific libraries; local LRU cache with TTL; supports Confluent Schema Registry API v1 |
| **Connection Manager** | Multi-cluster connection lifecycle management | Named cluster configurations; health checks; circuit breaker on repeated failures; packaged as a single deployable Go service binary per environment |

### 8.3 Technology Stack

| Layer | Technology | Rationale |
|---|---|---|
| **Language** | Go (Golang) | Primary implementation language from v0.1; strong Kafka client maturity, predictable performance, and operational simplicity through single-binary distribution |
| **Alternative (v1.0+)** | Rust (optional acceleration path) | Consider only for narrowly scoped performance-critical components if profiling shows Go is insufficient |
| **Kafka Client** | `confluent-kafka-go` | Battle-tested librdkafka wrapper with mature producer, consumer, and admin support |
| **MCP SDK** | Official MCP Go SDK or a maintained Go MCP implementation | Go-native server/tool definitions; supports stdio and HTTP-based MCP transports without shipping an additional language runtime |
| **HTTP Server** | Go `net/http` or `chi` / `echo` | Minimal dependency footprint for SSE/HTTP endpoints and middleware composition |
| **CLI Tooling** | `cobra` | Standard Go CLI ergonomics for local stdio mode, config validation, and admin/debug commands |
| **Concurrency Model** | Goroutines + channels | Efficient long-lived consumers, delivery-report fan-in, rate limiting, and graceful shutdown coordination |
| **Schema Registry** | HTTP client to Confluent Schema Registry REST API | Standard API; compatible with Confluent, Karapace, Apicurio |
| **Serialization** | `protobuf` / `encoding/json` | Native schema-aware encoding/decoding for Protobuf and JSON payloads; pair with Avro libraries as needed |
| **Configuration** | YAML via `gopkg.in/yaml.v3` | Human-readable and maps cleanly to tagged Go structs |
| **Logging** | `slog` or `zap` | Structured JSON logging with low overhead and strong ecosystem support |
| **Testing** | `go test` + `testcontainers-go` | Standard Go test runner plus integration tests with real Kafka and Schema Registry in Docker |
| **Packaging** | `go build` + Docker image + GoReleaser | Single-binary artifacts, no `node_modules`, easy cross-platform release automation |
| **Project Layout** | `cmd/`, `internal/`, `pkg/` | Idiomatic Go repository organization with clear separation between entrypoints, internal services, and reusable libraries |

### 8.4 Architecture Decision Records

| Decision | Choice | Alternatives Considered | Rationale |
|---|---|---|---|
| **Language for v0.1** | Go | Rust, Python, JVM sidecar | `confluent-kafka-go` provides mature Kafka support from day one, Go enables single-binary distribution, and the operational footprint is simpler than shipping a separate managed runtime plus dependency tree |
| **Kafka client library** | `confluent-kafka-go` | `franz-go`, `segmentio/kafka-go`, Java sidecar | Best balance of Kafka feature completeness, delivery semantics, and production maturity via librdkafka |
| **MCP transport** | stdio + SSE | Streamable HTTP | stdio is universally supported (Claude Desktop, VS Code); SSE covers remote agents; Streamable HTTP is not yet widely adopted |
| **Schema caching** | In-memory LRU with TTL | Redis, local SQLite | Schemas are small (< 100KB) and few (< 10K); in-memory is simplest and fastest; no external dependency |
| **Stateless design** | No persistent state in KafkaMCP | Embedded DB for offsets | All state lives in Kafka (offsets, topics) and Schema Registry; makes horizontal scaling trivial |
| **Default consumer group** | Ephemeral per-request | Persistent per-agent | Ephemeral groups avoid offset management complexity for ad-hoc reads; persistent groups via explicit `kafka_subscribe` |

---

## 9. API Design

### 9.1 MCP Tool Interface

All KafkaMCP tools follow the MCP tool specification. The wire format remains JSON, but the reference implementation is Go-first.

**Illustrative Go server bootstrap:**
```go
package main

import (
    "context"
    "log/slog"
    "net/http"
    "os"

    "github.com/go-chi/chi/v5"
)

type App struct {
    Config Config
    Logger *slog.Logger
    Kafka  *KafkaService
}

func main() {
    ctx := context.Background()
    cfg := mustLoadConfig("kafkamcp.yaml")
    logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
    app := NewApp(ctx, cfg, logger) // wraps the official MCP Go SDK transport/server primitives

    switch cfg.Server.Transport {
    case "stdio":
        must(app.ServeStdio(ctx, os.Stdin, os.Stdout))
    case "sse", "streamable-http":
        router := chi.NewRouter()
        router.Mount("/mcp", app.HTTPHandler())
        must(http.ListenAndServe(cfg.Server.Address(), router))
    default:
        logger.Error("unsupported transport", "transport", cfg.Server.Transport)
        os.Exit(1)
    }
}
```

**Illustrative Go tool registration and handler:**
```go
type ListTopicsInput struct {
    Filter          string `json:"filter,omitempty"`
    IncludeInternal bool   `json:"include_internal,omitempty"`
    Cluster         string `json:"cluster,omitempty"`
}

type ListTopicsOutput struct {
    Topics     []TopicSummary `json:"topics"`
    TotalCount int            `json:"total_count"`
    Cluster    string         `json:"cluster"`
}

func (a *App) RegisterTools() {
    a.MustRegisterTool("kafka_list_topics", "List all Kafka topics in the cluster.", a.ListTopics)
}

func (a *App) ListTopics(ctx context.Context, input ListTopicsInput) (ListTopicsOutput, error) {
    topics, err := a.Kafka.ListTopics(ctx, input.Cluster, input.Filter, input.IncludeInternal)
    if err != nil {
        return ListTopicsOutput{}, err
    }

    return ListTopicsOutput{
        Topics:     topics,
        TotalCount: len(topics),
        Cluster:    input.Cluster,
    }, nil
}
```

**Illustrative Go configuration model:**
```go
type Config struct {
    Server      ServerConfig    `yaml:"server" json:"server"`
    Clusters    []ClusterConfig `yaml:"clusters" json:"clusters"`
    Policies    PolicyConfig    `yaml:"policies" json:"policies"`
    SchemaCache CacheConfig     `yaml:"schema_cache" json:"schema_cache"`
    Audit       AuditConfig     `yaml:"audit" json:"audit"`
}

type ServerConfig struct {
    Transport string `yaml:"transport" json:"transport"`
    Host      string `yaml:"host" json:"host"`
    Port      int    `yaml:"port" json:"port"`
    LogLevel  string `yaml:"log_level" json:"log_level"`
}

type ClusterConfig struct {
    Name             string               `yaml:"name" json:"name"`
    BootstrapServers string               `yaml:"bootstrap_servers" json:"bootstrap_servers"`
    SecurityProtocol string               `yaml:"security_protocol" json:"security_protocol"`
    SASLMechanism    string               `yaml:"sasl_mechanism,omitempty" json:"sasl_mechanism,omitempty"`
    SASLUsername     string               `yaml:"sasl_username,omitempty" json:"sasl_username,omitempty"`
    SASLPassword     string               `yaml:"sasl_password,omitempty" json:"sasl_password,omitempty"`
    SSLCALocation    string               `yaml:"ssl_ca_location,omitempty" json:"ssl_ca_location,omitempty"`
    SchemaRegistry   SchemaRegistryConfig `yaml:"schema_registry,omitempty" json:"schema_registry,omitempty"`
    Default          bool                 `yaml:"default,omitempty" json:"default,omitempty"`
}

type SchemaRegistryConfig struct {
    URL  string     `yaml:"url" json:"url"`
    Auth BasicAuth  `yaml:"auth,omitempty" json:"auth,omitempty"`
}

type BasicAuth struct {
    Username string `yaml:"username" json:"username"`
    Password string `yaml:"password" json:"password"`
}

type CacheConfig struct {
    MaxEntries int `yaml:"max_entries" json:"max_entries"`
    TTLSeconds int `yaml:"ttl_seconds" json:"ttl_seconds"`
}

type PolicyConfig struct {
    DefaultDeny bool `yaml:"default_deny" json:"default_deny"`
}

type AuditConfig struct {
    Enabled    bool   `yaml:"enabled" json:"enabled"`
    MaxEntries int    `yaml:"max_entries" json:"max_entries"`
    LogFile    string `yaml:"log_file" json:"log_file"`
}
```

Below are the key tools with request/response examples.

#### 9.1.1 `kafka_list_topics`

**Description:** List all Kafka topics in the cluster.

**Input Schema:**
```json
{
  "type": "object",
  "properties": {
    "filter": {
      "type": "string",
      "description": "Glob pattern to filter topic names (e.g., 'orders.*', '*-dlq')"
    },
    "include_internal": {
      "type": "boolean",
      "default": false,
      "description": "Include internal topics (e.g., __consumer_offsets)"
    },
    "cluster": {
      "type": "string",
      "description": "Cluster name (for multi-cluster setups). Default: default cluster."
    }
  }
}
```

**Example Response:**
```json
{
  "topics": [
    {
      "name": "orders.created",
      "partitions": 12,
      "replication_factor": 3,
      "retention_ms": 604800000,
      "is_internal": false,
      "message_count_estimate": 14523891,
      "schema_subject": "orders.created-value"
    },
    {
      "name": "payments.completed",
      "partitions": 6,
      "replication_factor": 3,
      "retention_ms": 2592000000,
      "is_internal": false,
      "message_count_estimate": 8291044,
      "schema_subject": "payments.completed-value"
    }
  ],
  "total_count": 2,
  "cluster": "production"
}
```

#### 9.1.2 `kafka_consume`

**Description:** Read messages from a topic.

**Input Schema:**
```json
{
  "type": "object",
  "properties": {
    "topic": { "type": "string", "description": "Topic name" },
    "partition": { "type": "integer", "description": "Specific partition (optional, default: all)" },
    "offset": {
      "type": "string",
      "description": "Start position: 'earliest', 'latest', a numeric offset, or ISO 8601 timestamp",
      "default": "latest"
    },
    "limit": {
      "type": "integer",
      "default": 10,
      "minimum": 1,
      "maximum": 1000,
      "description": "Maximum messages to return"
    },
    "deserializer": {
      "type": "string",
      "enum": ["auto", "avro", "protobuf", "json", "string", "bytes"],
      "default": "auto",
      "description": "Deserialization format"
    },
    "timeout_ms": {
      "type": "integer",
      "default": 5000,
      "description": "Maximum wait time for messages"
    }
  },
  "required": ["topic"]
}
```

**Example Response:**
```json
{
  "messages": [
    {
      "partition": 3,
      "offset": 892341,
      "timestamp": "2025-07-15T14:32:01.234Z",
      "key": "order-7891234",
      "value": {
        "order_id": "order-7891234",
        "customer_id": "cust-456",
        "total_amount": 129.99,
        "currency": "USD",
        "items": [
          { "sku": "WIDGET-A", "quantity": 2, "price": 49.99 },
          { "sku": "GADGET-B", "quantity": 1, "price": 30.01 }
        ],
        "created_at": "2025-07-15T14:32:01.000Z"
      },
      "headers": {
        "correlation_id": "req-abc123",
        "source_service": "checkout-api"
      },
      "schema_id": 42,
      "schema_version": 3
    }
  ],
  "count": 1,
  "topic": "orders.created",
  "deserialization": "avro"
}
```

#### 9.1.3 `kafka_produce`

**Description:** Publish messages to a topic.

**Input Schema:**
```json
{
  "type": "object",
  "properties": {
    "topic": { "type": "string" },
    "messages": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "key": { "type": "string" },
          "value": { "description": "Message value (object for structured, string for raw)" },
          "headers": { "type": "object", "additionalProperties": { "type": "string" } },
          "partition": { "type": "integer" }
        },
        "required": ["value"]
      },
      "minItems": 1,
      "maxItems": 100
    },
    "serializer": {
      "type": "string",
      "enum": ["auto", "avro", "protobuf", "json", "string"],
      "default": "auto"
    },
    "schema_id": {
      "type": "integer",
      "description": "Explicit Schema Registry schema ID for serialization"
    },
    "acks": {
      "type": "string",
      "enum": ["0", "1", "all"],
      "default": "all"
    }
  },
  "required": ["topic", "messages"]
}
```

**Example Response:**
```json
{
  "deliveries": [
    {
      "partition": 7,
      "offset": 1293847,
      "timestamp": "2025-07-15T14:35:22.567Z",
      "status": "delivered"
    }
  ],
  "count": 1,
  "topic": "orders.created",
  "serialization": "avro",
  "schema_id": 42
}
```

#### 9.1.4 `kafka_get_schema`

**Description:** Retrieve a schema from the Schema Registry.

**Input Schema:**
```json
{
  "type": "object",
  "properties": {
    "subject": { "type": "string", "description": "Schema subject name (e.g., 'orders.created-value')" },
    "version": {
      "type": "string",
      "default": "latest",
      "description": "'latest', a specific version number, or 'all' for version history"
    }
  },
  "required": ["subject"]
}
```

**Example Response:**
```json
{
  "subject": "orders.created-value",
  "version": 3,
  "schema_id": 42,
  "schema_type": "AVRO",
  "compatibility_level": "BACKWARD",
  "schema": {
    "type": "record",
    "name": "OrderCreated",
    "namespace": "com.example.orders",
    "fields": [
      { "name": "order_id", "type": "string" },
      { "name": "customer_id", "type": "string" },
      { "name": "total_amount", "type": "double" },
      { "name": "currency", "type": "string", "default": "USD" },
      { "name": "items", "type": { "type": "array", "items": "OrderItem" } },
      { "name": "created_at", "type": { "type": "long", "logicalType": "timestamp-millis" } }
    ]
  },
  "field_summary": "OrderCreated { order_id: string, customer_id: string, total_amount: double, currency: string (default: USD), items: array<OrderItem>, created_at: timestamp-millis }",
  "references": []
}
```

#### 9.1.5 `kafka_describe_consumer_group`

**Input Schema:**
```json
{
  "type": "object",
  "properties": {
    "group_id": { "type": "string", "description": "Consumer group ID" }
  },
  "required": ["group_id"]
}
```

**Example Response:**
```json
{
  "group_id": "order-processor",
  "state": "Stable",
  "protocol_type": "consumer",
  "protocol": "range",
  "coordinator": { "id": 2, "host": "broker-2.kafka.internal", "port": 9092 },
  "members": [
    {
      "member_id": "order-processor-1-abc123",
      "client_id": "order-processor-1",
      "host": "/10.0.1.45",
      "assignment": [
        { "topic": "orders.created", "partitions": [0, 1, 2, 3] }
      ]
    },
    {
      "member_id": "order-processor-2-def456",
      "client_id": "order-processor-2",
      "host": "/10.0.1.46",
      "assignment": [
        { "topic": "orders.created", "partitions": [4, 5, 6, 7] }
      ]
    }
  ],
  "offsets": [
    { "topic": "orders.created", "partition": 0, "current_offset": 512340, "log_end_offset": 512345, "lag": 5 },
    { "topic": "orders.created", "partition": 1, "current_offset": 498210, "log_end_offset": 498210, "lag": 0 },
    { "topic": "orders.created", "partition": 2, "current_offset": 503100, "log_end_offset": 503890, "lag": 790 },
    { "topic": "orders.created", "partition": 3, "current_offset": 487600, "log_end_offset": 487600, "lag": 0 }
  ],
  "total_lag": 795
}
```

### 9.2 MCP Resources

| Resource URI | Description | Content |
|---|---|---|
| `kafka://cluster/metadata` | Cluster overview | Broker list, controller, version, topic/partition counts |
| `kafka://topics` | Topic catalog | List of all topics with partition counts and retention |
| `kafka://topics/{name}` | Individual topic detail | Full config, partition map, associated schemas |
| `kafka://schemas` | Schema catalog | All Schema Registry subjects with latest versions |
| `kafka://schemas/{subject}` | Individual schema | Full schema definition with field summary |
| `kafka://groups` | Consumer group catalog | All groups with state and total lag |
| `kafka://groups/{id}` | Individual group detail | Members, offsets, lag per partition |
| `kafka://audit/log` | Agent audit trail | Recent agent interactions (last 1000 entries) |

---

## 10. Data Model

### 10.1 Core Entities

```
┌─────────────────────────────────────────────────────────────────────┐
│                     KAFKAMCP DATA MODEL                             │
│                                                                     │
│  ┌──────────┐       ┌──────────────┐       ┌────────────────┐      │
│  │ Cluster  │1────*│    Topic      │1────*│   Partition     │      │
│  │          │       │              │       │                │      │
│  │ id       │       │ name         │       │ id             │      │
│  │ name     │       │ cluster_id   │       │ topic_name     │      │
│  │ brokers  │       │ partitions   │       │ leader_broker  │      │
│  │ version  │       │ repl_factor  │       │ replicas       │      │
│  │ mode     │       │ retention_ms │       │ isr            │      │
│  └──────────┘       │ configs{}    │       │ earliest_offset│      │
│                     └──────┬───────┘       │ latest_offset  │      │
│                            │               └────────────────┘      │
│                            │                                        │
│                     ┌──────▼───────┐                                │
│                     │ SchemaSubject │                                │
│                     │              │                                │
│                     │ subject_name │       ┌────────────────┐      │
│                     │ topic_name   │1────*│ SchemaVersion   │      │
│                     │ key_or_value │       │                │      │
│                     │ compat_level │       │ version        │      │
│                     └──────────────┘       │ schema_id      │      │
│                                            │ schema_type    │      │
│                                            │ definition{}   │      │
│  ┌──────────────┐       ┌──────────────┐   └────────────────┘      │
│  │ConsumerGroup │1────*│ GroupMember   │                            │
│  │              │       │              │                            │
│  │ group_id     │       │ member_id    │                            │
│  │ state        │       │ client_id    │                            │
│  │ protocol     │       │ host         │                            │
│  │ coordinator  │       │ assignments[]│                            │
│  └──────┬───────┘       └──────────────┘                            │
│         │                                                           │
│         │1────*┌──────────────┐                                     │
│               │ GroupOffset   │                                     │
│               │              │                                     │
│               │ topic        │                                     │
│               │ partition    │                                     │
│               │ current_off  │                                     │
│               │ log_end_off  │                                     │
│               │ lag          │                                     │
│               └──────────────┘                                     │
│                                                                     │
│  ┌──────────────┐                                                   │
│  │  AgentPolicy │       ┌──────────────┐                            │
│  │              │1────*│ TopicPolicy   │                            │
│  │ agent_id     │       │              │                            │
│  │ role         │       │ topic_pattern│                            │
│  │ clusters[]   │       │ read         │                            │
│  │ rate_limit   │       │ write        │                            │
│  └──────────────┘       │ admin        │                            │
│                         └──────────────┘                            │
│                                                                     │
│  ┌──────────────┐                                                   │
│  │  AuditEntry  │                                                   │
│  │              │                                                   │
│  │ timestamp    │                                                   │
│  │ agent_id     │                                                   │
│  │ tool_name    │                                                   │
│  │ parameters{} │                                                   │
│  │ result_status│                                                   │
│  │ latency_ms   │                                                   │
│  │ error_msg    │                                                   │
│  └──────────────┘                                                   │
└─────────────────────────────────────────────────────────────────────┘
```

### 10.2 Key Schemas

#### KafkaMCP Configuration Schema (`kafkamcp.yaml`)

```yaml
# KafkaMCP Server Configuration
server:
  transport: stdio          # stdio | sse | streamable-http
  host: 0.0.0.0             # For SSE/HTTP transports
  port: 3100                # For SSE/HTTP transports
  log_level: info           # debug | info | warn | error

clusters:
  - name: production
    bootstrap_servers: "broker-1:9092,broker-2:9092,broker-3:9092"
    security_protocol: SASL_SSL
    sasl_mechanism: SCRAM-SHA-512
    sasl_username: "${KAFKA_USERNAME}"
    sasl_password: "${KAFKA_PASSWORD}"
    ssl_ca_location: /etc/kafka/ca.pem
    schema_registry:
      url: "https://schema-registry.internal:8081"
      auth:
        username: "${SR_USERNAME}"
        password: "${SR_PASSWORD}"
    default: true

  - name: staging
    bootstrap_servers: "staging-broker-1:9092"
    security_protocol: PLAINTEXT

policies:
  default_deny: true
  agents:
    - id: "incident-agent"
      role: reader
      topics:
        - pattern: "*.dlq"
          permissions: [read]
        - pattern: "orders.*"
          permissions: [read]
        - pattern: "monitoring.*"
          permissions: [read, write]
      consumer_groups:
        permissions: [describe]
      schemas:
        permissions: [read]
      rate_limit:
        requests_per_minute: 120

    - id: "pipeline-agent"
      role: admin
      topics:
        - pattern: "*"
          permissions: [read, write, create, delete]
      consumer_groups:
        permissions: [describe, reset]
      schemas:
        permissions: [read, write]
      rate_limit:
        requests_per_minute: 600

schema_cache:
  max_entries: 10000
  ttl_seconds: 300

audit:
  enabled: true
  max_entries: 10000       # In-memory ring buffer size
  log_file: /var/log/kafkamcp/audit.jsonl
```

#### Audit Entry Schema

```json
{
  "timestamp": "2025-07-15T14:32:01.234Z",
  "agent_id": "incident-agent",
  "tool": "kafka_consume",
  "parameters": {
    "topic": "orders.dlq",
    "limit": 10,
    "offset": "latest"
  },
  "cluster": "production",
  "result": {
    "status": "success",
    "message_count": 10
  },
  "latency_ms": 127,
  "policy_matched": "reader:*.dlq:read"
}
```

---

## 11. Security Considerations

### 11.1 Threat Model

| Threat | Description | Severity | Mitigation |
|---|---|---|---|
| **Unauthorized topic access** | A compromised or misconfigured agent reads sensitive topics (PII, financial data) | Critical | Default-deny policy engine; topic-level ACLs per agent; audit logging |
| **Prompt injection via messages** | Malicious Kafka messages contain instructions that manipulate the consuming agent | High | KafkaMCP does not interpret message content; passes raw deserialized data; agent frameworks must sanitize |
| **Credential exposure** | Kafka credentials (SASL, mTLS certs) leaked via agent responses or logs | Critical | Credentials loaded from env vars / secrets manager; never included in MCP responses; redacted from logs |
| **Denial of service** | Agent floods KafkaMCP with high-volume consume/produce requests | High | Per-agent rate limiting; global rate limiting; max message limits on consume/produce |
| **Data exfiltration** | Agent consumes large volumes of data and forwards to unauthorized destinations | High | Rate limiting on consume; max message count per request (1,000); audit logging for anomaly detection |
| **Schema Registry poisoning** | Agent registers malicious or incompatible schemas | Medium | Schema write permission is opt-in; compatibility checks enforced by Schema Registry; schema registration audit logged |
| **Man-in-the-middle** | Unencrypted MCP transport exposes Kafka data in transit | High | TLS required for SSE/HTTP transport; stdio is local-only by design |
| **Consumer group hijacking** | Agent joins an existing consumer group and steals messages from legitimate consumers | Medium | KafkaMCP uses ephemeral consumer groups by default; persistent group subscription requires explicit opt-in and policy approval |

### 11.2 Authentication Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   AUTHENTICATION FLOW                       │
│                                                             │
│  Agent ──[MCP Connection]──▶ KafkaMCP Server               │
│     │                            │                          │
│     │  1. Agent presents         │                          │
│     │     API key or mTLS cert   │                          │
│     │                            │                          │
│     │                       2. Resolve agent_id             │
│     │                       3. Load agent policy            │
│     │                       4. Check rate limits            │
│     │                            │                          │
│     │                       KafkaMCP ──[SASL/mTLS]──▶ Kafka │
│     │                            │                          │
│     │                       5. Use cluster credentials      │
│     │                          (NOT agent credentials)      │
│     │                            │                          │
│     │  ◀─── Response ───────────│                          │
│     │                            │                          │
│     │                       6. Audit log entry written      │
└─────────────────────────────────────────────────────────────┘
```

**Key principle:** Agents never receive or handle Kafka cluster credentials. KafkaMCP acts as an authenticated proxy — agents authenticate to KafkaMCP, and KafkaMCP authenticates to Kafka using its own service credentials. Access is governed by the policy file, not by Kafka ACLs directly (though KafkaMCP's Kafka credentials should have the union of all agent permissions).

### 11.3 Data Protection

| Control | Implementation |
|---|---|
| **Encryption in transit** | TLS 1.3 for Kafka connections; TLS for SSE/HTTP MCP transport |
| **Encryption at rest** | Delegated to Kafka broker configuration (not KafkaMCP's responsibility) |
| **Field-level masking** | P1 feature: configurable regex/JSONPath rules to mask fields (e.g., `$.customer.ssn` → `"***"`) before returning to agents |
| **PII detection** | P2 feature: optional integration with PII detection libraries to auto-flag sensitive fields |
| **Log sanitization** | Message values are never logged; only metadata (topic, partition, offset, key) appears in audit logs |

---

## 12. Performance Requirements & SLAs

### 12.1 Latency SLOs

| Operation | p50 | p95 | p99 | Max |
|---|---|---|---|---|
| `kafka_list_topics` (1,000 topics) | 100ms | 500ms | 1s | 2s |
| `kafka_list_topics` (10,000 topics) | 500ms | 1s | 1.5s | 3s |
| `kafka_consume` (10 messages) | 50ms | 200ms | 500ms | 2s |
| `kafka_consume` (100 messages) | 100ms | 400ms | 800ms | 3s |
| `kafka_consume` (1,000 messages) | 300ms | 1s | 2s | 5s |
| `kafka_produce` (1 message, ack=all) | 20ms | 100ms | 300ms | 1s |
| `kafka_produce` (100 messages, ack=all) | 100ms | 400ms | 800ms | 2s |
| `kafka_get_schema` (cached) | 1ms | 5ms | 10ms | 50ms |
| `kafka_get_schema` (cache miss) | 50ms | 150ms | 300ms | 1s |
| `kafka_describe_consumer_group` | 50ms | 200ms | 500ms | 2s |

### 12.2 Throughput SLOs

| Metric | Target |
|---|---|
| Sustained produce rate | 10,000 msgs/sec across all agents |
| Sustained consume rate | 50,000 msgs/sec across all agents |
| Concurrent MCP connections | 100 agents |
| Concurrent streaming subscriptions | 50 agents |
| Tool calls per second (aggregate) | 500/sec |

### 12.3 Availability

| Tier | Target | Max Downtime/Year |
|---|---|---|
| OSS (self-hosted) | Best effort | Depends on operator |
| Cloud | 99.9% | 8.76 hours |
| Enterprise | 99.95% | 4.38 hours |

---

## 13. Deployment & Infrastructure

### 13.1 Deployment Models

#### Model 1: Local / Sidecar (stdio)

```
┌────────────────────────────────────────┐
│  Developer Machine / Agent Pod         │
│                                        │
│  ┌──────────┐    stdio    ┌──────────┐ │
│  │ AI Agent │◄──────────▶│ KafkaMCP │ │
│  │ (Claude) │             │ (process)│ │
│  └──────────┘             └────┬─────┘ │
│                                │       │
└────────────────────────────────┼───────┘
                                 │ SASL_SSL
                          ┌──────▼──────┐
                          │   Kafka     │
                          │   Cluster   │
                          └─────────────┘
```

**Use case:** Developer testing, Claude Desktop, single-agent deployments.

#### Model 2: Shared Server (SSE/HTTP)

```
┌──────────┐  ┌──────────┐  ┌──────────┐
│ Agent A  │  │ Agent B  │  │ Agent C  │
└────┬─────┘  └────┬─────┘  └────┬─────┘
     │ SSE         │ SSE         │ SSE
     ▼             ▼             ▼
┌────────────────────────────────────────┐
│          Load Balancer (L7)            │
└────────────────┬───────────────────────┘
                 │
     ┌───────────┼───────────┐
     ▼           ▼           ▼
┌──────────┐ ┌──────────┐ ┌──────────┐
│ KafkaMCP │ │ KafkaMCP │ │ KafkaMCP │
│ Instance │ │ Instance │ │ Instance │
└────┬─────┘ └────┬─────┘ └────┬─────┘
     │            │            │
     ▼            ▼            ▼
┌────────────────────────────────────────┐
│           Kafka Cluster                │
└────────────────────────────────────────┘
```

**Use case:** Multi-agent enterprise deployments, shared infrastructure.

### 13.2 Deployment Targets

| Target | Method | Notes |
|---|---|---|
| **Binary** | GitHub Releases (Linux, macOS, Windows amd64/arm64) | Primary distribution; single Go binary with no `node_modules` or external runtime |
| **Source build** | `go build ./cmd/kafkamcp` | Straightforward local builds and CI |
| **Docker** | `docker run ghcr.io/kafkamcp/kafkamcp:latest` | Standard for server deployments; built via multi-stage Go Dockerfile |
| **Kubernetes** | Helm chart (`helm install kafkamcp kafkamcp/kafkamcp`) | Production multi-instance |
| **Claude Desktop** | `claude_desktop_config.json` MCP server entry pointing to the binary | One-line setup for Claude users |
| **Release automation** | `goreleaser release` | Cross-platform archives, checksums, and GitHub Release publishing |

**Build & release workflow (Go-first):**
```bash
go build -o bin/kafkamcp ./cmd/kafkamcp
goreleaser release --snapshot --clean
```

**Reference multi-stage Docker build:**
```dockerfile
FROM golang:1.22 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/kafkamcp ./cmd/kafkamcp

FROM alpine:3.20
RUN apk add --no-cache ca-certificates librdkafka
COPY --from=builder /out/kafkamcp /usr/local/bin/kafkamcp
ENTRYPOINT ["/usr/local/bin/kafkamcp"]
```

Use `scratch` for fully static builds where the selected Kafka client packaging permits it; otherwise prefer a minimal `alpine` runtime when bundling librdkafka.

### 13.3 Minimal System Requirements

| Resource | Minimum | Recommended |
|---|---|---|
| CPU | 1 core | 2–4 cores |
| Memory | 256 MB | 1 GB |
| Disk | 50 MB (binary + logs) | 500 MB (with audit log retention) |
| Network | Connectivity to Kafka brokers and Schema Registry | Low-latency network to Kafka cluster |

### 13.4 Kubernetes Helm Values (Example)

```yaml
replicaCount: 3

image:
  repository: ghcr.io/kafkamcp/kafkamcp
  tag: "0.1.0"

service:
  type: ClusterIP
  port: 3100

config:
  transport: sse
  clusters:
    - name: production
      bootstrapServers: "kafka-headless.kafka:9092"
      securityProtocol: SASL_SSL
      saslMechanism: SCRAM-SHA-512
  policyConfigMap: kafkamcp-policies

resources:
  requests:
    cpu: 500m
    memory: 512Mi
  limits:
    cpu: 2000m
    memory: 1Gi

autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 10
  targetCPUUtilization: 70

podDisruptionBudget:
  minAvailable: 1
```

---

## 14. Metrics & KPIs

### 14.1 Business Metrics

| Metric | Target (6 months) | Target (18 months) | Measurement |
|---|---|---|---|
| GitHub stars | 1,000 | 5,000 | GitHub API |
| Binary downloads / month | 500 | 5,000 | GitHub Releases / package manager stats |
| Docker pulls | 2,000 | 20,000 | Docker Hub / GHCR stats |
| Production deployments (known) | 3 | 30 | Self-reported, surveys, case studies |
| Discord community members | 200 | 1,000 | Discord member count |
| Contributors (merged PRs) | 10 | 50 | GitHub contributor graph |
| Cloud tier paying customers | 0 | 10 | Revenue system |
| Enterprise contracts | 0 | 3 | CRM |

### 14.2 Technical Metrics (Exposed via Prometheus)

| Metric | Type | Labels | Description |
|---|---|---|---|
| `kafkamcp_tool_calls_total` | Counter | tool, agent_id, cluster, status | Total MCP tool invocations |
| `kafkamcp_tool_latency_seconds` | Histogram | tool, agent_id, cluster | Tool call latency distribution |
| `kafkamcp_messages_consumed_total` | Counter | topic, agent_id, cluster | Messages consumed via kafka_consume |
| `kafkamcp_messages_produced_total` | Counter | topic, agent_id, cluster | Messages produced via kafka_produce |
| `kafkamcp_schema_cache_hits_total` | Counter | cluster | Schema cache hit count |
| `kafkamcp_schema_cache_misses_total` | Counter | cluster | Schema cache miss count |
| `kafkamcp_active_connections` | Gauge | transport | Currently connected agents |
| `kafkamcp_active_subscriptions` | Gauge | cluster | Active streaming subscriptions |
| `kafkamcp_kafka_connection_errors_total` | Counter | cluster, error_type | Kafka connection failures |
| `kafkamcp_authz_denials_total` | Counter | agent_id, tool, reason | Authorization denials |

### 14.3 Success Criteria

| Milestone | Criteria | Timeline |
|---|---|---|
| **OSS Launch** | v0.1 published; README, docs, CI passing; 5 MCP tools working end-to-end | Month 2 |
| **Community Traction** | 500 stars; 10 issues filed by external users; 3 external PRs merged | Month 4 |
| **Production Validation** | 1 known production deployment; zero critical bugs in 30 days | Month 6 |
| **Cloud Beta** | Multi-tenant cloud deployment; 3 beta customers; < 200ms p95 latency | Month 12 |
| **Revenue** | First paying customer; $10K MRR | Month 18 |

---

## 15. Go-to-Market Strategy

### 15.1 OSS Distribution Strategy

| Channel | Action | Timeline |
|---|---|---|
| **GitHub** | Apache 2.0 repo; comprehensive README; `/examples` directory; GitHub Actions CI; issue templates | Month 1 |
| **Direct binary** | Publish signed binaries and checksums via GitHub Releases; one-command local install via package manager or direct download | Month 1 |
| **Docker Hub / GHCR** | Multi-arch Docker images (amd64, arm64) | Month 1 |
| **Homebrew** | `brew install kafkamcp` | Month 2 |
| **Helm Chart** | Published Helm chart for Kubernetes | Month 3 |

### 15.2 Content & Community

| Activity | Cadence | Channel |
|---|---|---|
| Launch blog post: "Making Kafka Agent-Accessible" | One-time | Personal blog, Hacker News, Reddit r/apachekafka, r/MachineLearning |
| Tutorial: "Connect Claude to Your Kafka Cluster in 5 Minutes" | One-time | Blog, YouTube |
| Conference talks: Kafka Summit, AI Engineer World's Fair | 2x/year | In-person |
| "KafkaMCP in the Wild" case studies | Quarterly | Blog |
| Discord community server | Ongoing | Discord |
| Weekly office hours / community call | Weekly | Discord voice / YouTube |
| Integration guides for LangChain, CrewAI, AutoGen | Per-framework | GitHub docs |

### 15.3 Partnership Strategy

| Partner | Value | Approach |
|---|---|---|
| **Confluent** | Largest Kafka vendor; Cloud customers need agent access | Apply for Confluent Partner Connect; build verified integration |
| **Anthropic** | MCP creator; showcase KafkaMCP as exemplary MCP server | Submit to MCP server directory; engage DevRel team |
| **LangChain / LangSmith** | Dominant agent framework | Publish official LangChain integration; co-author blog post |
| **Redpanda** | Kafka-compatible, developer-focused | Publish compatibility guide; target Redpanda community |

### 15.4 Monetization Roadmap

| Phase | Revenue Source | Pricing Model |
|---|---|---|
| **Phase 1 (0–6 mo)** | None — pure OSS adoption | Free |
| **Phase 2 (6–12 mo)** | KafkaMCP Cloud (managed multi-tenant) | Free tier (1 cluster, 100 tool calls/day) + Pro ($49/mo per cluster, unlimited) |
| **Phase 3 (12–24 mo)** | Enterprise tier | Annual contract ($50K–$200K) for on-prem, SSO, compliance, SLA |
| **Phase 4 (18–36 mo)** | Marketplace listings (Confluent, AWS, GCP) | Usage-based via marketplace billing |

---

## 16. Competitive Analysis

### 16.1 Feature Comparison Matrix

| Capability | **KafkaMCP** | Kafka REST Proxy | Custom Code | LangChain Kafka | Kafka Connect + DB | ksqlDB |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| MCP-native interface | ✅ | ❌ | ❌ | ⚠️ Partial | ❌ | ❌ |
| Topic discovery | ✅ | ⚠️ Manual | ❌ | ❌ | ❌ | ⚠️ |
| Schema awareness | ✅ Auto | ❌ | ⚠️ Manual | ❌ | ❌ | ✅ |
| Real-time consume | ✅ | ✅ | ✅ | ⚠️ | ❌ (batch) | ✅ |
| Real-time produce | ✅ | ✅ | ✅ | ⚠️ | ❌ | ✅ |
| Consumer group ops | ✅ | ❌ | ⚠️ | ❌ | ❌ | ❌ |
| Agent access control | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Agent audit logging | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Multi-cluster | ✅ | ❌ | ⚠️ | ❌ | ⚠️ | ❌ |
| Zero-config setup | ✅ (single binary / Homebrew) | ⚠️ (deploy) | ❌ (build) | ⚠️ | ❌ (deploy) | ❌ (deploy) |
| Maintained OSS | ✅ | ✅ (Confluent) | ❌ (in-house) | ⚠️ (community) | ✅ (Confluent) | ✅ (Confluent) |
| Streaming subscriptions | ✅ (P1) | ✅ | ✅ | ❌ | ❌ | ✅ |
| Message search/filter | ✅ (P1) | ❌ | ⚠️ | ❌ | ❌ | ✅ |
| Setup time | 5 min | 30 min | 2–4 weeks | 1 day | 1–2 weeks | 1 week |

### 16.2 Competitive Moat

1. **Network effects:** As adoption grows, community contributions (new tools, schema format support, agent framework integrations) make KafkaMCP more valuable for everyone — a classic OSS flywheel.
2. **MCP-native design:** Retrofitting MCP onto Kafka REST Proxy or ksqlDB would be architecturally awkward; KafkaMCP's MCP-first design is a structural advantage.
3. **Founder credibility:** Operating Kafka at GitHub scale provides deep credibility with the target audience (data engineers and platform teams).
4. **Category creation:** "Agent-accessible streaming infrastructure" does not exist as a product category today. First mover defines the category vocabulary, best practices, and community norms.

---

## 17. Risks & Mitigations

| ID | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| R1 | **MCP protocol instability** — breaking changes in the MCP spec invalidate KafkaMCP's interface | Medium | High | Track MCP spec closely; maintain compatibility layer; participate in MCP spec discussions; abstract MCP transport behind internal interfaces |
| R2 | **Confluent builds it** — Confluent adds native MCP support to their REST Proxy or Cloud | Medium | High | Move fast; establish community and mindshare before Confluent can react; differentiate on schema intelligence and agent-specific features (ACLs, audit, masking) |
| R3 | **Low agent adoption** — enterprises don't deploy agents to production at the expected rate | Low | High | KafkaMCP is also useful for developer productivity (ad-hoc Kafka queries via Claude Desktop); pivot to developer tooling positioning if agent adoption is slow |
| R4 | **Security incident** — agent misuse or vulnerability in KafkaMCP leads to data breach | Low | Critical | Default-deny policies; rate limiting; audit logging; security-first development practices; responsible disclosure program; third-party security audit at v1.0 |
| R5 | **librdkafka / CGO deployment complexity** — `confluent-kafka-go` introduces native dependency management and cross-compilation complexity | Medium | Medium | Standardize on tested Docker images, GoReleaser pipelines, and prebuilt binaries for Linux/macOS/Windows from month 1; document `alpine` vs `scratch` runtime trade-offs |
| R6 | **Solo founder burnout** — single founder cannot sustain OSS maintenance, community management, and product development | High | High | Prioritize ruthlessly (P0 only for v0.1); automate CI/CD, issue triage, and release processes; recruit 2–3 core maintainers from early community by month 6; apply for YC for co-founder matching and funding |
| R7 | **Schema Registry fragmentation** — users run different registries (Confluent, Karapace, Apicurio, AWS Glue) with incompatible APIs | Medium | Medium | Target Confluent Schema Registry API (de facto standard); add adapters for Karapace and Apicurio in P1; AWS Glue in P2 |

---

## 18. Milestones & Roadmap

### 18.1 Phased Delivery

```
┌─────────────────────────────────────────────────────────────────────┐
│                      KAFKAMCP ROADMAP                               │
│                                                                     │
│  Phase 0        Phase 1          Phase 2         Phase 3            │
│  Foundation     Core OSS         Growth          Enterprise        │
│  Month 0–2      Month 2–6        Month 6–12      Month 12–24       │
│                                                                     │
│  ┌───────────┐  ┌──────────────┐  ┌─────────────┐  ┌────────────┐  │
│  │ Prototype │  │ v0.1 Release │  │ v0.3 Release│  │ v1.0       │  │
│  │           │  │              │  │             │  │            │  │
│  │ • Project │  │ • P0 tools   │  │ • Streaming │  │ • Cloud    │  │
│  │   setup   │  │ • Schema     │  │ • Search    │  │   launch   │  │
│  │ • Go +    │  │   Registry   │  │ • Multi-    │  │ • Enterp.  │  │
│  │   Kafka   │  │ • stdio +    │  │   cluster   │  │   tier     │  │
│  │   PoC     │  │   SSE        │  │ • DLQ       │  │ • Data     │  │
│  │ • MCP Go  │  │ • ACL policy │  │   analysis  │  │   masking  │  │
│  │   SDK PoC │  │ • Docs +     │  │ • Audit log │  │ • SSO/SAML │  │
│  │ • 3 tools │  │   examples   │  │ • Helm chart│  │ • SLA      │  │
│  │   working │  │ • Binary +   │  │ • Compat.   │  │ • Paid     │  │
│  │           │  │ • HN launch  │  │   matrix    │  │   support  │  │
│  └───────────┘  └──────────────┘  └─────────────┘  └────────────┘  │
│                                                                     │
│  Key milestones:                                                    │
│  ✦ Month 2: v0.1 binary + GitHub Release (P0 tools)                │
│  ✦ Month 4: 500 GitHub stars                                       │
│  ✦ Month 6: First production deployment                            │
│  ✦ Month 9: v0.3 (streaming + multi-cluster)                       │
│  ✦ Month 12: Cloud beta launch                                     │
│  ✦ Month 18: First paying customer                                 │
│  ✦ Month 24: v1.0 GA + Enterprise tier                             │
└─────────────────────────────────────────────────────────────────────┘
```

### 18.2 Detailed Phase Breakdown

#### Phase 0: Foundation (Month 0–2)

| Deliverable | Description | Exit Criteria |
|---|---|---|
| Repository setup | Go repository with `cmd/kafkamcp`, `internal/`, `pkg/`, and CI via GitHub Actions | CI green; `go test ./...` passes |
| Kafka integration | `confluent-kafka-go` producer, consumer, and admin abstractions with connection pooling | Can produce/consume from a local Kafka cluster |
| MCP SDK integration | Official MCP Go SDK (or equivalent Go implementation) with stdio and SSE transports | Agent can call a test tool via stdio |
| 3 prototype tools | `kafka_list_topics`, `kafka_consume`, `kafka_produce` | End-to-end demo: agent lists topics, consumes, produces |
| Schema Registry client | Fetch schemas; Avro deserialization | Agent reads an Avro-encoded message with correct deserialization |
| Configuration loader | YAML config parsing and validation | Config file loaded and validated at startup |

#### Phase 1: Core OSS Release (Month 2–6)

| Deliverable | Description | Exit Criteria |
|---|---|---|
| All P0 tools (FR-01 through FR-10) | Full implementation with tests | All tools pass integration tests against real Kafka |
| Policy engine | YAML-based per-agent access control | Unauthorized tool calls rejected with clear error |
| Documentation | README, Getting Started guide, API reference, architecture diagram | Docs site published (GitHub Pages or docs.kafkamcp.dev) |
| Examples directory | 5+ example configurations for common setups (Claude Desktop, LangChain, Docker Compose) | Each example runs end-to-end |
| Binary + Docker publishing | Automated release pipeline via `goreleaser` | GitHub Release binaries and `docker run` both work |
| Launch campaign | Hacker News post, Reddit posts, Twitter/X threads, blog post | Published and promoted |

#### Phase 2: Growth (Month 6–12)

| Deliverable | Description | Exit Criteria |
|---|---|---|
| P1 features (FR-11 through FR-18) | Streaming subscriptions, message search, topic management, multi-cluster | All P1 tools pass integration tests |
| Helm chart | Production-ready Kubernetes deployment | Deployed in at least 1 production K8s cluster |
| Compatibility testing | Confluent Cloud, AWS MSK, Redpanda, WarpStream | CI matrix testing against each platform |
| Prometheus metrics | Full metrics exposure | Grafana dashboard template published |
| Cloud prototype | Multi-tenant KafkaMCP service with auth and usage tracking | Internal dogfood deployment running |

#### Phase 3: Enterprise & Cloud (Month 12–24)

| Deliverable | Description | Exit Criteria |
|---|---|---|
| KafkaMCP Cloud GA | Managed multi-tenant service | 10 beta customers; < 200ms p95 latency |
| Enterprise features | SSO/SAML, data masking, compliance logging, dedicated support | Enterprise pilot with 1 customer |
| v1.0 GA | Stable API; semantic versioning commitment; LTS policy | Published with migration guide |
| Performance tuning / optional Rust acceleration | Optimize hot paths only if profiling shows Go is insufficient for target workloads | 2–10x improvement on targeted produce/consume bottlenecks |

---

## 19. Open Questions & Future Considerations

### 19.1 Open Questions

| ID | Question | Impact | Owner | Deadline |
|---|---|---|---|---|
| OQ-1 | Should KafkaMCP support Kafka transactions (exactly-once semantics) in v0.1, or defer to P1? | Complexity vs. correctness for produce operations | Founder | Before v0.1 design freeze |
| OQ-2 | What is the right MCP transport for streaming subscriptions? SSE with long-lived connections, or the newer Streamable HTTP transport? | Determines streaming architecture | Founder | Month 2 |
| OQ-3 | Should the policy engine support dynamic policies (loaded from a remote API) or only static YAML files? | Enterprise flexibility vs. v0.1 complexity | Founder | Month 4 |
| OQ-4 | How should KafkaMCP handle multi-tenant Schema Registry deployments (e.g., Confluent Cloud with context-based subjects)? | Schema discovery correctness for Confluent Cloud users | Founder | Month 3 |
| OQ-5 | Should KafkaMCP expose Kafka Streams state stores as MCP resources? | Enables interactive queries against materialized views | Founder | Phase 2 |
| OQ-6 | What is the right approach for agent-to-agent communication via Kafka? Should KafkaMCP support pub/sub patterns natively? | Could unlock multi-agent coordination use cases | Founder | Phase 2 |
| OQ-7 | Should KafkaMCP support non-Confluent Schema Registries (Karapace, Apicurio, AWS Glue) in v0.1 or defer? | Broadens addressable market vs. scope creep | Founder | Before v0.1 design freeze |
| OQ-8 | Should KafkaMCP standardize on `confluent-kafka-go` from v0.1, or evaluate pure-Go Kafka clients later for easier static linking? | Trade-off between Kafka feature completeness and simpler build/runtime packaging | Founder | Month 0 |

### 19.2 Future Considerations

- **Beyond Kafka:** Expand to other streaming platforms (Amazon Kinesis, Google Pub/Sub, Apache Pulsar, NATS JetStream) under a unified "StreamMCP" umbrella — same MCP interface, different backend.
- **Agent-to-agent messaging:** Use Kafka topics as a communication bus between AI agents; KafkaMCP becomes the agent messaging layer.
- **Event-driven agent triggers:** Agents register declarative subscriptions ("notify me when a message matching X appears on topic Y") — KafkaMCP becomes an event-driven agent orchestrator.
- **Semantic search over topics:** Index Kafka messages with embeddings; expose a `kafka_semantic_search` tool that finds relevant messages by meaning, not just key/filter.
- **Cost attribution:** Track per-agent Kafka resource consumption (bytes consumed, messages produced) for chargeback in multi-team environments.
- **Kafka admin agent:** A pre-built agent (using KafkaMCP) that can manage Kafka clusters — creating topics, adjusting retention, rebalancing partitions — as a product built on top of KafkaMCP.

---

## 20. Appendix

### 20.1 Glossary

| Term | Definition |
|---|---|
| **MCP (Model Context Protocol)** | An open protocol, created by Anthropic, that standardizes how AI agents discover and invoke external tools and data sources. |
| **MCP Server** | A process that exposes tools and resources via the MCP protocol. KafkaMCP is an MCP server. |
| **MCP Client** | An AI agent or agent framework that connects to MCP servers to discover and invoke tools. |
| **MCP Tool** | A callable function exposed by an MCP server (e.g., `kafka_consume`). Agents call tools to perform actions. |
| **MCP Resource** | A read-only data source exposed by an MCP server (e.g., `kafka://cluster/metadata`). Agents read resources for context. |
| **Topic** | A named, ordered, append-only log in Kafka. Messages are published to and consumed from topics. |
| **Partition** | A subdivision of a topic that enables parallelism. Each partition is an ordered, immutable sequence of messages. |
| **Consumer Group** | A set of consumers that cooperatively read from a topic's partitions. Kafka tracks each group's read position (offset). |
| **Schema Registry** | A service that stores and serves Avro, Protobuf, and JSON Schema definitions for Kafka topics. Enables schema evolution and compatibility enforcement. |
| **KRaft** | Kafka's built-in consensus protocol (replacing ZooKeeper). KRaft-mode clusters are self-managing with no external coordination service. |
| **Dead-Letter Queue (DLQ)** | A topic where messages that fail processing are routed for later inspection and retry. |
| **Avro** | A binary serialization format with schema evolution support, widely used with Kafka. |
| **Protobuf** | Google's Protocol Buffers — a binary serialization format with strong typing and schema evolution. |
| **SSE (Server-Sent Events)** | A standard for servers to push updates to clients over HTTP. Used as an MCP transport for remote connections. |
| **stdio** | Standard input/output — a local-only MCP transport where the agent spawns the server as a child process. |
| **ACL (Access Control List)** | Kafka's native authorization mechanism for controlling client access to topics, groups, and operations. |
| **ISR (In-Sync Replicas)** | The set of replicas for a partition that are fully caught up with the leader. Kafka uses ISR for durability guarantees. |

### 20.2 References

| Resource | URL |
|---|---|
| MCP Specification | https://modelcontextprotocol.io/specification |
| MCP Go SDK | https://github.com/modelcontextprotocol/go-sdk |
| Apache Kafka Documentation | https://kafka.apache.org/documentation/ |
| Confluent Schema Registry API | https://docs.confluent.io/platform/current/schema-registry/develop/api.html |
| GoReleaser | https://goreleaser.com/ |
| Confluent kafka-go | https://github.com/confluentinc/confluent-kafka-go |
| Avro Specification | https://avro.apache.org/docs/current/specification/ |
| MCP Server Directory | https://github.com/modelcontextprotocol/servers |

### 20.3 Related Products in This Portfolio

| Product | Relationship to KafkaMCP |
|---|---|
| **FlowGraph** | Orchestration engine for agent execution loops; could use KafkaMCP as a tool in FlowGraph DAGs |
| **StreamMesh** | Kafka-compatible streaming for semiconductor supply chains; KafkaMCP could be a StreamMesh access layer |
| **AgentRegistry** | Service registry for agents; KafkaMCP-connected agents would register in AgentRegistry |
| **PulseIQ** | Observability platform; could monitor KafkaMCP's Prometheus metrics |

---

*End of KafkaMCP Product Requirements Document*
