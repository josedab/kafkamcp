# The Schema Blindness Problem: Why Your Agent Hallucinates Field Names

*By Jose D. · August 2025*

---

I watched an agent produce a message to a payments topic with a field called `total_price`. The schema expected `total_amount`. The message was accepted — no validation, no error, no warning — serialized as JSON, and dropped into the topic. A downstream consumer tried to deserialize it, failed on the missing `total_amount` field, and routed it to the dead-letter queue. Thirty thousand messages later, someone noticed.

The agent didn't make a mistake in reasoning. It made a mistake in vocabulary. It had no way to know that the field was called `total_amount` and not `total_price`, because nothing told it the schema. The agent was schema-blind.

This is the default state of every AI agent interacting with Kafka today.

## What schema blindness actually costs you

Schema blindness manifests in three ways, and all of them are worse than they sound.

**1. Agents hallucinate field names.** Large language models are autocomplete machines. When an agent needs to produce a message to a topic called `orders.created`, it guesses what the fields should be. Sometimes it guesses correctly. Sometimes it produces `order_id` when the schema says `orderId`. Sometimes it invents fields that don't exist at all. Without the actual schema definition, the agent is operating on vibes.

**2. Agents can't validate what they read.** When an agent consumes from a Kafka topic, it receives raw bytes. If the topic uses Avro, those bytes are Avro-encoded. If the agent doesn't know the schema, it can't decode the message. Most ad-hoc integrations skip Avro entirely and configure the consumer to treat everything as JSON — which works until someone changes the serialization format, at which point the agent silently returns garbage.

**3. Schema changes break agents silently.** Schema evolution is the norm in production Kafka. Fields get added, defaults change, optional fields become required. Confluent's Schema Registry exists precisely to manage this. But if your agent doesn't know the registry exists, it can't detect that a schema changed, can't verify backward compatibility, and can't adapt its behavior. The agent is the last to know.

## The concrete damage

Consider a data quality agent that monitors a `user_events` topic. Its job: consume recent messages, validate field presence, and alert on anomalies.

Without schema awareness, the agent is checking fields by guessing. "I expect `user_id`, `event_type`, `timestamp`, and `metadata`." If the team adds a new required field — say, `session_id` — the agent doesn't know. It continues reporting everything is fine. Meanwhile, 5% of producers haven't upgraded yet and are sending messages without `session_id`, which fail downstream. The agent is monitoring the wrong thing.

With schema awareness, the agent reads the schema from the Schema Registry, sees that `session_id` was added in v3 with no default value, and immediately flags: "New required field `session_id` added to `user_events` schema v3. Checking producer compliance." It then scans recent messages and reports: "5.2% of messages missing `session_id`. All from producer `legacy-web-v2`."

Same agent. Same Kafka topic. Radically different outcomes based entirely on whether the agent knows the schema.

## Why bolting on schema access doesn't work

The intuitive fix: give the agent a tool that calls the Schema Registry API. Just add one more wrapper.

I've tried this. It's insufficient for three reasons.

**First, the agent needs the schema at consume time, not as a separate step.** When `kafka_consume` returns a message, the agent needs to know that `value.total_amount` is a `double` and `value.items` is an `array<OrderItem>` — right there, inline, without making a second tool call. If schema retrieval is a separate step, the agent forgets to do it half the time, or caches the wrong version, or matches the schema to the wrong topic.

**Second, field summaries matter more than raw schema JSON.** An Avro schema is a nested JSON structure with unions, logical types, references, and defaults. An LLM can parse it, but it wastes tokens and introduces confusion. What the agent actually needs is: `OrderCreated { order_id: string, customer_id: string, total_amount: double, currency: string (default: USD), items: array<OrderItem>, created_at: timestamp-millis }`. A human-readable field summary that fits in a single line of context.

**Third, compatibility checking must be proactive.** When an agent wants to produce a message, it should check compatibility *before* writing, not after. "Is this message shape compatible with the registered schema?" is a question that should block the produce call, not an afterthought.

## How KafkaMCP solves this

KafkaMCP integrates schema awareness into every relevant operation, not as an add-on.

**Schema discovery.** `kafka_list_schemas` returns all Schema Registry subjects with their latest version, type (AVRO/PROTOBUF/JSON), and compatibility level. `kafka_get_schema` returns the full schema definition with a generated field summary:

```json
{
  "subject": "orders.created-value",
  "version": 3,
  "schema_id": 42,
  "schema_type": "AVRO",
  "compatibility_level": "BACKWARD",
  "field_summary": "OrderCreated { order_id: string, customer_id: string, total_amount: double, currency: string (default: USD), items: array<OrderItem>, created_at: timestamp-millis }",
  "schema": { ... }
}
```

That `field_summary` field is key. It's generated from the schema structure and gives the agent a single-line mental model of what the message looks like. The agent doesn't need to parse nested Avro JSON — it reads a type signature.

**Compatibility checking.** Before an agent produces a message, it can call `kafka_check_schema_compatibility`:

```
Agent: "Check if this schema is compatible with orders.created-value"
→ kafka_check_schema_compatibility(subject: "orders.created-value", schema: "...", schema_type: "AVRO")
→ { "is_compatible": true, "messages": [] }
```

If the schema is incompatible, the agent gets specific error messages explaining why. No silent failures.

**Schema evolution advice.** When an agent (or the human behind it) wants to change a schema, `kafka_suggest_schema_evolution` analyzes the change description and suggests compatible strategies:

```
Agent: "I want to add an email field to the user schema"
→ kafka_suggest_schema_evolution(subject: "users-value", change: "add a new field for email")
→ [
    { "strategy": "add_optional_field", "compatible": true, 
      "steps": ["Add with default value", "Register new version", "Update producers", "Update consumers"] },
    { "strategy": "add_nullable_field", "compatible": true,
      "steps": ["Define as union [null, string] with default null", ...] }
  ]
```

The agent doesn't just know the schema. It can reason about schema evolution and propose safe changes.

## The difference in practice

Here's the same scenario from the opening — an agent needs to produce a payment event — with and without schema awareness.

**Without (schema-blind):**
1. Agent guesses field names based on the topic name: `total_price`, `order_id`, `status`
2. Message serialized as JSON string, sent to Kafka
3. Downstream consumer fails to deserialize (expected `total_amount`, not `total_price`)
4. Message routed to DLQ
5. Alert fires hours later when DLQ count crosses threshold
6. Human investigates, finds the typo, updates the agent, replays messages

**With KafkaMCP (schema-aware):**
1. Agent calls `kafka_get_schema(subject: "payments.completed-value")`
2. Gets field summary: `PaymentCompleted { order_id: string, total_amount: double, currency: string, status: string, processed_at: timestamp-millis }`
3. Agent produces message with correct field names
4. Message deserializes successfully downstream
5. No DLQ, no alert, no human intervention

The schema cost nothing. It was already in the Schema Registry. The agent just needed a way to ask for it.

---

Schema blindness is the difference between agents that work in staging and agents that survive production. If your agents interact with Kafka and don't know the schema, they're guessing. Sometimes they guess right. Eventually they won't, and the failure mode is silent data corruption.

KafkaMCP makes the schema a first-class part of every agent interaction with Kafka. Because the alternative — agents operating on vibes in your production event streams — is not a real option.

---

*KafkaMCP is open source under the Apache 2.0 license. [GitHub](https://github.com/kafkamcp/kafkamcp) · [Docs](https://kafkamcp.dev) · [Discord](https://discord.gg/kafkamcp)*
