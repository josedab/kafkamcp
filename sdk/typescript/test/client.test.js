const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const { KafkaMCPClient } = require("../dist/index.js");

const supportedTools = new Set([
  "kafka_list_topics",
  "kafka_describe_topic",
  "kafka_create_topic",
  "kafka_delete_topic",
  "kafka_consume",
  "kafka_produce",
  "kafka_search",
  "kafka_list_consumer_groups",
  "kafka_describe_consumer_group",
  "kafka_reset_offsets",
  "kafka_list_schemas",
  "kafka_get_schema",
  "kafka_diagnose_lag",
]);

class MockTransport {
  constructor(responses = {}) {
    this.responses = responses;
    this.calls = [];
    this.closed = false;
  }

  async callTool(name, args) {
    this.calls.push({ name, args });
    assert.ok(supportedTools.has(name), `unsupported tool emitted: ${name}`);
    return this.responses[name] ?? {};
  }

  async readResource() {
    return {};
  }

  async close() {
    this.closed = true;
  }
}

test("every emitted tool name is supported", () => {
  const source = fs.readFileSync(path.join(__dirname, "../src/index.ts"), "utf8");
  const emitted = [...source.matchAll(/callTool\("([^"]+)"/g)].map((match) => match[1]);
  assert.ok(emitted.length > 0);
  for (const name of emitted) {
    assert.ok(supportedTools.has(name), `unsupported tool emitted: ${name}`);
  }
});

test("unwraps server response envelopes", async () => {
  const message = { partition: 0, offset: 1, timestamp: "now", key: "k", value: "v" };
  const transport = new MockTransport({
    kafka_consume: { messages: [message] },
    kafka_search: { messages: [message] },
    kafka_produce: { deliveries: [{ partition: 0, offset: 2, timestamp: "now", status: "success" }] },
  });
  const client = new KafkaMCPClient(transport);

  assert.deepEqual(await client.consume("orders"), [message]);
  assert.deepEqual(await client.search("orders", "v"), [message]);
  assert.equal(
    (await client.produce("orders", [{ value: "v" }], { approvalToken: "token" })).length,
    1
  );
});

test("maps schema subject wire fields to SDK fields", async () => {
  const transport = new MockTransport({
    kafka_list_schemas: {
      subjects: [
        {
          subject_name: "orders-value",
          latest_version: 3,
          schema_type: "AVRO",
          compatibility_level: "BACKWARD",
        },
      ],
    },
  });
  const client = new KafkaMCPClient(transport);

  assert.deepEqual(await client.listSchemas(), [
    {
      subject: "orders-value",
      version: 3,
      schema_type: "AVRO",
      compatibility_level: "BACKWARD",
    },
  ]);
});

test("uses exact mutation argument contracts", async () => {
  const transport = new MockTransport();
  const client = new KafkaMCPClient(transport);
  const options = {
    approvalToken: "approval-token",
    idempotencyKey: "request-123",
    cluster: "prod",
  };

  await client.createTopic("orders", 3, 2, undefined, options);
  await client.deleteTopic("orders", options);
  await client.resetOffsets("group", "orders", "earliest", options);

  assert.deepEqual(transport.calls[0], {
    name: "kafka_create_topic",
    args: {
      name: "orders",
      partitions: 3,
      replication_factor: 2,
      approval_token: "approval-token",
      idempotency_key: "request-123",
      cluster: "prod",
    },
  });
  assert.equal(transport.calls[1].args.name, "orders");
  assert.equal(transport.calls[1].args.approval_token, "approval-token");
  assert.equal(transport.calls[2].args.group_id, "group");
  assert.equal(transport.calls[2].args.approval_token, "approval-token");
});

test("closes the transport", async () => {
  const transport = new MockTransport();
  const client = new KafkaMCPClient(transport);
  await client.close();
  assert.equal(transport.closed, true);
});

test("rejects guarded mutations without approval", async () => {
  const client = new KafkaMCPClient(new MockTransport());
  await assert.rejects(
    client.deleteTopic("orders", { approvalToken: "" }),
    /approvalToken is required/
  );
});
