---
sidebar_position: 1
title: Claude Desktop
---

# Claude Desktop

Claude Desktop is the fastest way to use KafkaMCP locally.

## Prerequisites

- KafkaMCP installed and available on your `PATH`
- A valid `kafkamcp.yaml`
- Claude Desktop installed

For Claude Desktop, use **stdio** transport.

```yaml
server:
  transport: stdio
  log_level: info

clusters:
  - name: local
    bootstrap_servers: "localhost:9092"
    security_protocol: PLAINTEXT
    default: true

audit:
  enabled: true
  max_entries: 10000
```

:::tip
Run `kafkamcp validate --config /absolute/path/to/kafkamcp.yaml` before you wire KafkaMCP into Claude Desktop.
:::

## Find your Claude Desktop config file

Typical locations:

- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

If the file does not exist, create it.

## Add the KafkaMCP server entry

Add this entry under `mcpServers`:

```json
{
  "mcpServers": {
    "kafkamcp": {
      "command": "kafkamcp",
      "args": ["--config", "/absolute/path/to/kafkamcp.yaml"]
    }
  }
}
```

If `kafkamcp` is not on your `PATH`, use an absolute binary path instead:

```json
{
  "mcpServers": {
    "kafkamcp": {
      "command": "/usr/local/bin/kafkamcp",
      "args": ["--config", "/absolute/path/to/kafkamcp.yaml"]
    }
  }
}
```

## Restart Claude Desktop

Quit Claude Desktop completely and open it again.

After restart, KafkaMCP should appear as an available MCP server.

## Verify the integration

Ask Claude a simple question that forces a KafkaMCP tool call:

- "List all Kafka topics in my cluster"
- "Describe the topic orders.created"
- "Show me the last 5 messages from payments.events"

If the integration is working, Claude will call KafkaMCP tools directly.

## Troubleshooting

### `command not found`

Claude Desktop cannot find the binary.

**Fix:** use the full binary path in `command`.

### KafkaMCP exits immediately

The config is invalid or Kafka connectivity failed.

**Fix:** run this manually in a terminal:

```bash
kafkamcp validate --config /absolute/path/to/kafkamcp.yaml
kafkamcp --config /absolute/path/to/kafkamcp.yaml
```

Resolve any validation or connection errors before starting Claude Desktop again.

### No tools appear in Claude Desktop

Common causes:

- the JSON file is malformed
- the file is in the wrong location
- Claude Desktop was not fully restarted

**Fix:** validate the JSON and restart Claude Desktop completely.

### You configured `sse` instead of `stdio`

Claude Desktop launches local MCP servers as subprocesses.

**Fix:** set:

```yaml
server:
  transport: stdio
```

### `unauthorized` errors from KafkaMCP

KafkaMCP started correctly, but the agent identity is not allowed by your policy rules.

**Fix:** update the `policies.agents` section in `kafkamcp.yaml` to allow the topics, consumer groups, or schemas Claude needs.

### Rate limit errors

KafkaMCP is protecting the cluster from too many requests.

**Fix:** raise `requests_per_minute` for the relevant agent, or reduce repetitive prompts and polling.

## Recommended workflow

1. Validate the config.
2. Test KafkaMCP directly in a terminal.
3. Add the `claude_desktop_config.json` entry.
4. Restart Claude Desktop.
5. Start with low-risk read-only prompts.

That gives you a clean local MCP setup with no proxy layer and no extra wrapper code.
