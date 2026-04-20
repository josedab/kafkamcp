---
sidebar_position: 9
title: CLI Reference
---

# CLI Reference

KafkaMCP is distributed as a single binary named `kafkamcp`. This page documents all subcommands and flags.

## Global flags

| Flag | Short | Default | Description |
|---|---|---|---|
| `--config` | `-c` | `kafkamcp.yaml` | Path to configuration file. |
| `--log-level` | — | — | Override log level (`debug`, `info`, `warn`, `error`). When set, this takes precedence over `server.log_level` in the config file. |
| `--debug` | — | `false` | Enable debug logging (shorthand for `--log-level debug`). |

## Commands

### `kafkamcp` (root)

Start the MCP server using the configuration file.

```bash
kafkamcp --config /path/to/kafkamcp.yaml
```

The server runs in the foreground. Use the `server.transport` config value to control the transport mode (`stdio`, `sse`, or `streamable-http`).

**Override log level at runtime:**

```bash
kafkamcp --config kafkamcp.yaml --log-level debug
```

The `--log-level` flag overrides the `server.log_level` value in the configuration file without modifying it.

### `kafkamcp version`

Print the version string and exit.

```bash
kafkamcp version
```

**Example output:**

```
kafkamcp 0.1.0
```

### `kafkamcp validate`

Validate the configuration file and exit. Returns a non-zero exit code if validation fails.

```bash
kafkamcp validate --config /path/to/kafkamcp.yaml
```

**Example output (success):**

```
Configuration is valid.
```

**Example output (failure):**

```
Error: configuration validation failed: at least one cluster is required
```

Use this before deploying config changes to catch errors early.

### `kafkamcp init`

Generate a starter configuration file. Writes to stdout by default or to a file with `--output`.

```bash
kafkamcp init
kafkamcp init --transport sse --schema-registry http://localhost:8081
kafkamcp init --output kafkamcp.yaml
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--output` | `-o` | — | Write config to file instead of stdout. |
| `--transport` | — | `stdio` | Transport type (`stdio`, `sse`, `streamable-http`). |
| `--bootstrap-servers` | — | `localhost:9092` | Kafka bootstrap servers. |
| `--schema-registry` | — | — | Schema Registry URL (optional). |

## Examples

**Start with stdio transport (Claude Desktop):**

```bash
kafkamcp --config kafkamcp.yaml
```

**Start with verbose logging for debugging:**

```bash
kafkamcp --config kafkamcp.yaml --log-level debug
```

**Validate config in CI/CD pipeline:**

```bash
kafkamcp validate --config kafkamcp.yaml && echo "Config OK"
```

**Check installed version:**

```bash
kafkamcp version
```
