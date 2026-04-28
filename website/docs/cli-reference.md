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

### `kafkamcp validate-config`

Validate a configuration file with detailed output. Unlike `validate`, this command accepts a positional file argument and prints a summary of the parsed configuration (transport, clusters, policies, masking).

```bash
kafkamcp validate-config /path/to/kafkamcp.yaml
kafkamcp validate-config --config /path/to/kafkamcp.yaml
```

**Example output (success):**

```
Configuration /path/to/kafkamcp.yaml is valid.
  Transport: stdio
  Clusters:  1
    [0] local (localhost:9092)
  Policy:    default_deny=false (open access)
```

**Example output (failure):**

```
Error: configuration validation failed: at least one cluster is required
```

### `kafkamcp serve`

Start the MCP server. This is an alias for running `kafkamcp` without a subcommand — the behavior is identical.

```bash
kafkamcp serve --config /path/to/kafkamcp.yaml
kafkamcp serve --config kafkamcp.yaml --log-level debug
```

Use `serve` when you want an explicit subcommand for clarity in scripts or process managers.

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

## Exit codes

KafkaMCP uses standard exit codes for scripting and CI/CD integration.

| Code | Meaning | Example cause |
|------|---------|---------------|
| `0` | Success | Server shut down cleanly, config validated, version printed. |
| `1` | General error | Invalid flags, unsupported transport, server startup failure. |
| `2` | Configuration error | Config file not found, YAML parse error, validation failure (`kafkamcp validate`). |

In CI/CD pipelines you can rely on the exit code to gate deployments:

```bash
kafkamcp validate --config kafkamcp.yaml || exit 1
```

Error details are printed to stderr as structured JSON when `--log-level` is set, or as plain text otherwise.

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

## Environment variables

KafkaMCP recognizes the following environment variables at runtime:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKAMCP_TRANSPORT` | — | Overrides `server.transport` from the config file. Valid values: `stdio`, `sse`, `streamable-http`. |
| `KAFKAMCP_LOG_LEVEL` | — | Overrides `server.log_level` from the config file. Valid values: `debug`, `info`, `warn`, `error`. The `--log-level` CLI flag takes precedence over this variable. |
| `KAFKAMCP_ALLOW_INSECURE` | — | Set to `true` to allow `insecure_skip_verify` in `connect_tls` settings. Required as an explicit opt-in for skipping TLS certificate verification. **Not recommended for production.** |

Additionally, KafkaMCP expands `${VAR}` and `${VAR:-default}` syntax in the YAML config file using process environment variables. See [Configuration](/docs/configuration) for details.
