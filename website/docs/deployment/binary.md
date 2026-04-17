---
sidebar_position: 1
title: Binary Deployment
---

# Binary Deployment

Binary deployment is the simplest way to run KafkaMCP in production or on a workstation.

## Install from GitHub Releases

KafkaMCP publishes release archives from GitHub Actions and GoReleaser.

```bash
curl -sSL https://github.com/kafkamcp/kafkamcp/releases/latest/download/kafkamcp_$(uname -s | tr '[:upper:]' '[:lower:]')_$(uname -m).tar.gz | tar xz
sudo mv kafkamcp /usr/local/bin/
```

Then verify the install:

```bash
kafkamcp version
```

## Install with `go install`

If you already use Go, install directly from source:

```bash
go install github.com/kafkamcp/kafkamcp/cmd/kafkamcp@latest
```

## Platform support

KafkaMCP release builds target:

- Linux amd64
- Linux arm64
- macOS amd64
- macOS arm64
- Windows amd64
- Windows arm64

The release pipeline builds archives with `CGO_ENABLED=0`, which keeps deployment simple and portable.

:::tip
For most operators, the binary is the fastest path: one executable, one config file, no sidecar processes.
:::

## Run KafkaMCP

```bash
kafkamcp --config /etc/kafkamcp/kafkamcp.yaml
```

For networked deployments, use `sse` or `streamable-http` in the config.

## Example systemd unit

Use a dedicated user and a config file under `/etc/kafkamcp/`.

```ini
[Unit]
Description=KafkaMCP
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=kafkamcp
Group=kafkamcp
WorkingDirectory=/var/lib/kafkamcp
ExecStart=/usr/local/bin/kafkamcp --config /etc/kafkamcp/kafkamcp.yaml
Restart=on-failure
RestartSec=5
LimitNOFILE=65536
Environment=KAFKA_USERNAME=agent
Environment=KAFKA_PASSWORD=change-me
Environment=SR_USERNAME=agent
Environment=SR_PASSWORD=change-me

[Install]
WantedBy=multi-user.target
```

## Example systemd workflow

```bash
sudo useradd --system --home /var/lib/kafkamcp --shell /usr/sbin/nologin kafkamcp
sudo mkdir -p /etc/kafkamcp /var/lib/kafkamcp
sudo chown -R kafkamcp:kafkamcp /var/lib/kafkamcp
sudo cp kafkamcp.yaml /etc/kafkamcp/kafkamcp.yaml
sudo cp kafkamcp.service /etc/systemd/system/kafkamcp.service
sudo systemctl daemon-reload
sudo systemctl enable --now kafkamcp
sudo systemctl status kafkamcp
```

## Validate before rollout

```bash
kafkamcp validate --config /etc/kafkamcp/kafkamcp.yaml
```

## Graceful shutdown

KafkaMCP handles `SIGINT` and `SIGTERM` for graceful shutdown. When a signal is received:

1. The server stops accepting new connections.
2. In-flight requests are given up to **10 seconds** to complete (the drain period).
3. The metrics/health server is shut down with the same 10-second drain window.
4. The process exits cleanly.

For systemd deployments, the default `TimeoutStopSec=90s` is more than sufficient.

## Operational notes

- main HTTP endpoint uses `server.port`
- Prometheus metrics listen on `server.port + 1`
- set `audit.log_file` if you want durable JSONL audit logs
- use environment variables for secrets instead of hard-coding credentials

Binary deployment is the cleanest option when you want predictable startup, direct process supervision, and minimal runtime dependencies.
