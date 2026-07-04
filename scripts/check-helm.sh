#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
chart="$repo_root/deploy/helm/kafkamcp"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

helm lint "$chart"

helm template kafkamcp "$chart" >"$tmp_dir/default.yaml"
grep -q 'metrics_host: 0.0.0.0' "$tmp_dir/default.yaml"
grep -q 'path: /livez' "$tmp_dir/default.yaml"
grep -q 'path: /ready' "$tmp_dir/default.yaml"
grep -q 'containerPort: 3101' "$tmp_dir/default.yaml"

helm template kafkamcp "$chart" \
  --set persistence.enabled=true >"$tmp_dir/persistent.yaml"
grep -q 'kind: PersistentVolumeClaim' "$tmp_dir/persistent.yaml"
grep -q 'backend: sqlite' "$tmp_dir/persistent.yaml"
grep -q 'path: /var/lib/kafkamcp/state.db' "$tmp_dir/persistent.yaml"
grep -q 'type: Recreate' "$tmp_dir/persistent.yaml"
grep -q 'mountPath: /var/lib/kafkamcp' "$tmp_dir/persistent.yaml"

helm template kafkamcp "$chart" \
  --set service.sessionAffinity=ClientIP >"$tmp_dir/affinity.yaml"
grep -q 'sessionAffinity: ClientIP' "$tmp_dir/affinity.yaml"

helm template kafkamcp "$chart" \
  --set config.telemetry.enabled=true \
  --set networkPolicy.telemetry.enabled=true >"$tmp_dir/telemetry.yaml"
grep -q 'port: 4317' "$tmp_dir/telemetry.yaml"
grep -q 'port: 4318' "$tmp_dir/telemetry.yaml"

if helm template kafkamcp "$chart" --set replicaCount=2 --set persistence.enabled=true >/dev/null 2>&1; then
  echo "expected multi-replica SQLite rendering to fail" >&2
  exit 1
fi

if helm template kafkamcp "$chart" \
  --set autoscaling.enabled=true \
  --set persistence.enabled=true >/dev/null 2>&1; then
  echo "expected autoscaled SQLite rendering to fail" >&2
  exit 1
fi

if helm template kafkamcp "$chart" \
  --set replicaCount=2 \
  --set 'config.server.tool_packs[0]=core-read' \
  --set 'config.server.tool_packs[1]=ops-write' >/dev/null 2>&1; then
  echo "expected multi-replica guarded-pack rendering to fail" >&2
  exit 1
fi

echo "Helm production smoke checks passed."
