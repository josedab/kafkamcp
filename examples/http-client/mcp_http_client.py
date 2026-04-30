#!/usr/bin/env python3
"""Minimal HTTP client for KafkaMCP using streamable-http transport.

Usage:
    1. Start KafkaMCP:  kafkamcp --config kafkamcp.yaml --transport streamable-http
    2. Run this script:  python3 mcp_http_client.py

Requires: Python 3.8+ (no third-party dependencies).
"""

import json
import sys
import urllib.request

BASE_URL = "http://localhost:3100/mcp"
HEADERS = {"Content-Type": "application/json"}


def rpc(method: str, params: dict | None = None, req_id: int = 1) -> dict:
    """Send a JSON-RPC 2.0 request and return the parsed response."""
    body = {
        "jsonrpc": "2.0",
        "id": req_id,
        "method": method,
        "params": params or {},
    }
    req = urllib.request.Request(
        BASE_URL,
        data=json.dumps(body).encode(),
        headers=HEADERS,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def main() -> None:
    # 1. Initialize
    print("=== Initialize ===")
    init_resp = rpc("initialize", {
        "protocolVersion": "2025-03-26",
        "capabilities": {},
        "clientInfo": {"name": "python-example", "version": "1.0.0"},
    }, req_id=1)
    print(json.dumps(init_resp, indent=2))

    # 2. List tools
    print("\n=== List Tools ===")
    tools_resp = rpc("tools/list", req_id=2)
    tools = tools_resp.get("result", {}).get("tools", [])
    for tool in tools:
        print(f"  - {tool['name']}: {tool.get('description', '')[:80]}")

    # 3. List topics
    print("\n=== List Topics ===")
    topics_resp = rpc("tools/call", {
        "name": "kafka_list_topics",
        "arguments": {},
    }, req_id=3)
    print(json.dumps(topics_resp, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        print("Make sure KafkaMCP is running with --transport streamable-http", file=sys.stderr)
        sys.exit(1)
