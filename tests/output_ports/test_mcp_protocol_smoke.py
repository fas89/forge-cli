# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""End-to-end stdio smoke for `fluid mcp output-port serve`.

Spawns the CLI as a subprocess, drives it with a canonical
initialize → tools/list → tools/call sequence, and pins the JSON-RPC
envelope shape (protocol version, server info, tool catalogue,
structured content body).

Mirrors the pattern in tests/test_mcp_protocol_smoke.py for the
authoring server."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")  # ensure the local engine is installed


def _write_contract(tmp_path: Path) -> Path:
    csv_path = tmp_path / "customers.csv"
    csv_path.write_text(
        "customer_id,email,signup_date\n"
        "C0001,alice@example.com,2024-01-15\n"
        "C0002,bob@example.com,2024-02-10\n"
        "C0003,carol@example.com,2024-03-05\n",
        encoding="utf-8",
    )
    contract_path = tmp_path / "contract.fluid.yaml"
    contract_path.write_text(
        textwrap.dedent(
            f"""\
            fluidVersion: "0.7.3"
            kind: DataProduct
            id: gold.test.customers_v1
            name: Customers smoke
            metadata:
              domain: customer
              owner:
                team: qa
                email: qa@example.com
            exposes:
              - exposeId: customer_profiles
                kind: table
                contract:
                  schema:
                    - name: customer_id
                      type: STRING
                      required: true
                    - name: email
                      type: STRING
                    - name: signup_date
                      type: DATE
                binding:
                  platform: local
                  format: csv
                  location:
                    path: {csv_path}
                    table: customer_profiles
                semantics:
                  name: customer_profiles
                  measures:
                    - name: customer_count
                      agg: count_distinct
                      expr: customer_id
                  dimensions:
                    - name: signup_date
                      type: time
                  metrics:
                    - name: active_customers
                      type: simple
                      measure: customer_count
            """
        ),
        encoding="utf-8",
    )
    return contract_path


def test_mcp_output_port_stdio_initialize_tools_list_and_call(tmp_path: Path):
    repo_root = Path(__file__).resolve().parents[2]
    contract_path = _write_contract(tmp_path)
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root)
    env["FLUID_QUIET"] = "1"
    env["FLUID_NONINTERACTIVE"] = "1"

    messages = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "clientInfo": {"name": "pytest-output-port-smoke", "version": "1.0.0"},
                "capabilities": {},
            },
        },
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "describe", "arguments": {}},
        },
        {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {"name": "sample", "arguments": {"limit": 2}},
        },
        {
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "query",
                "arguments": {
                    "metric": "active_customers",
                    "dimensions": ["signup_date"],
                    "limit": 10,
                },
            },
        },
        {"jsonrpc": "2.0", "id": 6, "method": "resources/list", "params": {}},
    ]

    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "fluid_build",
            "mcp",
            "output-port",
            "serve",
            str(contract_path),
            "--expose-id",
            "customer_profiles",
            "--max-sample-rows",
            "10",
        ],
        input="\n".join(json.dumps(message) for message in messages) + "\n",
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert proc.returncode == 0, proc.stderr

    responses = [
        json.loads(line) for line in proc.stdout.splitlines() if line.strip().startswith("{")
    ]
    by_id = {response["id"]: response for response in responses}
    assert sorted(by_id.keys()) == [1, 2, 3, 4, 5, 6], proc.stdout

    init = by_id[1]
    assert init["result"]["protocolVersion"] == "2025-06-18"
    assert init["result"]["serverInfo"]["name"] == "forge-cli-output-port-mcp"

    tools = {tool["name"]: tool for tool in by_id[2]["result"]["tools"]}
    assert {"describe", "sample", "query"} <= tools.keys()
    assert "query_sql" not in tools  # default policy hides the gated tool
    for tool in tools.values():
        assert tool["inputSchema"]["type"] == "object"

    describe = json.loads(by_id[3]["result"]["content"][0]["text"])
    assert describe["exposeId"] == "customer_profiles"
    assert describe["engine"]["dialect"] == "duckdb"

    sample = json.loads(by_id[4]["result"]["content"][0]["text"])
    assert sample["rowCount"] == 2
    assert "customer_id" in sample["columns"]

    query = json.loads(by_id[5]["result"]["content"][0]["text"])
    assert "customer_count" in query["columns"]
    assert "GROUP BY signup_date" in query["compiledSql"]

    resources = by_id[6]["result"]["resources"]
    uris = [resource["uri"] for resource in resources]
    assert any(uri.startswith("fluid://expose/") for uri in uris)
