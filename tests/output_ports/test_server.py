# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Direct tests against :class:`OutputPortMcpServer` — same surface as
the stdio loop, but without the subprocess overhead."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")

from fluid_build.output_ports.mcp import (
    MCP_PROTOCOL_VERSION,
    OutputPortMcpServer,
    OutputPortPolicy,
    SERVER_NAME,
    SERVER_VERSION,
)

from ._fixtures import make_expose, write_customer_csv


def _server_with_csv(tmp_path, *, allow_free_form_sql: bool = False):
    csv_path = write_customer_csv(tmp_path / "customers.csv")
    expose = make_expose(
        semantics={
            "name": "customer_profiles",
            "measures": [
                {"name": "customer_count", "agg": "count_distinct", "expr": "customer_id"},
            ],
            "dimensions": [
                {"name": "signup_date", "type": "time"},
            ],
            "metrics": [
                {"name": "active_customers", "type": "simple", "measure": "customer_count"},
            ],
        },
        binding={
            "platform": "local",
            "format": "csv",
            "location": {
                "path": str(csv_path),
                "table": "customer_profiles",
            },
        },
    )
    contract = {"fluidVersion": "0.7.3", "exposes": [expose]}
    policy = OutputPortPolicy(
        contract_path=tmp_path / "contract.fluid.yaml",
        allow_free_form_sql=allow_free_form_sql,
        max_sample_rows=10,
    )
    server = OutputPortMcpServer(contract=contract, expose=expose, policy=policy)
    return server, expose


def test_initialize_returns_protocol_version(tmp_path):
    server, _ = _server_with_csv(tmp_path)
    response = server.handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
    )
    assert response["id"] == 1
    assert response["result"]["protocolVersion"] == MCP_PROTOCOL_VERSION
    info = response["result"]["serverInfo"]
    assert info["name"] == SERVER_NAME
    assert info["version"] == SERVER_VERSION


def test_tools_list_includes_describe_sample_query(tmp_path):
    server, _ = _server_with_csv(tmp_path)
    response = server.handle_request({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    names = {tool["name"] for tool in response["result"]["tools"]}
    assert {"describe", "sample", "query"} <= names
    assert "query_sql" not in names  # default policy hides the gated tool


def test_tools_list_with_allow_sql_includes_query_sql(tmp_path):
    server, _ = _server_with_csv(tmp_path, allow_free_form_sql=True)
    response = server.handle_request({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    names = {tool["name"] for tool in response["result"]["tools"]}
    assert "query_sql" in names


def test_describe_returns_contract_metadata(tmp_path):
    server, expose = _server_with_csv(tmp_path)
    response = server.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "describe", "arguments": {}},
        }
    )
    assert response["result"]["isError"] is False
    payload = response["result"]["structuredContent"]
    assert payload["exposeId"] == expose["exposeId"]
    assert payload["engine"]["dialect"] == "duckdb"
    assert payload["engine"]["tableReference"] == "customer_profiles"


def test_sample_returns_rows(tmp_path):
    server, _ = _server_with_csv(tmp_path)
    response = server.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {"name": "sample", "arguments": {"limit": 3}},
        }
    )
    payload = response["result"]["structuredContent"]
    assert payload["rowCount"] == 3
    assert "customer_id" in payload["columns"]


def test_sample_caps_at_max_rows(tmp_path):
    server, _ = _server_with_csv(tmp_path)
    # max_sample_rows=10 in fixture; ask for 50 → cap to 10 silently.
    response = server.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {"name": "sample", "arguments": {"limit": 50}},
        }
    )
    payload = response["result"]["structuredContent"]
    assert payload["rowCount"] <= 10


def test_query_sql_blocked_without_allow_sql(tmp_path):
    server, _ = _server_with_csv(tmp_path)
    response = server.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 6,
            "method": "tools/call",
            "params": {
                "name": "query_sql",
                "arguments": {"sql": "SELECT 1", "limit": 1},
            },
        }
    )
    assert "error" in response
    assert response["error"]["code"] == -32001
    assert "--allow-sql" in response["error"]["message"]


def test_query_sql_runs_with_allow_sql(tmp_path):
    server, _ = _server_with_csv(tmp_path, allow_free_form_sql=True)
    response = server.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {
                "name": "query_sql",
                "arguments": {
                    "sql": "SELECT customer_id FROM customer_profiles ORDER BY customer_id",
                    "limit": 2,
                },
            },
        }
    )
    payload = response["result"]["structuredContent"]
    assert payload["columns"] == ["customer_id"]
    assert payload["rowCount"] == 2


def test_resources_list_advertises_contract_and_expose(tmp_path):
    server, _ = _server_with_csv(tmp_path)
    response = server.handle_request({"jsonrpc": "2.0", "id": 8, "method": "resources/list"})
    uris = [resource["uri"] for resource in response["result"]["resources"]]
    assert any(uri.startswith("fluid://expose/") for uri in uris)
    assert any(uri.startswith("fluid://semantics/") for uri in uris)


def test_resources_read_expose_returns_json(tmp_path):
    server, expose = _server_with_csv(tmp_path)
    response = server.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 9,
            "method": "resources/read",
            "params": {"uri": f"fluid://expose/{expose['exposeId']}"},
        }
    )
    contents = response["result"]["contents"]
    assert contents[0]["mimeType"] == "application/json"
    parsed = json.loads(contents[0]["text"])
    assert parsed["exposeId"] == expose["exposeId"]


def test_unknown_method_returns_jsonrpc_error(tmp_path):
    server, _ = _server_with_csv(tmp_path)
    response = server.handle_request({"jsonrpc": "2.0", "id": 10, "method": "tools/missing"})
    assert response["error"]["code"] == -32601


# ---------------------------------------------------------------------
# Security regressions — validated end-to-end through the server
# dispatcher (not just the compiler) so the CLI surface is locked in.
# ---------------------------------------------------------------------


def _server_with_restricted_email(tmp_path, *, allow_free_form_sql: bool = True):
    csv_path = write_customer_csv(tmp_path / "customers.csv")
    expose = make_expose(
        binding={
            "platform": "local",
            "format": "csv",
            "location": {"path": str(csv_path), "table": "customer_profiles"},
        },
        column_restrictions=[
            {"principal": "*", "columns": ["email"], "access": "deny"}
        ],
    )
    contract = {"fluidVersion": "0.7.3", "exposes": [expose]}
    policy = OutputPortPolicy(
        contract_path=tmp_path / "contract.fluid.yaml",
        allow_free_form_sql=allow_free_form_sql,
        max_sample_rows=10,
    )
    return OutputPortMcpServer(contract=contract, expose=expose, policy=policy), expose


def test_query_sql_rejects_aliased_restricted_column(tmp_path):
    """Vuln-2 end-to-end: ``SELECT email AS not_email`` must be
    rejected when ``email`` is in the contract's column-restriction
    deny list."""
    server, _ = _server_with_restricted_email(tmp_path)
    response = server.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "query_sql",
                "arguments": {
                    "sql": "SELECT email AS not_email, customer_id FROM customer_profiles",
                    "limit": 10,
                },
            },
        }
    )
    assert "error" in response
    assert response["error"]["code"] == -32602
    assert "restricted" in response["error"]["message"].lower()


def test_query_sql_rejects_tab_separated_union_select(tmp_path):
    """Vuln-1 end-to-end: tab-separated UNION ALL SELECT used to
    bypass the reserved-word allowlist; the dispatcher must now
    reject the whole call."""
    server, _ = _server_with_restricted_email(tmp_path)
    response = server.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "query_sql",
                "arguments": {
                    "sql": "SELECT\tcustomer_id\tFROM\tcustomer_profiles\tUNION\tALL\tSELECT\t1\tFROM\tx",
                    "limit": 10,
                },
            },
        }
    )
    assert "error" in response
    assert response["error"]["code"] == -32602


def test_query_sql_unrestricted_query_still_works(tmp_path):
    """Sanity: a clean SELECT against an unrestricted column still
    executes after the security fix."""
    server, _ = _server_with_restricted_email(tmp_path)
    response = server.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "query_sql",
                "arguments": {
                    "sql": "SELECT customer_id FROM customer_profiles ORDER BY customer_id",
                    "limit": 2,
                },
            },
        }
    )
    payload = response["result"]["structuredContent"]
    assert payload["columns"] == ["customer_id"]
    assert payload["rowCount"] == 2
