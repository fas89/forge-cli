#!/usr/bin/env python3
# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Certify the consumer MCP output-port server against real clients.

Mirrors :mod:`scripts.mcp_client_certify` but for the consumer-side
server (``fluid mcp output-port serve``). Three checks:

1. Direct JSON-RPC lifecycle: initialize → tools/list →
   ``describe`` / ``sample`` / ``query``. No external tooling.
2. MCP Inspector CLI ``tools/list`` and ``tools/call`` against the
   stdio server, when ``npx`` is installed.
3. Claude Code config health-check via ``claude mcp get`` when the
   ``claude`` CLI is on PATH.

Designed to be run before cutting a release; emits a JSON or
human-friendly report and exits non-zero when any required check
fails. Optional clients are reported as "skipped" rather than
failing.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class CertificationCheck:
    name: str
    status: str  # "ok" / "fail" / "skipped"
    detail: str = ""
    stdout_tail: str = ""
    stderr_tail: str = ""


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _server_command(contract_path: Path, *, expose_id: str) -> List[str]:
    return [
        sys.executable,
        "-m",
        "fluid_build",
        "mcp",
        "output-port",
        "serve",
        str(contract_path),
        "--expose-id",
        expose_id,
        "--max-sample-rows",
        "10",
    ]


def _base_env(repo_root: Path) -> Dict[str, str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root)
    env["FLUID_QUIET"] = "1"
    env["FLUID_NONINTERACTIVE"] = "1"
    return env


def _tail(text: str, limit: int = 2000) -> str:
    return (text or "")[-limit:]


def _write_demo_contract(workspace: Path) -> Tuple[Path, str]:
    """Write a tiny self-contained DuckDB-backed contract for the
    cert run. Returns (contract_path, expose_id)."""
    csv_path = workspace / "customers.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["customer_id", "email", "signup_date"])
        for customer_id, email, signup in [
            ("C0001", "alice@example.com", "2024-01-15"),
            ("C0002", "bob@example.com", "2024-02-10"),
            ("C0003", "carol@example.com", "2024-03-05"),
        ]:
            writer.writerow([customer_id, email, signup])
    contract_path = workspace / "contract.fluid.yaml"
    contract_path.write_text(
        f"""fluidVersion: "0.7.3"
kind: DataProduct
id: gold.cert.customers_v1
name: Cert customers
metadata:
  layer: Gold
  owner:
    team: certifier
    email: cert@example.com
  businessContext:
    domain: Test
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
""",
        encoding="utf-8",
    )
    return contract_path, "customer_profiles"


def _check_direct_jsonrpc(
    repo_root: Path, contract_path: Path, expose_id: str, timeout: int
) -> CertificationCheck:
    env = _base_env(repo_root)
    messages = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "clientInfo": {"name": "forge-cli-output-port-certifier", "version": "1.0.0"},
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
        _server_command(contract_path, expose_id=expose_id),
        cwd=str(contract_path.parent),
        env=env,
        input="\n".join(json.dumps(message) for message in messages) + "\n",
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if proc.returncode != 0:
        return CertificationCheck(
            name="direct_jsonrpc",
            status="fail",
            detail=f"server exited with code {proc.returncode}",
            stdout_tail=_tail(proc.stdout),
            stderr_tail=_tail(proc.stderr),
        )
    responses: List[Dict[str, Any]] = []
    for line in proc.stdout.splitlines():
        candidate = line.strip()
        if not candidate.startswith("{"):
            continue
        try:
            responses.append(json.loads(candidate))
        except json.JSONDecodeError:
            continue
    by_id = {response.get("id"): response for response in responses}
    expected_ids = [1, 2, 3, 4, 5, 6]
    missing = [eid for eid in expected_ids if eid not in by_id]
    if missing:
        return CertificationCheck(
            name="direct_jsonrpc",
            status="fail",
            detail=f"missing JSON-RPC responses for ids {missing}",
            stdout_tail=_tail(proc.stdout),
            stderr_tail=_tail(proc.stderr),
        )
    init = by_id[1].get("result", {})
    if init.get("protocolVersion") != "2025-06-18":
        return CertificationCheck(
            name="direct_jsonrpc",
            status="fail",
            detail=f"unexpected protocol version {init.get('protocolVersion')}",
            stdout_tail=_tail(proc.stdout),
        )
    tools = {tool.get("name") for tool in by_id[2].get("result", {}).get("tools", [])}
    required_tools = {"describe", "sample", "query"}
    missing_tools = sorted(required_tools - tools)
    if missing_tools:
        return CertificationCheck(
            name="direct_jsonrpc",
            status="fail",
            detail=f"tools/list missing required tools: {missing_tools}",
            stdout_tail=_tail(proc.stdout),
        )
    return CertificationCheck(
        name="direct_jsonrpc",
        status="ok",
        detail=f"all 6 lifecycle messages succeeded; tools advertised: {sorted(tools)}",
    )


def _check_mcp_inspector(
    repo_root: Path, contract_path: Path, expose_id: str, timeout: int
) -> CertificationCheck:
    if shutil.which("npx") is None:
        return CertificationCheck(
            name="mcp_inspector",
            status="skipped",
            detail="npx not installed; install Node.js and re-run for full certification",
        )
    env = _base_env(repo_root)
    base = [
        "npx",
        "--yes",
        "@modelcontextprotocol/inspector",
        "--cli",
        "--transport",
        "stdio",
    ]
    server = ["--", *_server_command(contract_path, expose_id=expose_id)]
    inspector_args = [*base, "--method", "tools/list", *server]
    proc = subprocess.run(
        inspector_args,
        cwd=str(contract_path.parent),
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if proc.returncode != 0:
        return CertificationCheck(
            name="mcp_inspector",
            status="fail",
            detail=f"inspector exited with code {proc.returncode}",
            stdout_tail=_tail(proc.stdout),
            stderr_tail=_tail(proc.stderr),
        )
    if "describe" not in proc.stdout:
        return CertificationCheck(
            name="mcp_inspector",
            status="fail",
            detail="inspector tools/list did not advertise the 'describe' tool",
            stdout_tail=_tail(proc.stdout),
        )
    return CertificationCheck(
        name="mcp_inspector",
        status="ok",
        detail="tools/list advertises the consumer tool surface",
    )


def _check_claude_cli(repo_root: Path) -> CertificationCheck:
    claude_path = shutil.which("claude")
    if claude_path is None:
        return CertificationCheck(
            name="claude_mcp_get",
            status="skipped",
            detail="claude CLI not installed; skip Claude Code project-config check",
        )
    env = _base_env(repo_root)
    proc = subprocess.run(
        [claude_path, "mcp", "list"],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if proc.returncode != 0:
        return CertificationCheck(
            name="claude_mcp_get",
            status="skipped",
            detail=(
                "`claude mcp list` failed; ensure Claude Code is configured "
                "with at least one MCP server."
            ),
            stdout_tail=_tail(proc.stdout),
            stderr_tail=_tail(proc.stderr),
        )
    return CertificationCheck(
        name="claude_mcp_get",
        status="ok",
        detail=f"`claude mcp list` returned {len(proc.stdout.splitlines())} entries",
    )


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Certify forge-cli's MCP output-port server against real clients."
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON output.")
    parser.add_argument(
        "--timeout", type=int, default=120, help="Per-check timeout in seconds."
    )
    args = parser.parse_args(argv)

    repo_root = _repo_root()
    with tempfile.TemporaryDirectory(prefix="output-port-cert-") as workspace:
        workspace_path = Path(workspace)
        contract_path, expose_id = _write_demo_contract(workspace_path)
        checks: List[CertificationCheck] = [
            _check_direct_jsonrpc(repo_root, contract_path, expose_id, args.timeout),
            _check_mcp_inspector(repo_root, contract_path, expose_id, args.timeout),
            _check_claude_cli(repo_root),
        ]

    overall_status = "ok"
    for check in checks:
        if check.status == "fail":
            overall_status = "fail"
            break
    if args.json:
        print(
            json.dumps(
                {
                    "overall_status": overall_status,
                    "checks": [asdict(check) for check in checks],
                },
                indent=2,
            )
        )
    else:
        print(f"forge-cli MCP output-port certification: {overall_status.upper()}")
        for check in checks:
            print(f"  [{check.status:>7}] {check.name} — {check.detail}")
            if check.stdout_tail and check.status == "fail":
                print(f"    stdout: {check.stdout_tail[-200:]}")
            if check.stderr_tail and check.status == "fail":
                print(f"    stderr: {check.stderr_tail[-200:]}")
    return 0 if overall_status == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
