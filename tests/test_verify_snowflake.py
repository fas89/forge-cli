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

"""Snowflake-specific tests for ``fluid verify``."""

from __future__ import annotations

import argparse
from pathlib import Path
from unittest.mock import patch

from fluid_build.cli.verify import run, verify_snowflake_table


class _MockConnection:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def execute(self, sql, params=None):
        if "INFORMATION_SCHEMA.COLUMNS" in sql:
            return [
                ("ID", "NUMBER", "NO"),
                ("EMAIL", "VARCHAR", "YES"),
            ]
        if "COUNT(*)" in sql:
            return [(1,)]
        return []


def _args(contract: str, strict: bool = False):
    return argparse.Namespace(
        contract=contract,
        expose_id=None,
        strict=strict,
        out=None,
        show_diffs=False,
        env=None,
    )


def test_verify_snowflake_table_returns_match():
    with patch(
        "fluid_build.providers.snowflake.util.config.get_connection_params", return_value={}
    ):
        with patch(
            "fluid_build.providers.snowflake.connection.SnowflakeConnection", _MockConnection
        ):
            result = verify_snowflake_table(
                account="acme-account",
                warehouse="TRANSFORM_WH",
                database="ANALYTICS",
                schema="CURATED",
                table="CUSTOMERS",
                expected_schema=[
                    {"name": "ID", "type": "INTEGER", "required": True},
                    {"name": "EMAIL", "type": "STRING"},
                ],
                user="svc_forge",
                password="secret",
            )

    assert result["status"] == "match"
    assert result["severity"]["level"] == "SUCCESS"
    assert result["dimensions"]["location"]["actual"] == "ANALYTICS.CURATED"


def test_run_routes_snowflake_table_to_verify_function(tmp_path: Path):
    contract_file = tmp_path / "contract.fluid.yaml"
    contract_file.write_text("id: snowflake.test\n")
    contract = {
        "id": "snowflake.test",
        "exposes": [
            {
                "id": "customers",
                "binding": {
                    "platform": "snowflake",
                    "format": "snowflake_table",
                    "location": {
                        "database": "ANALYTICS",
                        "schema": "CURATED",
                        "table": "CUSTOMERS",
                    },
                },
                "contract": {
                    "schema": [
                        {"name": "ID", "type": "INTEGER", "required": True},
                        {"name": "EMAIL", "type": "STRING"},
                    ]
                },
            }
        ],
    }

    with patch("fluid_build.cli.verify.load_contract_with_overlay", return_value=contract):
        with patch(
            "fluid_build.providers.snowflake.util.config.resolve_snowflake_settings",
            return_value={
                "account": "acme-account",
                "warehouse": "TRANSFORM_WH",
                "user": "svc_forge",
                "password": "secret",
                "schema": "CURATED",
            },
        ):
            with patch(
                "fluid_build.cli.verify.verify_snowflake_table",
                return_value={
                    "status": "match",
                    "exists": True,
                    "severity": {
                        "symbol": "🟢",
                        "level": "SUCCESS",
                        "impact": "NONE",
                        "remediation": "NONE",
                        "reason": "All checks passed",
                        "actions": [],
                    },
                    "dimensions": {
                        "structure": {
                            "status": "pass",
                            "matching_fields": ["ID", "EMAIL"],
                            "missing_fields": [],
                            "extra_fields": [],
                            "total_expected": 2,
                            "total_actual": 2,
                        },
                        "types": {"status": "pass", "mismatches": []},
                        "constraints": {"status": "pass", "mismatches": []},
                        "location": {
                            "status": "pass",
                            "expected": "ANALYTICS.CURATED",
                            "actual": "ANALYTICS.CURATED",
                            "message": None,
                        },
                    },
                    "metadata": {"num_rows": 1, "created": None, "modified": None},
                },
            ) as verify_mock:
                exit_code = run(
                    _args(str(contract_file), strict=True),
                    logger=__import__("logging").getLogger("test"),
                )

    assert exit_code == 0
    verify_mock.assert_called_once()
