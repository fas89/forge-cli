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

"""Snowflake happy-path coverage for enterprise config resolution and planning."""

from __future__ import annotations

from unittest.mock import patch

from fluid_build.providers.snowflake.plan.planner import plan_actions
from fluid_build.providers.snowflake.util.auth import get_auth_report
from fluid_build.providers.snowflake.util.config import resolve_snowflake_settings


def _native_sql_contract() -> dict:
    return {
        "id": "silver.spec.customers_v1",
        "metadata": {"name": "customer-analytics"},
        "builds": [
            {
                "id": "bootstrap_customers",
                "pattern": "embedded-logic",
                "engine": "sql",
                "properties": {
                    "sql": (
                        'CREATE OR REPLACE TABLE "ANALYTICS"."CURATED"."CUSTOMERS" '
                        "(ID NUMBER, EMAIL VARCHAR)"
                    )
                },
                "execution": {
                    "runtime": {
                        "platform": "snowflake",
                        "resources": {"warehouse": "TRANSFORM_WH", "role": "TRANSFORMER"},
                    }
                },
            }
        ],
        "exposes": [
            {
                "id": "customers",
                "type": "table",
                "binding": {
                    "platform": "snowflake",
                    "format": "snowflake_table",
                    "location": {
                        "account": "{{ env.CONTRACT_ACCOUNT }}",
                        "database": "ANALYTICS",
                        "schema": "CURATED",
                        "table": "CUSTOMERS",
                    },
                    "properties": {"cluster_by": ["REGION", "CUSTOMER_SEGMENT"]},
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


def _dbt_contract() -> dict:
    return {
        "id": "silver.spec.billing_history_v1",
        "metadata": {"name": "billing-history"},
        "builds": [
            {
                "id": "billing_history_model",
                "engine": "dbt",
                "repository": "./models/billing_history",
                "properties": {"model": "billing_history"},
                "execution": {
                    "runtime": {
                        "platform": "snowflake",
                        "resources": {
                            "warehouse": "TRANSFORM_WH",
                            "database": "ANALYTICS",
                            "schema": "SILVER",
                            "role": "TRANSFORMER",
                        },
                    }
                },
            }
        ],
        "exposes": [
            {
                "exposeId": "billing_history_table",
                "kind": "table",
                "binding": {
                    "platform": "snowflake",
                    "format": "snowflake_table",
                    "location": {
                        "database": "ANALYTICS",
                        "schema": "SILVER",
                        "table": "BILLING_HISTORY_V1",
                    },
                },
                "contract": {
                    "schema": [
                        {"name": "billing_account_id", "type": "STRING", "required": True},
                        {"name": "total_amount", "type": "DECIMAL"},
                    ]
                },
            }
        ],
    }


def test_resolve_snowflake_settings_uses_enterprise_precedence(monkeypatch):
    monkeypatch.setenv("CONTRACT_ACCOUNT", "contract-account")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "env-account")
    monkeypatch.setenv("SNOWFLAKE_USER", "env-user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "env-password")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "env-wh")

    with patch(
        "fluid_build.providers.snowflake.util.config._resolve_with_adapter",
        return_value=(None, None),
    ):
        resolved = resolve_snowflake_settings(
            contract=_native_sql_contract(),
            account="explicit-account",
            warehouse="explicit-wh",
        )

    assert resolved["account"] == "explicit-account"
    assert resolved["warehouse"] == "explicit-wh"
    assert resolved["database"] == "ANALYTICS"
    assert resolved["schema"] == "CURATED"
    assert resolved["role"] == "TRANSFORMER"
    assert resolved["user"] == "env-user"
    assert resolved["password"] == "env-password"


def test_get_auth_report_surfaces_provider_readiness():
    config = {
        "account": "acme-account",
        "user": "svc_forge",
        "warehouse": "TRANSFORM_WH",
        "authenticator": "externalbrowser",
    }

    with patch(
        "fluid_build.providers.snowflake.util.auth._get_current_identity",
        side_effect=RuntimeError("connector unavailable"),
    ):
        report = get_auth_report(config)

    assert report["readiness"]["auth_ready"] is True
    assert report["readiness"]["provider_ready"] is False
    assert report["readiness"]["provider_ready_missing"] == ["database", "schema"]
    assert report["connection_test"] == "failed"


def test_plan_actions_supports_modern_native_sql_happy_path():
    actions = plan_actions(
        _native_sql_contract(),
        account="acme-account",
        warehouse="BOOTSTRAP_WH",
        database=None,
        schema="PUBLIC",
    )

    ops = [action["op"] for action in actions]
    assert "sf.database.ensure" in ops
    assert "sf.schema.ensure" in ops
    assert "sf.sql.execute" in ops
    assert "sf.table.ensure" in ops

    sql_action = next(action for action in actions if action["op"] == "sf.sql.execute")
    assert sql_action["database"] == "ANALYTICS"
    assert sql_action["schema"] == "CURATED"

    table_action = next(action for action in actions if action["op"] == "sf.table.ensure")
    assert table_action["cluster_by"] == ["REGION", "CUSTOMER_SEGMENT"]
    assert table_action["database"] == "ANALYTICS"
    assert table_action["schema"] == "CURATED"


def test_plan_actions_supports_dbt_style_contract_without_build_sql():
    actions = plan_actions(
        _dbt_contract(),
        account="acme-account",
        warehouse="BOOTSTRAP_WH",
        database=None,
        schema="PUBLIC",
    )

    ops = [action["op"] for action in actions]
    assert "sf.database.ensure" in ops
    assert "sf.schema.ensure" in ops
    assert "sf.table.ensure" in ops
    assert "sf.sql.execute" not in ops

    table_action = next(action for action in actions if action["op"] == "sf.table.ensure")
    assert table_action["database"] == "ANALYTICS"
    assert table_action["schema"] == "SILVER"
    assert table_action["table"] == "BILLING_HISTORY_V1"
