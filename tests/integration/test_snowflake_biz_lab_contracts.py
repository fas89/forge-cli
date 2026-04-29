# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Snowflake-biz-lab integration tests.

The ``snowflake-biz-lab`` repo (sibling checkout) carries production-grade
Snowflake contracts that the FLUID team uses for live demos and rehearsals.
This suite uses those contracts as a regression fixture: every schema /
provider change in forge-cli must keep biz-lab parseable, validate-able,
and plan-able.

Skips cleanly when the sibling repo isn't available (e.g., on CI not
configured to clone biz-lab alongside).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import List

import pytest
import yaml


def _biz_lab_root() -> Path | None:
    """Resolve the snowflake-biz-lab/fluid/contracts directory if present.

    Lookup order:
    1. ``FLUID_BIZ_LAB_DIR`` env var (an absolute path).
    2. ``../snowflake-biz-lab`` relative to the forge-cli repo root.
    """
    override = os.environ.get("FLUID_BIZ_LAB_DIR")
    if override:
        p = Path(override)
        if p.exists():
            return p / "fluid" / "contracts"
        return None

    forge_cli_root = Path(__file__).resolve().parents[2]
    sibling = forge_cli_root.parent / "snowflake-biz-lab" / "fluid" / "contracts"
    return sibling if sibling.exists() else None


def _list_biz_lab_contracts(contracts_dir: Path) -> List[Path]:
    """Return all ``contract.fluid.yaml`` files under the biz-lab contracts dir."""
    return sorted(contracts_dir.glob("**/contract.fluid.yaml"))


pytestmark = pytest.mark.skipif(
    _biz_lab_root() is None,
    reason="snowflake-biz-lab not available (set FLUID_BIZ_LAB_DIR or clone alongside forge-cli)",
)


@pytest.fixture
def biz_lab_contracts() -> List[Path]:
    root = _biz_lab_root()
    assert root is not None
    return _list_biz_lab_contracts(root)


# ── Schema regression ─────────────────────────────────────────────────────


class TestBizLabSchemaParity:
    def test_repo_has_at_least_one_contract(self, biz_lab_contracts: List[Path]):
        # Sanity: the repo must hold real fixtures, not be empty.
        assert len(biz_lab_contracts) >= 1, (
            "snowflake-biz-lab/fluid/contracts has no contract.fluid.yaml files; "
            "did you check out the wrong revision?"
        )

    def test_each_contract_yaml_parses(self, biz_lab_contracts: List[Path]):
        for path in biz_lab_contracts:
            try:
                doc = yaml.safe_load(path.read_text(encoding="utf-8"))
            except yaml.YAMLError as exc:
                pytest.fail(f"{path}: YAML parse error: {exc}")
            assert isinstance(doc, dict), f"{path}: top-level must be a mapping"
            assert doc.get("kind") == "DataProduct", f"{path}: expected kind=DataProduct"
            assert doc.get("id"), f"{path}: missing id"

    def test_each_contract_validates_under_current_schema(
        self, biz_lab_contracts: List[Path]
    ):
        # The biz-lab repo is a regression fixture — anything we add or
        # change in fluid_build/schemas/fluid-schema-0.7.3.json must
        # keep these contracts valid. If a biz-lab contract pins
        # fluidVersion 0.7.2 we coerce to that schema; otherwise the
        # default (latest bundled) schema is used.
        from fluid_build.schema_manager import FluidSchemaManager

        sm = FluidSchemaManager()
        failures = []
        for path in biz_lab_contracts:
            doc = yaml.safe_load(path.read_text(encoding="utf-8"))
            result = sm.validate_contract(
                doc,
                schema_version=doc.get("fluidVersion"),
                strict=False,
                offline_only=True,
            )
            if not result.is_valid:
                # Surface the first error — full lists can be huge.
                failures.append((path, result.errors[0] if result.errors else "unknown"))
        if failures:
            msg = "\n".join(f"  {p}: {m}" for p, m in failures)
            pytest.fail(f"biz-lab contracts failed schema validation:\n{msg}")


# ── Loader regression: contracts load with overlay machinery ──────────────


class TestBizLabLoaderParity:
    def test_each_contract_loads_via_loader(
        self, biz_lab_contracts: List[Path]
    ):
        # Even contracts that use ``${VAR}`` interpolation should load
        # cleanly when no env override is provided — interpolation is
        # lazy and only fires on resolved fields. This catches loader
        # regressions before they hit the user.
        from fluid_build.loader import load_contract

        failures = []
        for path in biz_lab_contracts:
            try:
                load_contract(path)
            except Exception as exc:  # noqa: BLE001
                failures.append((path, str(exc)))
        if failures:
            msg = "\n".join(f"  {p}: {m}" for p, m in failures)
            pytest.fail(f"biz-lab loader regressions:\n{msg}")


# ── Acquisition pattern: future biz-lab contracts can use it ─────────────


class TestBizLabAcquisitionCompatibility:
    """When biz-lab adds a Bronze acquisition contract, our acquisition
    stage extensions must accept it. We assert here against a synthesized
    biz-lab-style contract so the path is locked in.
    """

    def test_synthetic_biz_lab_acquisition_contract_passes_extensions(
        self, tmp_path: Path
    ):
        from fluid_build.cli._acquisition_stage_ext import (
            policy_apply_acquisition,
            schedule_sync_acquisition,
            verify_acquisition,
        )

        contract = {
            "fluidVersion": "0.7.3",
            "kind": "DataProduct",
            "id": "bronze.telco.party_seed",
            "metadata": {
                "layer": "Bronze",
                "owner": {"team": "telco-data-platform", "email": "x@example.com"},
            },
            "builds": [
                {
                    "id": "ingest_party",
                    "pattern": "acquisition",
                    "engine": "duckdb",
                    "execution": {"trigger": {"type": "scheduled", "schedule": "0 6 * * *"}},
                    "properties": {
                        "source": {
                            "kind": "filesystem",
                            "connection": {"uri": "/tmp/party.csv"},
                            "mode": "full_refresh",
                        },
                        "sink": {"format": "parquet"},
                        "catalog": {"register": ["datamesh_manager"]},
                    },
                    "outputs": ["party_raw"],
                }
            ],
            "exposes": [
                {
                    "exposeId": "party_raw",
                    "kind": "table",
                    "binding": {
                        "platform": "snowflake",
                        "format": "snowflake_table",
                        "location": {
                            "database": "TELCO_BRONZE",
                            "schema": "PARTY",
                            "table": "PARTY_RAW",
                        },
                    },
                }
            ],
            "retention": {"runState": "P30D"},
        }
        # All four pipeline-stage extensions accept the contract.
        verify_acquisition(contract, tmp_path)
        schedule_sync_acquisition(contract, tmp_path, orchestrators=["airflow", "cron"])
        policy_apply_acquisition(contract, tmp_path)
