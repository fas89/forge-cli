# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Regression tests for the forge-packaging review fixes.

Covers the ``fluid forge --seed-from`` ground-truth pipeline (the seed view
must be FLUID-0.7.2-valid and shared by the prompt and the guard; missing
exposes, FLUID inputs, unresolved output ports, YAML dates, failure
classification, directory provenance) plus the packaging metadata for the
``odcs-strict`` extra and the DMM ``requests`` dependency.

The copilot tests drive the real :func:`generate_copilot_artifacts` with the
real ``normalize_generation_payload`` and ``validate_generated_result`` (the
strict FLUID 0.7.2 schema check). Only the network call (``call_llm``) and the
provider adapter lookup are patched.
"""

from __future__ import annotations

import copy
import json
import shutil
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
import yaml

from fluid_build.cli.forge_copilot_prompts import build_system_prompt, build_user_prompt
from fluid_build.cli.forge_copilot_runtime import (
    CopilotGenerationError,
    generate_copilot_artifacts,
)
from fluid_build.cli.forge_copilot_seed import (
    SeedOptions,
    diff_against_seed,
    format_ground_truth_prompt_block,
    load_seed,
    resolve_at_path,
)
from fluid_build.providers.base import ProviderError
from fluid_build.schema_manager import FluidSchemaManager

REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURES = Path(__file__).parent / "fixtures"
BUNDLE_DIR = FIXTURES / "odps" / "product-bitol"
ODCS_FULL = FIXTURES / "odcs" / "contract-full.yaml"
ODCS_MINIMAL = FIXTURES / "odcs" / "contract-minimal.yaml"
UPSTREAM_FULL = FIXTURES / "upstream" / "odcs" / "full-example.v3.2.0.odcs.yaml"
UPSTREAM_ADVENTUREWORKS = (
    FIXTURES / "upstream" / "odcs" / "postgresql-adventureworks.v3.0.0.odcs.yaml"
)

SEED_INPUTS = [
    pytest.param(BUNDLE_DIR, id="odps-bundle-dir"),
    pytest.param(next(BUNDLE_DIR.glob("*.odps.yaml")), id="odps-file"),
    pytest.param(ODCS_FULL, id="odcs-contract-full"),
    pytest.param(ODCS_MINIMAL, id="odcs-contract-minimal"),
    pytest.param(FIXTURES / "odps" / "contracts-only", id="odcs-only-dir"),
    pytest.param(UPSTREAM_FULL, id="upstream-full-example-v3.2.0"),
    pytest.param(UPSTREAM_ADVENTUREWORKS, id="upstream-adventureworks-v3.0.0"),
]


# ---------------------------------------------------------------------------
# Helpers: a scripted "LLM" that copies the seed ground truth verbatim into a
# minimal FLUID 0.7.2 contract, the way the copilot is instructed to.
# ---------------------------------------------------------------------------


def _capability_matrix() -> Dict[str, Any]:
    return {
        "providers": ["local", "gcp", "aws", "snowflake"],
        "templates": {"starter": {}, "analytics": {}},
        "build_engines": ["sql", "python", "dbt"],
    }


def _llm_config():
    from fluid_build.cli.forge_copilot_llm_providers import LlmConfig

    return LlmConfig(
        provider="anthropic",
        model="mock-model",
        endpoint="https://mock.example.com/v1/messages",
        api_key="mock-key",
    )


def _discovery_report():
    from fluid_build.cli.forge_copilot_discovery import DiscoveryReport

    return DiscoveryReport(workspace_roots=["/tmp"])


def _ground_truth_from_prompt(user_prompt: str) -> Dict[str, Any]:
    return json.loads(user_prompt)["seed_ground_truth"]["ground_truth"]


def _contract_from_ground_truth(gt: Dict[str, Any]) -> Dict[str, Any]:
    """Wrap the ground truth (copied verbatim) in the parts the LLM fills in."""
    exposes = []
    for expose in gt["exposes"]:
        first_col = expose["contract"]["schema"][0]["name"] if expose["contract"]["schema"] else "x"
        built = dict(copy.deepcopy(expose))
        built.update(
            {
                "kind": "table",
                "binding": {
                    "platform": "local",
                    "format": "csv",
                    "location": {"path": f"runtime/out/{expose['exposeId']}.csv"},
                },
                "semantics": {
                    "name": expose["exposeId"],
                    "description": "Semantic model.",
                    "entities": [{"name": first_col, "type": "primary"}],
                    "measures": [{"name": "row_count", "agg": "count", "expr": first_col}],
                    "dimensions": [{"name": first_col, "type": "categorical"}],
                    "metrics": [{"name": "rows", "type": "simple", "measure": "row_count"}],
                },
            }
        )
        exposes.append(built)
    contract: Dict[str, Any] = {
        "fluidVersion": "0.7.2",
        "kind": "DataProduct",
        "id": gt.get("id") or "seeded.product",
        "name": gt.get("name") or "Seeded Product",
        "metadata": copy.deepcopy(gt.get("metadata"))
        or {"owner": {"team": "data-team", "email": "data-team@example.com"}},
        "builds": [
            {
                "id": "main_build",
                "pattern": "embedded-logic",
                "engine": "sql",
                "properties": {"sql": "SELECT 1 AS id"},
                "execution": {
                    "trigger": {"type": "manual", "iterations": 1},
                    "runtime": {"platform": "local", "resources": {"cpu": "1", "memory": "2Gi"}},
                },
            }
        ],
        "exposes": exposes,
    }
    for key in ("description", "domain", "tags"):
        if key in gt:
            contract[key] = copy.deepcopy(gt[key])
    contract["metadata"].setdefault("layer", "Bronze")
    return contract


def _payload(contract: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "recommended_template": "starter",
        "recommended_provider": "local",
        "readme_markdown": "# Seeded\n",
        "contract": contract,
    }


class _CopierLlm:
    """Each call reads the seed block from the user prompt and returns a
    contract built from it, optionally transformed per attempt."""

    def __init__(self, transforms: Optional[List[Optional[Callable]]] = None):
        self.transforms = transforms or [None]
        self.calls = 0
        self.user_prompts: List[str] = []
        self.system_prompts: List[str] = []

    def __call__(self, provider_adapter, llm_config, system_prompt, user_prompt):
        self.system_prompts.append(system_prompt)
        self.user_prompts.append(user_prompt)
        contract = _contract_from_ground_truth(_ground_truth_from_prompt(user_prompt))
        transform = self.transforms[min(self.calls, len(self.transforms) - 1)]
        self.calls += 1
        if transform is not None:
            contract = transform(contract)
        return json.dumps(_payload(contract))


def _run_copilot(seed_path: Path, llm: _CopierLlm):
    with (
        patch("fluid_build.cli.forge_copilot_runtime.call_llm", side_effect=llm),
        patch("fluid_build.cli.forge_copilot_runtime.get_llm_provider", return_value=MagicMock()),
    ):
        return generate_copilot_artifacts(
            context={"project_goal": "seeded product"},
            llm_config=_llm_config(),
            discovery_report=_discovery_report(),
            capability_matrix=_capability_matrix(),
            seed_options=SeedOptions(seed_from=seed_path, allow_remote=False),
        )


# ---------------------------------------------------------------------------
# Finding 12 — the seed view must be FLUID 0.7.2-valid and shared by the
# prompt and the guard.
# ---------------------------------------------------------------------------


class TestSeedViewIsFluid072Valid:
    @pytest.mark.parametrize("seed_path", SEED_INPUTS)
    def test_verbatim_copy_passes_strict_validation_and_guard(self, seed_path: Path) -> None:
        seed = load_seed(seed_path, allow_remote=False)
        block = format_ground_truth_prompt_block(seed)
        contract = _contract_from_ground_truth(json.loads(json.dumps(block))["ground_truth"])

        validation = FluidSchemaManager().validate_contract(
            contract, strict=True, offline_only=True
        )
        assert validation.errors == []
        assert diff_against_seed(seed, contract) == []

    @pytest.mark.parametrize("seed_path", SEED_INPUTS)
    def test_copilot_succeeds_first_attempt_with_real_validation(self, seed_path: Path) -> None:
        llm = _CopierLlm()
        result = _run_copilot(seed_path, llm)
        assert llm.calls == 1
        assert result.attempt_reports[0].validation_errors == []

    def test_raw_seed_quality_is_not_required_on_the_candidate(self) -> None:
        """The candidate may drop ODCS-only keys (quality/classification) and
        the guard still passes, while a raw seed copy also passes."""
        seed = load_seed(ODCS_FULL)
        assert diff_against_seed(seed, seed.fluid) == []
        stripped = copy.deepcopy(seed.fluid)
        for fld in stripped["exposes"][0]["contract"]["schema"]:
            fld.pop("quality", None)
        assert diff_against_seed(seed, stripped) == []

    def test_prompt_block_has_no_non_schema_keys(self) -> None:
        seed = load_seed(UPSTREAM_FULL)
        gt = format_ground_truth_prompt_block(seed)["ground_truth"]
        for expose in gt["exposes"]:
            assert "id" not in expose
            assert "exposeId" in expose
            for col in expose["contract"]["schema"]:
                assert "quality" not in col
                assert "classification" not in col
            qos = expose.get("qos") or {}
            # ODCS latency '4' (no unit) is not ISO-8601; it must not be
            # presented as ground truth.
            assert qos.get("freshnessSLO") != "4"

    def test_classification_maps_to_sensitivity_when_in_enum(self) -> None:
        seed = load_seed(UPSTREAM_FULL)
        gt = format_ground_truth_prompt_block(seed)["ground_truth"]
        cols = {c["name"]: c for c in gt["exposes"][0]["contract"]["schema"]}
        assert cols["rcvr_id"]["sensitivity"] == "restricted"
        assert "sensitivity" not in cols["rcvr_cntry_code"]  # 'public' is not a 0.7.2 level

    def test_schema_mutation_still_caught(self) -> None:
        seed = load_seed(ODCS_FULL)
        mutated = copy.deepcopy(seed.fluid)
        mutated["exposes"][0]["contract"]["schema"][0]["type"] = "integer"
        assert diff_against_seed(seed, mutated)

    def test_system_prompt_seed_clause_matches_schema(self) -> None:
        prompt = build_system_prompt(_capability_matrix(), ["sql"], has_seed_ground_truth=True)
        clause = prompt.split("GROUND TRUTH FROM SEED", 1)[1]
        assert "exposeId" in clause
        assert "exposes' ``id``" not in clause
        # The 0.7.2 root schema forbids a top-level governance key.
        assert "governance" not in clause


# ---------------------------------------------------------------------------
# Finding 44 — fewer exposes than the seed must be a mismatch, not IndexError
# ---------------------------------------------------------------------------


class TestMissingExposeIsAMismatch:
    def test_resolve_at_path_out_of_range_returns_none(self) -> None:
        assert resolve_at_path({"exposes": [{"id": "a"}]}, "exposes[3].contract.schema") is None
        assert resolve_at_path({"exposes": {"0": 1}}, "exposes[0]") is None
        assert resolve_at_path({"exposes": "abc"}, "exposes[0].x") is None

    def test_diff_reports_missing_expose(self) -> None:
        seed = load_seed(BUNDLE_DIR)
        candidate = copy.deepcopy(seed.fluid)
        candidate["exposes"] = candidate["exposes"][:1]
        mismatches = diff_against_seed(seed, candidate)
        assert any(m["path"].startswith("exposes[1]") for m in mismatches)

    def test_copilot_repairs_after_dropped_expose(self) -> None:
        def drop_second(contract):
            contract["exposes"] = contract["exposes"][:1]
            return contract

        llm = _CopierLlm([drop_second, None])
        result = _run_copilot(BUNDLE_DIR, llm)
        assert llm.calls == 2
        assert any("exposes[1]" in e for e in result.attempt_reports[0].validation_errors)


# ---------------------------------------------------------------------------
# Finding 45 — FLUID inputs and unresolved output ports must not seed silently
# ---------------------------------------------------------------------------


def _write_fluid_contract(path: Path) -> Path:
    path.write_text(
        yaml.safe_dump(
            {
                "fluidVersion": "0.7.2",
                "kind": "DataProduct",
                "id": "hello.world",
                "name": "Hello",
                "metadata": {"owner": {"team": "t"}},
                "exposes": [],
            }
        )
    )
    return path


class TestSeedRejectsSilentEmptyGroundTruth:
    def test_fluid_contract_file_is_rejected(self, tmp_path: Path) -> None:
        fluid_file = _write_fluid_contract(tmp_path / "contract.fluid.yaml")
        with pytest.raises(ProviderError, match="FLUID contract"):
            load_seed(fluid_file)

    def test_repo_example_fluid_contract_is_rejected(self) -> None:
        example = REPO_ROOT / "examples" / "01-hello-world" / "contract.fluid.yaml"
        if not example.exists():
            pytest.skip("example contract not present")
        with pytest.raises(ProviderError, match="FLUID contract"):
            load_seed(example)

    def test_fluid_contract_in_directory_is_not_taken_as_odps(self, tmp_path: Path) -> None:
        bundle = tmp_path / "bundle"
        shutil.copytree(BUNDLE_DIR, bundle)
        _write_fluid_contract(bundle / "contract.fluid.yaml")
        seed = load_seed(bundle, allow_remote=False)
        assert len(seed.fluid["exposes"]) == 2
        assert len(seed.ground_truth_paths) >= 2

    def test_directory_with_only_a_fluid_contract_is_rejected(self, tmp_path: Path) -> None:
        _write_fluid_contract(tmp_path / "contract.fluid.yaml")
        with pytest.raises(ProviderError):
            load_seed(tmp_path)

    def test_unresolved_output_port_in_directory_raises(self, tmp_path: Path) -> None:
        bundle = tmp_path / "bundle"
        shutil.copytree(BUNDLE_DIR, bundle)
        (bundle / "commerce.orders-product.daily_orders.odcs.yaml").unlink()
        with pytest.raises(ProviderError, match="daily_orders"):
            load_seed(bundle, allow_remote=False)

    def test_unresolved_output_port_in_odps_file_raises(self, tmp_path: Path) -> None:
        bundle = tmp_path / "bundle"
        shutil.copytree(BUNDLE_DIR, bundle)
        (bundle / "commerce.orders-product.daily_orders.odcs.yaml").unlink()
        with pytest.raises(ProviderError, match="daily_orders"):
            load_seed(next(bundle.glob("*.odps.yaml")), allow_remote=False)

    def test_odps_without_output_ports_is_rejected(self, tmp_path: Path) -> None:
        odps = yaml.safe_load((next(BUNDLE_DIR.glob("*.odps.yaml"))).read_text())
        odps["outputPorts"] = []
        odps["inputPorts"] = []
        target = tmp_path / "empty.odps.yaml"
        target.write_text(yaml.safe_dump(odps))
        with pytest.raises(ProviderError, match="no ground truth"):
            load_seed(target, allow_remote=False)


# ---------------------------------------------------------------------------
# Finding 60 — YAML dates in a seed must not crash the prompt or the guard
# ---------------------------------------------------------------------------


_DATE_ODCS = """\
version: "1.0.0"
apiVersion: v3.1.0
kind: DataContract
id: dated.orders
status: active
name: dated
slaProperties:
  - property: generalAvailability
    value: 2024-01-01
schema:
  - name: orders
    logicalType: object
    physicalType: table
    properties:
      - name: order_id
        logicalType: string
        required: true
        quality:
          - type: library
            metric: nullValues
            mustBe: 0
            customProperties:
              - property: effectiveFrom
                value: 2024-01-01
"""


class TestSeedWithDates:
    @pytest.fixture
    def dated_seed(self, tmp_path: Path) -> Path:
        target = tmp_path / "orders.odcs.yaml"
        target.write_text(_DATE_ODCS)
        return target

    def test_prompt_block_is_json_serialisable(self, dated_seed: Path) -> None:
        seed = load_seed(dated_seed)
        block = format_ground_truth_prompt_block(seed)
        json.dumps(block, sort_keys=True)
        prompt = build_user_prompt(
            context={"project_goal": "x"},
            discovery_report=_discovery_report(),
            capability_matrix=_capability_matrix(),
            seed_contract={},
            seed_template="starter",
            seed_provider="local",
            attempt_index=1,
            previous_errors=[],
            previous_payload=None,
            seed_ground_truth=block,
        )
        assert "seed_ground_truth" in prompt

    def test_json_round_tripped_seed_is_not_a_mismatch(self, dated_seed: Path) -> None:
        seed = load_seed(dated_seed)
        candidate = json.loads(json.dumps(seed.fluid, default=str))
        assert diff_against_seed(seed, candidate) == []

    def test_copilot_runs_with_dated_seed(self, dated_seed: Path) -> None:
        llm = _CopierLlm()
        _run_copilot(dated_seed, llm)
        assert llm.calls == 1


# ---------------------------------------------------------------------------
# Finding 61 — ground-truth-only failures are classified as seed mismatches
# ---------------------------------------------------------------------------


class TestSeedMismatchFailureClass:
    def test_ground_truth_only_failure_is_not_ambiguous_intent(self) -> None:
        def drop_column(contract):
            contract["exposes"][0]["contract"]["schema"] = contract["exposes"][0]["contract"][
                "schema"
            ][:1]
            return contract

        llm = _CopierLlm([drop_column])
        with pytest.raises(CopilotGenerationError) as exc_info:
            _run_copilot(ODCS_FULL, llm)
        assert llm.calls == 3
        assert exc_info.value.context["failure_class"] == "seed_mismatch"

    def test_non_seed_errors_still_classified_normally(self) -> None:
        def bad_provider(contract):
            contract["exposes"][0]["binding"]["platform"] = "nope"
            return contract

        llm = _CopierLlm([bad_provider])
        with pytest.raises(CopilotGenerationError) as exc_info:
            _run_copilot(ODCS_FULL, llm)
        assert exc_info.value.context["failure_class"] != "seed_mismatch"


# ---------------------------------------------------------------------------
# Finding 62 — directory provenance lists the contracts actually consumed
# ---------------------------------------------------------------------------


class TestDirectoryProvenance:
    def test_subdir_and_json_contracts_are_recorded(self, tmp_path: Path) -> None:
        bundle = tmp_path / "subdir"
        (bundle / "contracts").mkdir(parents=True)
        shutil.copy(next(BUNDLE_DIR.glob("*.odps.yaml")), bundle / "product.odps.yaml")
        daily = BUNDLE_DIR / "commerce.orders-product.daily_orders.odcs.yaml"
        hourly = BUNDLE_DIR / "commerce.orders-product.hourly_orders.odcs.yaml"
        shutil.copy(daily, bundle / "contracts" / "daily.odcs.yaml")
        (bundle / "contracts" / "hourly.odcs.json").write_text(
            json.dumps(yaml.safe_load(hourly.read_text()))
        )

        seed = load_seed(bundle, allow_remote=False)
        origins = [entry["origin"] for entry in seed.provenance]
        assert str(bundle / "contracts" / "daily.odcs.yaml") in origins
        assert str(bundle / "contracts" / "hourly.odcs.json") in origins
        assert seed.provenance[0]["origin"] == str(bundle)

    def test_odcs_only_directory_lists_every_contract(self, tmp_path: Path) -> None:
        root = tmp_path / "only"
        (root / "nested").mkdir(parents=True)
        shutil.copy(ODCS_FULL, root / "nested" / "orders.odcs.yml")
        shutil.copy(ODCS_MINIMAL, root / "minimal.yaml")
        seed = load_seed(root, allow_remote=False)
        origins = [entry["origin"] for entry in seed.provenance]
        assert str(root / "nested" / "orders.odcs.yml") in origins
        assert str(root / "minimal.yaml") in origins


# ---------------------------------------------------------------------------
# Findings 47 / 70 — packaging metadata
# ---------------------------------------------------------------------------


def _pyproject() -> Dict[str, Any]:
    if sys.version_info >= (3, 11):
        import tomllib
    else:  # pragma: no cover - exercised on 3.9/3.10 CI
        tomllib = pytest.importorskip("tomli")
    with open(REPO_ROOT / "pyproject.toml", "rb") as fh:
        return tomllib.load(fh)


def _requirements(specs: List[str]):
    from packaging.requirements import Requirement

    return {Requirement(spec).name: Requirement(spec) for spec in specs}


class TestPackagingMetadata:
    def test_odcs_strict_extra_resolves_on_python_39(self) -> None:
        reqs = _requirements(_pyproject()["project"]["optional-dependencies"]["odcs-strict"])
        for name in ("vowl", "open-data-contract-standard"):
            marker = reqs[name].marker
            assert marker is not None, f"{name} needs a python_version marker"
            assert not marker.evaluate({"python_version": "3.9"})
            assert marker.evaluate({"python_version": "3.10"})
            assert marker.evaluate({"python_version": "3.12"})
        # deepdiff supports 3.9 and stays unconditional
        assert reqs["deepdiff"].marker is None

    def test_requests_is_a_core_dependency(self) -> None:
        reqs = _requirements(_pyproject()["project"]["dependencies"])
        assert "requests" in reqs
        assert reqs["requests"].marker is None
