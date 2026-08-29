# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Mocked-LLM tests for the forge copilot's Phase-7 seed → guard → repair loop.

These tests verify the runtime wiring — the seed loads once, the system
prompt gains the preservation clause, every attempt's user prompt carries
the ``seed_ground_truth`` block, and the ground-truth diff after each
attempt drives the existing 3-attempt repair loop.

No real LLM is called. ``call_llm`` is patched to return controlled JSON,
and the normalize/validate seams are patched so the tests focus on the
Phase-7 additions without needing a schema-perfect contract.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest
import yaml

from fluid_build.cli.forge_copilot_runtime import (
    CopilotGenerationError,
    generate_copilot_artifacts,
)
from fluid_build.cli.forge_copilot_seed import SeedOptions, load_seed
from fluid_build.providers.odps_standard import BitolOdpsProvider

FLUID_FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "fluid"
    / "contract-multi-expose.fluid.yaml"
)


# ---------------------------------------------------------------------------
# Helpers — construct a bundle + a controlled "LLM" that returns whatever we
# want across the three attempt slots.
# ---------------------------------------------------------------------------


@pytest.fixture
def seed_bundle(tmp_path_factory) -> Path:
    bundle_dir = tmp_path_factory.mktemp("seed_bundle")
    with open(FLUID_FIXTURE) as f:
        fluid = yaml.safe_load(f)
    prov = BitolOdpsProvider()
    prov.strict_validation = False
    prov.render(fluid, out_dir=bundle_dir)
    return bundle_dir


@pytest.fixture
def seed_options(seed_bundle: Path) -> SeedOptions:
    return SeedOptions(seed_from=seed_bundle, allow_remote=False)


def _preserving_contract_from_seed(seed_bundle: Path) -> Dict[str, Any]:
    """Build a FLUID contract that preserves the seed's exposes verbatim —
    the shape the LLM is expected to return under the preservation prompt.
    """
    seed = load_seed(seed_bundle, allow_remote=False)
    return {
        "contract": {"id": (seed.fluid.get("contract") or {}).get("id")},
        "metadata": seed.fluid.get("metadata") or {},
        "exposes": [
            {
                "id": e["id"],
                "contract": {"schema": e["contract"]["schema"]},
                "qos": e.get("qos") or {},
            }
            for e in seed.fluid.get("exposes") or []
        ],
    }


def _mutated_contract(preserving: Dict[str, Any]) -> Dict[str, Any]:
    """Drop the first expose's schema — the classic ground-truth violation."""
    mutated = json.loads(json.dumps(preserving))
    if mutated["exposes"]:
        mutated["exposes"][0]["contract"]["schema"] = []
    return mutated


class _LlmScript:
    """Return canned raw-JSON strings across N attempts."""

    def __init__(self, payloads: List[Dict[str, Any]]):
        self._payloads = list(payloads)
        self.calls = 0

    def __call__(self, provider_adapter, llm_config, system_prompt, user_prompt):
        payload = self._payloads[min(self.calls, len(self._payloads) - 1)]
        self.calls += 1
        # Capture the prompts the runtime built so tests can inspect them
        self.last_system_prompt = system_prompt
        self.last_user_prompt = user_prompt
        return json.dumps(payload)


def _patched_seams(script: _LlmScript, preserving_contract: Dict[str, Any]):
    """Patch the runtime seams that don't matter for the Phase-7 logic under
    test — call_llm, normalize, validate — so the tests exercise only the
    seed loading, prompt injection, and ground-truth diff loop.
    """
    def fake_normalize(payload, **_):
        return {
            "suggestions": payload.get("suggestions") or {},
            "contract": payload.get("contract") or {},
            "readme_markdown": payload.get("readme_markdown") or "",
            "additional_files": payload.get("additional_files") or [],
        }

    def fake_validate(normalized, **_):
        return ([], [])

    return [
        patch("fluid_build.cli.forge_copilot_runtime.call_llm", side_effect=script),
        patch(
            "fluid_build.cli.forge_copilot_runtime.normalize_generation_payload",
            side_effect=fake_normalize,
        ),
        patch(
            "fluid_build.cli.forge_copilot_runtime.validate_generated_result",
            side_effect=fake_validate,
        ),
        # get_llm_provider is called before call_llm; give it a stub so no
        # real adapter is required.
        patch(
            "fluid_build.cli.forge_copilot_runtime.get_llm_provider",
            return_value=MagicMock(),
        ),
    ]


def _minimal_llm_config():
    from fluid_build.cli.forge_copilot_llm_providers import LlmConfig

    return LlmConfig(
        provider="anthropic",
        model="mock-model",
        endpoint="https://mock.example.com/v1/messages",
        api_key="mock-key",
    )


def _minimal_discovery_report():
    from fluid_build.cli.forge_copilot_discovery import DiscoveryReport

    return DiscoveryReport(workspace_roots=["/tmp"])


# ---------------------------------------------------------------------------
# Case 1 — happy path: LLM preserves ground truth on attempt 1
# ---------------------------------------------------------------------------


def test_seed_happy_path_returns_on_first_attempt(seed_options, seed_bundle):
    """A seed-aware LLM that preserves everything exits the loop after one
    attempt, and never triggers the guard's re-prompt path.
    """
    preserving = _preserving_contract_from_seed(seed_bundle)
    script = _LlmScript([{"contract": preserving}])

    patches = _patched_seams(script, preserving)
    for p in patches:
        p.start()
    try:
        result = generate_copilot_artifacts(
            context={"project_goal": "demo"},
            llm_config=_minimal_llm_config(),
            discovery_report=_minimal_discovery_report(),
            seed_options=seed_options,
        )
    finally:
        for p in patches:
            p.stop()

    assert script.calls == 1
    assert len(result.attempt_reports) == 1
    # System prompt gained the preservation clause
    assert "GROUND TRUTH FROM SEED" in script.last_system_prompt
    # User prompt carried the seed block
    assert "seed_ground_truth" in script.last_user_prompt


# ---------------------------------------------------------------------------
# Case 2 — mutation → guard catches → attempt 2 preserves → success
# ---------------------------------------------------------------------------


def test_seed_mutation_triggers_repair_then_succeeds(seed_options, seed_bundle):
    preserving = _preserving_contract_from_seed(seed_bundle)
    mutated = _mutated_contract(preserving)

    script = _LlmScript([{"contract": mutated}, {"contract": preserving}])
    patches = _patched_seams(script, preserving)
    for p in patches:
        p.start()
    try:
        result = generate_copilot_artifacts(
            context={"project_goal": "demo"},
            llm_config=_minimal_llm_config(),
            discovery_report=_minimal_discovery_report(),
            seed_options=seed_options,
        )
    finally:
        for p in patches:
            p.stop()

    assert script.calls == 2
    assert len(result.attempt_reports) == 2
    # Attempt 1's report carries the ground-truth-violation strings
    first_report_errors = result.attempt_reports[0].validation_errors
    assert first_report_errors, "attempt 1 should record the ground-truth violation"
    assert any("ground_truth" in e for e in first_report_errors)
    # Attempt 2's user prompt carries the repair feedback derived from the
    # attempt-1 mismatch report (via the existing previous_errors slot)
    assert "ground_truth" in script.last_user_prompt


# ---------------------------------------------------------------------------
# Case 3 — persistent mutation → all 3 attempts fail → CopilotGenerationError
# ---------------------------------------------------------------------------


def test_seed_persistent_mutation_exhausts_repair_loop(seed_options, seed_bundle):
    preserving = _preserving_contract_from_seed(seed_bundle)
    mutated = _mutated_contract(preserving)

    script = _LlmScript([{"contract": mutated}] * 3)
    patches = _patched_seams(script, preserving)
    for p in patches:
        p.start()
    try:
        with pytest.raises(CopilotGenerationError) as exc_info:
            generate_copilot_artifacts(
                context={"project_goal": "demo"},
                llm_config=_minimal_llm_config(),
                discovery_report=_minimal_discovery_report(),
                seed_options=seed_options,
            )
    finally:
        for p in patches:
            p.stop()

    assert script.calls == 3
    err_msg = str(exc_info.value)
    assert "3 attempts" in err_msg or "copilot_generation_failed" in err_msg


# ---------------------------------------------------------------------------
# Case 4 — no seed_options → prompts do NOT carry the seed block, loop
# behaves exactly as it did before Phase 7
# ---------------------------------------------------------------------------


def test_no_seed_options_leaves_prompts_unchanged(seed_bundle):
    preserving = _preserving_contract_from_seed(seed_bundle)
    script = _LlmScript([{"contract": preserving}])
    patches = _patched_seams(script, preserving)
    for p in patches:
        p.start()
    try:
        generate_copilot_artifacts(
            context={"project_goal": "demo"},
            llm_config=_minimal_llm_config(),
            discovery_report=_minimal_discovery_report(),
            seed_options=None,
        )
    finally:
        for p in patches:
            p.stop()

    # No seed → no preservation clause, no seed_ground_truth block
    assert "GROUND TRUTH FROM SEED" not in script.last_system_prompt
    assert "seed_ground_truth" not in script.last_user_prompt


# ---------------------------------------------------------------------------
# Case 5 — SeedOptions.from_args parses the CLI namespace correctly
# ---------------------------------------------------------------------------


def test_seed_options_from_args_none_when_flag_absent():
    from types import SimpleNamespace

    assert SeedOptions.from_args(SimpleNamespace()) is None
    assert SeedOptions.from_args(SimpleNamespace(seed_from=None)) is None


def test_seed_options_from_args_builds_dataclass():
    from types import SimpleNamespace

    opts = SeedOptions.from_args(
        SimpleNamespace(seed_from="/tmp/x.odps.yaml", seed_no_remote=True)
    )
    assert opts is not None
    assert str(opts.seed_from) == "/tmp/x.odps.yaml"
    assert opts.allow_remote is False


def test_seed_options_from_args_defaults_allow_remote_true():
    from types import SimpleNamespace

    opts = SeedOptions.from_args(SimpleNamespace(seed_from="/tmp/x.odcs.yaml"))
    assert opts is not None
    assert opts.allow_remote is True
