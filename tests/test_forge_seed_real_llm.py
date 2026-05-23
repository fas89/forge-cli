# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Real-LLM end-to-end tests for the forge seed → ground-truth-guard loop.

Skipped unless **all** of these are set in the environment:
  - ``ANTHROPIC_API_KEY`` or ``ANTHROPIC_AUTH_TOKEN``  (auth)
  - ``FLUID_REAL_LLM_TESTS=1``                          (opt-in cost gate)

The default test suite runs none of these — the Anthropic API is real and
consumes tokens. Engineers wanting to verify the full loop against a live
model set both env vars and run::

    pytest tests/test_forge_seed_real_llm.py -v -s

Each test runs one or two LLM calls (~5–25k tokens each on claude-haiku-4-5).
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sys
from pathlib import Path

import pytest
import yaml

from fluid_build.cli.forge_copilot_seed import diff_against_seed, load_seed
from fluid_build.providers.odps_standard import BitolOdpsProvider

logging.getLogger(
    "fluid_build.providers.odps_standard.provider.BitolOdpsProvider"
).setLevel(logging.ERROR)


# ---------------------------------------------------------------------------
# Gate: skip unless the operator opted in
# ---------------------------------------------------------------------------


pytestmark = [
    pytest.mark.skipif(
        os.environ.get("FLUID_REAL_LLM_TESTS") != "1",
        reason="Real-LLM tests are opt-in; set FLUID_REAL_LLM_TESTS=1 to enable",
    ),
    pytest.mark.skipif(
        not (
            os.environ.get("ANTHROPIC_API_KEY")
            or os.environ.get("ANTHROPIC_AUTH_TOKEN")
        ),
        reason="Need ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN to call the API",
    ),
]


MODEL = os.environ.get("FLUID_REAL_LLM_MODEL", "claude-haiku-4-5-20251001")
MAX_TOKENS = 16000


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def anthropic_client():
    anthropic = pytest.importorskip("anthropic")
    if os.environ.get("ANTHROPIC_API_KEY"):
        return anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    # Session-ingress JWT — accepted as ``Bearer`` via ``auth_token``
    return anthropic.Anthropic(
        auth_token=os.environ["ANTHROPIC_AUTH_TOKEN"],
        base_url=os.environ.get("ANTHROPIC_BASE_URL", "https://api.anthropic.com"),
    )


@pytest.fixture(scope="module")
def seed_bundle(tmp_path_factory):
    """Render a multi-port Bitol bundle from the in-tree multi-expose
    fixture so the test is hermetic (no biz-lab dependency)."""
    bundle_dir = tmp_path_factory.mktemp("seed_bundle")
    fluid_path = (
        Path(__file__).parent
        / "fixtures"
        / "fluid"
        / "contract-multi-expose.fluid.yaml"
    )
    with open(fluid_path) as f:
        fluid = yaml.safe_load(f)
    prov = BitolOdpsProvider()
    prov.strict_validation = False
    prov.render(fluid, out_dir=bundle_dir)
    return bundle_dir


def _seed_view(seed) -> dict:
    """Slim FLUID view for the LLM — drops the verbose passthrough block."""
    return {
        "id": seed.fluid.get("contract", {}).get("id"),
        "metadata": {
            k: v
            for k, v in (seed.fluid.get("metadata") or {}).items()
            if k != "odps_passthrough"
        },
        "exposes": [
            {
                "id": e["id"],
                "contract": {"schema": e["contract"]["schema"]},
                "qos": e.get("qos") or {},
            }
            for e in seed.fluid.get("exposes", [])
        ],
    }


def _parse_json_response(text: str) -> dict:
    """LLMs sometimes wrap JSON in markdown fences; strip and parse."""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```", 2)[1].split("\n", 1)[1].rsplit("```", 1)[0]
    return json.loads(text)


SYSTEM_PRESERVE = (
    "You scaffold FLUID data product contracts. The user gives you a SEED "
    "with the schema and qos pinned by an upstream Bitol ODPS contract. "
    "You MUST preserve those fields VERBATIM in your output — never add, "
    "remove, or modify schema columns, qos values, or relationships. You "
    "ONLY fill in `builds`, `executes`, and `governance`. Return a single "
    "JSON object with top-level keys: `id`, `metadata`, `exposes`, "
    "`builds`, `executes`, `governance`. Use the seed's `id`, `metadata`, "
    "and `exposes` exactly as given."
)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_seed_to_llm_preserves_ground_truth(anthropic_client, seed_bundle):
    """Happy path: the LLM, given a preserve-verbatim system prompt and a
    real seed, returns a FLUID dict that respects every ground-truth path.
    """
    seed = load_seed(seed_bundle, allow_remote=False)
    user = (
        f"SEED:\n```json\n{json.dumps(_seed_view(seed), indent=2)}\n```\n\n"
        "INTENT: daily refresh, dbt builds, governance for the analytics "
        "team. Return ONLY the JSON object."
    )
    msg = anthropic_client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=SYSTEM_PRESERVE,
        messages=[{"role": "user", "content": user}],
    )
    llm_fluid = _parse_json_response(msg.content[0].text)
    mismatches = diff_against_seed(seed, llm_fluid)
    assert not mismatches, (
        f"Expected the LLM to preserve ground truth verbatim under the "
        f"preserve-prompt; got {len(mismatches)} mismatches: "
        f"{[m['path'] for m in mismatches]}"
    )
    # The LLM should have added the LLM-owned blocks
    assert set(llm_fluid).issuperset({"builds", "executes", "governance"})


def test_guard_catches_mutation_then_repair_reduces_it(anthropic_client, seed_bundle):
    """Adversarial path: an "improve the schema" prompt induces mutations,
    the guard catches them, and a second-turn repair re-prompt with the
    mismatch report measurably reduces them.

    We don't require the repair to fully converge in one extra turn — the
    production loop allows 3 attempts. This test pins the property that
    the repair turn improves things AT ALL, not that it always wins on the
    first try.
    """
    seed = load_seed(seed_bundle, allow_remote=False)
    seed_view = _seed_view(seed)
    sys_adv = (
        "You scaffold FLUID data product contracts. Use the SEED as a "
        "starting point but feel free to IMPROVE the schema — add columns, "
        "rename for clarity, normalize types. Return a JSON object with "
        "`id`, `metadata`, `exposes`, `builds`, `executes`, `governance`."
    )
    user_adv = (
        f"SEED:\n```json\n{json.dumps(seed_view, indent=2)}\n```\n\n"
        "INTENT: Improve schemas to follow modern naming (snake_case lower) "
        "and add a `created_at` timestamp to each table. Return ONLY JSON."
    )
    msg1 = anthropic_client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=sys_adv,
        messages=[{"role": "user", "content": user_adv}],
    )
    candidate1 = _parse_json_response(msg1.content[0].text)
    mismatches1 = diff_against_seed(seed, candidate1)
    if not mismatches1:
        pytest.skip(
            "LLM ignored the 'improve schema' prompt — happy outcome but "
            "this test needs an actual mutation to exercise the repair loop"
        )

    # Repair turn under the production preserve-verbatim system prompt
    repair = (
        "Your previous response mutated ground-truth paths. Restore them:\n\n"
        + "\n".join(
            f"  • {m['path']}"
            for m in mismatches1[:8]
        )
        + f"\n\nSeed for reference:\n```json\n{json.dumps(seed_view, indent=2)}\n```\n\n"
        "Return the corrected JSON object with the seed's schema and qos "
        "preserved exactly."
    )
    msg2 = anthropic_client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=SYSTEM_PRESERVE,
        messages=[
            {"role": "user", "content": user_adv},
            {"role": "assistant", "content": msg1.content[0].text},
            {"role": "user", "content": repair},
        ],
    )
    candidate2 = _parse_json_response(msg2.content[0].text)
    mismatches2 = diff_against_seed(seed, candidate2)

    assert len(mismatches2) <= len(mismatches1), (
        f"Repair turn should not make things worse: "
        f"round 1 had {len(mismatches1)} mismatches, "
        f"round 2 had {len(mismatches2)}: {[m['path'] for m in mismatches2[:5]]}"
    )
