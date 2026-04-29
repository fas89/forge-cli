# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""AI-mode forge guardrails tests.

Asserts the contract that ``fluid forge --ai`` cannot drift into
unsafe territory: connection details, secrets, sovereignty,
image-signatures, cost budgets must NOT carry an ``ai`` provenance
when applied to a contract.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pytest

from fluid_build.cli._forge_ai_guardrails import (
    BLOCKED_PATH_PREFIXES,
    PROVENANCE_VALUES,
    FieldSuggestion,
    GuardrailViolation,
    Suggestion,
    apply_suggestion,
    assert_no_ai_in_critical_paths,
    is_blocked_path,
    read_suggestion_file,
    validate_suggestion,
    write_suggestion_file,
)


# ── Path matcher ──────────────────────────────────────────────────────────


class TestPathMatcher:
    @pytest.mark.parametrize(
        "path",
        [
            "builds[0].properties.source.connection",
            "builds[0].properties.source.connection.host",
            "builds[0].properties.source.connection.port",
            "builds[0].properties.source.connection.password",
            "builds[1].properties.source.connection.secretRef",
            "sovereignty.jurisdiction",
            "sovereignty.dataResidency.region",
            "exposes[0].contract.schema",
            "exposes[5].contract.schema[0].name",
            "builds[0].properties.airbyte.image_signature.publicKey",
            "builds[0].properties.cost.budget.monthly.rows",
        ],
    )
    def test_blocked_paths_recognized(self, path: str):
        assert is_blocked_path(path) is True, f"expected {path} to be blocked"

    @pytest.mark.parametrize(
        "path",
        [
            "id",
            "name",
            "metadata.owner.team",
            "metadata.owner.email",
            "metadata.classification",
            "builds[0].id",
            "builds[0].properties.source.streams",
            "builds[0].properties.source.streams[0]",
            "builds[0].properties.source.cursor_field",
            "builds[0].execution.trigger.schedule",
            "builds[0].properties.delivery.guarantee",
            "exposes[0].exposeId",
        ],
    )
    def test_unblocked_paths_pass(self, path: str):
        assert is_blocked_path(path) is False, f"expected {path} to be allowed"


# ── Validation ────────────────────────────────────────────────────────────


class TestValidation:
    def test_ai_provenance_on_unblocked_path_ok(self):
        s = Suggestion(
            contract_id="bronze.x",
            fields=[FieldSuggestion(path="metadata.classification", value="confidential")],
        )
        assert validate_suggestion(s) == []

    def test_ai_on_connection_blocked(self):
        s = Suggestion(
            contract_id="bronze.x",
            fields=[
                FieldSuggestion(
                    path="builds[0].properties.source.connection.host",
                    value="db.prod.internal",
                )
            ],
        )
        violations = validate_suggestion(s)
        assert len(violations) == 1
        assert "connection" in violations[0]

    def test_ai_on_secret_ref_blocked(self):
        s = Suggestion(
            contract_id="bronze.x",
            fields=[
                FieldSuggestion(
                    path="builds[0].properties.source.connection.secretRef",
                    value="vault://prod/db",
                )
            ],
        )
        assert validate_suggestion(s)

    def test_ai_on_sovereignty_blocked(self):
        s = Suggestion(
            contract_id="bronze.x",
            fields=[
                FieldSuggestion(path="sovereignty.jurisdiction", value="EU"),
            ],
        )
        assert validate_suggestion(s)

    def test_ai_on_cost_budget_blocked(self):
        s = Suggestion(
            contract_id="bronze.x",
            fields=[
                FieldSuggestion(
                    path="builds[0].properties.cost.budget.monthly.rows",
                    value=10_000_000,
                )
            ],
        )
        assert validate_suggestion(s)

    def test_introspection_provenance_allowed_on_blocked_paths(self):
        # Introspection (a live source schema fetch) IS allowed to fill
        # connection details — the database told us, it's ground truth.
        # Only AI provenance is blocked there.
        s = Suggestion(
            contract_id="bronze.x",
            fields=[
                FieldSuggestion(
                    path="builds[0].properties.source.connection.host",
                    value="db",
                    provenance="introspection",
                ),
                FieldSuggestion(
                    path="exposes[0].contract.schema",
                    value=[{"name": "id", "type": "long"}],
                    provenance="introspection",
                ),
            ],
        )
        assert validate_suggestion(s) == []

    def test_invalid_provenance_value_rejected(self):
        s = Suggestion(
            contract_id="bronze.x",
            fields=[
                FieldSuggestion(
                    path="metadata.classification",
                    value="x",
                    provenance="hallucinated",
                )
            ],
        )
        violations = validate_suggestion(s)
        assert len(violations) == 1
        assert "invalid provenance" in violations[0]

    def test_assert_no_ai_in_critical_raises(self):
        s = Suggestion(
            contract_id="bronze.x",
            fields=[
                FieldSuggestion(
                    path="builds[0].properties.source.connection.host",
                    value="db",
                )
            ],
        )
        with pytest.raises(GuardrailViolation):
            assert_no_ai_in_critical_paths(s)


# ── Apply ─────────────────────────────────────────────────────────────────


def _draft_contract() -> Dict[str, Any]:
    return {
        "fluidVersion": "0.7.3",
        "kind": "DataProduct",
        "id": "bronze.crm.salesforce",
        "metadata": {"layer": "Bronze"},
        "builds": [
            {
                "id": "ingest",
                "pattern": "acquisition",
                "engine": "airbyte",
                "properties": {
                    "source": {"kind": "salesforce", "mode": "incremental_append"},
                },
            }
        ],
        "exposes": [{"exposeId": "accounts_raw", "kind": "table"}],
    }


class TestApply:
    def test_apply_introspection_fields(self):
        s = Suggestion(
            contract_id="bronze.crm.salesforce",
            fields=[
                FieldSuggestion(
                    path="builds[0].properties.source.connection.instance_url",
                    value="https://acme.my.salesforce.com",
                    provenance="introspection",
                ),
                FieldSuggestion(
                    path="exposes[0].contract.schema",
                    value=[
                        {"name": "Id", "type": "string"},
                        {"name": "Name", "type": "string"},
                    ],
                    provenance="introspection",
                ),
            ],
        )
        out = apply_suggestion(_draft_contract(), s)
        assert (
            out["builds"][0]["properties"]["source"]["connection"]["instance_url"]
            == "https://acme.my.salesforce.com"
        )
        assert len(out["exposes"][0]["contract"]["schema"]) == 2

    def test_apply_ai_friendly_fields(self):
        # AI is allowed on names + classification + schedule.
        s = Suggestion(
            contract_id="bronze.crm.salesforce",
            fields=[
                FieldSuggestion(
                    path="metadata.classification",
                    value="confidential",
                    provenance="ai",
                    rationale="Customer PII present (Email, Phone)",
                ),
                FieldSuggestion(
                    path="builds[0].execution.trigger.schedule",
                    value="0 */4 * * *",
                    provenance="ai",
                    rationale="Salesforce typical sync cadence",
                ),
            ],
        )
        out = apply_suggestion(_draft_contract(), s)
        assert out["metadata"]["classification"] == "confidential"
        assert out["builds"][0]["execution"]["trigger"]["schedule"] == "0 */4 * * *"

    def test_apply_blocked_ai_field_raises(self):
        s = Suggestion(
            contract_id="bronze.crm.salesforce",
            fields=[
                FieldSuggestion(
                    path="builds[0].properties.source.connection.secretRef",
                    value="vault://hallucinated",
                    provenance="ai",
                )
            ],
        )
        with pytest.raises(GuardrailViolation):
            apply_suggestion(_draft_contract(), s)

    def test_apply_does_not_mutate_input(self):
        original = _draft_contract()
        s = Suggestion(
            contract_id="bronze.crm.salesforce",
            fields=[
                FieldSuggestion(
                    path="metadata.classification",
                    value="confidential",
                    provenance="ai",
                )
            ],
        )
        out = apply_suggestion(original, s)
        assert "classification" not in original["metadata"]
        assert out["metadata"]["classification"] == "confidential"

    def test_accept_only_introspection_filters_ai(self):
        s = Suggestion(
            contract_id="bronze.crm.salesforce",
            fields=[
                FieldSuggestion(
                    path="metadata.classification",
                    value="confidential",
                    provenance="ai",
                ),
                FieldSuggestion(
                    path="exposes[0].contract.schema",
                    value=[{"name": "Id", "type": "string"}],
                    provenance="introspection",
                ),
            ],
        )
        # Caller chooses to accept only the introspection field.
        out = apply_suggestion(_draft_contract(), s, accept_provenance=("introspection",))
        assert "classification" not in out["metadata"]
        assert out["exposes"][0]["contract"]["schema"][0]["name"] == "Id"

    def test_dotted_path_with_array_indices(self):
        # Multi-level deep array indexing handled correctly.
        contract = _draft_contract()
        s = Suggestion(
            contract_id="x",
            fields=[
                FieldSuggestion(
                    path="builds[0].properties.source.streams",
                    value=["Account", "Contact"],
                    provenance="introspection",
                )
            ],
        )
        out = apply_suggestion(contract, s)
        assert out["builds"][0]["properties"]["source"]["streams"] == ["Account", "Contact"]


# ── I/O round-trip ────────────────────────────────────────────────────────


class TestIO:
    def test_roundtrip(self, tmp_path: Path):
        s = Suggestion(
            contract_id="bronze.x",
            fields=[
                FieldSuggestion(
                    path="metadata.classification",
                    value="confidential",
                    provenance="ai",
                    rationale="reason",
                ),
                FieldSuggestion(
                    path="builds[0].properties.source.streams",
                    value=["a", "b"],
                    provenance="introspection",
                ),
            ],
        )
        path = write_suggestion_file(s, tmp_path / "bronze.x.suggested.json")
        assert path.exists()
        s2 = read_suggestion_file(path)
        assert s2.contract_id == s.contract_id
        assert len(s2.fields) == len(s.fields)
        assert s2.fields[0].rationale == "reason"

    def test_constants_exposed(self):
        # Public surface is stable — these symbols are imported by the
        # forge CLI and the validate stage.
        assert "ai" in PROVENANCE_VALUES
        assert "introspection" in PROVENANCE_VALUES
        assert "user" in PROVENANCE_VALUES
        assert any("connection" in p for p in BLOCKED_PATH_PREFIXES)
        assert any("sovereignty" in p for p in BLOCKED_PATH_PREFIXES)
