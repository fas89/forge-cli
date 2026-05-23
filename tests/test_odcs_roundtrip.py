# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Phase 1 — ODCS lossless round-trip tests.

For each fixture: load → import → render → assert zero-diff via
:meth:`OdcsProvider.roundtrip_check`. Section-by-section invariants drill
in on individual lossless guarantees (team object, schema properties, SLA,
quality, relationships, primaryKey, custom properties).
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from fluid_build.providers.odcs import OdcsProvider

FIXTURES = Path(__file__).parent / "fixtures" / "odcs"


def _load(name: str) -> dict:
    with open(FIXTURES / name) as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Whole-document round-trip canary
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("fixture", ["contract-full.yaml", "contract-minimal.yaml"])
def test_roundtrip_zero_diff(fixture: str) -> None:
    """ODCS → FLUID → ODCS reproduces the original document byte-for-byte."""
    odcs = _load(fixture)
    result = OdcsProvider().roundtrip_check(odcs)
    assert result["equal"], (
        f"Round-trip diff for {fixture}:\n"
        f"  missing: {result['missing']}\n"
        f"  extra  : {result['extra']}\n"
        f"  changed: {result['changed']}"
    )


# ---------------------------------------------------------------------------
# Section-by-section invariants on the full fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def full_contract() -> dict:
    return _load("contract-full.yaml")


@pytest.fixture(scope="module")
def imported(full_contract: dict) -> dict:
    return OdcsProvider().import_contract(full_contract)


@pytest.fixture(scope="module")
def reexported(imported: dict) -> dict:
    return OdcsProvider().render(imported)


class TestTeamObjectRoundTrip:
    def test_team_object_survives_export(self, reexported: dict) -> None:
        assert reexported["team"]["name"] == "commerce-team"
        members = reexported["team"]["members"]
        assert len(members) == 2
        assert members[0]["username"] == "alice@acme.com"
        assert members[0]["role"] == "data-owner"
        # dateIn is a non-FLUID-native field — must survive verbatim
        assert members[0]["dateIn"] == "2024-01-01"

    def test_team_object_lands_in_fluid_owner(self, imported: dict) -> None:
        owner = imported.get("owner") or {}
        assert owner["team"] == "commerce-team"
        assert owner["email"] == "alice@acme.com"
        assert owner["role"] == "data-owner"

    def test_owner_contacts_carry_extra_members(self, imported: dict) -> None:
        contacts = (imported.get("owner") or {}).get("contacts") or []
        assert any(c.get("email") == "bob@acme.com" for c in contacts)


class TestSchemaPropertyRoundTrip:
    def test_required_field_round_trips(self, reexported: dict) -> None:
        props = reexported["schema"][0]["properties"]
        order_id = next(p for p in props if p["name"] == "order_id")
        assert order_id["required"] is True

    def test_primary_key_round_trips_without_tag_pollution(
        self, full_contract: dict, reexported: dict
    ) -> None:
        original_tags = full_contract["schema"][0]["properties"][0]["tags"]
        reexported_tags = reexported["schema"][0]["properties"][0]["tags"]
        assert original_tags == reexported_tags, (
            "primary-key boolean must not bleed into the tags list on round-trip"
        )
        assert reexported["schema"][0]["properties"][0].get("primaryKey") is True

    def test_property_level_quality_preserved_verbatim(
        self, full_contract: dict, reexported: dict
    ) -> None:
        original_quality = full_contract["schema"][0]["properties"][0]["quality"]
        reexported_quality = reexported["schema"][0]["properties"][0]["quality"]
        assert reexported_quality == original_quality

    def test_physical_type_preserved(self, reexported: dict) -> None:
        props = reexported["schema"][0]["properties"]
        amount = next(p for p in props if p["name"] == "amount")
        assert amount["physicalType"] == "NUMERIC"


class TestObjectLevelRoundTrip:
    def test_relationships_pass_through(self, reexported: dict) -> None:
        rels = reexported["schema"][0]["relationships"]
        assert len(rels) == 1
        assert rels[0]["from"] == "customer_id"
        assert rels[0]["to"] == "customers.id"

    def test_object_quality_pass_through(self, reexported: dict) -> None:
        quality = reexported["schema"][0]["quality"]
        assert any(q.get("metric") == "rowCount" for q in quality)

    def test_physical_name_pass_through(self, reexported: dict) -> None:
        assert reexported["schema"][0]["physicalName"] == "orders_v1"


class TestSlaRoundTrip:
    def test_sla_properties_verbatim(
        self, full_contract: dict, reexported: dict
    ) -> None:
        # The verbatim pass-through path means the full slaProperties list is
        # reproduced byte-for-byte, units and all.
        original = full_contract["slaProperties"]
        reexported_sla = reexported["slaProperties"]
        assert len(reexported_sla) == len(original)
        for orig, new in zip(original, reexported_sla):
            assert new["property"] == orig["property"]
            assert new["value"] == orig["value"]
            assert new.get("unit") == orig.get("unit")


class TestTopLevelExtrasRoundTrip:
    @pytest.mark.parametrize(
        "field",
        ["description", "tags", "domain", "tenant", "support", "price", "customProperties"],
    )
    def test_field_present(self, full_contract: dict, reexported: dict, field: str) -> None:
        assert reexported[field] == full_contract[field], f"{field} did not round-trip"


# ---------------------------------------------------------------------------
# Regression: the two original Phase-1 import bugs
# ---------------------------------------------------------------------------


def test_team_object_import_does_not_crash_on_dict_form() -> None:
    """Original bug: _odcs_team_to_fluid_owner was typed for a string but
    ODCS v3.1.0 ``team`` is an object — this used to TypeError on import."""
    odcs = _load("contract-full.yaml")
    fluid = OdcsProvider().import_contract(odcs)
    assert "owner" in fluid
    assert fluid["owner"]["team"] == "commerce-team"


def test_required_is_read_not_isnullable() -> None:
    """Original bug: _odcs_schema_to_field read ``isNullable`` (a field that
    doesn't exist in v3.1.0) instead of ``required``."""
    odcs = _load("contract-full.yaml")
    fluid = OdcsProvider().import_contract(odcs)
    fields = fluid["exposes"][0]["contract"]["schema"]
    order_id = next(f for f in fields if f["name"] == "order_id")
    assert order_id["required"] is True
