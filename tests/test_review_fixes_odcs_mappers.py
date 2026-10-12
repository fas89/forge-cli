# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Regression tests for the ODCS mapper review findings.

Each test names the finding it pins. Every one of them failed on the branch
head the review ran against (``fa13e55``).
"""

from __future__ import annotations

import argparse
import contextlib
import copy
import io
import logging
from pathlib import Path
from typing import Any, Dict, List

import pytest
import yaml

from fluid_build.cli.bootstrap import load_contract_with_overlay
from fluid_build.providers.odcs import OdcsProvider
from fluid_build.providers.odcs import validation as odcs_validation
from fluid_build.providers.odcs.mappers import types as odcs_types

REPO = Path(__file__).resolve().parent.parent
FIXTURES = Path(__file__).parent / "fixtures"
LOG = logging.getLogger(__name__)


def _load(path: Path) -> Dict[str, Any]:
    with open(path) as f:
        return yaml.safe_load(f)


def _props(odcs: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [p for obj in odcs.get("schema") or [] for p in obj.get("properties") or []]


def _fresh(fields: List[Dict[str, Any]], binding: Any = None, **top: Any) -> Dict[str, Any]:
    expose: Dict[str, Any] = {"exposeId": "orders", "contract": {"schema": fields}}
    if binding is not None:
        expose["binding"] = binding
    fluid = {
        "fluidVersion": "0.7.2",
        "kind": "DataProduct",
        "id": "acme.orders",
        "metadata": {"version": "1.0.0", "status": "active"},
        "exposes": [expose],
    }
    fluid.update(top)
    return fluid


def _cli(argv: List[str]) -> Any:
    from fluid_build.cli import main as cli_main

    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            return cli_main(argv)
    except SystemExit as exc:
        return exc.code


# ---------------------------------------------------------------------------
# Findings 6, 13, 19, 21 — binding-level format reaches the ODCS server, so
# every shipped example / template exports valid ODCS.
# ---------------------------------------------------------------------------


def _shipped_contracts() -> List[Path]:
    found = sorted((REPO / "examples").rglob("*.yaml")) + sorted(
        (REPO / "fluid_build" / "templates").glob("*/contract.fluid.yaml")
    )
    return [p for p in found if "fluidVersion" in p.read_text()]


_SHIPPED = _shipped_contracts()


def _assert_valid_odcs_exports(path: Path) -> None:
    contract = load_contract_with_overlay(str(path), None, LOG)
    for prov in (OdcsProvider(), OdcsProvider(api_version="v3.2.0")):
        whole = prov.render(contract)
        odcs_validation.validate(whole)  # schema picked from its apiVersion
        for _eid, doc in prov.render_all_ports(contract):
            odcs_validation.validate(doc)


@pytest.mark.parametrize("path", _SHIPPED, ids=lambda p: str(p.relative_to(REPO)))
def test_every_shipped_contract_that_validates_exports_valid_odcs(path: Path) -> None:
    if _cli(["validate", str(path)]) != 0:
        pytest.skip("not a valid FLUID contract (fluid validate fails)")
    _assert_valid_odcs_exports(path)


@pytest.mark.parametrize(
    "path",
    sorted((REPO / "fluid_build" / "templates").glob("*/contract.fluid.yaml")),
    ids=lambda p: p.parent.name,
)
def test_every_template_exports_valid_odcs(path: Path) -> None:
    # Templates are what `fluid init` scaffolds; several fail `fluid
    # validate` for reasons unrelated to ODCS (e.g. builds[].pattern), but
    # their ODCS export must still be valid.
    _assert_valid_odcs_exports(path)


def test_local_binding_format_is_emitted_on_the_server() -> None:
    fluid = _fresh(
        [{"name": "a", "type": "string"}],
        binding={"platform": "local", "format": "csv", "location": {"path": "out/a.csv"}},
    )
    server = OdcsProvider().render(fluid)["servers"][0]
    assert server["format"] == "csv"
    assert server["path"] == "out/a.csv"


def test_location_format_wins_over_binding_format() -> None:
    fluid = _fresh(
        [{"name": "a", "type": "string"}],
        binding={
            "platform": "aws",
            "format": "parquet",
            "delimiter": "new_line",
            "location": {"bucket": "b", "path": "p", "format": "json"},
        },
    )
    server = OdcsProvider().render(fluid)["servers"][0]
    assert server["format"] == "json"
    assert server["delimiter"] == "new_line"


@pytest.mark.parametrize(
    "contract",
    [
        "examples/01-hello-world/contract.fluid.yaml",
        "fluid_build/templates/hello-world/contract.fluid.yaml",
    ],
)
def test_default_odps_export_succeeds_on_local_contracts(contract: str, tmp_path: Path) -> None:
    # Default spec (Bitol) + default strict validation.
    rc = _cli(["odps", "export", str(REPO / contract), "--out", str(tmp_path / "p.yaml")])
    assert rc == 0


def test_bitol_strict_render_of_local_contract() -> None:
    from fluid_build.providers.odps_standard import BitolOdpsProvider

    contract = load_contract_with_overlay(
        str(REPO / "examples/01-hello-world/contract.fluid.yaml"), None, LOG
    )
    prov = BitolOdpsProvider()
    prov.strict_validation = True
    bundle = prov.render(contract)
    servers = [s for doc in bundle["contracts"].values() for s in doc.get("servers", [])]
    assert servers and all(s.get("format") == "csv" for s in servers)


# ---------------------------------------------------------------------------
# Finding 1 — --no-quality / ODCS_INCLUDE_QUALITY=false suppress property quality
# ---------------------------------------------------------------------------


def test_no_quality_suppresses_fresh_property_quality() -> None:
    fluid = _fresh(
        [
            {"name": "id", "type": "string", "required": True, "tags": ["primary-key"]},
            {
                "name": "email",
                "type": "string",
                "validations": [{"type": "pattern", "value": "^.+@.+$"}],
                "quality": [{"type": "library", "metric": "rowCount", "mustBeGreaterThan": 0}],
            },
        ]
    )
    prov = OdcsProvider()
    assert any("quality" in p for p in _props(prov.render(fluid)))
    prov.include_quality_checks = False
    assert not any("quality" in p for p in _props(prov.render(fluid)))


def test_no_quality_suppresses_imported_property_quality() -> None:
    prov = OdcsProvider()
    fluid = prov.import_contract(_load(FIXTURES / "odcs" / "contract-full.yaml"))
    prov.include_quality_checks = False
    assert not any("quality" in p for p in _props(prov.render(fluid)))


def test_no_quality_env_var(monkeypatch) -> None:
    monkeypatch.setenv("ODCS_INCLUDE_QUALITY", "false")
    fluid = _fresh([{"name": "id", "type": "string", "required": True}])
    assert not any("quality" in p for p in _props(OdcsProvider().render(fluid)))


def test_no_quality_cli_flag(tmp_path: Path) -> None:
    from fluid_build.cli.odcs import _run_odcs_export

    out = tmp_path / "out.yaml"
    args = argparse.Namespace(
        contract=str(REPO / "examples/customer360/contract.fluid.yaml"),
        output=str(out),
        format="yaml",
        no_quality=True,
        no_sla=False,
    )
    assert _run_odcs_export(args) == 0
    assert not any("quality" in p for p in _props(_load(out)))


def test_legacy_field_property_shim_respects_no_quality() -> None:
    prov = OdcsProvider()
    prov.include_quality_checks = False
    prop = prov._fluid_field_to_odcs_property({"name": "id", "required": True}, {})
    assert "quality" not in prop


# ---------------------------------------------------------------------------
# Finding 2 — no binding.platform invented from SchemaObject.physicalType
# ---------------------------------------------------------------------------


def _postgres_odcs() -> Dict[str, Any]:
    return {
        "version": "1.0.0",
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "id": "pg.customers",
        "status": "active",
        "servers": [
            {
                "server": "customers",
                "type": "postgres",
                "host": "db",
                "port": 5432,
                "database": "crm",
                "schema": "public",
            }
        ],
        "schema": [
            {
                "name": "customers",
                "physicalType": "table",
                "properties": [{"name": "id", "logicalType": "integer", "physicalType": "int4"}],
            }
        ],
    }


def test_import_derives_binding_from_the_server_not_physical_type() -> None:
    fluid = OdcsProvider().import_contract(_postgres_odcs())
    binding = fluid["exposes"][0]["binding"]
    assert binding["platform"] == "postgres"
    assert binding["location"] == {
        "host": "db",
        "port": 5432,
        "database": "crm",
        "schema": "public",
    }


def test_import_without_servers_has_no_binding() -> None:
    odcs = _postgres_odcs()
    del odcs["servers"]
    fluid = OdcsProvider().import_contract(odcs)
    assert "binding" not in fluid["exposes"][0]


def test_import_with_ambiguous_servers_has_no_binding() -> None:
    odcs = _postgres_odcs()
    # Two schema-valid servers, neither named after the SchemaObject: which
    # one backs it is ambiguous, so no binding may be guessed.
    odcs["servers"] = [
        {
            "server": "a",
            "type": "postgres",
            "host": "h",
            "port": 5432,
            "database": "crm",
            "schema": "public",
        },
        {"server": "b", "type": "snowflake", "account": "x", "database": "d", "schema": "s"},
    ]
    fluid = OdcsProvider().import_contract(odcs)
    assert "binding" not in fluid["exposes"][0]


def test_field_added_to_imported_postgres_contract_gets_no_bigquery_type() -> None:
    prov = OdcsProvider()
    fluid = prov.import_contract(_postgres_odcs())
    fluid["exposes"][0]["contract"]["schema"].append({"name": "new_col", "type": "integer"})
    props = {p["name"]: p for p in _props(prov.render(fluid))}
    assert props["id"]["physicalType"] == "int4"
    assert props["new_col"].get("physicalType") != "INT64"


def test_odps_bundle_import_then_strict_export_succeeds(tmp_path: Path) -> None:
    from fluid_build.providers.odps_standard import BitolOdpsProvider

    prov = BitolOdpsProvider()
    fluid = prov.import_directory(FIXTURES / "odps" / "product-bitol")
    platforms = {e["exposeId"]: (e.get("binding") or {}).get("platform") for e in fluid["exposes"]}
    assert platforms == {"daily_orders": "gcp", "hourly_orders": "kafka"}
    prov.strict_validation = True
    bundle = prov.render(fluid)  # raised "'project' is a required property" before
    for doc in bundle["contracts"].values():
        odcs_validation.validate(doc)


# ---------------------------------------------------------------------------
# Finding 3 — array-form team and explicit empty slaProperties round-trip
# ---------------------------------------------------------------------------


def _minimal(api: str = "v3.1.0", **extra: Any) -> Dict[str, Any]:
    doc = {
        "version": "1.0.0",
        "apiVersion": api,
        "kind": "DataContract",
        "id": "t",
        "status": "active",
        "schema": [{"name": "t", "properties": [{"name": "a", "logicalType": "string"}]}],
    }
    doc.update(extra)
    return doc


_ARRAY_TEAM = [
    {"username": "ceastwood", "role": "Data Scientist", "dateIn": "2022-08-02"},
    {"username": "mhopper", "role": "Data Steward"},
]


@pytest.mark.parametrize("api", ["v3.0.2", "v3.1.0"])
def test_array_team_round_trips_and_projects_owner(api: str) -> None:
    odcs = _minimal(api, team=copy.deepcopy(_ARRAY_TEAM))
    prov = OdcsProvider()
    fluid = prov.import_contract(odcs)
    assert fluid["owner"]["email"] == "ceastwood"
    assert fluid["owner"]["contacts"] == [{"email": "mhopper", "role": "Data Steward"}]
    assert prov.roundtrip_check(odcs)["equal"]


@pytest.mark.parametrize("team", [[], {}])
def test_empty_team_round_trips(team: Any) -> None:
    odcs = _minimal(team=team)
    assert OdcsProvider().roundtrip_check(odcs)["equal"]


def test_empty_sla_properties_round_trip() -> None:
    odcs = _minimal(slaProperties=[])
    assert OdcsProvider().roundtrip_check(odcs)["equal"]


# ---------------------------------------------------------------------------
# Finding 22 — `fluid odcs validate` selects the schema from apiVersion
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fixture",
    [
        FIXTURES / "upstream" / "odcs" / "full-example.v3.2.0.odcs.yaml",
        FIXTURES / "odcs" / "contract-full.yaml",
    ],
    ids=["v3.2.0", "v3.1.0"],
)
def test_argparse_odcs_validate_is_version_selected(fixture: Path, monkeypatch) -> None:
    from fluid_build.cli.odcs import _run_odcs_validate

    monkeypatch.delenv("ODCS_API_VERSION", raising=False)
    assert _run_odcs_validate(argparse.Namespace(odcs_file=str(fixture))) == 0
    assert _cli(["odcs", "validate", str(fixture)]) == 0


def test_argparse_odcs_validate_still_rejects_invalid(tmp_path: Path) -> None:
    from fluid_build.cli.odcs import _run_odcs_validate

    bad = tmp_path / "bad.yaml"
    bad.write_text(yaml.safe_dump({"apiVersion": "v3.2.0", "kind": "DataContract"}))
    assert _run_odcs_validate(argparse.Namespace(odcs_file=str(bad))) == 1


def test_legacy_validate_shim_is_version_selected() -> None:
    doc = _load(FIXTURES / "upstream" / "odcs" / "full-example.v3.2.0.odcs.yaml")
    OdcsProvider()._validate_odcs(doc)  # raised "'v3.2.0' is not one of [...]"


# ---------------------------------------------------------------------------
# Findings 26 / 51 — v3.2.0-only logical types and server engines
# ---------------------------------------------------------------------------


def _map_vector_fluid() -> Dict[str, Any]:
    return _fresh([{"name": "attrs", "type": "map"}, {"name": "emb", "type": "vector"}])


def test_map_vector_fall_back_below_v320() -> None:
    out = OdcsProvider().render(_map_vector_fluid())
    assert out["apiVersion"] == "v3.1.0"
    types = {p["name"]: p["logicalType"] for p in _props(out)}
    assert types == {"attrs": "object", "emb": "array"}
    odcs_validation.validate(out)


def test_map_vector_emitted_at_v320() -> None:
    out = OdcsProvider(api_version="v3.2.0").render(_map_vector_fluid())
    props = {p["name"]: p for p in _props(out)}
    assert props["attrs"]["logicalType"] == "map"
    assert "map" in props["attrs"]
    assert props["emb"]["logicalType"] == "vector"
    odcs_validation.validate(out)


def test_logical_type_helper_is_version_aware() -> None:
    assert odcs_types.fluid_to_logical("map") == "object"
    assert odcs_types.fluid_to_logical("vector", "v3.1.0") == "array"
    assert odcs_types.fluid_to_logical("map", "v3.2.0") == "map"


@pytest.mark.parametrize("platform", ["iceberg", "teradata", "hana", "exasol", "actian"])
def test_v320_engines_map_to_custom_below_v320(platform: str) -> None:
    fluid = _fresh(
        [{"name": "a", "type": "string"}],
        binding={
            "platform": platform,
            "location": {
                "catalog": "c",
                "catalogUrl": "https://c/v1",
                "namespace": "n",
                "host": "h",
                "database": "d",
            },
        },
    )
    out = OdcsProvider().render(fluid)
    assert out["servers"][0]["type"] == "custom"
    odcs_validation.validate(out)
    out32 = OdcsProvider(api_version="v3.2.0").render(fluid)
    assert out32["servers"][0]["type"] != "custom"
    odcs_validation.validate(out32)


# ---------------------------------------------------------------------------
# Finding 28 — no aliasing between stash, FLUID, input and output
# ---------------------------------------------------------------------------


_UPSTREAM_V320 = FIXTURES / "upstream" / "odcs" / "full-example.v3.2.0.odcs.yaml"


def test_imported_fluid_dumps_without_yaml_anchors() -> None:
    fluid = OdcsProvider().import_contract(_load(_UPSTREAM_V320))
    dumped = yaml.dump(fluid, default_flow_style=False, sort_keys=False)
    assert "&id" not in dumped and "*id" not in dumped


def test_cli_import_edit_export_after_removing_a_quality_rule(tmp_path: Path) -> None:
    from fluid_build.cli.odcs import _run_odcs_import

    imported = tmp_path / "imported.fluid.yaml"
    args = argparse.Namespace(odcs_file=str(_UPSTREAM_V320), output=str(imported), format="yaml")
    assert _run_odcs_import(args) == 0
    text = imported.read_text()
    # An anchor on a field's quality item aliased from the stash meant that
    # deleting the rule in an editor left a dangling ``*id001``.
    assert "&id" not in text and "*id" not in text
    fluid = yaml.safe_load(text)
    for expose in fluid["exposes"]:
        for fld in expose["contract"]["schema"]:
            fld["quality"] = []
    edited = tmp_path / "edited.fluid.yaml"
    edited.write_text(yaml.dump(fluid, sort_keys=False))
    out = tmp_path / "out.yaml"
    from fluid_build.cli.odcs import _run_odcs_export

    export_args = argparse.Namespace(
        contract=str(edited), output=str(out), format="yaml", no_quality=False, no_sla=False
    )
    assert _run_odcs_export(export_args) == 0
    assert not any(p.get("quality") for p in _props(_load(out)))


def test_mutating_an_export_does_not_leak_into_later_exports_or_input() -> None:
    source = _load(_UPSTREAM_V320)
    pristine = copy.deepcopy(source)
    prov = OdcsProvider()
    fluid = prov.import_contract(source)
    fluid_before = copy.deepcopy(fluid)

    out1 = prov.render(fluid)
    out1["description"]["usage"] = "MUTATED"
    out1["schema"][0]["properties"][0].setdefault("quality", []).append({"type": "text"})
    out1["team"]["members"][0]["role"] = "MUTATED"
    out1["servers"][0]["type"] = "MUTATED"
    out1["slaProperties"][0]["value"] = "MUTATED"

    out2 = prov.render(fluid)
    assert out2["description"]["usage"] != "MUTATED"
    assert prov.roundtrip_check(pristine)["equal"]
    assert odcs_validation.roundtrip_check(pristine, out2)["equal"]
    assert fluid == fluid_before
    assert source == pristine


def test_import_does_not_alias_the_callers_input() -> None:
    source = _load(_UPSTREAM_V320)
    pristine = copy.deepcopy(source)
    fluid = OdcsProvider().import_contract(source)
    for expose in fluid["exposes"]:
        for fld in expose["contract"]["schema"]:
            for item in fld.get("quality") or []:
                item["MUTATED"] = True
    assert source == pristine


# ---------------------------------------------------------------------------
# Finding 29 — FLUID edits to owner / expects / qos win over the stash
# ---------------------------------------------------------------------------


@pytest.fixture
def full_import():
    prov = OdcsProvider()
    return prov, prov.import_contract(_load(FIXTURES / "odcs" / "contract-full.yaml"))


def test_owner_edit_reaches_team(full_import) -> None:
    prov, fluid = full_import
    fluid["owner"]["team"] = "new-team"
    fluid["owner"]["email"] = "carol@acme.com"
    out = prov.render(fluid)
    assert out["team"]["name"] == "new-team"
    assert out["team"]["members"][0]["username"] == "carol@acme.com"
    assert out["team"]["members"][0]["dateIn"] == "2024-01-01"  # same member, new email
    # unmodelled stashed keys survive
    assert out["team"]["description"] == "Owners of the commerce domain data"
    assert out["team"]["members"][1] == {
        "username": "bob@acme.com",
        "name": "Bob",
        "role": "steward",
    }
    odcs_validation.validate(out)


def test_contact_edit_keeps_member_extras(full_import) -> None:
    prov, fluid = full_import
    fluid["owner"]["contacts"][0]["role"] = "owner"
    out = prov.render(fluid)
    assert out["team"]["members"][0]["dateIn"] == "2024-01-01"
    assert out["team"]["members"][1]["role"] == "owner"


def test_owner_removal_drops_team(full_import) -> None:
    prov, fluid = full_import
    del fluid["owner"]
    assert "team" not in prov.render(fluid)


def test_expects_edits_reach_servers(full_import) -> None:
    prov, fluid = full_import
    fluid["expects"][0]["location"]["dataset"] = "orders_v2"
    fluid["expects"].append(
        {
            "id": "sf_raw",
            "provider": "snowflake",
            "location": {"account": "acme", "database": "RAW", "schema": "S"},
        }
    )
    out = prov.render(fluid)
    servers = {s["server"]: s for s in out["servers"]}
    assert servers["bq-prod"] == {
        "server": "bq-prod",
        "type": "bigquery",
        "project": "acme-prod",
        "dataset": "orders_v2",
    }
    assert servers["sf_raw"]["type"] == "snowflake"
    assert servers["sf_raw"]["account"] == "acme"
    odcs_validation.validate(out)


def test_expect_removal_drops_server(full_import) -> None:
    # The server is dropped only when neither its expect nor an expose
    # binding owns it any more: exposes[0].binding is derived from it, and a
    # binding that still selects it keeps it (round-2 defect 15).
    prov, fluid = full_import
    fluid["expects"] = []
    del fluid["exposes"][0]["binding"]
    assert prov.render(fluid)["servers"] == []


def test_expose_binding_edit_reaches_its_server(full_import) -> None:
    prov, fluid = full_import
    assert fluid["exposes"][0]["binding"]["location"]["dataset"] == "orders"
    fluid["exposes"][0]["binding"]["location"]["dataset"] = "orders_gold"
    out = prov.render(fluid)
    assert [s["dataset"] for s in out["servers"]] == ["orders_gold"]


def test_qos_edits_reach_sla_properties(full_import) -> None:
    prov, fluid = full_import
    qos = fluid["exposes"][0]["qos"]
    assert qos["availability"] == "99%"
    qos["availability"] = "50%"
    qos["freshnessSLO"] = "PT1H"
    qos["labels"] = {"tier": "gold"}
    out = prov.render(fluid)
    sla = {p["property"]: p for p in out["slaProperties"]}
    assert sla["availability"]["value"] == 0.5
    assert sla["interval"]["value"] == "PT1H"
    assert sla["retention"] == {"property": "retention", "value": "365", "unit": "d"}
    assert sla["label:tier"]["value"] == "gold"
    odcs_validation.validate(out)


def test_untouched_import_still_round_trips(full_import) -> None:
    prov, fluid = full_import
    original = _load(FIXTURES / "odcs" / "contract-full.yaml")
    assert odcs_validation.roundtrip_check(original, prov.render(fluid))["equal"]


# ---------------------------------------------------------------------------
# Finding 30 — SchemaObjects without properties survive round-trip
# ---------------------------------------------------------------------------


def test_schema_objects_without_properties_round_trip() -> None:
    odcs = _minimal()
    odcs["schema"] = [
        {"name": "documented_only", "physicalType": "table", "description": "doc"},
        {"name": "empty_props", "properties": []},
        {"name": "t", "properties": [{"name": "a", "logicalType": "string"}]},
    ]
    odcs_validation.validate(odcs)
    assert OdcsProvider().roundtrip_check(odcs)["equal"]


def test_fresh_expose_without_fields_is_still_skipped() -> None:
    fluid = _fresh([])
    assert OdcsProvider().render(fluid)["schema"] == []


# ---------------------------------------------------------------------------
# Finding 31 — common FLUID numeric/timestamp types
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fluid_type, logical, bigquery, snowflake",
    [
        ("number", "number", "FLOAT64", "NUMBER"),
        ("FLOAT64", "number", "FLOAT64", "FLOAT"),
        ("INT64", "integer", "INT64", "NUMBER"),
        ("smallint", "integer", "INT64", "NUMBER"),
        ("TIMESTAMP_NTZ", "timestamp", "DATETIME", "TIMESTAMP_NTZ"),
    ],
)
def test_numeric_and_timestamp_types(fluid_type, logical, bigquery, snowflake) -> None:
    assert odcs_types.fluid_to_logical(fluid_type) == logical
    assert odcs_types.fluid_to_physical(fluid_type, "gcp") == bigquery
    assert odcs_types.fluid_to_physical(fluid_type, "snowflake") == snowflake
    fluid = _fresh([{"name": "amt", "type": fluid_type}], binding={"platform": "gcp"})
    prop = _props(OdcsProvider().render(fluid))[0]
    assert (prop["logicalType"], prop["physicalType"]) == (logical, bigquery)


# ---------------------------------------------------------------------------
# Finding 32 — top-level FLUID name/description/tags/domain are exported
# ---------------------------------------------------------------------------


def test_top_level_fluid_fields_are_exported() -> None:
    fluid = _fresh(
        [{"name": "a", "type": "string"}],
        name="Customer 360",
        description="All about customers",
        domain="Customer",
        tags=["crm"],
        lifecycle={"state": "deprecated"},
    )
    out = OdcsProvider().render(fluid)
    assert out["name"] == "Customer 360"
    assert out["description"] == {"purpose": "All about customers"}
    assert out["domain"] == "Customer"
    assert out["tags"] == ["crm"]
    odcs_validation.validate(out)


def test_metadata_fields_still_win_over_top_level() -> None:
    fluid = _fresh([{"name": "a", "type": "string"}], name="top")
    fluid["metadata"]["name"] = "meta"
    assert OdcsProvider().render(fluid)["name"] == "meta"


def test_customer360_example_exports_name_and_domain() -> None:
    contract = load_contract_with_overlay(
        str(REPO / "examples/customer360/contract.fluid.yaml"), None, LOG
    )
    out = OdcsProvider().render(contract)
    assert out["name"] == contract["name"]
    assert out["domain"] == contract["domain"]


# ---------------------------------------------------------------------------
# Finding 33 — fresh field quality keeps auto-checks and normalises items
# ---------------------------------------------------------------------------


def test_fresh_custom_quality_is_merged_with_auto_checks() -> None:
    fld = {
        "name": "id",
        "required": True,
        "tags": ["primary-key"],
        "quality": [{"description": "IDs must match CRM"}],
    }
    quality = OdcsProvider()._extract_field_quality(fld)
    assert [q.get("metric") for q in quality[:2]] == ["nullValues", "duplicateValues"]
    assert quality[2] == {"type": "text", "description": "IDs must match CRM"}


def test_fresh_non_odcs_quality_items_are_dropped() -> None:
    fluid = _fresh(
        [
            {
                "name": "amt",
                "type": "number",
                "required": True,
                "quality": [{"rule": "positive", "severity": "high"}],
            }
        ]
    )
    out = OdcsProvider().render(fluid)
    assert [q["metric"] for q in _props(out)[0]["quality"]] == ["nullValues"]
    odcs_validation.validate(out)


# ---------------------------------------------------------------------------
# Finding 52 — removing the FLUID description keeps the rest of the object
# ---------------------------------------------------------------------------


def test_removing_description_purpose_keeps_usage_and_limitations() -> None:
    prov = OdcsProvider()
    fluid = prov.import_contract(_load(_UPSTREAM_V320))
    del fluid["metadata"]["description"]
    desc = prov.render(fluid)["description"]
    assert "purpose" not in desc
    assert desc["usage"] == "Predict sales over time"
    assert desc["authoritativeDefinitions"]


# ---------------------------------------------------------------------------
# Finding 53 — a type change drops stale physicalType / logicalTypeOptions
# ---------------------------------------------------------------------------


def test_type_change_drops_stale_type_details() -> None:
    odcs = _minimal()
    odcs["schema"][0]["properties"] = [
        {
            "name": "qty",
            "logicalType": "integer",
            "physicalType": "INT64",
            "logicalTypeOptions": {"minimum": 0},
        }
    ]
    prov = OdcsProvider()
    fluid = prov.import_contract(odcs)
    fluid["exposes"][0]["contract"]["schema"][0]["type"] = "string"
    out = prov.render(fluid)
    prop = _props(out)[0]
    assert prop["logicalType"] == "string"
    assert "logicalTypeOptions" not in prop
    assert prop.get("physicalType") != "INT64"
    odcs_validation.validate(out)


def test_unchanged_type_keeps_type_details() -> None:
    odcs = _minimal()
    odcs["schema"][0]["properties"] = [
        {
            "name": "qty",
            "logicalType": "integer",
            "physicalType": "INT64",
            "logicalTypeOptions": {"minimum": 0},
        }
    ]
    assert OdcsProvider().roundtrip_check(odcs)["equal"]


# ---------------------------------------------------------------------------
# Finding 65 — fresh servers keep a StableId ``id``
# ---------------------------------------------------------------------------


def test_fresh_servers_carry_a_stable_id() -> None:
    fluid = _fresh(
        [{"name": "a", "type": "string"}],
        binding={"platform": "gcp", "location": {"project": "p", "dataset": "d"}},
    )
    fluid["exposes"][0]["exposeId"] = "orders.v1 daily"
    fluid["expects"] = [{"id": "raw_events", "provider": "kafka", "location": {"host": "k"}}]
    for api in ("v3.1.0", "v3.2.0"):
        out = OdcsProvider(api_version=api).render(fluid)
        assert [(s["id"], s["server"]) for s in out["servers"]] == [
            ("orders_v1_daily", "orders.v1 daily"),
            ("raw_events", "raw_events"),
        ]
        odcs_validation.validate(out)


def test_validation_accepts_yaml_dates_as_iso_strings() -> None:
    import datetime

    from fluid_build.providers.odps_standard import validation as odps_validation

    doc = yaml.safe_load(
        (Path(__file__).parent / "fixtures" / "odcs" / "contract-minimal.yaml").read_text()
    )
    doc["slaProperties"] = [{"property": "effectiveFrom", "value": datetime.date(2024, 1, 1)}]
    before = copy.deepcopy(doc)
    odcs_validation.validate(doc)  # does not raise
    assert doc == before  # never mutates the caller's document
    assert odcs_validation.json_instance(doc)["slaProperties"][0]["value"] == "2024-01-01"
    assert odps_validation.json_instance is odcs_validation.json_instance
