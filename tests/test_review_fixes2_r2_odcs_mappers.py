# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Regression tests for the round-2 ODCS mapper defects.

Each test names the defect it pins. Every one of them failed on the branch
head the round-2 verification ran against (``385811f``).
"""

from __future__ import annotations

import contextlib
import copy
import io
import logging
from pathlib import Path
from typing import Any, Dict, List

import pytest
import yaml

from fluid_build.providers.odcs import OdcsProvider
from fluid_build.providers.odcs import validation as odcs_validation

FIXTURES = Path(__file__).parent / "fixtures"
FULL_V320 = FIXTURES / "upstream" / "odcs" / "full-example.v3.2.0.odcs.yaml"
CONTRACT_FULL = FIXTURES / "odcs" / "contract-full.yaml"


def _load(path: Path) -> Dict[str, Any]:
    with open(path) as f:
        return yaml.safe_load(f)


def _cli(argv: List[str]) -> Any:
    from fluid_build.cli import main as cli_main

    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            return cli_main(argv)
    except SystemExit as exc:
        return exc.code


def _v320_fixtures() -> List[Path]:
    """Every upstream ODCS fixture declaring apiVersion v3.2.0."""
    found = []
    for path in sorted((FIXTURES / "upstream" / "odcs").glob("*.yaml")):
        if str(_load(path).get("apiVersion")) == "v3.2.0":
            found.append(path)
    assert found, "no upstream v3.2.0 ODCS fixtures found"
    return found


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in (
        "ODCS_API_VERSION",
        "ODCS_INCLUDE_QUALITY",
        "ODCS_INCLUDE_SLA",
        "ODCS_VALIDATE",
        "ODCS_VOWL_VALIDATE",
    ):
        monkeypatch.delenv(var, raising=False)


# ---------------------------------------------------------------------------
# Defects 11 + 28 — slaProperties survive splitting a multi-SchemaObject ODCS
# into per-expose exports, and qos edits on any expose are honoured.
# ---------------------------------------------------------------------------


def _two_tables() -> Dict[str, Any]:
    return {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "id": "two-tables",
        "version": "1.0.0",
        "status": "active",
        "schema": [
            {"name": "orders", "properties": [{"name": "id", "logicalType": "string"}]},
            {"name": "customers", "properties": [{"name": "id", "logicalType": "string"}]},
        ],
        "slaProperties": [
            {"property": "availability", "value": 0.999},
            {"property": "latency", "value": 4, "unit": "h"},
            {"property": "generalAvailability", "value": "2022-05-12T09:30:10-08:00"},
        ],
    }


def test_defect11_every_scoped_expose_keeps_document_sla() -> None:
    src = _two_tables()
    prov = OdcsProvider()
    fluid = prov.import_contract(src)
    for eid in ("orders", "customers"):
        out = prov.render(fluid, expose_id=eid)
        assert out["slaProperties"] == src["slaProperties"], eid
        odcs_validation.validate(out)
    assert prov.render(fluid)["slaProperties"] == src["slaProperties"]


def test_defect11_upstream_v320_example_keeps_all_sla_per_expose() -> None:
    src = _load(FULL_V320)
    prov = OdcsProvider()
    fluid = prov.import_contract(src)
    assert len(fluid["exposes"]) == 2
    for expose in fluid["exposes"]:
        out = prov.render(fluid, expose_id=expose["exposeId"])
        assert out["slaProperties"] == src["slaProperties"], expose["exposeId"]


def test_defect11_opds_cli_split_keeps_sla_for_every_port(tmp_path) -> None:
    src_path = tmp_path / "two-tables.odcs.yaml"
    src_path.write_text(yaml.safe_dump(_two_tables(), sort_keys=False))
    fluid_path = tmp_path / "two.fluid.yaml"
    assert _cli(["opds", "import", str(src_path), "-o", str(fluid_path)]) in (0, None)
    out_dir = tmp_path / "out"
    rc = _cli(["opds", "export", str(fluid_path), "--out-dir", str(out_dir)])
    assert rc in (0, None)
    odcs_files = sorted(out_dir.glob("*.odcs.yaml"))
    assert len(odcs_files) == 2
    for path in odcs_files:
        assert _load(path)["slaProperties"] == _two_tables()["slaProperties"], path.name


def test_defect11_dmm_per_expose_payloads_keep_sla() -> None:
    from fluid_build.providers.datamesh_manager.datamesh_manager import (
        DataMeshManagerProvider,
    )

    fluid = OdcsProvider().import_contract(_two_tables())
    dmm = DataMeshManagerProvider(api_key="test-key", api_url="https://dmm.invalid")
    previews = dmm._preview_odcs_per_expose(fluid, "two-tables")
    assert len(previews) == 2
    for preview in previews:
        assert preview["payload"]["slaProperties"] == _two_tables()["slaProperties"]


def test_defect11_scoped_edit_on_projected_expose_still_applies() -> None:
    prov = OdcsProvider()
    fluid = prov.import_contract(_two_tables())
    fluid["exposes"][0]["qos"]["availability"] = "95%"
    out = prov.render(fluid, expose_id="orders")
    assert out["slaProperties"][0] == {"property": "availability", "value": 0.95}
    assert out["slaProperties"][1:] == _two_tables()["slaProperties"][1:]


def test_defect28_qos_edit_on_second_expose_is_honoured() -> None:
    src = _two_tables()
    src["slaProperties"] = [{"property": "availability", "value": 0.99}]
    prov = OdcsProvider()
    fluid = prov.import_contract(src)
    assert "qos" not in fluid["exposes"][1]
    fluid["exposes"][1]["qos"] = {"availability": "90%"}
    assert prov.render(fluid)["slaProperties"] == [{"property": "availability", "value": 0.9}]
    scoped = prov.render(fluid, expose_id="customers")
    assert scoped["slaProperties"] == [{"property": "availability", "value": 0.9}]
    # The untouched expose's own scoped contract keeps the source SLA.
    assert prov.render(fluid, expose_id="orders")["slaProperties"] == src["slaProperties"]


def test_defect28_freshness_and_label_edits_on_second_expose() -> None:
    prov = OdcsProvider()
    fluid = prov.import_contract(_two_tables())
    fluid["exposes"][1]["qos"] = {"freshnessSLO": "PT1H", "labels": {"tier": "gold"}}
    sla = prov.render(fluid)["slaProperties"]
    assert {"property": "availability", "value": 0.999} in sla
    latency = [p for p in sla if p["property"] == "latency"]
    assert latency and latency[0]["value"] == "PT1H"
    assert {"property": "label:tier", "value": "gold"} in sla


# ---------------------------------------------------------------------------
# Defects 13 + 26 — exporting an imported v3.2.0 contract at a lower
# apiVersion downgrades the v3.2.0-only content so the output validates.
# ---------------------------------------------------------------------------


def _every_v320_construct() -> Dict[str, Any]:
    """A valid v3.2.0 contract using every v3.2.0-only construct FLUID stashes."""
    return {
        "apiVersion": "v3.2.0",
        "kind": "DataContract",
        "id": "v320-only",
        "version": "1.0.0",
        "status": "active",
        "context": "Use for revenue analytics only.",
        "customProperties": [{"property": "costCenter", "value": "CC-1", "vendor": "acme"}],
        "servers": [
            {
                "server": "lake",
                "type": "iceberg",
                "catalog": "main",
                "catalogUrl": "https://iceberg.example.com",
                "namespace": "sales",
            },
            {
                "server": "pg",
                "type": "postgres",
                "host": "db",
                "port": "${DB_PORT}",
                "database": "d",
                "schema": "s",
            },
            {
                "server": "pg2",
                "type": "postgres",
                "host": "db",
                "port": "5432",
                "database": "d",
                "schema": "s",
            },
            {"server": "files", "type": "s3", "location": "s3://b/p", "encoding": "utf-8"},
            {
                "server": "ath",
                "type": "athena",
                "stagingDir": "s3://b/tmp",
                "schema": "s",
                "workgroup": "primary",
            },
        ],
        "schema": [
            {
                "name": "orders",
                "logicalType": "object",
                "context": {"instructions": "One row per order."},
                "synonyms": [{"synonym": "sales_orders"}],
                "deprecated": False,
                "relationships": [
                    {"id": "rel1", "type": "foreignKey", "from": "orders.cid", "to": "customers.id"}
                ],
                "properties": [
                    {
                        "name": "status",
                        "logicalType": "string",
                        "enum": [{"value": "open"}, {"value": "closed"}],
                        "semanticType": "dimension",
                        "deprecated": True,
                        "customProperties": [{"property": "x", "value": 1, "vendor": "soda"}],
                    },
                    {
                        "name": "attrs",
                        "logicalType": "map",
                        "physicalType": "MAP<STRING, STRING>",
                        "map": {
                            "key": {"logicalType": "string"},
                            "value": {"logicalType": "string"},
                        },
                    },
                    {
                        "name": "embedding",
                        "logicalType": "vector",
                        "logicalTypeOptions": {"dimensions": 3, "elementType": "float32"},
                    },
                    {
                        "name": "lines",
                        "logicalType": "array",
                        "items": {
                            "name": "line",
                            "logicalType": "object",
                            "properties": [
                                {"name": "sku", "logicalType": "string", "semanticType": "column"}
                            ],
                        },
                    },
                ],
            }
        ],
        "slaProperties": [
            {
                "property": "availability",
                "value": 0.99,
                "customProperties": [{"property": "note", "value": "x"}],
                "authoritativeDefinitions": [{"type": "sla", "url": "https://x.example.com"}],
            }
        ],
    }


def test_defect26_fixture_is_valid_v320_but_not_v310() -> None:
    doc = _every_v320_construct()
    odcs_validation.validate(doc)
    v310 = dict(doc, apiVersion="v3.1.0")
    with pytest.raises(Exception):
        odcs_validation.validate(v310)


@pytest.mark.parametrize("path", _v320_fixtures(), ids=lambda p: p.name)
def test_defect26_upstream_v320_fixtures_downgrade_to_v310(path, caplog) -> None:
    fluid = OdcsProvider().import_contract(_load(path))
    with caplog.at_level(logging.WARNING):
        out = OdcsProvider(api_version="v3.1.0").render(fluid)
    assert out["apiVersion"] == "v3.1.0"
    odcs_validation.validate(out)
    assert "v3.2.0" in caplog.text and "v3.1.0" in caplog.text
    # Per-port renders downgrade too.
    for expose in fluid["exposes"]:
        scoped = OdcsProvider(api_version="v3.1.0").render(fluid, expose_id=expose["exposeId"])
        odcs_validation.validate(scoped)


def test_defect26_map_becomes_object_with_shape_in_custom_properties() -> None:
    fluid = OdcsProvider().import_contract(_load(FULL_V320))
    out = OdcsProvider(api_version="v3.1.0").render(fluid)
    props = {p["name"]: p for p in out["schema"][1]["properties"]}
    attrs = props["receiver_attributes"]
    assert attrs["logicalType"] == "object"
    assert "map" not in attrs
    custom = {c["property"]: c["value"] for c in attrs["customProperties"]}
    assert custom["map"]["key"]["logicalType"] == "string"
    code = {p["name"]: p for p in out["schema"][0]["properties"]}["rcvr_cntry_code"]
    assert "enum" not in code
    assert [c for c in code["customProperties"] if c["property"] == "enum"]


def test_defect26_every_v320_construct_downgrades(caplog) -> None:
    fluid = OdcsProvider().import_contract(_every_v320_construct())
    with caplog.at_level(logging.WARNING):
        out = OdcsProvider(api_version="v3.1.0").render(fluid)
    odcs_validation.validate(out)
    assert "context" not in out
    assert {"property": "context", "value": "Use for revenue analytics only."} in out[
        "customProperties"
    ]
    assert all("vendor" not in c for c in out["customProperties"])
    servers = {s["server"]: s for s in out["servers"]}
    assert servers["lake"]["type"] == "custom"
    assert servers["lake"]["catalog"] == "main"
    lake_custom = {c["property"]: c["value"] for c in servers["lake"]["customProperties"]}
    assert lake_custom["namespace"] == "sales"
    assert lake_custom["serverType"] == "iceberg"
    assert servers["pg2"]["type"] == "postgres"
    assert servers["pg2"]["port"] == 5432
    # A ``${VAR}`` port cannot be a v3.1.0 Postgres port (required integer).
    assert servers["pg"]["type"] == "custom"
    assert servers["pg"]["host"] == "db"
    assert "port" not in servers["pg"]
    pg_custom = {c["property"]: c["value"] for c in servers["pg"]["customProperties"]}
    assert pg_custom == {"serverType": "postgres", "port": "${DB_PORT}"}
    assert "encoding" not in servers["files"]
    assert "workgroup" not in servers["ath"]
    obj = out["schema"][0]
    for key in ("context", "synonyms", "deprecated"):
        assert key not in obj
    assert "id" not in obj["relationships"][0]
    props = {p["name"]: p for p in obj["properties"]}
    assert props["embedding"]["logicalType"] == "array"
    assert "logicalTypeOptions" not in props["embedding"]
    assert props["attrs"]["logicalType"] == "object"
    for key in ("enum", "semanticType", "deprecated"):
        assert key not in props["status"]
    assert "semanticType" not in props["lines"]["items"]["properties"][0]
    sla = out["slaProperties"][0]
    assert sla == {"property": "availability", "value": 0.99}
    assert "downgraded" in caplog.text


def test_defect13_env_pin_downgrades_and_validate_env_passes(monkeypatch) -> None:
    fluid = OdcsProvider().import_contract(_load(FULL_V320))
    monkeypatch.setenv("ODCS_API_VERSION", "v3.1.0")
    monkeypatch.setenv("ODCS_VALIDATE", "true")
    out = OdcsProvider().render(fluid)
    assert out["apiVersion"] == "v3.1.0"
    odcs_validation.validate(out)


def test_defect13_cli_export_with_env_pin_validates(tmp_path, monkeypatch) -> None:
    fluid_path = tmp_path / "full.fluid.yaml"
    assert _cli(["odcs", "import", str(FULL_V320), "-o", str(fluid_path)]) in (0, None)
    monkeypatch.setenv("ODCS_API_VERSION", "v3.1.0")
    out_path = tmp_path / "out.yaml"
    assert _cli(["odcs", "export", str(fluid_path), "-o", str(out_path)]) in (0, None)
    assert _load(out_path)["apiVersion"] == "v3.1.0"
    assert _cli(["odcs", "validate", str(out_path)]) in (0, None)


def test_defect26_default_reexport_of_v320_is_still_verbatim() -> None:
    src = _every_v320_construct()
    assert OdcsProvider().roundtrip_check(src)["equal"]
    assert OdcsProvider().roundtrip_check(_load(FULL_V320))["equal"]


def test_defect26_v310_import_rendered_at_v310_is_untouched() -> None:
    src = _load(CONTRACT_FULL)
    fluid = OdcsProvider().import_contract(src)
    out = OdcsProvider(api_version="v3.1.0").render(fluid)
    assert odcs_validation.roundtrip_check(src, out)["equal"]


# ---------------------------------------------------------------------------
# Defect 14 — owner edits on a team without a name update members.
# ---------------------------------------------------------------------------


def _with_team(team: Any) -> Dict[str, Any]:
    return {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "id": "t",
        "version": "1.0.0",
        "status": "active",
        "team": team,
    }


_ARRAY_TEAM = [
    {"username": "ceastwood", "role": "Data Scientist", "dateIn": "2022-08-02"},
    {"username": "mhopper", "role": "Data Steward"},
]


def test_defect14_array_team_email_edit_keeps_members() -> None:
    prov = OdcsProvider()
    fluid = prov.import_contract(_with_team(copy.deepcopy(_ARRAY_TEAM)))
    fluid["owner"]["email"] = "clint@example.com"
    out = prov.render(fluid)
    assert out["team"] == [
        {"username": "clint@example.com", "role": "Data Scientist", "dateIn": "2022-08-02"},
        {"username": "mhopper", "role": "Data Steward"},
    ]
    odcs_validation.validate(out)


def test_defect14_array_team_role_edit_and_new_contact() -> None:
    prov = OdcsProvider()
    fluid = prov.import_contract(_with_team(copy.deepcopy(_ARRAY_TEAM)))
    fluid["owner"]["role"] = "Owner"
    fluid["owner"]["contacts"].append({"email": "new@example.com"})
    out = prov.render(fluid)
    assert out["team"] == [
        {"username": "ceastwood", "role": "Owner", "dateIn": "2022-08-02"},
        {"username": "mhopper", "role": "Data Steward"},
        {"username": "new@example.com"},
    ]


def test_defect14_object_team_without_name_edit_keeps_team() -> None:
    team = {"description": "no name", "members": copy.deepcopy(_ARRAY_TEAM)}
    prov = OdcsProvider()
    fluid = prov.import_contract(_with_team(team))
    fluid["owner"]["email"] = "clint@example.com"
    out = prov.render(fluid)
    assert "name" not in out["team"]
    assert out["team"]["description"] == "no name"
    assert out["team"]["members"][0] == {
        "username": "clint@example.com",
        "role": "Data Scientist",
        "dateIn": "2022-08-02",
    }
    odcs_validation.validate(out)


def test_defect14_setting_team_name_on_nameless_team() -> None:
    prov = OdcsProvider()
    fluid = prov.import_contract(_with_team(copy.deepcopy(_ARRAY_TEAM)))
    fluid["owner"]["team"] = "data-science"
    out = prov.render(fluid)
    assert out["team"]["name"] == "data-science"
    assert out["team"]["members"][0]["dateIn"] == "2022-08-02"


def test_defect14_owner_removed_still_drops_team() -> None:
    prov = OdcsProvider()
    fluid = prov.import_contract(_with_team(copy.deepcopy(_ARRAY_TEAM)))
    del fluid["owner"]
    assert "team" not in prov.render(fluid)


# ---------------------------------------------------------------------------
# Defect 15 — a stashed server stays while an expose binding selects it.
# ---------------------------------------------------------------------------


def test_defect15_removing_expects_keeps_bound_server() -> None:
    src = _load(CONTRACT_FULL)
    prov = OdcsProvider()
    fluid = prov.import_contract(src)
    fluid["expects"] = []
    out = prov.render(fluid)
    assert out["servers"] == src["servers"]
    assert odcs_validation.roundtrip_check(src, out)["equal"]


def test_defect15_removing_expects_then_editing_binding_keeps_server_identity() -> None:
    prov = OdcsProvider()
    fluid = prov.import_contract(_load(CONTRACT_FULL))
    fluid["expects"] = []
    binding = fluid["exposes"][0]["binding"]
    binding.pop("physical_name", None)
    binding["format"] = "parquet"
    binding["location"]["dataset"] = "orders_gold"
    out = prov.render(fluid)
    assert len(out["servers"]) == 1
    server = out["servers"][0]
    assert server["server"] == "bq-prod"
    assert server["type"] == "bigquery"
    assert server["project"] == "acme-prod"
    assert server["dataset"] == "orders_gold"
    odcs_validation.validate(out)


def test_defect15_cli_round_trip_without_expects(tmp_path) -> None:
    fluid_path = tmp_path / "cf.fluid.yaml"
    assert _cli(["odcs", "import", str(CONTRACT_FULL), "-o", str(fluid_path)]) in (0, None)
    fluid = _load(fluid_path)
    del fluid["expects"]
    fluid_path.write_text(yaml.safe_dump(fluid, sort_keys=False))
    out_path = tmp_path / "out.yaml"
    assert _cli(["odcs", "export", str(fluid_path), "-o", str(out_path)]) in (0, None)
    assert _load(out_path)["servers"] == _load(CONTRACT_FULL)["servers"]


# ---------------------------------------------------------------------------
# Defect 25 — --no-quality suppresses object-level and nested quality too.
# ---------------------------------------------------------------------------


def _quality_paths(node: Any, path: str = "") -> List[str]:
    found: List[str] = []
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "quality":
                found.append(f"{path}.quality")
            found.extend(_quality_paths(value, f"{path}.{key}"))
    elif isinstance(node, list):
        for i, value in enumerate(node):
            found.extend(_quality_paths(value, f"{path}[{i}]"))
    return found


def test_defect25_no_quality_env_suppresses_object_quality(monkeypatch) -> None:
    fluid = OdcsProvider().import_contract(_load(CONTRACT_FULL))
    monkeypatch.setenv("ODCS_INCLUDE_QUALITY", "false")
    out = OdcsProvider().render(fluid)
    assert _quality_paths(out) == []
    odcs_validation.validate(out)


def test_defect25_no_quality_on_upstream_v320_and_nested_properties() -> None:
    src = _load(FULL_V320)
    src["schema"][1]["properties"].append(
        {
            "name": "nested",
            "logicalType": "object",
            "properties": [
                {
                    "name": "inner",
                    "logicalType": "string",
                    "quality": [{"type": "library", "metric": "nullValues", "mustBe": 0}],
                }
            ],
        }
    )
    fluid = OdcsProvider().import_contract(src)
    prov = OdcsProvider()
    prov.include_quality_checks = False
    out = prov.render(fluid)
    assert _quality_paths(out) == []
    # With quality on, every level is replayed.
    assert len(_quality_paths(OdcsProvider().render(fluid))) == 3


def test_defect25_cli_no_quality(tmp_path) -> None:
    fluid_path = tmp_path / "cf.fluid.yaml"
    assert _cli(["odcs", "import", str(CONTRACT_FULL), "-o", str(fluid_path)]) in (0, None)
    out_path = tmp_path / "out.yaml"
    rc = _cli(["odcs", "export", str(fluid_path), "--no-quality", "-o", str(out_path)])
    assert rc in (0, None)
    assert _quality_paths(_load(out_path)) == []


# ---------------------------------------------------------------------------
# Defect 27 — FLUID → ODCS status mapping is the 0.7.8 one (case-sensitive).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "status, expected",
    [
        ("development", "draft"),
        ("Development", "active"),
        ("draft", "draft"),
        ("deprecated", "deprecated"),
        ("retired", "retired"),
        ("whatever", "active"),
    ],
)
def test_defect27_status_mapping_matches_078(status, expected) -> None:
    assert OdcsProvider()._map_status_to_odcs(status) == expected
    fluid = {
        "id": "acme.orders",
        "metadata": {"version": "1.0.0", "status": status},
        "exposes": [{"exposeId": "o", "contract": {"schema": [{"name": "a", "type": "string"}]}}],
    }
    assert OdcsProvider().render(fluid)["status"] == expected


def test_defect27_snowflake_smoke_example_exports_active() -> None:
    from fluid_build.cli.bootstrap import load_contract_with_overlay

    paths = sorted((Path(__file__).resolve().parent.parent / "examples" / "snowflake").rglob("*"))
    smoke = [
        p
        for p in paths
        if p.is_file()
        and p.suffix in (".yaml", ".yml")
        and "smoke" in str(p)
        and "Development" in p.read_text()
    ]
    if not smoke:
        pytest.skip("snowflake smoke example not present")
    fluid = load_contract_with_overlay(str(smoke[0]), None, logging.getLogger(__name__))
    assert OdcsProvider().render(fluid)["status"] == "active"


# ---------------------------------------------------------------------------
# Defect 29 — a contract imported without ``schema`` gains no ``schema: []``.
# ---------------------------------------------------------------------------


def test_defect29_no_schema_round_trip_is_lossless() -> None:
    src = _with_team({"name": "x"})
    result = OdcsProvider().roundtrip_check(src)
    assert result["equal"], result


def test_defect29_schema_added_after_import_is_emitted() -> None:
    prov = OdcsProvider()
    fluid = prov.import_contract(_with_team({"name": "x"}))
    fluid["exposes"].append(
        {"exposeId": "orders", "contract": {"schema": [{"name": "id", "type": "string"}]}}
    )
    out = prov.render(fluid)
    assert [o["name"] for o in out["schema"]] == ["orders"]
    odcs_validation.validate(out)


def test_defect29_explicit_empty_schema_still_round_trips() -> None:
    src = _with_team({"name": "x"})
    src["schema"] = []
    assert OdcsProvider().roundtrip_check(src)["equal"]
