# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""ODCS v3.1.0 / v3.2.0 and ODPS v1.0.0 / v1.1.0 — version matrix.

Three guarantees, each checked against every fixture we have (hand-rolled
*and* the verbatim upstream examples under ``tests/fixtures/upstream``):

1. **Lossless round-trip** — import → export reproduces the document
   byte-for-byte, whatever version it declares (including the older-than-
   vendored v3.0.0 adventureworks contract).
2. **Version fidelity** — the re-export declares the *same* ``apiVersion``
   the source did, and validates against the schema for that version.
3. **Version selection** — a document is judged by the schema it claims:
   a v3.2.0 / v1.1.0 doc is never rejected by the older schema's ``enum``,
   and a doc using new-version fields *is* rejected when explicitly checked
   against the older schema.

Refreshing the upstream fixtures (Bitol repos are Apache-2.0)::

    B=https://raw.githubusercontent.com/bitol-io
    curl -sSL $B/open-data-contract-standard/main/docs/examples/all/full-example.odcs.yaml \\
         -o tests/fixtures/upstream/odcs/full-example.v3.2.0.odcs.yaml
    for n in customer-data-product deprecated-data-product simple-data-product variables vendor-custom-properties; do
      curl -sSL $B/open-data-product-standard/main/docs/examples/$n.odps.yaml \\
           -o tests/fixtures/upstream/odps/$n.v1.1.0.odps.yaml; done
"""

from __future__ import annotations

import copy
import logging
from pathlib import Path
from typing import Any, Dict

import pytest
import yaml

from fluid_build.providers.base import ProviderError
from fluid_build.providers.odcs import OdcsProvider
from fluid_build.providers.odcs import validation as odcs_validation
from fluid_build.providers.odcs.validation import roundtrip_check
from fluid_build.providers.odps_standard import BitolOdpsProvider
from fluid_build.providers.odps_standard import validation as odps_validation
from fluid_build.providers.odps_standard.mappers import EXPORT_PIPELINE as ODPS_EXPORT
from fluid_build.providers.odps_standard.mappers import IMPORT_PIPELINE as ODPS_IMPORT
from fluid_build.providers.odps_standard.mappers.base import ExportCtx, ImportCtx

FIXTURES = Path(__file__).parent / "fixtures"
LOG = logging.getLogger("test_spec_version_matrix")

ODCS_FIXTURES = sorted(
    list((FIXTURES / "odcs").glob("*.yaml"))
    + list((FIXTURES / "upstream" / "odcs").glob("*.yaml"))
    + list((FIXTURES / "odps" / "product-bitol").glob("*.odcs.yaml"))
)
ODPS_FIXTURES = sorted(
    list((FIXTURES / "odps" / "product-bitol").glob("*.odps.yaml"))
    + list((FIXTURES / "upstream" / "odps").glob("*.yaml"))
)
_ids = lambda paths: [p.name for p in paths]  # noqa: E731


def _load(path: Path) -> Dict[str, Any]:
    with open(path) as f:
        return yaml.safe_load(f)


def _odps_mapper_roundtrip(odps: Dict[str, Any]) -> Dict[str, Any]:
    """Run the ODPS mapper pipelines only (no contract resolver)."""
    fluid: Dict[str, Any] = {}
    for m in ODPS_IMPORT:
        m.to_fluid(ImportCtx(odps=odps, fluid=fluid, logger=LOG))
    out: Dict[str, Any] = {}
    ectx = ExportCtx(fluid=fluid, odps=out, logger=LOG, options={"include_custom_properties": True})
    for m in ODPS_EXPORT:
        m.to_odps(ectx)
    return out


def _fail_msg(name: str, r: Dict[str, Any]) -> str:
    return (
        f"Round-trip diff for {name}:\n"
        f"  missing: {r['missing'][:10]}\n"
        f"  extra  : {r['extra'][:10]}\n"
        f"  changed: {r['changed'][:10]}"
    )


# ---------------------------------------------------------------------------
# 1 + 2: lossless round-trip, version fidelity, re-export validity
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", ODCS_FIXTURES, ids=_ids(ODCS_FIXTURES))
def test_odcs_roundtrip_is_lossless_and_version_faithful(path: Path) -> None:
    odcs = _load(path)
    prov = OdcsProvider()
    out = prov.render(prov.import_contract(odcs))
    r = roundtrip_check(odcs, out)
    assert r["equal"], _fail_msg(path.name, r)
    assert out["apiVersion"] == odcs["apiVersion"]
    prov.validate_contract(out)  # judged by the version it declares


@pytest.mark.parametrize("path", ODPS_FIXTURES, ids=_ids(ODPS_FIXTURES))
def test_odps_mapper_roundtrip_is_lossless_and_version_faithful(path: Path) -> None:
    odps = _load(path)
    out = _odps_mapper_roundtrip(odps)
    r = roundtrip_check(odps, out)
    assert r["equal"], _fail_msg(path.name, r)
    assert out["apiVersion"] == odps["apiVersion"]
    BitolOdpsProvider().validate_product(out)


def test_odps_output_port_contract_id_is_preserved_not_reminted() -> None:
    """Regression: export used to overwrite an imported port's contractId with
    the ``{productId}.{port}`` convention (input ports were already right)."""
    odps = _load(FIXTURES / "upstream" / "odps" / "customer-data-product.v1.1.0.odps.yaml")
    out = _odps_mapper_roundtrip(odps)
    assert [p["contractId"] for p in out["outputPorts"]] == [
        p["contractId"] for p in odps["outputPorts"]
    ]


def test_odcs_explicit_false_and_empty_list_survive() -> None:
    """Regression: ``required: false`` / ``primaryKey: false`` / ``tags: []``
    were dropped by truthiness checks (800+ diffs on adventureworks)."""
    odcs = {
        "version": "1.0.0",
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "id": "t",
        "status": "active",
        "schema": [
            {
                "name": "t",
                "properties": [
                    {
                        "name": "a",
                        "logicalType": "string",
                        "required": False,
                        "primaryKey": False,
                        "tags": [],
                    },
                ],
            }
        ],
    }
    prov = OdcsProvider()
    out = prov.render(prov.import_contract(odcs))
    assert out["schema"][0]["properties"][0] == odcs["schema"][0]["properties"][0]


def test_odcs_imported_doc_without_servers_does_not_synthesize_one() -> None:
    odcs = {
        "version": "1.0.0",
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "id": "t",
        "status": "active",
        "schema": [
            {
                "name": "t",
                "physicalType": "table",
                "properties": [{"name": "a", "logicalType": "string"}],
            }
        ],
    }
    prov = OdcsProvider()
    out = prov.render(prov.import_contract(odcs))
    assert "servers" not in out
    prov.validate_contract(out)


def test_odcs_fluid_edit_wins_over_stash() -> None:
    """The stash is a base layer, not a freeze: a FLUID-side edit is exported."""
    odcs = _load(FIXTURES / "odcs" / "contract-full.yaml")
    prov = OdcsProvider()
    fluid = prov.import_contract(odcs)
    fld = fluid["exposes"][0]["contract"]["schema"][1]  # customer_id
    fld["description"] = "EDITED"
    fld["required"] = False
    out = prov.render(fluid)
    prop = out["schema"][0]["properties"][1]
    assert prop["description"] == "EDITED"
    assert prop["required"] is False
    # untouched siblings still verbatim
    assert out["schema"][0]["properties"][0] == odcs["schema"][0]["properties"][0]


# ---------------------------------------------------------------------------
# 3: version selection
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "declared, expected",
    [
        ("v3.2.0", "v3.2.0"),
        ("v3.1.0", "v3.1.0"),
        ("3.1.0", "v3.1.0"),  # bare
        ("v3.0.0", "v3.2.0"),  # older-than-vendored → newest superset
        ("v9.9.9", "v3.2.0"),  # unknown-future → newest (still fails on enum)
        (None, "v3.2.0"),
    ],
)
def test_odcs_resolve_schema_version(declared, expected) -> None:
    doc = {"apiVersion": declared} if declared is not None else {}
    assert odcs_validation.resolve_schema_version(doc) == expected


@pytest.mark.parametrize(
    "declared, expected",
    [("v1.1.0", "v1.1.0"), ("v1.0.0", "v1.0.0"), ("v0.9.0", "v1.1.0"), (None, "v1.1.0")],
)
def test_odps_resolve_schema_version(declared, expected) -> None:
    doc = {"apiVersion": declared} if declared is not None else {}
    assert odps_validation.resolve_schema_version(doc) == expected


def test_load_schema_rejects_unknown_version() -> None:
    with pytest.raises(ProviderError, match="Unsupported ODCS apiVersion"):
        odcs_validation.load_schema("v9.9.9")
    with pytest.raises(ProviderError, match="Unsupported ODPS apiVersion"):
        odps_validation.load_schema("v9.9.9")


def test_odcs_v320_doc_passes_auto_but_fails_v310_schema() -> None:
    doc = _load(FIXTURES / "upstream" / "odcs" / "full-example.v3.2.0.odcs.yaml")
    odcs_validation.validate(doc)  # auto → v3.2.0
    with pytest.raises(ProviderError, match="v3.2.0"):
        odcs_validation.validate(doc, odcs_validation.load_schema("v3.1.0"))


def test_odps_v110_features_pass_auto_but_fail_v100_schema() -> None:
    doc = _load(FIXTURES / "upstream" / "odps" / "customer-data-product.v1.1.0.odps.yaml")
    odps_validation.validate(doc)  # auto → v1.1.0
    with pytest.raises(ProviderError):
        odps_validation.validate(doc, odps_validation.load_schema("v1.0.0"))


def test_odps_v110_status_is_optional_but_required_in_v100() -> None:
    doc = {"apiVersion": "v1.1.0", "kind": "DataProduct", "id": "x", "name": "x"}
    odps_validation.validate(doc)
    legacy = {**doc, "apiVersion": "v1.0.0"}
    with pytest.raises(ProviderError, match="status"):
        odps_validation.validate(legacy)


def test_older_than_vendored_odcs_validates_via_superset() -> None:
    doc = _load(FIXTURES / "upstream" / "odcs" / "postgresql-adventureworks.v3.0.0.odcs.yaml")
    odcs_validation.validate(doc)


# ---------------------------------------------------------------------------
# Emit version: default unchanged, opt-in to newer, preserved on re-export
# ---------------------------------------------------------------------------


@pytest.fixture
def fresh_fluid() -> Dict[str, Any]:
    return _load(FIXTURES / "fluid" / "contract-multi-expose.fluid.yaml")


def test_fresh_export_defaults_unchanged(fresh_fluid, monkeypatch) -> None:
    monkeypatch.delenv("ODCS_API_VERSION", raising=False)
    monkeypatch.delenv("ODPS_API_VERSION", raising=False)
    assert OdcsProvider().render(fresh_fluid)["apiVersion"] == "v3.1.0"
    prov = BitolOdpsProvider()
    prov.strict_validation = False
    assert prov.render(fresh_fluid)["product"]["apiVersion"] == "v1.0.0"


def test_opt_in_newer_via_constructor(fresh_fluid) -> None:
    assert OdcsProvider(api_version="v3.2.0").render(fresh_fluid)["apiVersion"] == "v3.2.0"
    assert OdcsProvider(api_version="3.2.0").render(fresh_fluid)["apiVersion"] == "v3.2.0"
    prov = BitolOdpsProvider(api_version="v1.1.0")
    prov.strict_validation = False
    assert prov.render(fresh_fluid)["product"]["apiVersion"] == "v1.1.0"


def test_opt_in_newer_via_env(fresh_fluid, monkeypatch) -> None:
    monkeypatch.setenv("ODCS_API_VERSION", "v3.2.0")
    monkeypatch.setenv("ODPS_API_VERSION", "v1.1.0")
    assert OdcsProvider().render(fresh_fluid)["apiVersion"] == "v3.2.0"
    prov = BitolOdpsProvider()
    prov.strict_validation = False
    assert prov.render(fresh_fluid)["product"]["apiVersion"] == "v1.1.0"


def test_fresh_v110_export_is_strictly_valid_and_uses_native_type(fresh_fluid) -> None:
    fluid = copy.deepcopy(fresh_fluid)
    fluid.setdefault("metadata", {})["product_type"] = "consumerAligned"
    prov = BitolOdpsProvider(api_version="v1.1.0")
    prov.strict_validation = True  # fail-loud against the v1.1.0 schema
    product = prov.render(fluid)["product"]
    assert product["type"] == "consumerAligned"
    assert not any(p.get("property") == "type" for p in product.get("customProperties", []))


def test_fresh_v100_export_keeps_legacy_custom_property_type(fresh_fluid) -> None:
    fluid = copy.deepcopy(fresh_fluid)
    fluid.setdefault("metadata", {})["product_type"] = "analytical"
    prov = BitolOdpsProvider(api_version="v1.0.0")
    prov.strict_validation = True
    product = prov.render(fluid)["product"]
    assert "type" not in product
    assert {"property": "type", "value": "analytical"} in product["customProperties"]


def test_explicit_version_overrides_preserved_on_reexport() -> None:
    odcs = _load(FIXTURES / "odcs" / "contract-minimal.yaml")  # declares v3.1.0
    fluid = OdcsProvider().import_contract(odcs)
    out = OdcsProvider(api_version="v3.2.0").render(fluid)
    assert out["apiVersion"] == "v3.2.0"
    odcs_validation.validate(out)


# ---------------------------------------------------------------------------
# v3.2.0 content: map / vector logical types, new server engines
# ---------------------------------------------------------------------------


def test_map_and_vector_logical_types_round_trip() -> None:
    odcs = {
        "version": "1.0.0",
        "apiVersion": "v3.2.0",
        "kind": "DataContract",
        "id": "t",
        "schema": [
            {
                "name": "t",
                "properties": [
                    {
                        "name": "attrs",
                        "logicalType": "map",
                        "map": {
                            "key": {"logicalType": "string"},
                            "value": {"logicalType": "integer"},
                        },
                    },
                    {
                        "name": "emb",
                        "logicalType": "vector",
                        "logicalTypeOptions": {"dimensions": 384, "elementType": "float32"},
                    },
                ],
            }
        ],
    }
    prov = OdcsProvider()
    fluid = prov.import_contract(odcs)
    types = {f["name"]: f["type"] for f in fluid["exposes"][0]["contract"]["schema"]}
    assert types == {"attrs": "map", "emb": "vector"}
    out = prov.render(fluid)
    r = roundtrip_check(odcs, out)
    assert r["equal"], _fail_msg("map/vector", r)
    prov.validate_contract(out)


@pytest.mark.parametrize(
    "platform, location, expected_keys",
    [
        (
            "iceberg",
            {
                "catalog": "c",
                "catalogUrl": "https://x/v1",
                "namespace": "db.s",
                "warehouse": "w",
                "junk": 1,
            },
            {"catalog", "catalogUrl", "namespace", "warehouse"},
        ),
        (
            "hana",
            {"host": "h", "port": 30015, "database": "d", "schema": "s", "junk": 1},
            {"host", "port", "database", "schema"},
        ),
        (
            "teradata",
            {"host": "h", "port": 1025, "database": "d", "schema": "dropped"},
            {"host", "port", "database"},
        ),
        (
            "exasol",
            {"host": "h", "port": 8563, "schema": "s", "database": "dropped"},
            {"host", "port", "schema"},
        ),
        ("actian", {"host": "h", "port": 1, "database": "d"}, {"host", "port", "database"}),
    ],
)
def test_new_server_engines_emit_only_allowed_fields_and_validate(
    platform, location, expected_keys
) -> None:
    fluid = {
        "id": "p",
        "metadata": {"name": "p", "version": "1.0.0", "status": "active"},
        "exposes": [
            {
                "id": "e",
                "binding": {"platform": platform, "location": location},
                "contract": {"schema": [{"name": "a", "type": "string"}]},
            }
        ],
    }
    out = OdcsProvider(api_version="v3.2.0").render(fluid)
    server = out["servers"][0]
    assert set(server) - {"server", "type"} == expected_keys
    odcs_validation.validate(out)  # unevaluatedProperties: false would catch leaks


# ---------------------------------------------------------------------------
# Tooling gaps handled gracefully
# ---------------------------------------------------------------------------


def test_vowl_skips_with_warning_on_unsupported_api_version(caplog) -> None:
    pytest.importorskip("vowl")
    doc = _load(FIXTURES / "upstream" / "odcs" / "full-example.v3.2.0.odcs.yaml")
    with caplog.at_level(logging.WARNING):
        result = odcs_validation.validate_via_vowl(doc)
    if result is None:
        assert any("vowl second-pass skipped" in r.message for r in caplog.records)
    else:  # a newer vowl that supports v3.2.0 — must report it correctly
        assert result["api_version"] == "v3.2.0"


def test_to_model_typed_view_or_clear_error() -> None:
    doc = _load(FIXTURES / "odcs" / "contract-full.yaml")
    prov = OdcsProvider()
    try:
        import open_data_contract_standard  # noqa: F401
    except ImportError:
        with pytest.raises(ProviderError, match="odcs-strict"):
            prov.to_model(doc)
        return
    model = prov.to_model(doc)
    assert model.id == doc["id"]
    assert model.model_dump(by_alias=True, mode="json", exclude_unset=True) == doc


# ---------------------------------------------------------------------------
# CLI: --spec bitol-1.1.0
# ---------------------------------------------------------------------------


def test_cli_spec_bitol_1_1_0_exports_v110(tmp_path: Path) -> None:
    from argparse import Namespace

    from fluid_build.cli.opds import SPEC_BITOL_1_1_0, cmd_opds_export, resolve_spec

    args = Namespace(
        contract=str(FIXTURES / "fluid" / "contract-multi-expose.fluid.yaml"),
        spec=SPEC_BITOL_1_1_0,
        version=None,
        out="-",
        out_dir=str(tmp_path),
        format="yaml",
        env=None,
        validate_strict=True,
        pretty=True,
    )
    assert resolve_spec(args) == SPEC_BITOL_1_1_0
    assert cmd_opds_export(args, LOG) == 0
    product = _load(next(tmp_path.glob("*.odps.yaml")))
    assert product["apiVersion"] == "v1.1.0"
    for odcs_file in tmp_path.glob("*.odcs.yaml"):
        odcs_validation.validate(_load(odcs_file))
