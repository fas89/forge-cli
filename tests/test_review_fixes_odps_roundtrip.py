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

"""Regression tests for the ODPS / ODCS round-trip review findings.

Covers the Bitol bundle import → export path (per-port ODCS state, the
contractId linking invariant, same-name ports), the ODPS v1.0.0 downgrade,
the legacy output-port custom properties, strict ODCS import, resolver
safety (id match, path traversal, remote limits) and bundle-export paths.
"""

from __future__ import annotations

import copy
import http.server
import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, Iterator

import pytest
import yaml

from fluid_build.providers.base import ProviderError
from fluid_build.providers.odcs import OdcsProvider
from fluid_build.providers.odcs.validation import roundtrip_check
from fluid_build.providers.odps_standard import BitolOdpsProvider, OdpsStandardProvider
from fluid_build.providers.odps_standard.resolver import (
    ContractNotFound,
    ContractResolver,
)
from fluid_build.providers.odps_standard.validation import validate as validate_odps

FIXTURES = Path(__file__).parent / "fixtures"
BUNDLE_DIR = FIXTURES / "odps" / "product-bitol"
UPSTREAM_ODPS = sorted((FIXTURES / "upstream" / "odps").glob("*.odps.yaml"))
MULTI_EXPOSE_FLUID = FIXTURES / "fluid" / "contract-multi-expose.fluid.yaml"

ORDERS_ID = "3f2b1c4e-0000-4000-8000-000000000001"
EVENTS_ID = "shop-events-contract"


def _load(path: Path) -> Any:
    with open(path) as f:
        return yaml.safe_load(f)


def _write(path: Path, doc: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(doc, sort_keys=False))
    return path


def _odps_doc() -> Dict[str, Any]:
    return {
        "apiVersion": "v1.1.0",
        "kind": "DataProduct",
        "id": "shop",
        "name": "Shop",
        "version": "3.0.0",
        "status": "active",
        "type": "sourceAligned",
        "outputPorts": [
            # No description: the ODCS SchemaObject's must not leak onto it.
            {"name": "orders", "version": "2.0.0", "contractId": ORDERS_ID, "type": "postgres"},
            {
                "name": "events",
                "version": "1.0.0",
                "contractId": EVENTS_ID,
                "description": "Event stream",
            },
        ],
    }


def _orders_odcs() -> Dict[str, Any]:
    """ODCS v3.2.0 with a postgres server, document-level fields and v3.2.0 enum."""
    return {
        "apiVersion": "v3.2.0",
        "kind": "DataContract",
        "id": ORDERS_ID,
        "version": "2.3.0",
        "status": "draft",
        "name": "Orders contract",
        "description": {"purpose": "Orders", "limitations": "None known", "usage": "Reporting"},
        "customProperties": [{"property": "ownerTeam", "value": "sales"}],
        "authoritativeDefinitions": [
            {"type": "businessDefinition", "url": "https://example.com/orders"}
        ],
        "schema": [
            {
                "name": "tbl_orders",
                "businessName": "Orders table",
                "physicalType": "table",
                "description": "The orders table (from ODCS)",
                "logicalType": "object",
                "properties": [
                    {
                        "name": "order_id",
                        "logicalType": "string",
                        "physicalType": "varchar(36)",
                        "required": True,
                    },
                    {
                        "name": "state",
                        "logicalType": "string",
                        "enum": [{"value": "open"}, {"value": "closed"}],
                    },
                ],
            }
        ],
        "servers": [
            {
                "server": "prod",
                "type": "postgres",
                "host": "db.example.com",
                "port": 5432,
                "database": "shop",
                "schema": "public",
            }
        ],
        "slaProperties": [{"property": "latency", "value": 4, "unit": "d"}],
    }


def _events_odcs() -> Dict[str, Any]:
    """ODCS v3.1.0 with a team, a kafka server and two SchemaObjects."""
    return {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "id": EVENTS_ID,
        "version": "1.0.0",
        "status": "active",
        "name": "Events",
        "team": {
            "name": "streaming",
            "members": [{"username": "a@example.com", "role": "owner"}],
        },
        "schema": [
            {
                "name": "clicks",
                "physicalType": "topic",
                "logicalType": "object",
                "properties": [{"name": "click_id", "logicalType": "string"}],
            },
            {
                "name": "views",
                "physicalType": "topic",
                "logicalType": "object",
                "properties": [{"name": "view_id", "logicalType": "string"}],
            },
        ],
        "servers": [{"server": "events", "type": "kafka", "host": "kafka:9092"}],
    }


@pytest.fixture
def hand_bundle(tmp_path: Path) -> Path:
    bundle = tmp_path / "bundle"
    _write(bundle / "shop.odps.yaml", _odps_doc())
    _write(bundle / f"{ORDERS_ID}.odcs.yaml", _orders_odcs())
    _write(bundle / f"{EVENTS_ID}.odcs.yaml", _events_odcs())
    return bundle


def _strict_provider() -> BitolOdpsProvider:
    prov = BitolOdpsProvider()
    prov.strict_validation = True
    return prov


def _via_yaml(fluid: Dict[str, Any]) -> Dict[str, Any]:
    """Mimic `fluid opds import -o f.yaml` + `fluid opds export f.yaml`."""
    return yaml.safe_load(yaml.safe_dump(fluid, sort_keys=False))


def _expose(fluid: Dict[str, Any], expose_id: str) -> Dict[str, Any]:
    return next(e for e in fluid["exposes"] if (e.get("exposeId") or e.get("id")) == expose_id)


# ---------------------------------------------------------------------------
# Findings 0 / 17 / 23 / 24 (+54): per-port ODCS state survives the bundle
# ---------------------------------------------------------------------------


class TestBundleRoundTripIsLosslessPerPort:
    @pytest.mark.parametrize("entry", ["directory", "file"])
    def test_repo_fixture_bundle_round_trips_every_document_strictly(self, entry: str) -> None:
        prov = _strict_provider()
        if entry == "directory":
            fluid = prov.import_directory(BUNDLE_DIR)
        else:
            fluid = prov.import_contract(next(BUNDLE_DIR.glob("*.odps.yaml")))
        bundle = prov.render(_via_yaml(fluid))  # strict validation runs inside render

        assert bundle["product"] == _load(BUNDLE_DIR / "commerce.orders-product.odps.yaml")
        on_disk = sorted(p.name for p in BUNDLE_DIR.glob("*.odcs.yaml"))
        assert sorted(f"{cid}.odcs.yaml" for cid in bundle["contracts"]) == on_disk
        for cid, odcs in bundle["contracts"].items():
            assert odcs == _load(BUNDLE_DIR / f"{cid}.odcs.yaml"), cid

    def test_hand_built_bundle_round_trips_every_document_strictly(self, hand_bundle: Path) -> None:
        prov = _strict_provider()
        bundle = prov.render(_via_yaml(prov.import_directory(hand_bundle)))
        assert roundtrip_check(_odps_doc(), bundle["product"])["equal"]
        assert bundle["product"] == _odps_doc()
        assert bundle["contracts"][ORDERS_ID] == _orders_odcs()
        assert bundle["contracts"][EVENTS_ID] == _events_odcs()
        # apiVersion is per-document and preserved (v3.2.0 stays v3.2.0).
        assert bundle["contracts"][ORDERS_ID]["apiVersion"] == "v3.2.0"
        assert bundle["contracts"][EVENTS_ID]["apiVersion"] == "v3.1.0"

    def test_written_bundle_matches_sources(self, hand_bundle: Path, tmp_path: Path) -> None:
        prov = _strict_provider()
        out = tmp_path / "out"
        prov.render(prov.import_directory(hand_bundle), out_dir=out)
        for src in hand_bundle.iterdir():
            assert _load(out / src.name) == _load(src), src.name

    def test_fluid_origin_bundle_round_trips_zero_diff(self, tmp_path: Path) -> None:
        prov = _strict_provider()
        first = prov.render(_load(MULTI_EXPOSE_FLUID), out_dir=tmp_path / "a")
        again = prov.render(_via_yaml(prov.import_directory(tmp_path / "a")))
        assert again["product"] == first["product"]
        assert again["contracts"] == first["contracts"]

    def test_import_keeps_port_state_on_the_expose(self) -> None:
        fluid = BitolOdpsProvider().import_directory(BUNDLE_DIR)
        daily = _expose(fluid, "daily_orders")
        doc = daily["odcs_passthrough"]["_document"]
        doc_pt = doc["metadata"]["odcs_passthrough"]
        assert doc_pt["_verbatim"]["apiVersion"] == "v3.1.0"
        assert doc_pt["servers"][0]["project"] == "acme-prod"
        assert daily["odcs_passthrough"]["_verbatim"]["name"] == "daily_orders"
        # FLUID-native binding comes from the port's own ODCS server.
        assert daily["binding"] == {
            "platform": "gcp",
            "location": {"project": "acme-prod", "dataset": "orders"},
        }
        # The input port gets back the upstream server the contract describes.
        customers = next(e for e in fluid["expects"] if e["id"] == "customers_raw")
        assert customers["location"]["account"] == "company-prod"

    def test_port_description_is_not_invented_from_odcs(self, hand_bundle: Path) -> None:
        # Finding 54: the port had no description; the SchemaObject had one.
        prov = _strict_provider()
        fluid = prov.import_directory(hand_bundle)
        assert "description" not in _expose(fluid, "orders")
        bundle = prov.render(fluid)
        assert "description" not in bundle["product"]["outputPorts"][0]
        schema_object = bundle["contracts"][ORDERS_ID]["schema"][0]
        assert schema_object["description"] == "The orders table (from ODCS)"

    def test_expose_edits_still_reach_the_per_port_odcs(self, hand_bundle: Path) -> None:
        prov = _strict_provider()
        fluid = prov.import_directory(hand_bundle)
        orders = _expose(fluid, "orders")
        orders["contract"]["schema"][0]["description"] = "Primary key"
        orders["description"] = "All orders"
        bundle = prov.render(fluid)
        odcs = bundle["contracts"][ORDERS_ID]
        assert odcs["schema"][0]["properties"][0]["description"] == "Primary key"
        assert odcs["schema"][0]["description"] == "All orders"
        assert bundle["product"]["outputPorts"][0]["description"] == "All orders"
        # Everything else is still the source document.
        assert odcs["servers"] == _orders_odcs()["servers"]
        assert odcs["version"] == "2.3.0"


# ---------------------------------------------------------------------------
# Findings 4 / 16 / 25: per-port ODCS id follows the port's real contractId
# ---------------------------------------------------------------------------


class TestLinkingInvariant:
    def test_odcs_render_accepts_contract_id(self) -> None:
        fluid = _load(MULTI_EXPOSE_FLUID)
        odcs = OdcsProvider().render(fluid, expose_id="daily_orders", contract_id="custom-id")
        assert odcs["id"] == "custom-id"
        default = OdcsProvider().render(fluid, expose_id="daily_orders")
        assert default["id"] == "commerce.orders-product.daily_orders"

    @pytest.mark.parametrize("path", UPSTREAM_ODPS, ids=lambda p: p.name)
    def test_upstream_examples_re_export(self, path: Path) -> None:
        prov = BitolOdpsProvider()
        prov.strict_validation = False  # their contracts are not vendored
        fluid = prov.import_contract(path, allow_remote=False, lenient=True)
        bundle = prov.render(_via_yaml(fluid))
        source = _load(path)
        assert bundle["product"] == source
        expected = [p["contractId"] for p in source["outputPorts"] if p.get("contractId")]
        assert list(bundle["contracts"]) == expected
        for cid, odcs in bundle["contracts"].items():
            assert odcs["id"] == cid

    def test_uuid_contract_ids_survive_renames(self, hand_bundle: Path) -> None:
        prov = _strict_provider()
        fluid = prov.import_directory(hand_bundle)
        renamed = _expose(fluid, "orders")
        renamed["id"] = renamed["exposeId"] = "orders_v2"
        fluid["contract"]["id"] = "shop2"
        bundle = prov.render(fluid)
        port = bundle["product"]["outputPorts"][0]
        assert port["name"] == "orders_v2"
        assert port["contractId"] == ORDERS_ID
        assert bundle["contracts"][ORDERS_ID]["id"] == ORDERS_ID

    def test_port_without_contract_id_does_not_crash(self) -> None:
        odps = {
            "apiVersion": "v1.1.0",
            "kind": "DataProduct",
            "id": "p2",
            "name": "P2",
            "outputPorts": [{"name": "orders"}, {"name": "other", "contractId": "p2.other"}],
        }
        prov = BitolOdpsProvider()
        prov.strict_validation = False
        fluid = prov.import_contract(odps, allow_remote=False, lenient=True)
        bundle = prov.render(fluid)
        assert bundle["product"] == odps  # contractId stays absent
        assert list(bundle["contracts"]) == ["p2.other"]


# ---------------------------------------------------------------------------
# Finding 36: same-name ports that differ only in version
# ---------------------------------------------------------------------------


class TestSameNamePorts:
    def _bundle(self, root: Path) -> Path:
        odps = {
            "apiVersion": "v1.1.0",
            "kind": "DataProduct",
            "id": "shop",
            "name": "Shop",
            "status": "active",
            "outputPorts": [
                {"name": "orders", "version": "1.0.0", "contractId": "shop.orders.v1"},
                {"name": "orders", "version": "2.0.0", "contractId": "shop.orders.v2"},
            ],
        }
        _write(root / "shop.odps.yaml", odps)
        for cid, col in (("shop.orders.v1", "id_v1"), ("shop.orders.v2", "id_v2")):
            _write(
                root / f"{cid}.odcs.yaml",
                {
                    "apiVersion": "v3.1.0",
                    "kind": "DataContract",
                    "id": cid,
                    "version": "1.0.0",
                    "status": "active",
                    "schema": [
                        {
                            "name": "orders",
                            "logicalType": "object",
                            "properties": [{"name": col, "logicalType": "string"}],
                        }
                    ],
                },
            )
        return root

    def test_each_port_gets_its_own_contract_and_re_exports(self, tmp_path: Path) -> None:
        root = self._bundle(tmp_path / "b")
        prov = _strict_provider()
        fluid = prov.import_directory(root)
        ids = [e["id"] for e in fluid["exposes"]]
        assert ids == ["orders@1.0.0", "orders@2.0.0"]
        cols = [e["contract"]["schema"][0]["name"] for e in fluid["exposes"]]
        assert cols == ["id_v1", "id_v2"]
        bundle = prov.render(_via_yaml(fluid))
        assert bundle["product"] == _load(root / "shop.odps.yaml")
        for cid in ("shop.orders.v1", "shop.orders.v2"):
            assert bundle["contracts"][cid] == _load(root / f"{cid}.odcs.yaml")


# ---------------------------------------------------------------------------
# Finding 5 (provider part): a v1.0.0 target never carries v1.1.0-only keys
# ---------------------------------------------------------------------------


class TestDowngradeToV100:
    def test_imported_v110_product_rendered_at_v100_is_valid(self) -> None:
        odps = {
            "apiVersion": "v1.1.0",
            "kind": "DataProduct",
            "id": "p11",
            "name": "P11",
            "status": "active",
            "type": "sourceAligned",
            "deprecated": False,
            "synonyms": [{"synonym": "Product eleven"}],
            "outputPorts": [
                {
                    "id": "port-1",
                    "name": "orders",
                    "contractId": "p11.orders",
                    "deprecated": False,
                    "customProperties": [{"id": "cp1", "property": "tier", "value": "gold"}],
                }
            ],
            "inputPorts": [{"name": "raw", "version": "1.0.0"}],
        }
        fluid = BitolOdpsProvider().import_contract(odps, allow_remote=False, lenient=True)
        prov = BitolOdpsProvider(api_version="v1.0.0")
        prov.strict_validation = False  # the per-port ODCS is an unresolved stub
        product = prov.render(fluid)["product"]
        assert product["apiVersion"] == "v1.0.0"
        for key in ("type", "deprecated", "context", "synonyms"):
            assert key not in product
        custom = {c["property"]: c["value"] for c in product["customProperties"]}
        assert custom == {
            "type": "sourceAligned",
            "deprecated": False,
            "synonyms": [{"synonym": "Product eleven"}],
        }
        port = product["outputPorts"][0]
        assert not {"id", "deprecated"} & set(port)
        assert port["version"] == "1"  # required by v1.0.0
        assert {"property": "tier", "value": "gold"} in port["customProperties"]
        assert product["inputPorts"][0]["contractId"] == "p11.input.raw"
        validate_odps(product)  # v1.0.0 schema
        # Without an explicit version the source's v1.1.0 is kept untouched.
        assert BitolOdpsProvider().render(fluid)["product"] == odps

    def test_fresh_v100_product_moves_deprecated_to_custom_properties(self) -> None:
        fluid = _load(MULTI_EXPOSE_FLUID)
        fluid.setdefault("metadata", {})["deprecated"] = True
        prov = BitolOdpsProvider(api_version="v1.0.0")
        product = prov.render(fluid)["product"]
        assert "deprecated" not in product
        assert {"property": "deprecated", "value": True} in product["customProperties"]
        validate_odps(product)


# ---------------------------------------------------------------------------
# Finding 18: fresh output ports keep main's custom properties / overrides
# ---------------------------------------------------------------------------


def _pii_fluid() -> Dict[str, Any]:
    return {
        "fluidVersion": "0.7.1",
        "kind": "DataProduct",
        "id": "sales.orders_v1",
        "name": "Orders",
        "metadata": {"environment": "production", "status": "active"},
        "access_control": {"auto_approve": True},
        "exposes": [
            {
                "id": "orders",
                "contract_id": "orders-contract-v2",
                "classification": "pii",
                "location": {"account": "acme", "database": "SALES"},
                "binding": {"platform": "snowflake"},
                "contract": {"schema": [{"name": "email", "type": "string"}]},
            },
            {
                "id": "totals",
                "contract": {"schema": [{"name": "total", "type": "number"}]},
            },
        ],
    }


class TestFreshOutputPortSurface:
    def test_custom_properties_contract_id_and_default_version(self, monkeypatch) -> None:
        monkeypatch.setenv("ODPS_DEFAULT_PORT_VERSION", "2")
        prov = BitolOdpsProvider()
        prov.strict_validation = False
        bundle = prov.render(_pii_fluid())
        orders, totals = bundle["product"]["outputPorts"]
        assert orders["version"] == "2"
        assert orders["contractId"] == "orders-contract-v2"
        custom = {c["property"]: c["value"] for c in orders["customProperties"]}
        assert custom == {
            "server": {"account": "acme", "database": "SALES"},
            "environment": "production",
            "containsPii": True,
            "autoApprove": False,
            "status": "active",
        }
        totals_custom = {c["property"]: c["value"] for c in totals["customProperties"]}
        assert totals_custom["containsPii"] is False
        assert totals_custom["autoApprove"] is True
        # The sibling ODCS follows the overridden contractId.
        assert bundle["contracts"]["orders-contract-v2"]["id"] == "orders-contract-v2"
        validate_odps(bundle["product"])

    def test_legacy_alias_emits_them_too(self, monkeypatch) -> None:
        monkeypatch.delenv("ODPS_DEFAULT_PORT_VERSION", raising=False)
        product = OdpsStandardProvider().render(_pii_fluid())
        assert product["outputPorts"][0]["version"] == "1"
        names = [c["property"] for c in product["outputPorts"][0]["customProperties"]]
        assert names == ["server", "environment", "containsPii", "autoApprove", "status"]

    def test_pii_in_field_classification_disables_auto_approve(self) -> None:
        fluid = _pii_fluid()
        fluid["exposes"][1]["contract"]["schema"][0]["classification"] = "Sensitive"
        prov = BitolOdpsProvider()
        prov.strict_validation = False
        port = prov.render(fluid)["product"]["outputPorts"][1]
        custom = {c["property"]: c["value"] for c in port["customProperties"]}
        assert custom["containsPii"] is True and custom["autoApprove"] is False


# ---------------------------------------------------------------------------
# Finding 27: ODCS import rejects schema-invalid input unless lenient
# ---------------------------------------------------------------------------


def _invalid_odcs() -> Dict[str, Any]:
    doc = _events_odcs()
    doc["schema"][0]["properties"][0]["logicalType"] = "not-a-type"
    return doc


class TestStrictOdcsImport:
    def test_invalid_odcs_raises_by_default(self) -> None:
        with pytest.raises(ProviderError, match="ODCS validation failed"):
            OdcsProvider().import_contract(_invalid_odcs())

    def test_lenient_opt_in_warns_and_converts(self) -> None:
        fluid = OdcsProvider().import_contract(_invalid_odcs(), lenient=True)
        assert fluid["exposes"]

    def test_odcs_only_directory_honours_lenient(self, tmp_path: Path) -> None:
        _write(tmp_path / "bad.odcs.yaml", _invalid_odcs())
        with pytest.raises(ProviderError):
            BitolOdpsProvider().import_directory(tmp_path)
        fluid = BitolOdpsProvider().import_directory(tmp_path, lenient=True)
        assert len(fluid["exposes"]) == 2


# ---------------------------------------------------------------------------
# Finding 34: resolution problems are skipped when lenient
# ---------------------------------------------------------------------------


class TestLenientResolution:
    def test_url_input_port_with_remote_disabled_is_skipped(self, hand_bundle: Path) -> None:
        odps = _odps_doc()
        odps["inputPorts"] = [
            {
                "name": "crm",
                "version": "1.0.0",
                "contractId": "https://contracts.example.invalid/crm.odcs.yaml",
            }
        ]
        _write(hand_bundle / "shop.odps.yaml", odps)
        fluid = BitolOdpsProvider().import_directory(hand_bundle)  # allow_remote=False
        assert [e["id"] for e in fluid["expects"]] == ["crm"]
        assert "contract" not in fluid["expects"][0]

    def test_invalid_local_input_contract_is_skipped(self, hand_bundle: Path) -> None:
        odps = _odps_doc()
        odps["inputPorts"] = [{"name": "up", "version": "1.0.0", "contractId": "up.contract"}]
        _write(hand_bundle / "shop.odps.yaml", odps)
        bad = _invalid_odcs()
        bad["id"] = "up.contract"
        _write(hand_bundle / "upstream" / "up.contract.odcs.yaml", bad)
        fluid = BitolOdpsProvider().import_directory(hand_bundle)
        assert "contract" not in fluid["expects"][0]

    def test_output_port_remote_disabled_is_skipped_when_lenient(self, tmp_path: Path) -> None:
        odps = {
            "apiVersion": "v1.0.0",
            "kind": "DataProduct",
            "id": "p",
            "status": "active",
            "outputPorts": [
                {"name": "o", "version": "1", "contractId": "https://example.invalid/o.yaml"}
            ],
        }
        prov = BitolOdpsProvider()
        with pytest.raises(ProviderError):
            prov.import_contract(odps, base_path=tmp_path, allow_remote=False)
        fluid = prov.import_contract(odps, base_path=tmp_path, allow_remote=False, lenient=True)
        assert len(fluid["exposes"]) == 1


# ---------------------------------------------------------------------------
# Finding 35: a local probe must find the requested contract, not a namesake
# ---------------------------------------------------------------------------


class TestResolverIdMatch:
    def _contract(self, cid: str) -> Dict[str, Any]:
        return {
            "apiVersion": "v3.1.0",
            "kind": "DataContract",
            "id": cid,
            "version": "1.0.0",
            "status": "active",
            "schema": [],
        }

    def test_last_segment_namesake_is_not_returned(self, tmp_path: Path) -> None:
        _write(tmp_path / "contracts" / "orders.odcs.yaml", self._contract("shop.orders"))
        with pytest.raises(ContractNotFound):
            ContractResolver(base_path=tmp_path).resolve("warehouse.orders")

    def test_last_segment_file_with_matching_id_still_resolves(self, tmp_path: Path) -> None:
        _write(tmp_path / "orders.odcs.yaml", self._contract("warehouse.orders"))
        resolved = ContractResolver(base_path=tmp_path).resolve("warehouse.orders")
        assert resolved.odcs["id"] == "warehouse.orders"

    def test_namesake_does_not_leak_into_an_input_expect(self, tmp_path: Path) -> None:
        own = self._contract("shop.orders")
        own["schema"] = [
            {
                "name": "orders",
                "logicalType": "object",
                "properties": [{"name": "shop_order_id", "logicalType": "string"}],
            }
        ]
        _write(tmp_path / "contracts" / "orders.odcs.yaml", own)
        odps = {
            "apiVersion": "v1.0.0",
            "kind": "DataProduct",
            "id": "shop",
            "status": "active",
            "outputPorts": [{"name": "orders", "version": "1", "contractId": "shop.orders"}],
            "inputPorts": [{"name": "orders", "version": "1", "contractId": "warehouse.orders"}],
        }
        _write(tmp_path / "shop.odps.yaml", odps)
        fluid = BitolOdpsProvider().import_directory(tmp_path)
        assert fluid["exposes"][0]["contract"]["schema"][0]["name"] == "shop_order_id"
        assert "contract" not in fluid["expects"][0]


# ---------------------------------------------------------------------------
# Findings 49 / 50 / 56: ids never escape the bundle / output directory
# ---------------------------------------------------------------------------


class TestPathTraversal:
    def _secret(self, root: Path) -> Path:
        return _write(
            root / "secret" / "private.odcs.yaml",
            {
                "apiVersion": "v3.1.0",
                "kind": "DataContract",
                "id": str(root / "secret" / "private"),
                "version": "1.0.0",
                "status": "active",
                "schema": [],
            },
        )

    def test_absolute_contract_id_is_not_read(self, tmp_path: Path) -> None:
        self._secret(tmp_path)
        bundle = tmp_path / "bundle"
        bundle.mkdir()
        resolver = ContractResolver(base_path=bundle, allow_remote=False)
        with pytest.raises(ContractNotFound):
            resolver.resolve(str(tmp_path / "secret" / "private"))

    def test_dot_dot_contract_id_is_not_read(self, tmp_path: Path) -> None:
        secret = self._secret(tmp_path)
        doc = _load(secret)
        doc["id"] = "../secret/private"
        _write(secret, doc)
        bundle = tmp_path / "bundle"
        bundle.mkdir()
        resolver = ContractResolver(base_path=bundle, allow_remote=False)
        with pytest.raises(ContractNotFound):
            resolver.resolve("../secret/private")

    def test_symlink_out_of_the_bundle_is_ignored(self, tmp_path: Path) -> None:
        secret = self._secret(tmp_path)
        doc = _load(secret)
        doc["id"] = "linked"
        _write(secret, doc)
        bundle = tmp_path / "bundle"
        bundle.mkdir()
        try:
            (bundle / "linked.odcs.yaml").symlink_to(secret)
        except OSError:
            pytest.skip("symlinks not supported")
        resolver = ContractResolver(base_path=bundle, allow_remote=False)
        resolver.index_directory(bundle)
        with pytest.raises(ContractNotFound):
            resolver.resolve("linked")
        with pytest.raises(ProviderError, match="no ODPS doc and no ODCS files"):
            BitolOdpsProvider().import_directory(bundle)

    @pytest.mark.parametrize("bad_id", ["../escaped/orders", "/abs/orders", "a\\b", ".."])
    def test_export_refuses_ids_that_escape_out_dir(self, tmp_path: Path, bad_id: str) -> None:
        fluid = _load(MULTI_EXPOSE_FLUID)
        fluid["id"] = bad_id
        fluid.get("contract", {}).pop("id", None)
        out = tmp_path / "trav" / "out"
        prov = BitolOdpsProvider()
        prov.strict_validation = False
        with pytest.raises(ProviderError, match="Refusing to write bundle file"):
            prov.render(fluid, out_dir=out)
        assert not (tmp_path / "trav").exists()

    def test_export_refuses_contract_ids_that_escape_out_dir(self, tmp_path: Path) -> None:
        fluid = _pii_fluid()
        fluid["exposes"][0]["contract_id"] = "../../x/p.daily"
        prov = BitolOdpsProvider()
        prov.strict_validation = False
        with pytest.raises(ProviderError, match="Refusing to write bundle file"):
            prov.render(fluid, out_dir=tmp_path / "out")
        assert not (tmp_path / "out").exists()
        assert not list(tmp_path.rglob("*.odcs.yaml"))


# ---------------------------------------------------------------------------
# Finding 37: FLUID contracts are not mistaken for ODPS docs
# ---------------------------------------------------------------------------


class TestFluidContractsAreNotOdps:
    @pytest.mark.parametrize("kind", ["DataProduct", "DataContract"])
    def test_project_dir_with_fluid_contract_and_bundle(self, tmp_path: Path, kind: str) -> None:
        fluid = _load(MULTI_EXPOSE_FLUID)
        fluid["kind"] = kind  # FLUID's kind enum overlaps both Bitol kinds
        _write(tmp_path / "contract.fluid.yaml", fluid)
        BitolOdpsProvider().render(fluid, out_dir=tmp_path)
        imported = BitolOdpsProvider().import_directory(tmp_path)
        assert len(imported["exposes"]) == 2
        assert imported["contract"]["id"] == "commerce.orders-product"

    @pytest.mark.parametrize("kind", ["DataProduct", "DataContract"])
    def test_directory_with_only_a_fluid_contract_raises(self, tmp_path: Path, kind: str) -> None:
        fluid = _load(MULTI_EXPOSE_FLUID)
        fluid["kind"] = kind
        _write(tmp_path / "contract.fluid.yaml", fluid)
        with pytest.raises(ProviderError, match="no ODPS doc and no ODCS files"):
            BitolOdpsProvider().import_directory(tmp_path)


# ---------------------------------------------------------------------------
# Finding 57: `fluid opds import .` names the product after the directory
# ---------------------------------------------------------------------------


def test_odcs_only_import_of_dot_uses_the_real_directory_name(monkeypatch) -> None:
    monkeypatch.chdir(FIXTURES / "odps" / "contracts-only")
    fluid = BitolOdpsProvider().import_directory(Path("."))
    assert fluid["metadata"]["name"] == "contracts-only"
    assert fluid["contract"]["id"] == "contracts-only"


# ---------------------------------------------------------------------------
# Finding 71: remote fetch limits
# ---------------------------------------------------------------------------


def _valid_contract_json() -> bytes:
    return json.dumps(
        {
            "apiVersion": "v3.1.0",
            "kind": "DataContract",
            "id": "remote.contract",
            "version": "1.0.0",
            "status": "active",
            "schema": [],
        }
    ).encode()


class _Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args: Any) -> None:  # silence test output
        pass

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/ok":
            body = _valid_contract_json()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/big":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()  # no Content-Length: the cap must hold while reading
            for _ in range(64):
                self.wfile.write(b" " * 1024)
        elif self.path == "/ftp":
            self.send_response(302)
            self.send_header("Location", "ftp://127.0.0.1/x.odcs.yaml")
            self.end_headers()
        elif self.path == "/slow":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            try:
                for _ in range(40):
                    self.wfile.write(b" ")
                    self.wfile.flush()
                    time.sleep(0.1)
            except OSError:
                pass


@pytest.fixture
def http_base(monkeypatch) -> Iterator[str]:
    for var in ("http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY"):
        monkeypatch.delenv(var, raising=False)
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


class TestRemoteFetchLimits:
    def test_small_body_is_fetched(self, http_base: str) -> None:
        resolved = ContractResolver(allow_remote=True).resolve(f"{http_base}/ok")
        assert resolved.odcs["id"] == "remote.contract"

    def test_body_over_the_cap_is_refused(self, http_base: str) -> None:
        resolver = ContractResolver(allow_remote=True, max_bytes=4096)
        with pytest.raises(ContractNotFound):
            resolver.resolve(f"{http_base}/big")

    def test_redirect_to_ftp_is_refused(self, http_base: str, monkeypatch) -> None:
        import urllib.request

        ftp_calls = []

        def _no_ftp(self: Any, req: Any) -> Any:
            ftp_calls.append(req.full_url)
            raise urllib.error.URLError("ftp disabled in tests")

        monkeypatch.setattr(urllib.request.FTPHandler, "ftp_open", _no_ftp)
        with pytest.raises(ContractNotFound):
            ContractResolver(allow_remote=True).resolve(f"{http_base}/ftp")
        assert ftp_calls == []  # the redirect was refused, not followed

    def test_total_deadline_is_enforced(self, http_base: str) -> None:
        resolver = ContractResolver(allow_remote=True, timeout=2, total_timeout=0.5)
        started = time.monotonic()
        with pytest.raises(ContractNotFound):
            resolver.resolve(f"{http_base}/slow")
        assert time.monotonic() - started < 3

    def test_undecodable_body_is_contract_not_found(self, http_base: str, monkeypatch) -> None:
        from fluid_build.providers.odps_standard import resolver as resolver_mod

        class _Resp:
            headers = {"Content-Type": "application/json"}

            def __init__(self) -> None:
                self._body = b"\xff\xfe\x00bad"

            def read(self, amt: int = -1) -> bytes:
                body, self._body = self._body, b""
                return body

            def __enter__(self):
                return self

            def __exit__(self, *_: Any) -> bool:
                return False

        monkeypatch.setattr(resolver_mod, "urlopen", lambda url, timeout: _Resp())
        with pytest.raises(ContractNotFound):
            ContractResolver(allow_remote=True).resolve("https://example.invalid/c.yaml")


def test_to_model_message_mentions_python_floor(monkeypatch) -> None:
    import builtins

    real_import = builtins.__import__

    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name.startswith("open_data_contract_standard"):
            raise ImportError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(ProviderError, match=r"Python >= 3\.10"):
        OdcsProvider().to_model(_events_odcs())


def test_render_does_not_mutate_the_source_fluid(hand_bundle: Path) -> None:
    prov = _strict_provider()
    fluid = prov.import_directory(hand_bundle)
    before = copy.deepcopy(fluid)
    prov.render(fluid)
    assert fluid == before
    assert os.path.isdir(hand_bundle)
