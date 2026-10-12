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

"""Round-2 regression tests for the catalogs / Data Mesh Manager workstream.

Every expected payload below was captured from origin/main (release 0.7.8)
with the same fake HTTP server, so these tests pin 0.7.8 behaviour for:

* the deprecated ``CatalogAsset`` API on the built-in catalog providers
  (asset fields, keyword forms, ``map_contract_to_asset`` overrides),
* legacy / duck-typed third-party catalog plugins in ``fluid publish``,
* rejecting non-contract files in ``fluid publish``,
* ``fluid publish --catalog datamesh-manager`` status/domain/version,
* the ODPS ``team.name`` without ``--team-id``,
* the ``dps`` spec token.
"""

from __future__ import annotations

import asyncio
import copy
import json
import warnings
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml

from fluid_build.loader import load_contract
from fluid_build.providers.catalogs import (
    CATALOG_PROVIDERS,
    BaseCatalogProvider,
    CatalogAsset,
    CatalogTarget,
    PublishResult,
)
from fluid_build.providers.catalogs.datamesh_manager import DataMeshManagerCatalogProvider
from fluid_build.providers.catalogs.fluid_cc import FluidCommandCenterProvider
from fluid_build.providers.datamesh_manager import DataMeshManagerProvider

REPO_ROOT = Path(__file__).resolve().parents[1]
HELLO = REPO_ROOT / "examples" / "01-hello-world" / "contract.fluid.yaml"
C360 = REPO_ROOT / "examples" / "customer360" / "contract.fluid.yaml"
MARKET_CONFIG = REPO_ROOT / "examples" / "market-config.yaml"


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


@pytest.fixture(autouse=True)
def _no_spec_env(monkeypatch):
    monkeypatch.delenv("DMM_DATA_PRODUCT_SPEC", raising=False)


def _catalog_deprecations(caught) -> List[str]:
    """The catalog API's own deprecation notices among *caught* (unrelated
    ones, e.g. Python 3.12's ``datetime.utcnow``, are ignored)."""
    markers = ("CatalogAsset", "asset_id", "map_contract_to_asset", "deprecated publish")
    return [
        str(w.message)
        for w in caught
        if issubclass(w.category, DeprecationWarning)
        and any(marker in str(w.message) for marker in markers)
    ]


def _quiet_map(provider, contract):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        return provider.map_contract_to_asset(contract)


def _contract(**overrides: Any) -> Dict[str, Any]:
    contract: Dict[str, Any] = {
        "fluidVersion": "0.7.2",
        "kind": "DataProduct",
        "id": "sales.orders",
        "name": "Orders",
        "description": "Orders data product",
        "domain": "sales",
        "metadata": {"owner": {"team": "sales-team", "email": "sales@example.com"}},
        "exposes": [],
    }
    contract.update(overrides)
    return contract


# ---------------------------------------------------------------------------
# Fake Command Center (httpx) server
# ---------------------------------------------------------------------------


class _FakeResp:
    def __init__(self, data: Any, status: int = 200) -> None:
        self._data = data
        self.status_code = status
        self.text = json.dumps(data)

    def json(self) -> Any:
        return self._data

    def raise_for_status(self) -> None:
        return None


class _FakeCC:
    """Stands in for ``httpx.AsyncClient``; records POST/PATCH bodies."""

    sent: List[Dict[str, Any]] = []

    def __init__(self, *a: Any, **k: Any) -> None:
        pass

    async def __aenter__(self) -> "_FakeCC":
        return self

    async def __aexit__(self, *a: Any) -> bool:
        return False

    async def get(self, url, params=None, headers=None, timeout=None):
        return _FakeResp({"items": []})

    async def post(self, url, json=None, headers=None):
        _FakeCC.sent.append(json)
        return _FakeResp({"id": "new"})

    async def patch(self, url, json=None, headers=None):
        _FakeCC.sent.append(json)
        return _FakeResp({"id": "new"})


@pytest.fixture
def fake_cc():
    _FakeCC.sent = []
    with patch("httpx.AsyncClient", _FakeCC):
        yield _FakeCC.sent


def _without_exposes_summary(payload: Dict[str, Any]) -> Dict[str, Any]:
    """``metadata.exposes`` is the additive multi-port summary the branch
    adds; everything else must equal 0.7.8's payload."""
    out = copy.deepcopy(payload)
    out["metadata"].pop("exposes", None)
    return out


def _edited_c360_asset(provider) -> CatalogAsset:
    asset = _quiet_map(provider, load_contract(str(C360)))
    asset.sensitivity = "restricted"
    asset.tags = ["custom-tag"]
    asset.description = "edited"
    asset.name = "Edited Name"
    asset.domain = "edited-domain"
    asset.owner = "edited-owner"
    asset.version = "9.9.9"
    asset.layer = "Platinum"
    asset.platform = "snowflake"
    asset.location = {"db": "X"}
    asset.schema = [{"name": "only", "type": "string"}]
    asset.contract_yaml = "x: 1\n"
    return asset


# Captured from origin/main for ``_edited_c360_asset``.
MAIN_CC_EDITED = {
    "contract_hash": "d09edadd173a8bbc233d47dcafc30cc876af2fb569812f2bbe0e92a1905bbce8",
    "contract_yaml": "x: 1\n",
    "description": "edited",
    "is_public": False,
    "metadata": {
        "domain": "edited-domain",
        "fluid_contract_id": "gold.customer360_v1",
        "layer": "Platinum",
        "location": {"db": "X"},
        "owner": "edited-owner",
        "owner_email": "cust-platform@example.com",
        "platform": "snowflake",
        "schema": [{"name": "only", "type": "string"}],
        "sensitivity": "restricted",
    },
    "name": "Edited Name",
    "owner_id": "placeholder",
    "tags": ["custom-tag"],
    "type": "dataproduct",
    "version": "9.9.9",
}


def _hand_asset() -> CatalogAsset:
    return CatalogAsset(
        id="hand.built",
        name="Hand",
        description="hd",
        type="dataproduct",
        domain="d",
        owner="o",
        owner_email="o@x",
        layer="Gold",
        tags=["t"],
        version="2.0.0",
        platform="unknown",
        location={},
        schema=[{"id": "id", "type": "string"}],
        sensitivity="restricted",
    )


# Captured from origin/main for ``_hand_asset``.
MAIN_CC_HAND = {
    "description": "hd",
    "is_public": False,
    "metadata": {
        "domain": "d",
        "fluid_contract_id": "hand.built",
        "layer": "Gold",
        "location": {},
        "owner": "o",
        "owner_email": "o@x",
        "platform": "unknown",
        "schema": [{"id": "id", "type": "string"}],
        "sensitivity": "restricted",
    },
    "name": "Hand",
    "owner_id": "placeholder",
    "tags": ["t"],
    "type": "dataproduct",
    "version": "2.0.0",
}


def _cc() -> FluidCommandCenterProvider:
    return FluidCommandCenterProvider({"endpoint": "http://cc"})


# ---------------------------------------------------------------------------
# Defect 0 — publish(CatalogAsset) honours every field set on the asset
# ---------------------------------------------------------------------------


class TestLegacyAssetFieldsHonoured:
    @pytest.mark.parametrize("method", ["publish", "update", "publish_asset", "update_asset"])
    def test_cc_edited_asset_matches_0_7_8_payload(self, fake_cc, method):
        provider = _cc()
        asset = _edited_c360_asset(provider)

        with pytest.warns(DeprecationWarning):
            result = _run(getattr(provider, method)(asset))

        assert result.success, result.error
        assert _without_exposes_summary(fake_cc[0]) == MAIN_CC_EDITED

    def test_cc_hand_built_asset_matches_0_7_8_payload(self, fake_cc):
        with pytest.warns(DeprecationWarning):
            result = _run(_cc().publish(_hand_asset()))

        assert result.success, result.error
        assert _without_exposes_summary(fake_cc[0]) == MAIN_CC_HAND
        assert fake_cc[0]["is_public"] is False

    def test_cc_unedited_asset_matches_new_api_payload(self, fake_cc):
        provider = _cc()
        contract = load_contract(str(C360))
        asset = _quiet_map(provider, contract)
        with pytest.warns(DeprecationWarning):
            _run(provider.publish(asset))
        _run(provider.publish(contract))
        assert fake_cc[0] == fake_cc[1]

    def test_minimal_fluid_keeps_schema_and_sensitivity(self):
        from fluid_build.providers.catalogs.base import _fluid_target_from_asset

        fluid, target = _fluid_target_from_asset(_hand_asset())

        assert target.contract_id == "hand.built"
        expose = fluid["exposes"][0]
        assert expose["contract"]["schema"] == [{"id": "id", "type": "string"}]
        assert expose["sensitivity"] == "restricted"

    def test_third_party_new_api_provider_sees_asset_edits(self):
        seen: Dict[str, Any] = {}

        class NewApi(BaseCatalogProvider):
            async def publish(self, fluid, target=None):
                seen["fluid"], seen["target"] = fluid, target
                return PublishResult(True, "n", target.contract_id)

            async def update(self, fluid, target=None):
                return await self.publish(fluid, target)

            async def verify(self, contract_id):
                return True

        provider = NewApi({})
        contract = load_contract(str(C360))
        asset = _quiet_map(provider, contract)
        asset.name = "Renamed"
        asset.description = "edited"
        asset.domain = "edited-domain"
        asset.tags = ["custom-tag"]
        asset.version = "9.9.9"
        asset.sensitivity = "restricted"
        asset.owner = "edited-owner"

        with pytest.warns(DeprecationWarning):
            _run(provider.publish_asset(asset))

        fluid = seen["fluid"]
        assert seen["target"].contract_name == "Renamed"
        assert fluid["name"] == fluid["metadata"]["name"] == "Renamed"
        assert fluid["description"] == "edited"
        assert fluid["domain"] == fluid["metadata"]["domain"] == "edited-domain"
        assert fluid["metadata"]["tags"] == ["custom-tag"]
        assert fluid["version"] == "9.9.9"
        assert fluid["exposes"][0]["sensitivity"] == "restricted"
        assert fluid["metadata"]["owner"]["team"] == "edited-owner"
        # unedited parts of the original contract survive
        assert fluid["exposes"][0]["binding"] == contract["exposes"][0]["binding"]
        assert fluid["builds"] == contract["builds"]
        # the caller's asset / raw contract are not mutated
        assert contract["name"] == "Customer 360 (Demo)"
        assert asset.raw_contract["name"] == "Customer 360 (Demo)"


# ---------------------------------------------------------------------------
# Fake Data Mesh Manager
# ---------------------------------------------------------------------------


def _dmm(config: Optional[Dict[str, Any]] = None, cls=DataMeshManagerCatalogProvider):
    cfg: Dict[str, Any] = {"endpoint": "http://dmm", "auth": {"api_key": "k"}}
    cfg.update(config or {})
    catalog = cls(cfg)
    calls: List[Tuple[str, str, Any]] = []

    def fake_request(method, path, json_body=None):
        calls.append((method, path, json_body))
        return SimpleNamespace(status_code=200, json=lambda: {}, text="")

    catalog._provider._request = fake_request  # type: ignore[assignment]
    catalog._provider._session = lambda: SimpleNamespace(  # type: ignore[assignment]
        get=lambda *a, **k: SimpleNamespace(status_code=404)
    )
    return catalog, calls


def _product(calls) -> Dict[str, Any]:
    puts = [c[2] for c in calls if c[0] == "PUT" and c[1].startswith("/api/dataproducts/")]
    assert len(puts) == 1
    return puts[0]


def _team_puts(calls) -> List[str]:
    return [c[1].rsplit("/", 1)[-1] for c in calls if c[1].startswith("/api/teams/")]


# Captured from origin/main for ``_hand_asset`` (main's catalog path is DPS).
MAIN_DMM_HAND = {
    "custom": {"domain": "d", "layer": "Gold", "version": "2.0.0"},
    "dataProductSpecification": "0.0.1",
    "id": "hand.built",
    "info": {"description": "hd", "owner": "o", "status": "active", "title": "Hand"},
    "tags": ["t"],
    "teamId": "o",
}


class TestDmmLegacyAsset:
    def test_hand_built_asset_dps_payload_equals_0_7_8(self):
        catalog, calls = _dmm({"data_product_spec": "0.0.1"})
        with pytest.warns(DeprecationWarning):
            result = _run(catalog.publish(_hand_asset()))
        assert result.success, result.error
        assert _product(calls) == MAIN_DMM_HAND

    @pytest.mark.parametrize("method", ["publish", "update", "publish_asset", "update_asset"])
    def test_edited_asset_fields_reach_dps_payload(self, method):
        catalog, calls = _dmm({"data_product_spec": "0.0.1"})
        asset = _quiet_map(catalog, load_contract(str(C360)))
        asset.name = "Renamed"
        asset.domain = "edited-domain"
        asset.tags = ["custom-tag"]
        asset.description = "edited"
        asset.owner = "Edited Owner"
        asset.version = "9.9.9"
        asset.layer = "Platinum"

        with pytest.warns(DeprecationWarning):
            result = _run(getattr(catalog, method)(asset))

        assert result.success, result.error
        product = _product(calls)
        # Values origin/main sent for the same asset.
        assert product["info"]["title"] == "Renamed"
        assert product["info"]["description"] == "edited"
        assert product["info"]["status"] == "active"
        assert product["info"]["owner"] == "edited-owner"
        assert product["teamId"] == "edited-owner"
        assert product["tags"] == ["custom-tag"]
        assert product["custom"]["domain"] == "edited-domain"
        assert product["custom"]["version"] == "9.9.9"
        assert product["custom"]["layer"] == "Platinum"
        assert _team_puts(calls) == ["edited-owner"]

    def test_edited_asset_fields_reach_odps_payload(self):
        catalog, calls = _dmm()
        asset = _quiet_map(catalog, load_contract(str(C360)))
        asset.name = "Renamed"
        asset.domain = "edited-domain"
        asset.tags = ["custom-tag"]
        asset.version = "9.9.9"
        asset.owner = "Edited Owner"

        with pytest.warns(DeprecationWarning):
            result = _run(catalog.publish(asset))

        assert result.success, result.error
        product = _product(calls)
        assert product["name"] == "Renamed"
        assert product["domain"] == "edited-domain"
        assert product["tags"] == ["custom-tag"]
        assert product["version"] == "9.9.9"
        assert product["status"] == "active"
        assert product["team"]["name"] == "edited-owner"


# ---------------------------------------------------------------------------
# Defect 4 — keyword forms of the 0.7.8 API
# ---------------------------------------------------------------------------


class TestLegacyKeywordForms:
    @pytest.mark.parametrize("method", ["publish", "update"])
    def test_cc_asset_keyword(self, fake_cc, method):
        provider = _cc()
        with pytest.warns(DeprecationWarning):
            result = _run(getattr(provider, method)(asset=_edited_c360_asset(provider)))
        assert result.success, result.error
        assert _without_exposes_summary(fake_cc[0]) == MAIN_CC_EDITED

    @pytest.mark.parametrize("method", ["publish", "update"])
    def test_dmm_asset_keyword(self, method):
        catalog, calls = _dmm({"data_product_spec": "0.0.1"})
        with pytest.warns(DeprecationWarning):
            result = _run(getattr(catalog, method)(asset=_hand_asset()))
        assert result.success, result.error
        assert _product(calls) == MAIN_DMM_HAND

    def test_cc_verify_asset_id_keyword(self, fake_cc):
        provider = _cc()
        with pytest.warns(DeprecationWarning, match="asset_id"):
            assert _run(provider.verify(asset_id="x")) is False
        # positional and the new keyword keep working without a warning
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", DeprecationWarning)
            assert _run(provider.verify("x")) is False
            assert _run(provider.verify(contract_id="x")) is False
        assert _catalog_deprecations(caught) == []

    def test_cc_delete_asset_id_keyword(self):
        provider = _cc()
        with pytest.warns(DeprecationWarning, match="asset_id"):
            assert _run(provider.delete(asset_id="x")) is False

    def test_dmm_verify_and_delete_asset_id_keyword(self):
        catalog, calls = _dmm()
        with pytest.warns(DeprecationWarning, match="asset_id"):
            assert _run(catalog.verify(asset_id="p1")) is True
        with pytest.warns(DeprecationWarning, match="asset_id"):
            assert _run(catalog.delete(asset_id="p1")) is True
        assert ("GET", "/api/dataproducts/p1", None) in calls
        assert ("DELETE", "/api/dataproducts/p1", None) in calls

    def test_conflicting_or_missing_ids_raise_type_error(self):
        provider = _cc()
        with pytest.raises(TypeError):
            _run(provider.verify())
        with pytest.raises(TypeError):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                _run(provider.verify("a", asset_id="b"))
        with pytest.raises(TypeError):
            _run(provider.publish())


# ---------------------------------------------------------------------------
# Defect 6 — non-contract files are rejected
# ---------------------------------------------------------------------------


def _cli_publish(path: Path, catalog, catalog_name="x", **kwargs) -> PublishResult:
    from fluid_build.cli import publish as pub_mod

    config = MagicMock()
    config.get_catalog_config.return_value = {"enabled": True}
    with patch.object(pub_mod, "get_catalog_provider", return_value=catalog):
        return _run(
            pub_mod.publish_contract(
                contract_path=path, catalog_name=catalog_name, config=config, **kwargs
            )
        )


class TestNonContractRejected:
    def test_validate_target_rejects_dict_without_identity(self):
        provider = _cc()
        fluid = {"fluidVersion": "0.7.2", "builds": []}
        ok, err = provider.validate_target(fluid, CatalogTarget.from_fluid(fluid))
        assert ok is False
        assert "name" in err and "FLUID contract" in err

    @pytest.mark.parametrize("dry_run", [False, True])
    def test_cc_cli_rejects_market_config(self, fake_cc, dry_run):
        result = _cli_publish(MARKET_CONFIG, _cc(), skip_health_check=True, dry_run=dry_run)
        assert result.success is False
        assert "FLUID contract" in result.error
        assert fake_cc == []

    def test_cc_cli_verify_only_rejects_non_contract(self, tmp_path):
        path = tmp_path / "frag.yaml"
        path.write_text("sovereignty:\n  jurisdiction: EU\n", encoding="utf-8")
        provider = _cc()
        provider.verify = AsyncMock(return_value=True)  # type: ignore[assignment]
        result = _cli_publish(path, provider, verify_only=True)
        assert result.success is False
        assert "FLUID contract" in result.error
        provider.verify.assert_not_called()

    def test_dmm_cli_rejects_fragment(self, tmp_path):
        path = tmp_path / "frag.yaml"
        path.write_text("sovereignty:\n  jurisdiction: EU\n", encoding="utf-8")
        catalog, calls = _dmm()
        result = _cli_publish(path, catalog, skip_health_check=True)
        assert result.success is False
        assert "FLUID contract" in result.error
        assert calls == []

    def test_dmm_publish_rejects_dict_without_identity(self):
        catalog, calls = _dmm()
        result = _run(catalog.publish({"kind": "DataProduct", "metadata": {}}))
        assert result.success is False
        assert "FLUID contract" in result.error
        assert calls == []

    def test_non_mapping_yaml_is_rejected(self, tmp_path):
        from fluid_build.cli import publish as pub_mod

        path = tmp_path / "list.yaml"
        path.write_text("- a\n- b\n", encoding="utf-8")
        # The loader rejects this itself ...
        result = _cli_publish(path, _cc(), skip_health_check=True)
        assert result.success is False
        # ... and publish_contract does not crash if a loader hands it one.
        with patch.object(pub_mod, "load_contract", return_value=["a", "b"]):
            result = _cli_publish(path, _cc(), skip_health_check=True)
        assert result.success is False
        assert "FLUID contract" in result.error

    @pytest.mark.parametrize(
        "contract",
        [
            {"id": "a.b", "name": "A B"},
            {"name": "Only name"},
            {"metadata": {"name": "Only metadata name"}},
        ],
    )
    def test_minimal_identity_still_passes(self, contract):
        provider = _cc()
        assert provider.validate_target(contract, CatalogTarget.from_fluid(contract)) == (
            True,
            None,
        )

    def test_fragment_with_only_an_id_is_rejected(self):
        # e.g. examples/0.7.1/bitcoin-multifile/fragments/builds/ingestion.yaml:
        # 0.7.8 rejected it ("Failed to map contract to asset: 'name'").
        provider = _cc()
        fragment = {"id": "bitcoin_price_ingestion", "pattern": "embedded-logic"}
        ok, err = provider.validate_target(fragment, CatalogTarget.from_fluid(fragment))
        assert ok is False
        assert "'name' is required" in err


# ---------------------------------------------------------------------------
# Defects 7 / 21 — legacy-plugin detection is positive for the new API
# ---------------------------------------------------------------------------


class _LegacyBase(BaseCatalogProvider):
    events: List[Tuple[str, Any]] = []

    async def update(self, asset):
        return await self.publish(asset)

    async def verify(self, asset_id):
        self.events.append(("verify", asset_id))
        return True


class LegacyExtraOptional(_LegacyBase):
    async def publish(self, asset: CatalogAsset, dry_run: bool = False) -> PublishResult:
        self.events.append(("publish", asset))
        return PublishResult(True, "legacykw", asset.id)


class LegacyVarArgs(_LegacyBase):
    async def publish(self, asset, *args, **kw):
        self.events.append(("publish", asset))
        return PublishResult(True, "legacyva", asset.id)


class NewApiExtraKw(BaseCatalogProvider):
    async def publish(self, fluid, target=None, **kw):
        return PublishResult(True, "n", target.contract_id if target else "?")

    async def update(self, fluid, target=None):
        return await self.publish(fluid, target)

    async def verify(self, contract_id):
        return True


class NewApiAnnotated(BaseCatalogProvider):
    async def publish(self, fluid, tgt: Optional[CatalogTarget] = None):
        return PublishResult(True, "n", "x")

    async def update(self, fluid, tgt: Optional[CatalogTarget] = None):
        return PublishResult(True, "n", "x")

    async def verify(self, contract_id):
        return True


class TestLegacyDetection:
    @pytest.mark.parametrize("cls", [LegacyExtraOptional, LegacyVarArgs])
    def test_detected_as_legacy(self, cls):
        assert cls._uses_legacy_asset_api() is True

    @pytest.mark.parametrize(
        "cls",
        [
            NewApiExtraKw,
            NewApiAnnotated,
            FluidCommandCenterProvider,
            DataMeshManagerCatalogProvider,
        ],
    )
    def test_detected_as_new(self, cls):
        assert cls._uses_legacy_asset_api() is False

    @pytest.mark.parametrize("cls", [LegacyExtraOptional, LegacyVarArgs])
    def test_cli_publishes_catalog_asset(self, tmp_path, cls):
        cls.events = []
        path = tmp_path / "c.fluid.yaml"
        path.write_text(yaml.safe_dump(_contract()), encoding="utf-8")
        with pytest.warns(DeprecationWarning):
            result = _cli_publish(path, cls({}))
        assert result.success, result.error
        published = [e for k, e in cls.events if k == "publish"]
        assert len(published) == 1 and isinstance(published[0], CatalogAsset)
        assert published[0].id == "sales.orders"

    @pytest.mark.parametrize("cls", [LegacyExtraOptional, LegacyVarArgs])
    def test_publish_asset_shim_routes_legacy(self, cls):
        cls.events = []
        with pytest.warns(DeprecationWarning):
            result = _run(cls({}).publish_asset(_hand_asset()))
        assert result.success
        assert isinstance(cls.events[0][1], CatalogAsset)


class DuckLegacy:
    """A 0.7.8 catalog plugin that implements the interface without
    subclassing BaseCatalogProvider."""

    def __init__(self) -> None:
        self.events: List[Tuple[str, Any]] = []

    def map_contract_to_asset(self, contract):
        self.events.append(("map", contract))
        return CatalogAsset(
            id=contract["id"],
            name=contract["name"],
            description="",
            type="dataproduct",
            domain=contract.get("domain", "general"),
            owner="o",
            owner_email="",
            layer="Bronze",
            tags=[],
            version="1.0.0",
            platform="unknown",
            location={},
        )

    def validate_asset(self, asset):
        self.events.append(("validate", asset))
        return (asset.domain != "blocked", "duck says no")

    async def health_check(self):
        return True

    async def publish(self, asset):
        self.events.append(("publish", asset))
        return PublishResult(True, "duck", asset.id)

    async def verify(self, asset_id):
        self.events.append(("verify", asset_id))
        return True


class TestDuckTypedLegacyPlugin:
    def _path(self, tmp_path, **overrides) -> Path:
        path = tmp_path / "c.fluid.yaml"
        path.write_text(yaml.safe_dump(_contract(**overrides)), encoding="utf-8")
        return path

    def test_publish(self, tmp_path):
        duck = DuckLegacy()
        with pytest.warns(DeprecationWarning):
            result = _cli_publish(self._path(tmp_path), duck)
        assert result.success, result.error
        assert [k for k, _ in duck.events] == ["map", "publish"]

    def test_dry_run_uses_validate_asset(self, tmp_path):
        duck = DuckLegacy()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            ok = _cli_publish(self._path(tmp_path), duck, dry_run=True)
            blocked = _cli_publish(self._path(tmp_path, domain="blocked"), duck, dry_run=True)
        assert ok.success is True
        assert (blocked.success, blocked.error) == (False, "duck says no")

    def test_verify_only(self, tmp_path):
        duck = DuckLegacy()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = _cli_publish(self._path(tmp_path), duck, verify_only=True)
        assert result.success is True
        assert ("verify", "sales.orders") in duck.events

    def test_magicmock_provider_takes_asset_route(self, tmp_path):
        """origin/main's own tests drive publish_contract with MagicMocks."""
        provider = MagicMock()
        asset = MagicMock(id="sales.orders")
        provider.map_contract_to_asset.return_value = asset
        provider.validate_asset.return_value = (True, None)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = _cli_publish(self._path(tmp_path), provider, dry_run=True)
        assert result.success is True
        provider.validate_asset.assert_called_once_with(asset)


# ---------------------------------------------------------------------------
# Defect 8 — map_contract_to_asset overrides on built-in subclasses
# ---------------------------------------------------------------------------


class CCSub(FluidCommandCenterProvider):
    def map_contract_to_asset(self, contract):
        asset = super().map_contract_to_asset(contract)
        asset.domain = "plugin-domain"
        return asset


class DMMSub(DataMeshManagerCatalogProvider):
    def map_contract_to_asset(self, contract):
        asset = super().map_contract_to_asset(contract)
        asset.name = "Renamed By Plugin"
        asset.domain = "plugin-domain"
        return asset


class TestMapOverrideHonoured:
    def test_cc_subclass_override_reaches_payload(self, fake_cc, tmp_path):
        path = tmp_path / "c.fluid.yaml"
        path.write_text(yaml.safe_dump(_contract()), encoding="utf-8")
        with pytest.warns(DeprecationWarning, match="map_contract_to_asset"):
            result = _cli_publish(path, CCSub({"endpoint": "http://cc"}), skip_health_check=True)
        assert result.success, result.error
        assert fake_cc[0]["metadata"]["domain"] == "plugin-domain"
        assert fake_cc[0]["contract_yaml"] == path.read_text(encoding="utf-8")

    def test_dmm_subclass_override_reaches_dps_payload(self):
        catalog, calls = _dmm({"data_product_spec": "0.0.1"}, cls=DMMSub)
        with pytest.warns(DeprecationWarning, match="map_contract_to_asset"):
            result = _run(catalog.publish(_contract()))
        assert result.success, result.error
        product = _product(calls)
        assert product["info"]["title"] == "Renamed By Plugin"
        assert product["custom"]["domain"] == "plugin-domain"

    def test_dmm_subclass_override_reaches_odps_payload(self):
        catalog, calls = _dmm(cls=DMMSub)
        with pytest.warns(DeprecationWarning, match="map_contract_to_asset"):
            result = _run(catalog.publish(_contract()))
        assert result.success, result.error
        product = _product(calls)
        assert product["name"] == "Renamed By Plugin"
        assert product["domain"] == "plugin-domain"

    def test_dry_run_validates_overridden_asset(self, tmp_path):
        class EmptyDomain(FluidCommandCenterProvider):
            def map_contract_to_asset(self, contract):
                asset = super().map_contract_to_asset(contract)
                asset.domain = ""
                return asset

        path = tmp_path / "c.fluid.yaml"
        path.write_text(yaml.safe_dump(_contract()), encoding="utf-8")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = _cli_publish(path, EmptyDomain({"endpoint": "http://cc"}), dry_run=True)
        assert (result.success, result.error) == (False, "Asset domain is required")

    def test_no_override_no_warning(self, fake_cc):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", DeprecationWarning)
            result = _run(_cc().publish(_contract()))
        assert result.success, result.error
        assert _catalog_deprecations(caught) == []


# ---------------------------------------------------------------------------
# Defect 1 — `fluid publish --catalog datamesh-manager` keeps 0.7.8 semantics
# ---------------------------------------------------------------------------

# custom / info.status / teamId origin/main sent for the shipped examples.
MAIN_DMM_CLI = {
    HELLO: {
        "custom": {"domain": "example", "layer": "Bronze", "version": "1.0.0"},
        "status": "active",
        "team": "learning-team",
    },
    C360: {
        "custom": {"domain": "Customer", "layer": "Gold", "version": "1.0.0"},
        "status": "active",
        "team": "customer-platform",
    },
}


class TestDmmCatalogPathCompat:
    @pytest.mark.parametrize("example", [HELLO, C360], ids=["hello", "c360"])
    @pytest.mark.parametrize("spec", ["0.0.1", "dps"])
    def test_dps_opt_out_matches_0_7_8(self, example, spec):
        catalog, calls = _dmm({"data_product_spec": spec})
        result = _cli_publish(example, catalog, skip_health_check=True)
        assert result.success, result.error
        product = _product(calls)
        expected = MAIN_DMM_CLI[example]
        assert product["dataProductSpecification"] == "0.0.1"
        for key, value in expected["custom"].items():
            assert product["custom"][key] == value
        assert product["info"]["status"] == expected["status"]
        assert product["teamId"] == expected["team"]
        assert _team_puts(calls) == [expected["team"]]

    @pytest.mark.parametrize("example", [HELLO, C360], ids=["hello", "c360"])
    def test_odps_default_is_active_and_on_created_team(self, example):
        catalog, calls = _dmm()
        result = _cli_publish(example, catalog, skip_health_check=True)
        assert result.success, result.error
        product = _product(calls)
        expected = MAIN_DMM_CLI[example]
        assert product["status"] == "active"
        assert product["domain"] == expected["custom"]["domain"]
        assert product["version"] == expected["custom"]["version"]
        assert product["team"]["name"] == expected["team"]
        assert _team_puts(calls) == [expected["team"]]

    def test_declared_status_is_kept(self):
        catalog, calls = _dmm({"data_product_spec": "0.0.1"})
        contract = _contract(metadata={"status": "deprecated", "owner": {"team": "t"}})
        result = _run(catalog.publish(contract))
        assert result.success, result.error
        assert _product(calls)["info"]["status"] == "deprecated"

    def test_metadata_domain_wins_over_top_level(self):
        catalog, calls = _dmm({"data_product_spec": "0.0.1"})
        contract = _contract(metadata={"domain": "meta-domain", "version": "3.0.0"})
        result = _run(catalog.publish(contract))
        assert result.success, result.error
        custom = _product(calls)["custom"]
        assert custom["domain"] == "meta-domain"
        assert custom["version"] == "3.0.0"

    def test_caller_contract_not_mutated(self):
        catalog, _ = _dmm()
        contract = _contract()
        before = copy.deepcopy(contract)
        _run(catalog.publish(contract))
        assert contract == before


# ---------------------------------------------------------------------------
# Defect 2 — ODPS team.name equals the team _ensure_team creates
# ---------------------------------------------------------------------------


class TestOdpsTeamNameIsSlug:
    def _provider(self):
        provider = DataMeshManagerProvider(api_key="k", api_url="http://dmm")
        calls: List[Tuple[str, str, Any]] = []

        def fake_request(method, path, json_body=None):
            calls.append((method, path, json_body))
            return SimpleNamespace(status_code=200, json=lambda: {}, text="")

        provider._request = fake_request  # type: ignore[assignment]
        provider._session = lambda: SimpleNamespace(  # type: ignore[assignment]
            get=lambda *a, **k: SimpleNamespace(status_code=404)
        )
        return provider, calls

    def test_live_publish_customer360(self):
        provider, calls = self._provider()
        provider.apply(load_contract(str(C360)))
        assert _team_puts(calls) == ["customer-platform"]
        assert _product(calls)["team"]["name"] == "customer-platform"

    def test_dry_run_customer360(self):
        provider, _ = self._provider()
        result = provider.apply(load_contract(str(C360)), dry_run=True)
        assert result["payload"]["team"]["name"] == "customer-platform"


# ---------------------------------------------------------------------------
# Defect 23 — the `dps` token means DPS 0.0.1 wherever a spec is accepted
# ---------------------------------------------------------------------------


class TestDpsToken:
    @pytest.mark.parametrize("token", ["dps", "DPS", " Dps "])
    def test_env_var(self, monkeypatch, token):
        monkeypatch.setenv("DMM_DATA_PRODUCT_SPEC", token)
        provider = DataMeshManagerProvider(api_key="k", api_url="http://dmm")
        result = provider.apply(_contract(), dry_run=True)
        assert result["payload"]["dataProductSpecification"] == "0.0.1"

    @pytest.mark.parametrize("token", ["dps", "DPS"])
    def test_explicit_value(self, token):
        provider = DataMeshManagerProvider(api_key="k", api_url="http://dmm")
        result = provider.apply(_contract(), dry_run=True, data_product_specification=token)
        assert result["payload"]["dataProductSpecification"] == "0.0.1"
        plan = provider.plan(_contract(), data_product_specification=token)
        assert plan[0]["payload"]["dataProductSpecification"] == "0.0.1"

    def test_catalog_config(self):
        catalog, calls = _dmm({"data_product_spec": "dps"})
        result = _run(catalog.publish(_contract()))
        assert result.success, result.error
        assert _product(calls)["dataProductSpecification"] == "0.0.1"

    def test_cli_flag(self, tmp_path):
        from fluid_build.cli.datamesh_manager import _cmd_publish

        path = tmp_path / "c.fluid.yaml"
        path.write_text(yaml.safe_dump(_contract()), encoding="utf-8")
        args = SimpleNamespace(
            contract=str(path),
            overlay=None,
            team_id=None,
            dry_run=True,
            with_contract=False,
            no_create_team=False,
            contract_format="odcs",
            data_product_spec="dps",
            validate_generated_contracts=False,
            validation_mode="warn",
            fail_on_contract_error=False,
            api_key="k",
            api_url="http://dmm",
        )
        with patch("fluid_build.cli.datamesh_manager._print_dry_run") as shown:
            assert _cmd_publish(args) == 0
        assert shown.call_args[0][0]["payload"]["dataProductSpecification"] == "0.0.1"

    def test_other_values_pass_through(self):
        provider = DataMeshManagerProvider(api_key="k", api_url="http://dmm")
        resolve = provider._resolve_data_product_specification
        assert resolve("0.0.1") == "0.0.1"
        assert resolve("odps") == "odps"
        assert resolve(" 0.0.2 ") == "0.0.2"


def test_legacy_registry_entry_uses_asset_route(tmp_path, monkeypatch):
    """A legacy plugin registered by name in CATALOG_PROVIDERS (as the
    verifier's ``legacykw`` plugin was) publishes through the asset flow."""
    from fluid_build.cli import publish as pub_mod

    LegacyExtraOptional.events = []
    monkeypatch.setitem(CATALOG_PROVIDERS, "legacykw", LegacyExtraOptional)
    path = tmp_path / "c.fluid.yaml"
    path.write_text(yaml.safe_dump(_contract()), encoding="utf-8")
    config = MagicMock()
    config.get_catalog_config.return_value = {"enabled": True}
    with pytest.warns(DeprecationWarning):
        result = _run(
            pub_mod.publish_contract(contract_path=path, catalog_name="legacykw", config=config)
        )
    assert result.success, result.error
    assert isinstance(LegacyExtraOptional.events[-1][1], CatalogAsset)
