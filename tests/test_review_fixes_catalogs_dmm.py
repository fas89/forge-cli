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

"""Regression tests for the catalogs / Data Mesh Manager review findings.

Covers: DMM data-product-spec resolution (env-var opt-out, catalog adapter
opt-outs), ``--team-id`` on the ODPS path, the deprecated CatalogAsset
publish API (built-in providers and third-party plugins written against the
pre-0.7.9 ``BaseCatalogProvider``), lenient publish validation, user-config
layering and ``${VAR}`` handling in ``save_user_config``.
"""

from __future__ import annotations

import asyncio
import warnings
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml

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
BITCOIN_EXAMPLE = (
    REPO_ROOT / "examples" / "bitcoin-price-api-imperative-part-a" / "contract.fluid.yaml"
)


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _schema_valid_contract(**overrides: Any) -> Dict[str, Any]:
    """A FLUID data product as the schema requires it: ``kind`` is mandatory
    and (for data products) always ``DataProduct``."""
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


@pytest.fixture
def no_spec_env(monkeypatch):
    monkeypatch.delenv("DMM_DATA_PRODUCT_SPEC", raising=False)


# ---------------------------------------------------------------------------
# Finding 7 — DMM_DATA_PRODUCT_SPEC must work for kind: DataProduct
# ---------------------------------------------------------------------------


class TestDmmSpecEnvVarOptOut:
    def test_env_var_selects_dps_for_kind_dataproduct_apply(self, monkeypatch):
        monkeypatch.setenv("DMM_DATA_PRODUCT_SPEC", "0.0.1")
        provider = DataMeshManagerProvider(api_key="k", api_url="https://dmm.example")

        result = provider.apply(_schema_valid_contract(), dry_run=True)

        assert result["payload"]["dataProductSpecification"] == "0.0.1"
        assert "apiVersion" not in result["payload"]

    def test_env_var_selects_dps_for_kind_dataproduct_plan(self, monkeypatch):
        monkeypatch.setenv("DMM_DATA_PRODUCT_SPEC", "0.0.1")
        provider = DataMeshManagerProvider(api_key="k", api_url="https://dmm.example")

        actions = provider.plan(_schema_valid_contract())

        assert actions[0]["payload"]["dataProductSpecification"] == "0.0.1"

    def test_resolution_order(self, monkeypatch):
        provider = DataMeshManagerProvider(api_key="k", api_url="https://dmm.example")
        resolve = provider._resolve_data_product_specification
        odps = provider.DATA_PRODUCT_SPEC_ODPS
        dps = provider.DATA_PRODUCT_SPEC_DPS

        monkeypatch.delenv("DMM_DATA_PRODUCT_SPEC", raising=False)
        assert resolve(None) == odps
        assert resolve(None, fluid=_schema_valid_contract()) == odps

        monkeypatch.setenv("DMM_DATA_PRODUCT_SPEC", "0.0.1")
        assert resolve(None, fluid=_schema_valid_contract()) == "0.0.1"
        # provider hint beats the env var, explicit value beats both
        assert resolve(None, provider_hint="odps") == odps
        assert resolve(None, provider_hint="dps") == dps
        assert resolve("odps", provider_hint="dps") == "odps"

    def test_default_without_env_is_odps(self, no_spec_env):
        provider = DataMeshManagerProvider(api_key="k", api_url="https://dmm.example")

        result = provider.apply(_schema_valid_contract(), dry_run=True)

        assert result["payload"]["apiVersion"]
        assert "dataProductSpecification" not in result["payload"]


# ---------------------------------------------------------------------------
# Finding 8 — --team-id must win on the ODPS path
# ---------------------------------------------------------------------------


class TestDmmTeamIdOverrideOdps:
    def test_dry_run_payload_uses_team_id_override(self, no_spec_env):
        provider = DataMeshManagerProvider(api_key="k", api_url="https://dmm.example")

        result = provider.apply(_schema_valid_contract(), dry_run=True, team_id="platform-team")

        assert result["payload"]["team"]["name"] == "platform-team"

    def test_dry_run_payload_uses_team_id_override_explicit_odps(self, no_spec_env):
        provider = DataMeshManagerProvider(api_key="k", api_url="https://dmm.example")

        result = provider.apply(
            _schema_valid_contract(),
            dry_run=True,
            team_id="platform-team",
            data_product_specification="odps",
        )

        assert result["payload"]["team"]["name"] == "platform-team"

    def test_without_override_owner_team_is_kept(self, no_spec_env):
        provider = DataMeshManagerProvider(api_key="k", api_url="https://dmm.example")

        result = provider.apply(_schema_valid_contract(), dry_run=True)

        assert result["payload"]["team"]["name"] == "sales-team"

    def test_live_publish_puts_product_on_override_team(self, no_spec_env):
        provider = DataMeshManagerProvider(api_key="k", api_url="https://dmm.example")
        calls: List[Tuple[str, str, Any]] = []

        def fake_request(method, path, json_body=None):
            calls.append((method, path, json_body))
            return SimpleNamespace(status_code=200, json=lambda: {})

        provider._request = fake_request  # type: ignore[assignment]
        ensured: List[str] = []
        provider._ensure_team = lambda fluid, tid: ensured.append(tid)  # type: ignore

        result = provider.apply(_schema_valid_contract(), team_id="platform-team")

        assert ensured == ["platform-team"]
        assert result["team_id"] == "platform-team"
        put = [c for c in calls if c[1] == "/api/dataproducts/sales.orders"]
        assert put and put[0][2]["team"]["name"] == "platform-team"

    def test_cli_dry_run_shows_team_id_override(self, tmp_path, no_spec_env, capsys):
        from fluid_build.cli.datamesh_manager import _cmd_publish

        contract_path = tmp_path / "contract.fluid.yaml"
        contract_path.write_text(yaml.safe_dump(_schema_valid_contract()), encoding="utf-8")
        args = SimpleNamespace(
            contract=str(contract_path),
            overlay=None,
            team_id="platform-team",
            dry_run=True,
            with_contract=False,
            no_create_team=False,
            contract_format="odcs",
            data_product_spec=None,
            validate_generated_contracts=False,
            validation_mode="warn",
            fail_on_contract_error=False,
            api_key="k",
            api_url="https://dmm.example",
        )

        with patch(
            "fluid_build.cli.datamesh_manager._print_dry_run"
        ) as print_dry_run:  # capture the exact result the CLI would render
            rc = _cmd_publish(args)

        assert rc == 0
        shown = print_dry_run.call_args[0][0]
        assert shown["payload"]["team"]["name"] == "platform-team"


# ---------------------------------------------------------------------------
# Finding 42 — catalog adapter goes through the resolver + has opt-outs
# ---------------------------------------------------------------------------


def _dmm_catalog(config_extra: Optional[Dict[str, Any]] = None):
    config: Dict[str, Any] = {"endpoint": "https://dmm.example", "auth": {"api_key": "k"}}
    config.update(config_extra or {})
    catalog = DataMeshManagerCatalogProvider(config)
    puts: List[Dict[str, Any]] = []

    def fake_request(method, path, json_body=None):
        if method == "PUT" and path.startswith("/api/dataproducts/"):
            puts.append(json_body)
        return SimpleNamespace(status_code=200, json=lambda: {})

    catalog._provider._request = fake_request  # type: ignore[assignment]
    catalog._provider._ensure_team = lambda fluid, tid: None  # type: ignore[assignment]
    return catalog, puts


class TestDmmCatalogAdapterSpecSelection:
    def test_default_is_odps(self, no_spec_env):
        catalog, puts = _dmm_catalog()
        contract = _schema_valid_contract()

        result = _run(catalog.publish(contract, CatalogTarget.from_fluid(contract)))

        assert result.success, result.error
        assert puts[0]["apiVersion"]
        assert "dataProductSpecification" not in puts[0]

    def test_env_var_opt_out(self, monkeypatch):
        monkeypatch.setenv("DMM_DATA_PRODUCT_SPEC", "0.0.1")
        catalog, puts = _dmm_catalog()
        contract = _schema_valid_contract()

        result = _run(catalog.publish(contract, CatalogTarget.from_fluid(contract)))

        assert result.success, result.error
        assert puts[0]["dataProductSpecification"] == "0.0.1"

    @pytest.mark.parametrize(
        "config_extra",
        [{"data_product_spec": "0.0.1"}, {"provider_hint": "dps"}],
    )
    def test_catalog_config_opt_out(self, no_spec_env, config_extra):
        catalog, puts = _dmm_catalog(config_extra)
        contract = _schema_valid_contract()

        result = _run(catalog.publish(contract, CatalogTarget.from_fluid(contract)))

        assert result.success, result.error
        assert puts[0]["dataProductSpecification"] == "0.0.1"

    def test_catalog_hints_opt_out_and_override_config(self, no_spec_env):
        catalog, puts = _dmm_catalog({"data_product_spec": "odps"})
        contract = _schema_valid_contract()
        target = CatalogTarget.from_fluid(
            contract, catalog_hints={"data_product_specification": "0.0.1"}
        )

        result = _run(catalog.publish(contract, target))

        assert result.success, result.error
        assert puts[0]["dataProductSpecification"] == "0.0.1"

    def test_fluid_publish_cli_path_honours_config_opt_out(self, tmp_path, no_spec_env):
        """End to end through ``publish_contract`` with the catalog config
        key set, as ``fluid publish --catalog datamesh-manager`` would run."""
        from fluid_build.cli import publish as pub_mod

        contract = _schema_valid_contract()
        contract_path = tmp_path / "c.fluid.yaml"
        contract_path.write_text(yaml.safe_dump(contract), encoding="utf-8")
        catalog, puts = _dmm_catalog({"data_product_spec": "0.0.1"})
        config = MagicMock()
        config.get_catalog_config.return_value = {"enabled": True}

        with patch.object(pub_mod, "load_contract", return_value=contract):
            with patch.object(pub_mod, "get_catalog_provider", return_value=catalog):
                result = _run(
                    pub_mod.publish_contract(
                        contract_path=contract_path,
                        catalog_name="datamesh-manager",
                        config=config,
                        skip_health_check=True,
                    )
                )

        assert result.success, result.error
        assert puts[0]["dataProductSpecification"] == "0.0.1"


# ---------------------------------------------------------------------------
# Findings 9 / 41 — deprecated CatalogAsset API keeps working for one release
# ---------------------------------------------------------------------------


def _asset() -> CatalogAsset:
    return CatalogAsset(
        id="sales.orders",
        name="Orders",
        description="d",
        type="dataproduct",
        domain="sales",
        owner="sales-team",
        owner_email="s@example.com",
        layer="Gold",
        tags=[],
        version="1.0.0",
        platform="unknown",
        location={},
        contract_yaml="id: sales.orders\n",
    )


class TestBuiltinProvidersAcceptLegacyAsset:
    @pytest.mark.parametrize("method", ["publish", "update"])
    def test_fluid_cc_accepts_catalog_asset(self, method):
        provider = FluidCommandCenterProvider({"endpoint": "http://cc.example"})
        seen: Dict[str, Any] = {}

        async def fake_impl(fluid, target):
            seen["fluid"], seen["target"] = fluid, target
            return PublishResult(True, provider.name, target.contract_id)

        provider.health_check = AsyncMock(return_value=True)  # type: ignore[assignment]
        provider._publish_impl = fake_impl  # type: ignore[assignment]

        with pytest.warns(DeprecationWarning):
            result = _run(getattr(provider, method)(_asset()))

        assert isinstance(result, PublishResult)
        assert result.success, result.error
        assert seen["target"].contract_id == "sales.orders"
        assert seen["target"].contract_yaml == "id: sales.orders\n"
        assert seen["fluid"]["id"] == "sales.orders"

    @pytest.mark.parametrize("method", ["publish", "update"])
    def test_dmm_catalog_accepts_catalog_asset(self, method):
        provider = DataMeshManagerCatalogProvider({"endpoint": "http://dmm.example"})
        seen: Dict[str, Any] = {}

        def fake_apply(fluid, **kwargs):
            seen["fluid"] = fluid
            return {"url": "http://dmm.example/x"}

        provider._provider.apply = fake_apply  # type: ignore[assignment]

        with pytest.warns(DeprecationWarning):
            result = _run(getattr(provider, method)(_asset()))

        assert result.success, result.error
        assert result.asset_id == "sales.orders"
        assert seen["fluid"]["id"] == "sales.orders"

    def test_target_defaults_from_fluid(self):
        provider = DataMeshManagerCatalogProvider({"endpoint": "http://dmm.example"})
        provider._provider.apply = lambda fluid, **kw: {}  # type: ignore[assignment]
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            result = _run(provider.publish(_schema_valid_contract()))
        assert result.asset_id == "sales.orders"

    @pytest.mark.parametrize("method", ["publish_asset", "update_asset"])
    def test_publish_asset_shims_forward_full_fluid(self, method):
        provider = DataMeshManagerCatalogProvider({"endpoint": "http://dmm.example"})
        seen: Dict[str, Any] = {}

        def fake_apply(fluid, **kwargs):
            seen["fluid"] = fluid
            return {}

        provider._provider.apply = fake_apply  # type: ignore[assignment]
        contract = _schema_valid_contract(
            exposes=[{"exposeId": "a", "kind": "table"}, {"exposeId": "b", "kind": "table"}]
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            asset = provider.map_contract_to_asset(contract)

        with pytest.warns(DeprecationWarning):
            result = _run(getattr(provider, method)(asset))

        assert result.success, result.error
        assert len(seen["fluid"]["exposes"]) == 2


class LegacyCatalog(BaseCatalogProvider):
    """A third-party plugin written exactly against origin/main's
    ``BaseCatalogProvider`` API (``publish(asset)``, ``update(asset)``,
    ``verify(asset_id)``, ``validate_asset`` / ``map_contract_to_asset``
    overrides)."""

    name = "legacy"
    events: List[Tuple[str, Any]] = []

    async def publish(self, asset: CatalogAsset) -> PublishResult:
        self.events.append(("publish", asset))
        return PublishResult(
            success=True,
            catalog_id=self.name,
            asset_id=asset.id,
            catalog_url=f"http://legacy.example/{asset.id}",
        )

    async def update(self, asset: CatalogAsset) -> PublishResult:
        self.events.append(("update", asset))
        return await self.publish(asset)

    async def verify(self, asset_id: str) -> bool:
        self.events.append(("verify", asset_id))
        return True

    def map_contract_to_asset(self, contract: Dict[str, Any]) -> CatalogAsset:
        self.events.append(("map", contract))
        asset = super().map_contract_to_asset(contract)
        asset.tags = ["mapped-by-plugin"]
        return asset

    def validate_asset(self, asset: CatalogAsset) -> Tuple[bool, Optional[str]]:
        self.events.append(("validate", asset))
        if asset.domain == "blocked":
            return False, "plugin says no"
        return super().validate_asset(asset)


@pytest.fixture
def legacy_catalog(monkeypatch):
    LegacyCatalog.events = []
    monkeypatch.setitem(CATALOG_PROVIDERS, "legacy-cat", LegacyCatalog)
    return LegacyCatalog


def _publish_via_cli(tmp_path, contract, **kwargs) -> PublishResult:
    from fluid_build.cli import publish as pub_mod

    contract_path = tmp_path / "c.fluid.yaml"
    contract_path.write_text(yaml.safe_dump(contract), encoding="utf-8")
    config = MagicMock()
    config.get_catalog_config.return_value = {"enabled": True}
    with patch.object(pub_mod, "load_contract", return_value=contract):
        return _run(
            pub_mod.publish_contract(
                contract_path=contract_path,
                catalog_name="legacy-cat",
                config=config,
                **kwargs,
            )
        )


class TestLegacyThirdPartyCatalogPlugin:
    def test_detected_as_legacy(self, legacy_catalog):
        assert LegacyCatalog._uses_legacy_asset_api() is True
        assert FluidCommandCenterProvider._uses_legacy_asset_api() is False
        assert DataMeshManagerCatalogProvider._uses_legacy_asset_api() is False

    def test_publish_routes_through_asset_flow(self, tmp_path, legacy_catalog):
        with pytest.warns(DeprecationWarning, match="publish\\(fluid, target\\)"):
            result = _publish_via_cli(tmp_path, _schema_valid_contract())

        assert result.success, result.error
        assert result.asset_id == "sales.orders"
        kinds = [k for k, _ in legacy_catalog.events]
        assert kinds == ["map", "publish"]
        published = legacy_catalog.events[-1][1]
        assert isinstance(published, CatalogAsset)
        assert published.tags == ["mapped-by-plugin"]  # plugin's mapper honoured
        assert published.contract_yaml and "sales.orders" in published.contract_yaml

    def test_dry_run_uses_plugin_validate_asset(self, tmp_path, legacy_catalog):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            blocked = _publish_via_cli(
                tmp_path, _schema_valid_contract(domain="blocked"), dry_run=True
            )
            allowed = _publish_via_cli(tmp_path, _schema_valid_contract(), dry_run=True)

        assert blocked.success is False
        assert blocked.error == "plugin says no"
        assert allowed.success is True
        assert "validate" in [k for k, _ in legacy_catalog.events]
        assert not [k for k, _ in legacy_catalog.events if k == "publish"]

    def test_verify_only_uses_asset_id(self, tmp_path, legacy_catalog):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = _publish_via_cli(tmp_path, _schema_valid_contract(), verify_only=True)

        assert result.success is True
        assert ("verify", "sales.orders") in legacy_catalog.events

    def test_health_check_still_runs(self, tmp_path, legacy_catalog):
        unhealthy = AsyncMock(return_value=False)
        with patch.object(LegacyCatalog, "health_check", unhealthy):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                result = _publish_via_cli(tmp_path, _schema_valid_contract())

        assert result.success is False
        assert "health check failed" in result.error
        assert not [k for k, _ in legacy_catalog.events if k == "publish"]

    @pytest.mark.parametrize("method", ["publish_asset", "update_asset"])
    def test_publish_asset_shim_on_legacy_plugin(self, legacy_catalog, method):
        plugin = LegacyCatalog({})
        with pytest.warns(DeprecationWarning):
            result = _run(getattr(plugin, method)(_asset()))
        assert result.success
        assert legacy_catalog.events[0][0] == method.split("_")[0]

    def test_new_api_validate_target_consults_overridden_validate_asset(self):
        class NewApiWithLegacyValidator(BaseCatalogProvider):
            async def publish(self, fluid, target=None):
                return PublishResult(True, "x", "x")

            async def update(self, fluid, target=None):
                return PublishResult(True, "x", "x")

            async def verify(self, contract_id):
                return True

            def validate_asset(self, asset):
                return (asset.domain != "blocked", "nope")

        p = NewApiWithLegacyValidator({})
        bad = _schema_valid_contract(domain="blocked")
        ok, err = p.validate_target(bad, CatalogTarget.from_fluid(bad))
        assert (ok, err) == (False, "nope")


# ---------------------------------------------------------------------------
# Findings 10 / 20 — lenient validation (domain/owner optional)
# ---------------------------------------------------------------------------


class _NewApiStub(BaseCatalogProvider):
    async def publish(self, fluid, target=None):
        return PublishResult(True, "stub", "x")

    async def update(self, fluid, target=None):
        return PublishResult(True, "stub", "x")

    async def verify(self, contract_id):
        return True


class TestLenientValidateTarget:
    @pytest.mark.parametrize(
        "contract",
        [
            {"id": "a.b", "name": "No domain", "metadata": {"owner": {"team": "t"}}},
            {"id": "a.b", "name": "Email owner", "metadata": {"owner": {"email": "x@y.z"}}},
            {"id": "a.b", "name": "Nothing else"},
            {
                "id": "a.b",
                "name": "Business ctx",
                "metadata": {"businessContext": {"domain": "sales"}},
            },
        ],
    )
    def test_missing_domain_or_owner_is_valid(self, contract):
        p = _NewApiStub({})
        assert p.validate_target(contract, CatalogTarget.from_fluid(contract)) == (True, None)

    def test_id_and_name_still_required(self):
        p = _NewApiStub({})
        ok, err = p.validate_target({}, CatalogTarget(contract_id="", contract_name="x"))
        assert ok is False and "id" in err
        ok, err = p.validate_target({}, CatalogTarget(contract_id="x", contract_name=""))
        assert ok is False and "name" in err

    def test_fluid_cc_publishes_shipped_example_without_domain(self):
        from fluid_build.loader import load_contract

        contract = load_contract(str(BITCOIN_EXAMPLE))
        assert not contract.get("domain")
        provider = FluidCommandCenterProvider({"endpoint": "http://cc.example"})
        sent: Dict[str, Any] = {}

        async def fake_impl(fluid, target):
            from fluid_build.providers.catalogs.fluid_cc.provider import (
                _build_cc_asset_payload,
            )

            sent.update(_build_cc_asset_payload(fluid, target))
            return PublishResult(True, provider.name, target.contract_id)

        provider.health_check = AsyncMock(return_value=True)  # type: ignore[assignment]
        provider._publish_impl = fake_impl  # type: ignore[assignment]

        result = _run(provider.publish(contract, CatalogTarget.from_fluid(contract)))

        assert result.success, result.error
        assert sent["metadata"]["domain"] == "general"

    def test_dry_run_accepts_shipped_example_without_domain(self):
        from fluid_build.cli import publish as pub_mod

        config = MagicMock()
        config.get_catalog_config.return_value = {"endpoint": "http://cc.example"}
        result = _run(
            pub_mod.publish_contract(
                contract_path=BITCOIN_EXAMPLE,
                catalog_name="fluid-command-center",
                config=config,
                dry_run=True,
            )
        )
        assert result.success is True, result.error


# ---------------------------------------------------------------------------
# Finding 11 — ~/.fluid/config.yaml is an extra, lowest-priority user layer
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_home(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    project = tmp_path / "project"
    project.mkdir()
    monkeypatch.setattr("pathlib.Path.home", lambda: home)
    monkeypatch.chdir(project)
    for var in ("FLUID_CC_ENDPOINT", "FLUID_API_KEY"):
        monkeypatch.delenv(var, raising=False)
    return home


class TestUserConfigLayering:
    def test_dot_fluid_config_does_not_hide_fluidrc(self, fake_home):
        from fluid_build.config_manager import FluidConfig

        (fake_home / ".fluid").mkdir()
        (fake_home / ".fluid" / "config.yaml").write_text(
            "command_center:\n  url: http://cc.example\n  api_key: cc-key\n"
        )
        (fake_home / ".fluidrc.yaml").write_text(
            "catalogs:\n  datamesh-manager:\n    endpoint: http://dmm.example\n"
        )

        cfg = FluidConfig()

        assert cfg.get("command_center.url") == "http://cc.example"
        assert cfg.get_catalog_config("datamesh-manager")["endpoint"] == "http://dmm.example"

    def test_legacy_file_wins_on_conflict(self, fake_home):
        from fluid_build.config_manager import FluidConfig

        (fake_home / ".fluid").mkdir()
        (fake_home / ".fluid" / "config.yaml").write_text(
            "logging:\n  level: DEBUG\nnetwork:\n  timeout: 99\n"
        )
        (fake_home / ".config" / "fluid").mkdir(parents=True)
        (fake_home / ".config" / "fluid" / "config.yaml").write_text("logging:\n  level: ERROR\n")

        cfg = FluidConfig()

        assert cfg.get("logging.level") == "ERROR"
        assert cfg.get("network.timeout") == 99

    def test_legacy_first_found_order_preserved(self, fake_home):
        from fluid_build.config_manager import FluidConfig

        (fake_home / ".fluidrc.yaml").write_text("logging:\n  level: WARNING\n")
        (fake_home / ".config" / "fluid").mkdir(parents=True)
        (fake_home / ".config" / "fluid" / "config.yaml").write_text(
            "logging:\n  level: ERROR\nnetwork:\n  timeout: 77\n"
        )

        cfg = FluidConfig()

        # main's behaviour: only the first found legacy file is read
        assert cfg.get("logging.level") == "WARNING"
        assert cfg.get("network.timeout") != 77

    def test_file_config_does_not_leak_into_later_instances(self, fake_home):
        from fluid_build.config_manager import DEFAULT_CONFIG, FluidConfig

        (fake_home / ".fluidrc.yaml").write_text(
            "catalogs:\n  leaky:\n    endpoint: http://leak.example\n"
        )
        FluidConfig()
        assert "leaky" not in DEFAULT_CONFIG["catalogs"]


# ---------------------------------------------------------------------------
# Finding 43 — save_user_config never writes resolved ${VAR} secrets
# ---------------------------------------------------------------------------


class TestConfigEnvPlaceholdersOnSave:
    def test_save_keeps_placeholder(self, fake_home, tmp_path, monkeypatch):
        from fluid_build.config_manager import FluidConfig

        monkeypatch.setenv("MY_DMM_KEY", "sk-live-SECRET")
        (fake_home / ".fluidrc.yaml").write_text(
            "catalogs:\n  datamesh-manager:\n    endpoint: http://dmm.example\n"
            "    api_key: ${MY_DMM_KEY}\n"
        )
        cfg = FluidConfig()
        # Reads still see the expanded value
        assert cfg.get_catalog_config("datamesh-manager")["api_key"] == "sk-live-SECRET"
        assert cfg.get("catalogs.datamesh-manager.api_key") == "sk-live-SECRET"
        assert cfg.to_dict()["catalogs"]["datamesh-manager"]["api_key"] == "sk-live-SECRET"

        cfg.set("logging.level", "DEBUG")
        out = tmp_path / "saved.yaml"
        cfg.save_user_config(path=out)

        text = out.read_text()
        assert "sk-live-SECRET" not in text
        saved = yaml.safe_load(text)
        assert saved["catalogs"]["datamesh-manager"]["api_key"] == "${MY_DMM_KEY}"
        assert saved["logging"]["level"] == "DEBUG"

    def test_catalog_env_override_not_persisted(self, fake_home, tmp_path, monkeypatch):
        from fluid_build.config_manager import FluidConfig

        monkeypatch.setenv("FLUID_API_KEY", "fluid-SECRET")
        cfg = FluidConfig()
        assert cfg.get_catalog_config("fluid-command-center")["auth"]["api_key"] == "fluid-SECRET"

        out = tmp_path / "saved.yaml"
        cfg.save_user_config(path=out)
        assert "fluid-SECRET" not in out.read_text()


# ---------------------------------------------------------------------------
# Findings 58 / 72 — live e2e script hygiene (static checks; never runs DMM)
# ---------------------------------------------------------------------------


class TestDmmLiveE2EScript:
    SCRIPT = REPO_ROOT / "scripts" / "dmm_live_e2e.sh"

    def test_bash_syntax(self):
        import shutil
        import subprocess

        bash = shutil.which("bash")
        if not bash:
            pytest.skip("bash not available")
        proc = subprocess.run([bash, "-n", str(self.SCRIPT)], capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr

    def test_no_predictable_workdir_no_key_in_argv_no_home_overwrite(self):
        text = self.SCRIPT.read_text(encoding="utf-8")
        assert "mktemp -d" in text
        assert "dmm_live_e2e.$$" not in text
        assert '-H "x-api-key:' not in text
        assert '-H @"$HDR_FILE"' in text
        assert 'FLUID_CFG="$HOME/.fluid/config.yaml"' not in text
        assert 'HOME="$FLUID_HOME"' in text
