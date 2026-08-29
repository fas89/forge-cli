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

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import requests

from fluid_build.cli.datamesh_manager import _cmd_publish, _publish_exit_code
from fluid_build.providers.base import ProviderError
from fluid_build.providers.datamesh_manager import DataMeshManagerProvider


def _sample_contract():
    return {
        "id": "sales-product",
        "metadata": {
            "name": "Sales Product",
            "description": "demo",
            "status": "active",
            "owner": {"team": "analytics"},
        },
        "owner": {"team": "analytics"},
        "exposes": [],
        "expects": [],
    }


def _sample_contract_with_exposes():
    return {
        "id": "sales-product",
        "metadata": {
            "name": "Sales Product",
            "description": "demo",
            "status": "active",
            "owner": {"team": "analytics"},
        },
        "owner": {"team": "analytics"},
        "exposes": [
            {"id": "orders", "provider": "gcp", "contract": {"schema": []}},
            {"id": "customers", "provider": "gcp", "contract": {"schema": []}},
        ],
        "expects": [],
    }


def _sample_odps_alias_contract():
    return {
        "id": "sales-product",
        "metadata": {
            "name": "Sales Product",
            "description": "demo",
            "status": "active",
            "owner": {"team": "analytics"},
            "type": "analytical",
        },
        "tags": ["sales", "gold"],
        "owner": {"team": "analytics"},
        "exposes": [],
        "expects": [],
    }


def _sample_odps_binding_platform_contract():
    return {
        "id": "sales-product",
        "metadata": {
            "name": "Sales Product",
            "description": "demo",
            "status": "active",
            "owner": {"team": "analytics"},
        },
        "owner": {"team": "analytics"},
        "exposes": [
            {
                "id": "orders",
                "binding": {
                    "platform": "gcp",
                    "location": {
                        "project": "demo-project",
                        "dataset": "sales",
                        "table": "orders",
                    },
                },
                "contract": {"schema": []},
            }
        ],
        "expects": [],
    }


def test_apply_dry_run_defaults_to_dps_spec():
    provider = DataMeshManagerProvider(api_key="dummy", api_url="https://api.entropy-data.com")

    result = provider.apply(_sample_contract(), dry_run=True)

    assert result["payload"]["dataProductSpecification"] == "0.0.1"


def test_apply_dry_run_uses_odps_spec_when_provider_hint_is_odps():
    provider = DataMeshManagerProvider(api_key="dummy", api_url="https://api.entropy-data.com")

    result = provider.apply(_sample_contract(), dry_run=True, provider_hint="odps")

    payload = result["payload"]
    assert payload["kind"] == "DataProduct"
    assert "apiVersion" in payload
    assert "info" not in payload


def test_apply_dry_run_uses_odps_spec_when_contract_kind_is_dataproduct():
    """Bitol-style FLUID contracts declare ``kind: DataProduct``; the publish
    payload must therefore default to ODPS so Entropy CE (configured for
    ``odps`` only) accepts it. Regression test for the issue surfaced by
    running ``publish:pre`` against the snowflake-biz-lab repo.
    """
    provider = DataMeshManagerProvider(api_key="dummy", api_url="https://api.entropy-data.com")
    contract = _sample_contract()
    contract["kind"] = "DataProduct"

    result = provider.apply(contract, dry_run=True)

    payload = result["payload"]
    # ODPS path emits apiVersion + kind, not the DPS dataProductSpecification field
    assert payload.get("kind") == "DataProduct"
    assert "apiVersion" in payload


def test_apply_dry_run_dps_spec_unchanged_for_legacy_contract():
    """A FLUID contract without ``kind: DataProduct`` and no ODPS hint still
    falls back to the legacy DPS specification."""
    provider = DataMeshManagerProvider(api_key="dummy", api_url="https://api.entropy-data.com")

    result = provider.apply(_sample_contract(), dry_run=True)

    assert result["payload"]["dataProductSpecification"] == "0.0.1"


def test_ensure_team_payload_includes_type_field():
    """Entropy CE 2.0.x's ``PUT /api/teams/{id}`` requires a non-null ``type``
    field. The fluid CLI's team payload must include it (default ``internal``)
    or the publish fails with HTTP 400 "Failed to read request".
    """
    provider = DataMeshManagerProvider(api_key="dummy", api_url="https://api.entropy-data.com")

    captured: dict = {}

    def fake_request(method, path, json_body=None, **kw):
        captured["method"] = method
        captured["path"] = path
        captured["json_body"] = json_body

        class _Resp:
            status_code = 200

        return _Resp()

    fake_session = MagicMock()
    # Force the GET to return 404 so _ensure_team falls through to PUT
    fake_session.get.return_value = SimpleNamespace(status_code=404)

    with (
        patch.object(provider, "_session", return_value=fake_session),
        patch.object(provider, "_request", side_effect=fake_request),
    ):
        provider._ensure_team(
            {"owner": {"team": "telco-data-platform", "email": "data-platform@example.com"}},
            "telco-data-platform",
        )

    assert captured["method"] == "PUT"
    assert captured["path"] == "/api/teams/telco-data-platform"
    assert captured["json_body"]["type"] == "internal"
    assert captured["json_body"]["id"] == "telco-data-platform"


def test_ensure_team_type_override_via_owner_team_type():
    """``owner.team_type`` overrides the default ``internal``."""
    provider = DataMeshManagerProvider(api_key="dummy", api_url="https://api.entropy-data.com")
    captured: dict = {}

    def fake_request(method, path, json_body=None, **kw):
        captured["json_body"] = json_body

        class _Resp:
            status_code = 200

        return _Resp()

    fake_session = MagicMock()
    fake_session.get.return_value = SimpleNamespace(status_code=404)

    with (
        patch.object(provider, "_session", return_value=fake_session),
        patch.object(provider, "_request", side_effect=fake_request),
    ):
        provider._ensure_team(
            {"owner": {"team": "bu-analytics", "team_type": "business-unit"}},
            "bu-analytics",
        )

    assert captured["json_body"]["type"] == "business-unit"


def test_apply_dry_run_allows_explicit_spec_override():
    provider = DataMeshManagerProvider(api_key="dummy", api_url="https://api.entropy-data.com")

    result = provider.apply(
        _sample_contract(),
        dry_run=True,
        provider_hint="odps",
        data_product_specification="4.1.0",
    )

    assert result["payload"]["dataProductSpecification"] == "4.1.0"


def test_apply_dry_run_keeps_per_expose_data_contract_ids_for_dps():
    provider = DataMeshManagerProvider(api_key="dummy", api_url="https://api.entropy-data.com")

    result = provider.apply(_sample_contract_with_exposes(), dry_run=True, publish_contract=True)

    output_ports = result["payload"].get("outputPorts", [])
    data_contract_ids = [port.get("dataContractId") for port in output_ports]
    assert data_contract_ids == ["sales-product.orders", "sales-product.customers"]


def test_apply_dry_run_sets_per_expose_contract_ids_for_odps():
    provider = DataMeshManagerProvider(api_key="dummy", api_url="https://api.entropy-data.com")

    result = provider.apply(
        _sample_contract_with_exposes(),
        dry_run=True,
        publish_contract=True,
        provider_hint="odps",
    )

    output_ports = result["payload"].get("outputPorts", [])
    contract_ids = [port.get("contractId") for port in output_ports]
    assert contract_ids == ["sales-product.orders", "sales-product.customers"]


def test_apply_dry_run_odps_includes_top_level_tags_when_metadata_missing():
    provider = DataMeshManagerProvider(api_key="dummy", api_url="https://api.entropy-data.com")

    result = provider.apply(
        _sample_odps_alias_contract(),
        dry_run=True,
        provider_hint="odps",
    )

    payload = result["payload"]
    assert payload["tags"] == ["sales", "gold"]


def test_apply_dry_run_odps_maps_metadata_type_to_custom_property_type():
    provider = DataMeshManagerProvider(api_key="dummy", api_url="https://api.entropy-data.com")

    result = provider.apply(
        _sample_odps_alias_contract(),
        dry_run=True,
        provider_hint="odps",
    )

    custom_props = result["payload"].get("customProperties", [])
    assert {"property": "type", "value": "analytical"} in custom_props


def test_apply_dry_run_odps_sets_output_port_type_from_binding_platform():
    provider = DataMeshManagerProvider(api_key="dummy", api_url="https://api.entropy-data.com")

    result = provider.apply(
        _sample_odps_binding_platform_contract(),
        dry_run=True,
        provider_hint="odps",
    )

    output_ports = result["payload"].get("outputPorts", [])
    assert output_ports[0]["type"] == "bigquery"


def test_cmd_publish_passes_provider_hint_to_apply():
    args = SimpleNamespace(
        contract="contract.fluid.yaml",
        overlay=None,
        dry_run=True,
        team_id=None,
        no_create_team=False,
        with_contract=False,
        contract_format="odcs",
        data_product_spec=None,
        validate_generated_contracts=True,
        validation_mode="strict",
        fail_on_contract_error=False,
        provider="odps",
    )

    mock_provider = MagicMock()
    mock_provider.apply.return_value = {
        "dry_run": True,
        "method": "PUT",
        "url": "https://api.entropy-data.com/api/dataproducts/sales-product",
        "payload": {"id": "sales-product", "kind": "DataProduct", "apiVersion": "v1.0.0"},
    }

    with patch(
        "fluid_build.cli.datamesh_manager.load_contract_with_overlay",
        return_value=_sample_contract(),
    ):
        with patch("fluid_build.cli.datamesh_manager._make_provider", return_value=mock_provider):
            with patch("fluid_build.cli.datamesh_manager._print_dry_run"):
                code = _cmd_publish(args)

    assert code == 0
    _, kwargs = mock_provider.apply.call_args
    assert kwargs["provider_hint"] == "odps"
    assert kwargs["data_product_specification"] is None
    assert kwargs["validate_generated_contracts"] is True
    assert kwargs["validation_mode"] == "strict"


def test_cmd_publish_fail_on_contract_error_returns_non_zero():
    args = SimpleNamespace(
        contract="contract.fluid.yaml",
        overlay=None,
        dry_run=False,
        team_id=None,
        no_create_team=False,
        with_contract=True,
        contract_format="odcs",
        data_product_spec=None,
        validate_generated_contracts=False,
        validation_mode="warn",
        fail_on_contract_error=True,
        provider="odps",
    )

    mock_provider = MagicMock()
    mock_provider.apply.return_value = {
        "success": True,
        "product_id": "sales-product",
        "odcs_contracts": [
            {"contract_id": "sales-product.a", "success": True},
            {"contract_id": "sales-product.b", "success": False, "error": "boom"},
        ],
    }

    with patch(
        "fluid_build.cli.datamesh_manager.load_contract_with_overlay",
        return_value=_sample_contract(),
    ):
        with patch("fluid_build.cli.datamesh_manager._make_provider", return_value=mock_provider):
            with patch("fluid_build.cli.datamesh_manager._print_publish_result"):
                code = _cmd_publish(args)

    assert code == 1


def test_request_wraps_retry_error_as_provider_error():
    provider = DataMeshManagerProvider(api_key="dummy", api_url="https://api.entropy-data.com")
    session = MagicMock()
    session.request.side_effect = requests.exceptions.RetryError("too many 500 responses")
    provider._session_instance = session

    with pytest.raises(ProviderError) as excinfo:
        provider._request("PUT", "/api/datacontracts/x.y", json_body={"id": "x.y"})

    assert "HTTP request failed" in str(excinfo.value)


def test_publish_exit_code_strict_mode_on_invalid_contract():
    args = SimpleNamespace(validation_mode="strict", fail_on_contract_error=False)
    result = {
        "odcs_contracts": [
            {"contract_id": "a", "success": True, "valid": True},
            {"contract_id": "b", "success": True, "valid": False},
        ]
    }

    assert _publish_exit_code(result, args) == 1


def test_publish_exit_code_fail_on_contract_error():
    args = SimpleNamespace(validation_mode="warn", fail_on_contract_error=True)
    result = {
        "odcs_contracts": [
            {"contract_id": "a", "success": True},
            {"contract_id": "b", "success": False, "error": "boom"},
        ]
    }

    assert _publish_exit_code(result, args) == 1


def test_publish_odcs_strict_validation_skips_put_on_invalid(monkeypatch):
    provider = DataMeshManagerProvider(api_key="dummy", api_url="https://api.entropy-data.com")
    contract = {
        "id": "sales-product",
        "metadata": {"name": "Sales Product"},
        "owner": {"team": "analytics"},
        "exposes": [{"id": "port_a"}],
        "expects": [],
    }

    class _FakeOdcsProvider:
        def render(self, fluid, expose_id=None):
            return {"id": f"sales-product.{expose_id}", "kind": "DataContract"}

    monkeypatch.setattr(
        "fluid_build.providers.odcs.OdcsProvider",
        _FakeOdcsProvider,
        raising=True,
    )
    monkeypatch.setattr(
        provider,
        "_validate_generated_odcs_contract",
        lambda _odcs_provider, _odcs_body: (False, "ODCS validation failed"),
    )

    request_calls = []
    monkeypatch.setattr(
        provider,
        "_request",
        lambda *args, **kwargs: request_calls.append((args, kwargs)),
    )

    results = provider._publish_odcs_per_expose(
        contract,
        "sales-product",
        validate_generated_contracts=True,
        validation_mode="strict",
    )

    assert request_calls == []
    assert results[0]["success"] is False
    assert results[0]["valid"] is False
    assert results[0]["error_type"] == "VALIDATION_FAILED"


def test_publish_odcs_warn_validation_still_puts(monkeypatch):
    provider = DataMeshManagerProvider(api_key="dummy", api_url="https://api.entropy-data.com")
    contract = {
        "id": "sales-product",
        "metadata": {"name": "Sales Product"},
        "owner": {"team": "analytics"},
        "exposes": [{"id": "port_a"}],
        "expects": [],
    }

    class _FakeOdcsProvider:
        def render(self, fluid, expose_id=None):
            return {"id": f"sales-product.{expose_id}", "kind": "DataContract"}

    monkeypatch.setattr(
        "fluid_build.providers.odcs.OdcsProvider",
        _FakeOdcsProvider,
        raising=True,
    )
    monkeypatch.setattr(
        provider,
        "_validate_generated_odcs_contract",
        lambda _odcs_provider, _odcs_body: (False, "ODCS validation failed"),
    )

    class _Resp:
        status_code = 200

    request_calls = []
    monkeypatch.setattr(
        provider,
        "_request",
        lambda *args, **kwargs: (request_calls.append((args, kwargs)) or _Resp()),
    )

    results = provider._publish_odcs_per_expose(
        contract,
        "sales-product",
        validate_generated_contracts=True,
        validation_mode="warn",
    )

    assert len(request_calls) == 1
    assert results[0]["success"] is True
    assert results[0]["valid"] is False
    assert "validation_error" in results[0]
    assert "schema_objects" in results[0]
    assert "schema_properties" in results[0]


# =====================================================================
# Catalog adapter: FLUID dict pass-through (bug 6 — post-redesign)
# =====================================================================


class TestCatalogFluidPassthrough:
    """The catalog-provider publish path threads the full FLUID dict through
    so multi-port contracts emit one ODCS per output port.

    Regression test for the issue surfaced by running ``task publish:pre``
    against the local snowflake-biz-lab DMM stack — the redesign kills the
    flat CatalogAsset intermediate (``_asset_to_fluid``) that used to
    collapse multi-port contracts into a single wrapper.
    """

    def test_dmm_catalog_provider_forwards_full_fluid(self):
        """The new ``publish(fluid, target)`` signature hands the underlying
        DMM provider the raw FLUID dict — every output port is preserved.
        """
        import asyncio

        from fluid_build.providers.catalogs import CatalogTarget
        from fluid_build.providers.catalogs.datamesh_manager import (
            DataMeshManagerCatalogProvider,
        )

        provider = DataMeshManagerCatalogProvider(
            {"endpoint": "http://localhost", "auth": {"api_key": "k"}}
        )
        captured: dict = {}

        def fake_apply(fluid, **kwargs):
            captured["fluid"] = fluid
            captured["kwargs"] = kwargs
            return {"id": fluid.get("id"), "url": "http://x"}

        provider._provider.apply = fake_apply  # type: ignore[assignment]

        full = {
            "id": "bronze.test",
            "name": "Test",
            "kind": "DataProduct",
            "exposes": [
                {"id": "port_a", "binding": {"platform": "snowflake"}},
                {"id": "port_b", "binding": {"platform": "snowflake"}},
            ],
        }
        target = CatalogTarget.from_fluid(full)

        loop = asyncio.new_event_loop()
        try:
            result = loop.run_until_complete(provider.publish(full, target))
        finally:
            loop.close()
        assert result.success
        assert result.asset_id == "bronze.test"
        # The underlying provider must have seen the full multi-port FLUID.
        assert len(captured["fluid"]["exposes"]) == 2
        assert captured["kwargs"].get("provider_hint") == "odps"
        assert captured["kwargs"].get("publish_contract") is True

    def test_dmm_catalog_provider_respects_provider_hint_override(self):
        """``catalog_hints['provider_hint']`` overrides the odps default."""
        import asyncio

        from fluid_build.providers.catalogs import CatalogTarget
        from fluid_build.providers.catalogs.datamesh_manager import (
            DataMeshManagerCatalogProvider,
        )

        provider = DataMeshManagerCatalogProvider(
            {"endpoint": "http://localhost", "auth": {"api_key": "k"}}
        )
        captured: dict = {}

        def fake_apply(fluid, **kwargs):
            captured["kwargs"] = kwargs
            return {"id": fluid.get("id")}

        provider._provider.apply = fake_apply  # type: ignore[assignment]

        full = {"id": "x", "name": "X", "exposes": []}
        target = CatalogTarget.from_fluid(
            full, catalog_hints={"provider_hint": "dps"}
        )

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(provider.publish(full, target))
        finally:
            loop.close()
        assert captured["kwargs"].get("provider_hint") == "dps"

    def test_deprecated_map_contract_to_asset_still_works(self):
        """The CatalogAsset builder stays available for one release with a
        DeprecationWarning — external callers that build a CatalogAsset by
        hand still round-trip through the ``publish_asset`` shim.
        """
        import warnings

        from fluid_build.providers.catalogs import BaseCatalogProvider

        class _Stub(BaseCatalogProvider):
            async def publish(self, fluid, target): ...
            async def update(self, fluid, target): ...
            async def verify(self, contract_id): ...
            async def health_check(self): ...

        provider = _Stub({})
        contract = {
            "id": "bronze.test",
            "name": "Test",
            "kind": "DataProduct",
            "exposes": [
                {"id": "port_a", "binding": {"platform": "snowflake"}},
                {"id": "port_b", "binding": {"platform": "snowflake"}},
            ],
        }
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            asset = provider.map_contract_to_asset(contract)
        assert any(issubclass(w.category, DeprecationWarning) for w in caught)
        assert asset.raw_contract is not None
        assert len(asset.raw_contract["exposes"]) == 2
