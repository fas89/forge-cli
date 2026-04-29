from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fluid_build.cli import atlan as atlan_cli
from fluid_build.providers.atlan import AtlanProvider
from fluid_build.providers.catalogs.atlan import AtlanCatalogProvider


def _sample_contract():
    return {
        "id": "marketing.attribution",
        "name": "Marketing Attribution",
        "description": "Attribution metrics",
        "domain": "marketing",
        "version": "3.0.0",
        "metadata": {"layer": "Gold", "status": "active", "owner": {"team": "growth-team"}},
        "owner": {"team": "growth-team", "email": "growth@example.com"},
        "consumes": [
            {
                "id": "events",
                "provider": "snowflake",
                "location": {"account": "acct", "database": "RAW", "schema": "WEB", "table": "EVENTS"},
            }
        ],
        "exposes": [
            {
                "id": "touchpoints",
                "binding": {
                    "provider": "gcp",
                    "location": {"project": "mkt", "dataset": "gold", "table": "touchpoints"},
                },
            },
            {
                "id": "channel_roi",
                "binding": {
                    "provider": "redshift",
                    "location": {
                        "cluster": "cluster-1",
                        "database": "MART",
                        "schema": "PUBLIC",
                        "table": "CHANNEL_ROI",
                    },
                },
            },
        ],
        "members": [
            {
                "id": "channel_roi",
                "external_ref": "custom/redshift/channel_roi",
                "metadata": {"port_id": "channel_roi"},
            }
        ],
    }


def test_atlan_apply_dry_run_builds_exact_asset_selection_from_exposes():
    provider = AtlanProvider(
        base_url="https://tenant.atlan.com",
        api_key="secret",
        default_domain_prefix="default/domain",
    )

    result = provider.apply(_sample_contract(), dry_run=True)

    assert result["qualified_name"] == "default/product/marketing-attribution"
    assert result["domain_qualified_name"] == "default/domain/marketing"
    assert result["asset_qualified_names"] == [
        "bigquery/mkt/gold/touchpoints",
        "custom/redshift/channel_roi",
    ]
    payload = result["payload"]["entities"][0]
    # Asset selection now uses dataProductAssetsDSL (ES DSL query string)
    import json as _json
    dsl = _json.loads(payload["attributes"]["dataProductAssetsDSL"])
    assert dsl["query"]["bool"]["filter"][0]["terms"]["qualifiedName"] == result["asset_qualified_names"]
    # Domain linkage uses parentDomainQualifiedName + relationshipAttributes
    assert payload["attributes"]["parentDomainQualifiedName"] == "default/domain/marketing"
    assert payload["relationshipAttributes"]["dataDomain"]["typeName"] == "DataDomain"
    assert payload["relationshipAttributes"]["dataDomain"]["uniqueAttributes"]["qualifiedName"] == "default/domain/marketing"
    assert payload["businessAttributes"]["FLUID"]["fluidContractId"] == "marketing.attribution"


def test_atlan_apply_updates_when_product_exists(monkeypatch):
    provider = AtlanProvider(base_url="https://tenant.atlan.com", api_key="secret")
    monkeypatch.setattr(provider, "_fetch_product", lambda qualified_name: {"guid": "123", "qualifiedName": qualified_name})
    request_mock = MagicMock(return_value={"mutatedEntities": {"UPDATE": [{"guid": "123"}]}})
    monkeypatch.setattr(provider, "_request_json", request_mock)

    result = provider.apply(_sample_contract())

    assert result["success"] is True
    assert result["operation"] == "update"
    request_args = request_mock.call_args
    assert request_args[0] == ("POST", "/api/meta/entity/bulk")
    assert request_args[1]["json_body"]["entities"][0]["attributes"]["qualifiedName"] == "default/product/marketing-attribution"


def test_atlan_catalog_adapter_uses_source_contract(monkeypatch):
    contract = _sample_contract()
    provider = AtlanCatalogProvider({"base_url": "https://tenant.atlan.com", "api_key": "secret"})
    product = provider.map_contract_to_product(contract)
    product.source_contract = contract
    apply_mock = MagicMock(return_value={"qualified_name": "default/product/marketing-attribution"})
    provider._provider.apply = apply_mock

    result = asyncio.run(provider.publish(product))

    assert result.success is True
    assert apply_mock.call_args[0][0] == contract


@patch.object(atlan_cli, "load_contract_with_overlay", return_value=_sample_contract())
@patch.object(atlan_cli, "AtlanProvider")
def test_atlan_cli_publish_delegates_to_native_provider(mock_provider_cls, mock_loader):
    provider = MagicMock()
    provider.apply.return_value = {"qualified_name": "default/product/marketing-attribution"}
    mock_provider_cls.return_value = provider
    args = SimpleNamespace(
        contract="contract.yaml",
        overlay=None,
        base_url="https://tenant.atlan.com",
        api_key="secret",
        dry_run=False,
    )

    code = atlan_cli._cmd_publish(args)

    assert code == 0
    mock_loader.assert_called_once()
    provider.apply.assert_called_once()
