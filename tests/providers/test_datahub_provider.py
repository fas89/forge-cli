from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fluid_build.cli import datahub as datahub_cli
from fluid_build.providers.catalogs.datahub import DataHubCatalogProvider
from fluid_build.providers.datahub import DataHubProvider


def _sample_contract():
    return {
        "id": "commerce.sales",
        "name": "Commerce Sales",
        "description": "Sales mart",
        "domain": "commerce",
        "version": "1.2.0",
        "metadata": {"layer": "Gold", "status": "active", "owner": {"team": "team-alpha"}},
        "owner": {"team": "team-alpha", "email": "alpha@example.com"},
        "expects": [
            {
                "id": "orders",
                "provider": "bigquery",
                "location": {"project": "raw", "dataset": "sales", "table": "orders"},
            }
        ],
        "exposes": [
            {
                "id": "customer_rollup",
                "binding": {
                    "provider": "gcp",
                    "location": {
                        "project": "analytics",
                        "dataset": "gold",
                        "table": "customer_rollup",
                    },
                },
                "contract": {"schema": [{"name": "customer_id", "type": "string"}]},
            },
            {
                "id": "daily_metrics",
                "binding": {
                    "provider": "snowflake",
                    "location": {
                        "account": "acct",
                        "database": "MART",
                        "schema": "PUBLIC",
                        "table": "DAILY_METRICS",
                    },
                },
            },
            {"id": "unresolved_stream", "binding": {"provider": "kafka", "location": {"topic": "sales"}}},
        ],
        "members": [
            {
                "id": "daily_metrics",
                "external_ref": "urn:li:dataset:(urn:li:dataPlatform:snowflake,override.MART.PUBLIC.DAILY_METRICS,PROD)",
                "metadata": {"port_id": "daily_metrics"},
            }
        ],
    }


def test_datahub_apply_dry_run_uses_exposes_for_membership_and_contract_metadata():
    provider = DataHubProvider(
        server_url="https://datahub.example.com",
        token="secret",
        domain_urn_map={"commerce": "urn:li:domain:commerce"},
        owner_urn_map={"team-alpha": "urn:li:corpGroup:team-alpha"},
    )

    result = provider.apply(_sample_contract(), dry_run=True)

    assert result["product_urn"] == "urn:li:dataProduct:commerce-sales"
    assert result["resource_urns"] == [
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,analytics.gold.customer_rollup,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,override.MART.PUBLIC.DAILY_METRICS,PROD)",
    ]
    assert "unresolved_stream" in result["warnings"][0]
    payload = result["payload"]
    assert payload["domainUrn"] == "urn:li:domain:commerce"
    assert payload["id"] == "commerce-sales"
    assert payload["properties"]["name"] == "Commerce Sales"
    assert payload["properties"]["description"] == "Sales mart"
    # Ownership, tags, customProperties are set via separate calls
    assert "ownerUrn" not in payload
    assert "tags" not in payload
    assert "customProperties" not in payload
    # Custom properties are returned separately for dry-run inspection
    assert result["custom_properties"]["fluid_contract_id"] == "commerce.sales"
    assert "commerce.sales.customer_rollup" in result["custom_properties"]["fluid_contract_refs"]


def test_datahub_apply_create_then_sets_membership(monkeypatch):
    provider = DataHubProvider(server_url="https://datahub.example.com", token="secret")
    calls = []

    def fake_graphql(query, variables=None):
        calls.append((query, variables))
        if "query GetDataProduct" in query:
            return {"dataProduct": None}
        if "mutation CreateDataProduct" in query:
            return {"createDataProduct": "ok"}
        if "mutation BatchSetDataProduct" in query:
            return {"batchSetDataProduct": True}
        if "mutation AddOwner" in query:
            return {"addOwner": True}
        if "mutation BatchAddTags" in query:
            return {"batchAddTags": True}
        raise AssertionError(query)

    def fake_request_json(method, path, **kwargs):
        calls.append(("REST", method, path))
        return {}

    monkeypatch.setattr(provider, "_graphql", fake_graphql)
    monkeypatch.setattr(provider, "_request_json", fake_request_json)

    result = provider.apply(_sample_contract())

    assert result["success"] is True
    assert result["operation"] == "create"
    assert any("CreateDataProduct" in query for query, _ in calls if isinstance(query, str))
    membership_call = next(
        variables for query, variables in calls
        if isinstance(query, str) and "BatchSetDataProduct" in query
    )
    assert membership_call["resourceUrns"][0].startswith("urn:li:dataset:")
    # Verify REST call for custom properties
    assert any(
        entry[0] == "REST" and entry[2] == "/entities"
        for entry in calls
        if isinstance(entry, tuple) and len(entry) == 3
    )


def test_datahub_catalog_adapter_uses_source_contract(monkeypatch):
    contract = _sample_contract()
    provider = DataHubCatalogProvider(
        {"endpoint": "https://datahub.example.com", "auth": {"token": "secret"}}
    )
    product = provider.map_contract_to_product(contract)
    product.source_contract = contract
    apply_mock = MagicMock(return_value={"product_urn": "urn:li:dataProduct:commerce-sales"})
    provider._provider.apply = apply_mock

    result = asyncio.run(provider.publish(product))

    assert result.success is True
    assert apply_mock.call_args[0][0] == contract


@patch.object(datahub_cli, "load_contract_with_overlay", return_value=_sample_contract())
@patch.object(datahub_cli, "DataHubProvider")
def test_datahub_cli_publish_delegates_to_native_provider(mock_provider_cls, mock_loader):
    provider = MagicMock()
    provider.apply.return_value = {"product_urn": "urn:li:dataProduct:commerce-sales"}
    mock_provider_cls.return_value = provider
    args = SimpleNamespace(
        contract="contract.yaml",
        overlay=None,
        server_url="https://datahub.example.com",
        token="secret",
        environment="PROD",
        dry_run=False,
    )

    code = datahub_cli._cmd_publish(args)

    assert code == 0
    mock_loader.assert_called_once()
    provider.apply.assert_called_once()
