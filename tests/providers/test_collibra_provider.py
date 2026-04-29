from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fluid_build.cli import collibra as collibra_cli
from fluid_build.providers.catalogs.collibra import CollibraCatalogProvider
from fluid_build.providers.collibra import CollibraProvider


def _sample_contract():
    return {
        "id": "risk.scoring",
        "name": "Risk Scoring",
        "description": "Risk score product",
        "domain": "risk",
        "version": "1.0.1",
        "metadata": {"layer": "Gold", "status": "active", "owner": {"team": "risk-team"}},
        "owner": {"team": "risk-team", "email": "risk@example.com"},
        "expects": [
            {
                "id": "applicants",
                "provider": "bigquery",
                "location": {"project": "raw", "dataset": "credit", "table": "applicants"},
            }
        ],
        "exposes": [
            {
                "id": "risk_scores",
                "binding": {
                    "provider": "snowflake",
                    "location": {
                        "account": "acct",
                        "database": "MART",
                        "schema": "RISK",
                        "table": "RISK_SCORES",
                    },
                },
                "contract": {"schema": [{"name": "application_id", "type": "string"}]},
            }
        ],
    }


def test_collibra_dry_run_builds_product_port_and_contract_graph():
    provider = CollibraProvider(
        base_url="https://collibra.example.com",
        username="user",
        password="pass",
        default_catalog_domain="Risk Catalog",
    )

    result = provider.apply(_sample_contract(), dry_run=True)

    assert result["asset_id"] == "risk.scoring"
    payload = result["payload"]
    asset_types = [asset["type"]["name"] for asset in payload["assets"]]
    assert asset_types.count("Data Product") == 1
    assert asset_types.count("Data Product Port") == 2
    assert asset_types.count("Data Contract") == 1
    relation_types = [relation["type"]["name"] for relation in payload["relations"]]
    assert "exposes data as" in relation_types
    assert "consumes data through" in relation_types
    assert "governs" in relation_types
    # Attributes use Collibra Import API v2 format: {key: [{value: "..."}]}
    product_asset = next(a for a in payload["assets"] if a["type"]["name"] == "Data Product")
    assert product_asset["attributes"]["description"] == [{"value": "Risk score product"}]


def test_collibra_apply_submits_import_job(monkeypatch):
    provider = CollibraProvider(
        base_url="https://collibra.example.com",
        username="user",
        password="pass",
        default_catalog_domain="Risk Catalog",
    )
    monkeypatch.setattr(provider, "_submit_import_job", lambda payload: {"jobId": "job-123"})
    monkeypatch.setattr(provider, "_wait_for_job", lambda job_id: {"jobId": job_id, "status": "COMPLETED"})

    result = provider.apply(_sample_contract())

    assert result["success"] is True
    assert result["job"]["jobId"] == "job-123"


def test_collibra_catalog_adapter_uses_source_contract(monkeypatch):
    contract = _sample_contract()
    provider = CollibraCatalogProvider(
        {
            "base_url": "https://collibra.example.com",
            "username": "user",
            "password": "pass",
            "default_catalog_domain": "Risk Catalog",
        }
    )
    product = provider.map_contract_to_product(contract)
    product.source_contract = contract
    apply_mock = MagicMock(return_value={"success": True, "job": {"jobId": "job-123"}})
    provider._provider.apply = apply_mock

    result = asyncio.run(provider.publish(product))

    assert result.success is True
    assert apply_mock.call_args[0][0] == contract


@patch.object(collibra_cli, "load_contract_with_overlay", return_value=_sample_contract())
@patch.object(collibra_cli, "CollibraProvider")
def test_collibra_cli_publish_delegates_to_native_provider(mock_provider_cls, mock_loader):
    provider = MagicMock()
    provider.apply.return_value = {"success": True}
    mock_provider_cls.return_value = provider
    args = SimpleNamespace(
        contract="contract.yaml",
        overlay=None,
        base_url="https://collibra.example.com",
        username="user",
        password="pass",
        dry_run=False,
    )

    code = collibra_cli._cmd_publish(args)

    assert code == 0
    mock_loader.assert_called_once()
    provider.apply.assert_called_once()
