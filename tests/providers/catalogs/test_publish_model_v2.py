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

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

from fluid_build.cli.publish import publish_contract
from fluid_build.providers.catalogs.base import BaseCatalogProvider, PublishResult
from fluid_build.providers.catalogs.datamesh_manager import DataMeshManagerCatalogProvider
from fluid_build.providers.catalogs.fluid_cc import provider as fluid_cc_provider_module
from fluid_build.providers.catalogs.fluid_cc.provider import FluidCommandCenterProvider


class DummyCatalogProvider(BaseCatalogProvider):
    name = "dummy"

    def __init__(self, config):
        super().__init__(config)
        self.captured_product = None

    async def publish(self, product):
        self.captured_product = product
        return PublishResult(success=True, catalog_id=self.name, asset_id=product.id)

    async def update(self, product):
        self.captured_product = product
        return PublishResult(success=True, catalog_id=self.name, asset_id=product.id)

    async def verify(self, asset_id: str) -> bool:
        return asset_id == "example.analytics"

    def validate_product(self, product):
        self.captured_product = product
        return True, None


def _rich_contract():
    return {
        "id": "example.analytics",
        "name": "Example Analytics",
        "kind": "DataProduct",
        "domain": "analytics",
        "description": "Aggregated analytics product",
        "version": "2.0.0",
        "metadata": {
            "layer": "Gold",
            "status": "active",
            "owner": "analytics.team@example.com",
            "tags": ["analytics", "gold"],
            "documentation": "https://docs.example.com/products/example-analytics",
            "custom": {"steward": "governance@example.com"},
        },
        "links": {"catalog": "https://catalog.example.com/example.analytics"},
        "consumes": [
            {
                "id": "orders",
                "productId": "raw.orders",
                "exposeId": "orders_table",
                "description": "Orders dependency",
                "binding": {
                    "provider": "gcp",
                    "location": {
                        "project": "raw-project",
                        "dataset": "sales",
                        "table": "orders",
                    },
                },
            }
        ],
        "expects": [
            {
                "id": "payments",
                "name": "Payments",
                "description": "Payments dependency",
                "provider": "snowflake",
                "location": {"database": "RAW", "schema": "FINANCE", "table": "PAYMENTS"},
            }
        ],
        "exposes": [
            {
                "exposeId": "customer_rollup",
                "title": "Customer Rollup",
                "kind": "table",
                "binding": {
                    "provider": "gcp",
                    "format": "bigquery_table",
                    "location": {
                        "project": "analytics-project",
                        "dataset": "gold",
                        "table": "customer_rollup",
                    },
                },
                "contract": {
                    "schema": [
                        {"name": "customer_id", "type": "string", "required": True},
                        {"name": "email", "type": "string", "classification": "pii"},
                    ]
                },
                "links": {"schema": "https://docs.example.com/schema/customer-rollup"},
            },
            {
                "id": "daily_metrics",
                "name": "Daily Metrics",
                "kind": "table",
                "binding": {
                    "platform": "snowflake",
                    "format": "table",
                    "location": {
                        "account": "analytics",
                        "database": "MART",
                        "schema": "METRICS",
                        "table": "DAILY_METRICS",
                    },
                },
                "schema": {
                    "type": "object",
                    "required": ["metric_date"],
                    "properties": {
                        "metric_date": {"type": "string", "format": "date"},
                        "orders_total": {"type": "integer"},
                    },
                },
            },
        ],
    }


def test_map_contract_to_product_preserves_multi_port_graph():
    provider = DummyCatalogProvider({})

    product = provider.map_contract_to_product(_rich_contract())

    assert product.id == "example.analytics"
    assert product.owner.email == "analytics.team@example.com"
    assert product.owner.team == "analytics-team"
    assert product.status == "active"
    assert product.platform == "gcp"
    assert len(product.input_ports) == 2
    assert len(product.output_ports) == 2
    assert [port.id for port in product.output_ports] == ["customer_rollup", "daily_metrics"]
    assert [ref.id for ref in product.contracts] == [
        "example.analytics.customer_rollup",
        "example.analytics.daily_metrics",
    ]
    assert product.output_ports[0].contains_pii is True
    assert product.output_ports[1].platform == "snowflake"
    assert product.output_ports[1].schema[0]["name"] == "metric_date"
    assert product.links["catalog"] == "https://catalog.example.com/example.analytics"
    assert product.links["documentation"] == "https://docs.example.com/products/example-analytics"


def test_publish_contract_attaches_source_contract_and_yaml(tmp_path, monkeypatch):
    contract = _rich_contract()
    contract_path = tmp_path / "contract.fluid.yaml"
    contract_path.write_text("id: example.analytics\nname: Example Analytics\n", encoding="utf-8")

    provider = DummyCatalogProvider({})
    config = MagicMock()
    config.get_catalog_config.return_value = {"enabled": True}

    monkeypatch.setattr("fluid_build.cli.publish.load_contract", lambda _: contract)
    monkeypatch.setattr("fluid_build.cli.publish.get_catalog_provider", lambda *_: provider)

    result = asyncio.run(
        publish_contract(
            contract_path=contract_path,
            catalog_name="dummy",
            config=config,
            dry_run=True,
        )
    )

    assert result.success is True
    assert provider.captured_product.source_contract == contract
    assert provider.captured_product.contract_yaml == contract_path.read_text(encoding="utf-8")
    assert len(provider.captured_product.output_ports) == 2


def test_datamesh_manager_catalog_provider_uses_source_contract(monkeypatch):
    contract = _rich_contract()
    provider = DataMeshManagerCatalogProvider(
        {"endpoint": "https://api.entropy-data.com", "auth": {"api_key": "test-key"}}
    )
    product = provider.map_contract_to_product(contract)
    product.source_contract = contract

    apply_mock = MagicMock(return_value={"url": "https://api.entropy-data.com/dataproducts/example"})
    provider._provider.apply = apply_mock

    result = asyncio.run(provider.publish(product))

    assert result.success is True
    apply_args, apply_kwargs = apply_mock.call_args
    assert apply_args[0] == contract
    assert len(apply_args[0]["exposes"]) == 2
    assert apply_kwargs["publish_contract"] is True


def test_fluid_command_center_publish_impl_preserves_richer_metadata(monkeypatch):
    contract = _rich_contract()
    provider = FluidCommandCenterProvider(
        {
            "endpoint": "https://catalog.example.com",
            "auth": {"type": "api_key", "api_key": "secret"},
        }
    )
    product = provider.map_contract_to_product(contract)
    product.contract_yaml = "id: example.analytics\n"

    captured_request = {}

    class FakeResponse:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {"id": "asset-123"}

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            captured_request["url"] = url
            captured_request["json"] = json
            captured_request["headers"] = headers
            return FakeResponse()

    async def _missing_existing(*args, **kwargs):
        return None

    monkeypatch.setattr(provider, "_find_by_contract_id", _missing_existing)
    monkeypatch.setattr(fluid_cc_provider_module.httpx, "AsyncClient", FakeAsyncClient)

    result = asyncio.run(provider._publish_impl(product))

    assert result.success is True
    assert captured_request["json"]["type"] == "dataproduct"
    metadata = captured_request["json"]["metadata"]
    assert len(metadata["input_ports"]) == 2
    assert len(metadata["output_ports"]) == 2
    assert len(metadata["contracts"]) == 2
    assert metadata["owner_team"] == "analytics-team"
    assert metadata["links"]["catalog"] == "https://catalog.example.com/example.analytics"
    assert metadata["custom"]["steward"] == "governance@example.com"
