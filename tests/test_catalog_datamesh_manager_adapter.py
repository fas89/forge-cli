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

"""Tests for the Data Mesh Manager catalog adapter."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

from fluid_build.providers.catalogs.base import CatalogAsset
from fluid_build.providers.catalogs.datamesh_manager import DataMeshManagerCatalogProvider


def _run(coro):
    return asyncio.run(coro)


def _make_asset(*, contract_yaml: str | None = None) -> CatalogAsset:
    return CatalogAsset(
        id="bronze.telco.seed_sources_v1",
        name="Telco Seed Sources",
        description="Bronze telco sources.",
        type="dataproduct",
        domain="telco",
        owner="telco-data-platform",
        owner_email="data-platform@example.com",
        layer="Bronze",
        tags=["demo"],
        version="1.0.0",
        platform="snowflake",
        location={"database": "TELCO_LAB", "schema": "TELCO_STAGE_LOAD", "table": "ACCOUNT"},
        schema=[{"name": "ACCOUNT_ID", "type": "STRING"}],
        contract_yaml=contract_yaml,
    )


class TestDataMeshManagerCatalogProvider:
    @patch("fluid_build.providers.datamesh_manager.DataMeshManagerProvider")
    def test_publish_passes_spec_and_hint_to_provider(self, MockProvider):
        provider_inst = MockProvider.return_value
        provider_inst.apply.return_value = {"url": "https://catalog.example.com/products/bronze"}

        adapter = DataMeshManagerCatalogProvider(
            {
                "endpoint": "https://catalog.example.com",
                "auth": {"type": "api_key", "api_key": "test-key"},
                "data_product_specification": "odps",
                "provider_hint": "odps",
            }
        )

        asset = _make_asset(
            contract_yaml="""
id: bronze.telco.seed_sources_v1
name: Telco Seed Sources
metadata:
  owner:
    team: telco-data-platform
"""
        )
        result = _run(adapter.publish(asset))

        assert result.success is True
        apply_kwargs = provider_inst.apply.call_args.kwargs
        assert apply_kwargs["data_product_specification"] == "odps"
        assert apply_kwargs["provider_hint"] == "odps"
        assert apply_kwargs["publish_contract"] is True

    @patch("fluid_build.providers.datamesh_manager.DataMeshManagerProvider")
    def test_publish_retries_with_odps_when_server_rejects_dps(self, MockProvider):
        provider_inst = MockProvider.return_value
        provider_inst.apply.side_effect = [
            RuntimeError(
                "Entropy Data API error 400 on PUT /api/dataproducts/id: "
                "Data product type 'dps' is not supported in this organization. "
                "Supported types: odps"
            ),
            {"url": "https://catalog.example.com/products/bronze"},
        ]

        adapter = DataMeshManagerCatalogProvider(
            {
                "endpoint": "https://catalog.example.com",
                "auth": {"type": "api_key", "api_key": "test-key"},
            }
        )

        result = _run(adapter.publish(_make_asset()))

        assert result.success is True
        assert provider_inst.apply.call_count == 2
        first_kwargs = provider_inst.apply.call_args_list[0].kwargs
        second_kwargs = provider_inst.apply.call_args_list[1].kwargs
        assert first_kwargs["data_product_specification"] is None
        assert first_kwargs["provider_hint"] is None
        assert second_kwargs["data_product_specification"] == "odps"
        assert second_kwargs["provider_hint"] == "odps"

    @patch("fluid_build.providers.datamesh_manager.DataMeshManagerProvider")
    def test_publish_does_not_retry_when_spec_is_explicit(self, MockProvider):
        provider_inst = MockProvider.return_value
        provider_inst.apply.side_effect = RuntimeError(
            "Data product type 'dps' is not supported in this organization. Supported types: odps"
        )

        adapter = DataMeshManagerCatalogProvider(
            {
                "endpoint": "https://catalog.example.com",
                "auth": {"type": "api_key", "api_key": "test-key"},
                "data_product_specification": "0.0.1",
            }
        )

        result = _run(adapter.publish(_make_asset()))

        assert result.success is False
        assert provider_inst.apply.call_count == 1
        assert "supported types: odps" in result.error.lower()

    def test_asset_to_fluid_prefers_raw_contract_yaml(self):
        asset = _make_asset(
            contract_yaml="""
id: bronze.telco.seed_sources_v1
name: Telco Seed Sources
description: Canonical bronze contract.
metadata:
  owner:
    team: telco-data-platform
consumes:
  - productId: upstream.seed
"""
        )

        fluid = DataMeshManagerCatalogProvider._asset_to_fluid(asset)
        assert fluid["id"] == "bronze.telco.seed_sources_v1"
        assert fluid["consumes"][0]["productId"] == "upstream.seed"

    def test_should_retry_with_odps_only_for_unsupported_dps_errors(self):
        adapter = DataMeshManagerCatalogProvider(
            {
                "endpoint": "https://catalog.example.com",
                "auth": {"type": "api_key", "api_key": "test-key"},
            }
        )

        assert adapter._should_retry_with_odps(
            RuntimeError("Type 'dps' is not supported in this organization. Supported types: odps")
        )
        assert not adapter._should_retry_with_odps(RuntimeError("401 unauthorized"))
