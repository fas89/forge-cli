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

"""
Entropy Data / Data Mesh Manager catalog adapter.

Wraps :class:`DataMeshManagerProvider` behind the
:class:`BaseCatalogProvider` interface so that
``fluid publish --catalog datamesh-manager`` works.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from .base import BaseCatalogProvider, CatalogProduct, PublishResult

LOG = logging.getLogger(__name__)


class DataMeshManagerCatalogProvider(BaseCatalogProvider):
    """Catalog adapter for Entropy Data / Data Mesh Manager."""

    name = "datamesh-manager"

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        # Lazy-import to avoid hard dependency at load time
        from fluid_build.providers.datamesh_manager import DataMeshManagerProvider

        api_url = config.get("endpoint") or config.get("url", "")
        self._provider = DataMeshManagerProvider(
            api_url=api_url or None,
        )

    # -- BaseCatalogProvider interface --------------------------------------

    async def publish(self, product: CatalogProduct) -> PublishResult:
        """Publish *product* as a data product to Entropy Data."""
        fluid = product.source_contract or self._product_to_fluid(product)
        try:
            result = self._provider.apply(fluid, publish_contract=True)
            return PublishResult(
                success=True,
                catalog_id=self.name,
                asset_id=product.id,
                catalog_url=result.get("url"),
                details=result,
            )
        except Exception as exc:
            return PublishResult(
                success=False,
                catalog_id=self.name,
                asset_id=product.id,
                error=str(exc),
            )

    async def update(self, product: CatalogProduct) -> PublishResult:
        # PUT is idempotent — publish == update
        return await self.publish(product)

    async def verify(self, asset_id: str) -> bool:
        try:
            self._provider.verify(asset_id)
            return True
        except Exception:
            return False

    async def delete(self, asset_id: str) -> bool:
        try:
            return self._provider.delete(asset_id)
        except Exception:
            return False

    async def health_check(self) -> bool:
        try:
            self._provider.list_products()
            return True
        except Exception:
            return False

    # -- helpers ------------------------------------------------------------

    @staticmethod
    def _product_to_fluid(product: CatalogProduct) -> Dict[str, Any]:
        """Fallback reconstruction when callers did not preserve source_contract."""
        fluid: Dict[str, Any] = {
            "id": product.id,
            "name": product.name,
            "description": product.description,
            "kind": product.kind,
            "domain": product.domain,
            "metadata": {
                "name": product.name,
                "description": product.description,
                "domain": product.domain,
                "version": product.version,
                "tags": product.tags,
                "layer": product.layer,
                "status": product.status or "active",
            },
            "owner": {
                "team": product.owner.team,
                "email": product.owner.email,
                "name": product.owner.name,
            },
        }

        if product.input_ports:
            fluid["expects"] = []
            for port in product.input_ports:
                expect: Dict[str, Any] = {
                    "id": port.id,
                    "name": port.name,
                    "description": port.description,
                    "provider": port.platform,
                }
                if port.location is not None:
                    expect["location"] = port.location
                if port.source_system_id:
                    expect["sourceSystem"] = port.source_system_id
                fluid["expects"].append(expect)

        if product.output_ports:
            fluid["exposes"] = []
            for port in product.output_ports:
                expose: Dict[str, Any] = {
                    "id": port.id,
                    "exposeId": port.id,
                    "name": port.name,
                    "description": port.description,
                    "kind": port.kind,
                    "provider": port.platform,
                }
                binding: Dict[str, Any] = {}
                if port.platform and port.platform != "unknown":
                    binding["platform"] = port.platform
                if port.location is not None:
                    binding["location"] = port.location
                if isinstance(port.custom, dict) and port.custom.get("format") is not None:
                    binding["format"] = port.custom["format"]
                if binding:
                    expose["binding"] = binding
                if port.schema:
                    expose["contract"] = {"schema": port.schema}
                fluid["exposes"].append(expose)

        return fluid
