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
from typing import Any, Dict, Optional

from .base import BaseCatalogProvider, CatalogTarget, PublishResult

LOG = logging.getLogger(__name__)


class DataMeshManagerCatalogProvider(BaseCatalogProvider):
    """Catalog adapter for Entropy Data / Data Mesh Manager."""

    name = "datamesh-manager"

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        # Lazy-import to avoid hard dependency at load time
        from fluid_build.providers.datamesh_manager import DataMeshManagerProvider

        api_key = config.get("api_key") or config.get("auth", {}).get("api_key", "")
        api_url = config.get("endpoint") or config.get("url", "")
        self._provider = DataMeshManagerProvider(
            api_key=api_key or None,
            api_url=api_url or None,
        )

    # -- BaseCatalogProvider interface --------------------------------------

    def _spec_selection(self, target: CatalogTarget) -> Dict[str, Optional[str]]:
        """Collect the data-product-spec opt-outs for this publish call.

        Per-call ``target.catalog_hints`` win over the catalog config
        (``catalogs.datamesh-manager.data_product_spec`` / ``provider_hint``).
        When neither is set both values stay ``None`` so
        :meth:`DataMeshManagerProvider._resolve_data_product_specification`
        applies the same order as ``fluid dmm publish``: explicit spec →
        provider hint → ``DMM_DATA_PRODUCT_SPEC`` → ODPS default.
        """
        hints = target.catalog_hints or {}
        spec = (
            hints.get("data_product_specification")
            or hints.get("data_product_spec")
            or self.config.get("data_product_specification")
            or self.config.get("data_product_spec")
        )
        hint = hints.get("provider_hint") or self.config.get("provider_hint")
        return {
            "data_product_specification": str(spec) if spec else None,
            "provider_hint": str(hint) if hint else None,
        }

    async def publish(self, fluid: Any, target: Optional[CatalogTarget] = None) -> PublishResult:
        """Publish *fluid* as a data product to Entropy Data.

        The FLUID dict is forwarded to the underlying provider verbatim so
        every output port is preserved — one per-port ODCS contract is
        emitted instead of the single redundant wrapper the old flat-asset
        path used to build.

        Passing a deprecated :class:`CatalogAsset` as the only argument
        (the pre-0.7.9 ``publish(asset)`` API) still works for one release
        and emits a :class:`DeprecationWarning`.
        """
        fluid, target = self._coerce_publish_args(fluid, target, "publish")
        try:
            result = self._provider.apply(
                fluid, publish_contract=True, **self._spec_selection(target)
            )
            return PublishResult(
                success=True,
                catalog_id=self.name,
                asset_id=target.contract_id,
                catalog_url=result.get("url"),
                details=result,
            )
        except Exception as exc:
            return PublishResult(
                success=False,
                catalog_id=self.name,
                asset_id=target.contract_id,
                error=str(exc),
            )

    async def update(self, fluid: Any, target: Optional[CatalogTarget] = None) -> PublishResult:
        # PUT is idempotent — publish == update
        fluid, target = self._coerce_publish_args(fluid, target, "update")
        return await self.publish(fluid, target)

    async def verify(self, contract_id: str) -> bool:
        try:
            self._provider.verify(contract_id)
            return True
        except Exception:
            return False

    async def delete(self, contract_id: str) -> bool:
        try:
            return self._provider.delete(contract_id)
        except Exception:
            return False

    async def health_check(self) -> bool:
        try:
            self._provider.list_products()
            return True
        except Exception:
            return False
