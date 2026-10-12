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
from typing import Any, Dict, Mapping, Optional

from .base import (
    BaseCatalogProvider,
    CatalogAsset,
    CatalogTarget,
    PublishResult,
    _coerce_contract_id,
    _fluid_from_asset,
    _fluid_identity_error,
)

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

    async def publish(
        self,
        fluid: Any = None,
        target: Optional[CatalogTarget] = None,
        *,
        asset: Optional[CatalogAsset] = None,
    ) -> PublishResult:
        """Publish *fluid* as a data product to Entropy Data.

        The FLUID dict is forwarded to the underlying provider (a copy with
        the 0.7.8 catalog-path defaults applied, see
        :func:`_catalog_compat_fluid`) so every output port is preserved —
        one per-port ODCS contract is emitted instead of the single
        redundant wrapper the old flat-asset path used to build.

        Passing a deprecated :class:`CatalogAsset` (the 0.7.8
        ``publish(asset)`` / ``publish(asset=asset)`` API) still works for
        one release and emits a :class:`DeprecationWarning`; the asset's own
        fields are published. A subclass's ``map_contract_to_asset``
        override is honoured the same way.
        """
        fluid, target = self._coerce_publish_args(fluid, target, "publish", asset=asset)
        try:
            target = self._with_legacy_asset(fluid, target)
            legacy_asset = target._legacy_asset
            if legacy_asset is not None:
                payload_fluid = self._asset_to_fluid(legacy_asset)
            else:
                identity_error = _fluid_identity_error(fluid)
                if identity_error:
                    return PublishResult(
                        success=False,
                        catalog_id=self.name,
                        asset_id=target.contract_id,
                        error=f"Validation failed: {identity_error}",
                    )
                payload_fluid = fluid
            result = self._provider.apply(
                _catalog_compat_fluid(payload_fluid),
                publish_contract=True,
                **self._spec_selection(target),
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

    async def update(
        self,
        fluid: Any = None,
        target: Optional[CatalogTarget] = None,
        *,
        asset: Optional[CatalogAsset] = None,
    ) -> PublishResult:
        # PUT is idempotent — publish == update
        fluid, target = self._coerce_publish_args(fluid, target, "update", asset=asset)
        return await self.publish(fluid, target)

    async def verify(
        self, contract_id: Optional[str] = None, *, asset_id: Optional[str] = None
    ) -> bool:
        contract_id = _coerce_contract_id(contract_id, asset_id, "verify")
        try:
            self._provider.verify(contract_id)
            return True
        except Exception:
            return False

    async def delete(
        self, contract_id: Optional[str] = None, *, asset_id: Optional[str] = None
    ) -> bool:
        contract_id = _coerce_contract_id(contract_id, asset_id, "delete")
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

    # -- helpers ------------------------------------------------------------

    @staticmethod
    def _asset_to_fluid(asset: CatalogAsset) -> Dict[str, Any]:
        """Convert a deprecated CatalogAsset to the FLUID dict to publish.

        An asset produced by ``map_contract_to_asset`` (it carries the
        original ``raw_contract``) keeps every output port of that contract,
        with the asset's own fields applied on top. A hand-built asset is
        converted exactly as 0.7.8 did.
        """
        if asset.raw_contract:
            return _fluid_from_asset(asset)

        fluid: Dict[str, Any] = {
            "id": asset.id,
            "name": asset.name,
            "description": asset.description,
            "metadata": {
                "name": asset.name,
                "description": asset.description,
                "domain": asset.domain,
                "version": asset.version,
                "tags": asset.tags,
                "layer": asset.layer,
                "status": "active",
            },
            "owner": {
                "team": asset.owner,
                "email": asset.owner_email,
            },
        }

        # Build a minimal expose from location info
        if asset.location or asset.platform != "unknown":
            expose: Dict[str, Any] = {
                "id": asset.id,
                "provider": asset.platform,
            }
            if asset.location:
                expose["location"] = (
                    asset.location if isinstance(asset.location, str) else str(asset.location)
                )
            if asset.schema:
                expose["schema"] = {"fields": asset.schema}
            fluid["exposes"] = [expose]

        return fluid


def _catalog_compat_fluid(fluid: Mapping[str, Any]) -> Dict[str, Any]:
    """Apply the 0.7.8 ``fluid publish --catalog datamesh-manager`` defaults.

    0.7.8 rebuilt the FLUID dict from the flat CatalogAsset before calling
    the provider, which always sent ``metadata.status: active`` and took
    ``metadata.domain`` / ``metadata.version`` from the top-level FLUID
    fields (defaulting to ``general`` / ``1.0.0``). Forwarding the raw
    contract would instead publish every product as ``draft`` (moving live
    products back to draft on each republish) and drop ``custom.domain`` /
    ``custom.version`` from the DPS payload. A copy is returned; values the
    contract declares in ``metadata`` win.
    """
    out: Dict[str, Any] = dict(fluid)
    raw_meta = fluid.get("metadata")
    meta: Dict[str, Any] = dict(raw_meta) if isinstance(raw_meta, Mapping) else {}
    if not meta.get("status"):
        meta["status"] = "active"
    if not meta.get("domain"):
        meta["domain"] = fluid.get("domain") or "general"
    if not meta.get("version"):
        meta["version"] = fluid.get("version") or "1.0.0"
    out["metadata"] = meta
    return out
