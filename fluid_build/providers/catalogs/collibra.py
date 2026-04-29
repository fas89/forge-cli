"""Collibra catalog adapter."""

from __future__ import annotations

from typing import Any, Dict

from fluid_build.providers.common.catalog_publish import product_to_contract

from .base import BaseCatalogProvider, CatalogProduct, PublishResult


class CollibraCatalogProvider(BaseCatalogProvider):
    """Catalog adapter for Collibra."""

    name = "collibra"

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        from fluid_build.providers.collibra import CollibraProvider

        self._provider = CollibraProvider(
            endpoint=config.get("endpoint") or config.get("base_url") or config.get("url"),
            data_product_catalog_domain_map=config.get("data_product_catalog_domain_map"),
            default_catalog_domain=config.get("default_catalog_domain"),
            asset_type_names=config.get("asset_type_names"),
            relation_type_names=config.get("relation_type_names"),
            timeout=float(config.get("timeout", 30.0)),
        )

    async def publish(self, product: CatalogProduct) -> PublishResult:
        fluid = product.source_contract or product_to_contract(product)
        try:
            result = self._provider.apply(fluid)
            return PublishResult(
                success=True,
                catalog_id=self.name,
                asset_id=product.id,
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
        return await self.publish(product)

    async def verify(self, asset_id: str) -> bool:
        try:
            self._provider.verify(asset_id)
            return True
        except Exception:
            return False

    async def health_check(self) -> bool:
        return self._provider.health_check()
