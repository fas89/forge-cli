"""Atlan catalog adapter."""

from __future__ import annotations

from typing import Any, Dict

from fluid_build.providers.common.catalog_publish import product_to_contract

from .base import BaseCatalogProvider, CatalogProduct, PublishResult


class AtlanCatalogProvider(BaseCatalogProvider):
    """Catalog adapter for Atlan."""

    name = "atlan"

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        from fluid_build.providers.atlan import AtlanProvider

        self._provider = AtlanProvider(
            endpoint=config.get("endpoint") or config.get("base_url") or config.get("url"),
            domain_qualified_name_map=config.get("domain_qualified_name_map"),
            default_domain_prefix=config.get("default_domain_prefix"),
            product_qualified_name_prefix=config.get("product_qualified_name_prefix", "default/product"),
            owner_map=config.get("owner_map"),
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
                catalog_url=result.get("qualified_name"),
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
