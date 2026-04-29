"""DataHub catalog adapter."""

from __future__ import annotations

from typing import Any, Dict

from fluid_build.providers.common.catalog_publish import product_to_contract

from .base import BaseCatalogProvider, CatalogProduct, PublishResult


class DataHubCatalogProvider(BaseCatalogProvider):
    """Catalog adapter for DataHub."""

    name = "datahub"

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        from fluid_build.providers.datahub import DataHubProvider

        self._provider = DataHubProvider(
            endpoint=config.get("endpoint") or config.get("server_url") or config.get("url"),
            environment=config.get("environment", "PROD"),
            domain_urn_map=config.get("domain_urn_map"),
            owner_urn_map=config.get("owner_urn_map"),
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
                catalog_url=result.get("product_urn"),
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
