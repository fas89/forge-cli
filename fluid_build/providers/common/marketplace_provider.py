"""Shared base for native marketplace publish providers."""

from __future__ import annotations

import logging
from abc import abstractmethod
from typing import Any, Dict, Iterable, List, Optional

from fluid_build.providers.base import BaseProvider, ProviderError
from fluid_build.providers.catalogs.base import CatalogProduct
from fluid_build.providers.common.auth import get_auth_headers
from fluid_build.providers.common.catalog_publish import map_contract_to_product

try:
    import requests

    REQUESTS_AVAILABLE = True
except ImportError:  # pragma: no cover
    requests = None  # type: ignore[assignment]
    REQUESTS_AVAILABLE = False


class BaseMarketplaceProvider(BaseProvider):
    """Common provider workflow for catalog-style marketplace publishing."""

    name = "marketplace"

    def __init__(
        self,
        *,
        endpoint: str,
        auth: Optional[Dict[str, Any]] = None,
        logger: Optional[logging.Logger] = None,
        timeout: float = 30.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(logger=logger, **kwargs)
        if not REQUESTS_AVAILABLE:
            raise ProviderError("The 'requests' library is required for marketplace providers.")
        self.endpoint = endpoint.rstrip("/")
        self.auth = dict(auth or {})
        self.timeout = timeout
        self._log = logger or logging.getLogger(self.__class__.__module__)
        self._session_instance = None

    def capabilities(self) -> Dict[str, bool]:
        return {
            "plan": True,
            "apply": True,
            "verify": True,
            "get": True,
            "list": True,
        }

    def plan(
        self, contract: Any, out: Any = None, fmt: str = "yaml", **kw: Any
    ) -> List[Dict[str, Any]]:
        items = contract if isinstance(contract, list) else [contract]
        previews: List[Dict[str, Any]] = []
        for item in items:
            product = self._ensure_product(item)
            preview = self._publish_product(product, dry_run=True, **kw)
            previews.append(preview)
        return previews

    def apply(self, contract: Any, out: Any = None, fmt: str = "yaml", **kw: Any) -> Dict[str, Any]:
        items = contract if isinstance(contract, list) else [contract]
        results: List[Dict[str, Any]] = []
        for item in items:
            product = self._ensure_product(item)
            results.append(self._publish_product(product, **kw))
        if len(results) == 1:
            return results[0]
        return {"published": len(results), "results": results}

    def verify(self, product_id: str) -> Dict[str, Any]:
        return self.get_product(product_id)

    @abstractmethod
    def _publish_product(self, product: CatalogProduct, **kw: Any) -> Dict[str, Any]:
        """Publish one normalized product to the target marketplace."""

    @abstractmethod
    def get_product(self, product_id: str) -> Dict[str, Any]:
        """Return one published product or raise on failure."""

    @abstractmethod
    def list_products(self) -> List[Dict[str, Any]]:
        """Return published products."""

    @abstractmethod
    def health_check(self) -> bool:
        """Check target availability."""

    def _ensure_product(self, value: Any) -> CatalogProduct:
        if isinstance(value, CatalogProduct):
            return value
        if isinstance(value, dict):
            return map_contract_to_product(value)
        raise ProviderError(f"Unsupported publish input for {self.name}: {type(value)!r}")

    def _headers(self) -> Dict[str, str]:
        return get_auth_headers(self.endpoint, self.auth)

    def _session(self):
        if self._session_instance is None:
            self._session_instance = requests.Session()
        return self._session_instance

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Any = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ):
        url = path if path.startswith("http://") or path.startswith("https://") else f"{self.endpoint}{path}"
        try:
            response = self._session().request(
                method.upper(),
                url,
                json=json_body,
                params=params,
                headers=headers or self._headers(),
                timeout=self.timeout,
            )
        except Exception as exc:
            raise ProviderError(f"{self.name}: HTTP request failed: {exc}") from exc

        if response.status_code >= 400:
            body = getattr(response, "text", "")[:1000]
            raise ProviderError(f"{self.name}: HTTP {response.status_code} for {url}: {body}")

        return response

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        json_body: Any = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Any:
        response = self._request(
            method,
            path,
            json_body=json_body,
            params=params,
            headers=headers,
        )
        if not hasattr(response, "json"):
            return {}
        try:
            return response.json()
        except Exception as exc:
            raise ProviderError(f"{self.name}: Failed to decode JSON response: {exc}") from exc
