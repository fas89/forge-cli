"""Native Atlan publish integration."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from fluid_build.credentials import CatalogCredentialAdapter
from fluid_build.providers.base import ProviderError
from fluid_build.providers.catalogs.base import CatalogPort, CatalogProduct
from fluid_build.providers.common.catalog_publish import (
    contract_refs_for_port,
    member_refs_for_port,
    slugify,
)
from fluid_build.providers.common.marketplace_provider import BaseMarketplaceProvider

LOG = logging.getLogger(__name__)


class AtlanProvider(BaseMarketplaceProvider):
    """Publish FLUID contracts as Atlan data products."""

    name = "atlan"
    DEFAULT_ENDPOINT = "https://atlan.example.com"

    def __init__(
        self,
        *,
        endpoint: Optional[str] = None,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        auth: Optional[Dict[str, Any]] = None,
        domain_qualified_name_map: Optional[Dict[str, str]] = None,
        default_domain_prefix: Optional[str] = None,
        product_qualified_name_prefix: str = "default/product",
        owner_map: Optional[Dict[str, str]] = None,
        logger: Optional[logging.Logger] = None,
        timeout: float = 30.0,
        **kwargs: Any,
    ) -> None:
        auth_config = dict(auth or {})
        self._credential_adapter = CatalogCredentialAdapter("atlan")
        endpoint_value = (
            endpoint
            or base_url
            or os.getenv("ATLAN_ENDPOINT")
            or os.getenv("ATLAN_BASE_URL")
            or self.DEFAULT_ENDPOINT
        )
        api_key_value = self._credential_adapter.get_credential(
            "api_key",
            required=False,
            cli_value=api_key or auth_config.get("api_key") or auth_config.get("token"),
        )
        if api_key_value:
            auth_config = {"type": "bearer", "token": api_key_value}
        super().__init__(
            endpoint=endpoint_value,
            auth=auth_config,
            logger=logger,
            timeout=timeout,
            **kwargs,
        )
        self.domain_qualified_name_map = dict(
            domain_qualified_name_map or kwargs.get("domain_qualified_name_map") or {}
        )
        self.default_domain_prefix = default_domain_prefix or kwargs.get("default_domain_prefix") or ""
        self.product_qualified_name_prefix = (
            product_qualified_name_prefix or kwargs.get("product_qualified_name_prefix") or "default/product"
        ).strip("/")
        self.owner_map = dict(owner_map or kwargs.get("owner_map") or {})
        self.api_key = api_key_value or ""

    def _headers(self) -> Dict[str, str]:
        self._ensure_api_key()
        headers = super()._headers()
        if self.api_key:
            headers.setdefault("Authorization", f"Bearer {self.api_key}")
        return headers

    def _publish_product(self, product: CatalogProduct, **kw: Any) -> Dict[str, Any]:
        dry_run = bool(kw.get("dry_run", False))
        qualified_name = self._build_product_qualified_name(product.id)
        domain_qn = self._resolve_domain_qualified_name(product.domain)
        asset_qns, warnings = self._resolve_output_assets(product)
        entity = self._to_atlan_product(product, qualified_name, domain_qn, asset_qns)

        if dry_run:
            return {
                "dry_run": True,
                "qualified_name": qualified_name,
                "domain_qualified_name": domain_qn,
                "asset_qualified_names": asset_qns,
                "warnings": warnings,
                "payload": {"entities": [entity]},
            }

        existing = self._fetch_product(qualified_name)
        response = self._request_json(
            "POST",
            "/api/meta/entity/bulk",
            json_body={"entities": [entity]},
        )
        return {
            "success": True,
            "operation": "update" if existing else "create",
            "qualified_name": qualified_name,
            "domain_qualified_name": domain_qn,
            "asset_qualified_names": asset_qns,
            "warnings": warnings,
            "response": response,
        }

    def get_product(self, product_id: str) -> Dict[str, Any]:
        qualified_name = (
            product_id if "/" in str(product_id) else self._build_product_qualified_name(product_id)
        )
        product = self._fetch_product(qualified_name)
        if not product:
            raise ProviderError(f"Atlan data product not found: {product_id}")
        return product

    def list_products(self) -> List[Dict[str, Any]]:
        data = self._request_json(
            "POST",
            "/api/meta/search/indexsearch",
            json_body={
                "dsl": {
                    "from": 0,
                    "size": 100,
                    "query": {"term": {"typeName.keyword": "DataProduct"}},
                }
            },
        )
        return data.get("entities", data.get("items", []))

    def health_check(self) -> bool:
        try:
            self._request("GET", "/api/meta/types/typedef/name/DataProduct")
            return True
        except Exception as exc:
            self._log.warning("Atlan health check failed: %s", exc)
            return False

    def ping(self) -> Dict[str, Any]:
        """Verify connectivity and return endpoint info for auth probes."""
        self._request("GET", "/api/meta/types/typedef/name/DataProduct")
        return {"endpoint": self.endpoint}

    def _fetch_product(self, qualified_name: str) -> Optional[Dict[str, Any]]:
        try:
            data = self._request_json(
                "GET",
                "/api/meta/entity/uniqueAttribute/type/DataProduct",
                params={"attr:qualifiedName": qualified_name},
            )
        except ProviderError:
            return None
        entity = data.get("entity") or data.get("item")
        return entity if isinstance(entity, dict) else None

    def _to_atlan_product(
        self,
        product: CatalogProduct,
        qualified_name: str,
        domain_qualified_name: str,
        asset_qualified_names: List[str],
    ) -> Dict[str, Any]:
        """Build an Atlan DataProduct entity payload.

        Per the Atlan developer docs:
        - Domain linkage uses ``parentDomainQualifiedName`` and
          ``superDomainQualifiedName`` attributes plus a
          ``relationshipAttributes.dataDomain`` reference.
        - Asset membership uses ``dataProductAssetsDSL`` — a serialised
          Elasticsearch DSL query string — rather than a ``assetSelection``
          object.
        - Custom metadata keys in ``businessAttributes`` should ideally use
          hashed-string identifiers.  The SDK handles this transparently;
          when calling the raw REST API the Atlan server may accept display
          names in some versions but hashed IDs are canonical.
        """
        owner_value = self._resolve_owner(product)

        # Build ES DSL query that selects assets by qualified name
        assets_dsl = json.dumps({
            "query": {
                "bool": {
                    "filter": [
                        {"terms": {"qualifiedName": asset_qualified_names}},
                    ]
                }
            }
        }) if asset_qualified_names else ""

        attributes: Dict[str, Any] = {
            "qualifiedName": qualified_name,
            "name": product.name,
            "description": product.description,
            "userDescription": product.description,
            "parentDomainQualifiedName": domain_qualified_name,
            "superDomainQualifiedName": domain_qualified_name,
            "owner": owner_value,
        }
        if assets_dsl:
            attributes["dataProductAssetsDSL"] = assets_dsl

        custom_metadata = {
            "fluidContractId": product.id,
            "fluidVersion": product.version,
            "fluidDomain": product.domain,
            "fluidLayer": product.layer,
            "fluidStatus": product.status,
            "fluidOwnerTeam": product.owner.team,
            "fluidOwnerEmail": product.owner.email,
            "fluidLinks": product.links,
            "fluidTags": product.tags,
            "fluidContracts": [
                {"id": ref.id, "url": ref.url, "portId": ref.port_id} for ref in product.contracts
            ],
            "fluidOutputs": [self._port_metadata(port, product) for port in product.output_ports],
            "fluidInputs": [self._port_metadata(port, product) for port in product.input_ports],
        }
        return {
            "typeName": "DataProduct",
            "attributes": attributes,
            "relationshipAttributes": {
                "dataDomain": {
                    "typeName": "DataDomain",
                    "uniqueAttributes": {
                        "qualifiedName": domain_qualified_name,
                    },
                },
            },
            "businessAttributes": {"FLUID": custom_metadata},
        }

    def _resolve_output_assets(self, product: CatalogProduct) -> Tuple[List[str], List[str]]:
        asset_qns: List[str] = []
        warnings: List[str] = []

        for port in product.output_ports:
            overrides = [
                (member.external_ref or member.id)
                for member in member_refs_for_port(product, port)
                if (member.external_ref or member.id)
            ]
            if overrides:
                for ref in overrides:
                    if ref not in asset_qns:
                        asset_qns.append(ref)
                continue

            inferred = self._infer_asset_qualified_name(port)
            if inferred:
                asset_qns.append(inferred)
            else:
                warnings.append(
                    f"Could not resolve Atlan asset qualified name for output port '{port.id}'"
                )

        return asset_qns, warnings

    def _infer_asset_qualified_name(self, port: CatalogPort) -> Optional[str]:
        location = port.location if isinstance(port.location, dict) else {}
        platform = (port.platform or "").lower()

        if platform in {"gcp", "bigquery"}:
            project = location.get("project")
            dataset = location.get("dataset")
            table = location.get("table") or location.get("view")
            if project and dataset and table:
                return f"bigquery/{project}/{dataset}/{table}"

        if platform == "snowflake":
            account = location.get("account") or "default"
            database = location.get("database")
            schema = location.get("schema")
            table = location.get("table") or location.get("view")
            if database and schema and table:
                return f"snowflake/{account}/{database}/{schema}/{table}"

        if platform == "redshift":
            cluster = location.get("cluster") or "default"
            database = location.get("database")
            schema = location.get("schema")
            table = location.get("table") or location.get("view")
            if database and schema and table:
                return f"redshift/{cluster}/{database}/{schema}/{table}"

        return None

    def _resolve_domain_qualified_name(self, domain: str) -> str:
        mapped = self.domain_qualified_name_map.get(domain) or self.domain_qualified_name_map.get(
            slugify(domain)
        )
        if mapped:
            return mapped
        if self.default_domain_prefix:
            return f"{self.default_domain_prefix.rstrip('/')}/{slugify(domain)}"
        return slugify(domain)

    def _resolve_owner(self, product: CatalogProduct) -> str:
        for key in (
            product.owner.id,
            product.owner.team,
            slugify(product.owner.team),
            product.owner.email,
        ):
            if key and key in self.owner_map:
                return self.owner_map[key]
        return product.owner.display_name

    def _build_product_qualified_name(self, product_id: str) -> str:
        return f"{self.product_qualified_name_prefix}/{slugify(product_id)}"

    def _port_metadata(self, port: CatalogPort, product: CatalogProduct) -> Dict[str, Any]:
        return {
            "id": port.id,
            "direction": port.direction,
            "platform": port.platform,
            "location": port.location,
            "links": port.links,
            "contracts": [ref.id for ref in contract_refs_for_port(product, port)],
            "custom": port.custom,
        }

    def _ensure_api_key(self) -> None:
        if not self.api_key:
            raise ProviderError(
                "Atlan API key is required.\n"
                "Set ATLAN_API_KEY, or run: fluid auth set --provider atlan --key api_key\n"
                "Then verify with: fluid auth status --provider atlan"
            )
