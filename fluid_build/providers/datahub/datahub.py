"""Native DataHub publish integration."""

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


class DataHubProvider(BaseMarketplaceProvider):
    """Publish FLUID contracts as DataHub data products."""

    name = "datahub"
    DEFAULT_ENDPOINT = "http://localhost:8080"
    GRAPHQL_PATH = "/api/graphql"

    def __init__(
        self,
        *,
        endpoint: Optional[str] = None,
        server_url: Optional[str] = None,
        token: Optional[str] = None,
        auth: Optional[Dict[str, Any]] = None,
        environment: str = "PROD",
        domain_urn_map: Optional[Dict[str, str]] = None,
        owner_urn_map: Optional[Dict[str, str]] = None,
        logger: Optional[logging.Logger] = None,
        timeout: float = 30.0,
        **kwargs: Any,
    ) -> None:
        auth_config = dict(auth or {})
        self._credential_adapter = CatalogCredentialAdapter("datahub")
        endpoint_value = (
            endpoint
            or server_url
            or os.getenv("DATAHUB_ENDPOINT")
            or os.getenv("DATAHUB_SERVER_URL")
            or self.DEFAULT_ENDPOINT
        )
        token_value = self._credential_adapter.get_credential(
            "token",
            required=False,
            cli_value=token or auth_config.get("token"),
        )
        if token_value:
            auth_config = {"type": "bearer", "token": token_value}
        super().__init__(
            endpoint=endpoint_value,
            auth=auth_config,
            logger=logger,
            timeout=timeout,
            **kwargs,
        )
        self.environment = environment or os.getenv("DATAHUB_ENV", "PROD")
        self.domain_urn_map = dict(domain_urn_map or kwargs.get("domain_urn_map") or {})
        self.owner_urn_map = dict(owner_urn_map or kwargs.get("owner_urn_map") or {})
        self.token = token_value or ""

    def _publish_product(self, product: CatalogProduct, **kw: Any) -> Dict[str, Any]:
        dry_run = bool(kw.get("dry_run", False))
        product_urn = self._build_data_product_urn(product.id)
        domain_urn = self._resolve_domain_urn(product.domain)
        owner_urn = self._resolve_owner_urn(product)
        resource_urns, warnings = self._resolve_output_assets(product)
        product_input = self._to_datahub_product(product, domain_urn)
        custom_properties = self._build_custom_properties(product)
        external_url = product.links.get("catalog") or product.links.get("documentation") or ""

        if dry_run:
            return {
                "dry_run": True,
                "product_urn": product_urn,
                "resource_urns": resource_urns,
                "warnings": warnings,
                "payload": product_input,
                "custom_properties": custom_properties,
                "operations": [
                    "createOrUpdateDataProduct",
                    "batchSetDataProduct",
                    "setOwnership",
                    "addTags",
                    "setCustomProperties",
                ],
            }

        existing = self._fetch_product(product_urn)
        if existing:
            mutation = self._update_mutation()
            mutation_variables = {"urn": product_urn, "input": product_input}
            operation = "update"
        else:
            mutation = self._create_mutation()
            mutation_variables = {"input": product_input}
            operation = "create"

        publish_result = self._graphql(mutation, mutation_variables)

        membership_result = self._graphql(
            self._membership_mutation(),
            {"dataProductUrn": product_urn, "resourceUrns": resource_urns},
        )

        # Set ownership via separate mutation (ownerUrn is not a valid
        # field on Create/UpdateDataProductInput).
        if owner_urn:
            try:
                self._graphql(
                    self._add_owner_mutation(),
                    {
                        "input": {
                            "ownerUrn": owner_urn,
                            "resourceUrn": product_urn,
                            "ownerEntityType": "CORP_GROUP",
                        }
                    },
                )
            except Exception as exc:
                warnings.append(f"Failed to set ownership: {exc}")

        # Set tags via batchAddTags (tags is not a valid field on the
        # GraphQL input types).
        if product.tags:
            tag_urns = [f"urn:li:tag:{slugify(tag)}" for tag in product.tags]
            try:
                self._graphql(
                    self._batch_add_tags_mutation(),
                    {
                        "input": {
                            "tagUrns": tag_urns,
                            "resources": [{"resourceUrn": product_urn}],
                        }
                    },
                )
            except Exception as exc:
                warnings.append(f"Failed to set tags: {exc}")

        # Set custom properties and externalUrl via the REST Entities API
        # (these fields are not available on the GraphQL mutation inputs).
        if custom_properties or external_url:
            try:
                self._set_entity_properties(
                    product_urn, custom_properties, external_url
                )
            except Exception as exc:
                warnings.append(f"Failed to set custom properties: {exc}")

        return {
            "success": True,
            "operation": operation,
            "product_urn": product_urn,
            "resource_urns": resource_urns,
            "warnings": warnings,
            "publish_result": publish_result,
            "membership_result": membership_result,
        }

    def get_product(self, product_id: str) -> Dict[str, Any]:
        product = self._fetch_product(self._normalize_product_urn(product_id))
        if not product:
            raise ProviderError(f"DataHub data product not found: {product_id}")
        return product

    def list_products(self) -> List[Dict[str, Any]]:
        data = self._graphql(
            """
            query ListDataProducts($query: String!, $start: Int!, $count: Int!) {
              searchAcrossEntities(
                input: {
                  types: [DATA_PRODUCT]
                  query: $query
                  start: $start
                  count: $count
                }
              ) {
                searchResults {
                  entity {
                    ... on DataProduct {
                      urn
                      name
                      description
                    }
                  }
                }
              }
            }
            """,
            {"query": "*", "start": 0, "count": 100},
        )
        results = data.get("searchAcrossEntities", {}).get("searchResults", [])
        return [item.get("entity", {}) for item in results if isinstance(item, dict)]

    def health_check(self) -> bool:
        try:
            self._graphql("query { me { urn } }")
            return True
        except Exception as exc:
            self._log.warning("DataHub health check failed: %s", exc)
            return False

    def ping(self) -> Dict[str, Any]:
        """Verify connectivity and return user info for auth probes."""
        data = self._graphql("query { me { urn } }")
        user_urn = data.get("me", {}).get("urn") if isinstance(data.get("me"), dict) else None
        result: Dict[str, Any] = {"endpoint": self.endpoint}
        if user_urn:
            result["user_urn"] = user_urn
        return result

    def _graphql(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        self._ensure_token()
        response = self._request_json(
            "POST",
            self.GRAPHQL_PATH,
            json_body={"query": query, "variables": variables or {}},
        )
        if isinstance(response, dict) and response.get("errors"):
            raise ProviderError(f"DataHub GraphQL error: {response['errors']}")
        data = response.get("data", {}) if isinstance(response, dict) else {}
        if not isinstance(data, dict):
            raise ProviderError("DataHub GraphQL response missing data payload")
        return data

    def _fetch_product(self, product_urn: str) -> Optional[Dict[str, Any]]:
        data = self._graphql(
            """
            query GetDataProduct($urn: String!) {
              dataProduct(urn: $urn) {
                urn
                name
                description
              }
            }
            """,
            {"urn": product_urn},
        )
        product = data.get("dataProduct")
        return product if isinstance(product, dict) else None

    def _ensure_token(self) -> None:
        if not self.token:
            raise ProviderError(
                "DataHub token is required.\n"
                "Set DATAHUB_TOKEN, or run: fluid auth set --provider datahub --key token\n"
                "Then verify with: fluid auth status --provider datahub"
            )

    def _to_datahub_product(
        self,
        product: CatalogProduct,
        domain_urn: str,
    ) -> Dict[str, Any]:
        """Build the GraphQL CreateDataProductInput / UpdateDataProductInput.

        Per the DataHub GraphQL schema, CreateDataProductInput accepts:
          - properties: { name, description }  (required)
          - domainUrn (required)
          - id (optional custom identifier)

        Ownership, tags, custom properties, and externalUrl are set via
        separate mutations / REST calls after the product is created.
        """
        return {
            "id": slugify(product.id),
            "properties": {
                "name": product.name,
                "description": product.description or "",
            },
            "domainUrn": domain_urn,
        }

    def _build_custom_properties(self, product: CatalogProduct) -> Dict[str, str]:
        """Build the custom properties dict for the REST Entities API."""
        return {
            "fluid_contract_id": product.id,
            "fluid_version": product.version,
            "fluid_domain": product.domain,
            "fluid_layer": product.layer,
            "fluid_status": product.status,
            "fluid_owner_team": product.owner.team,
            "fluid_owner_email": product.owner.email,
            "fluid_links": json.dumps(product.links, sort_keys=True),
            "fluid_tags": json.dumps(product.tags),
            "fluid_contract_refs": json.dumps(
                [
                    {"id": ref.id, "port_id": ref.port_id, "format": ref.format, "url": ref.url}
                    for ref in product.contracts
                ],
                sort_keys=True,
            ),
            "fluid_output_ports": json.dumps(
                [self._port_metadata(port, product) for port in product.output_ports],
                sort_keys=True,
            ),
            "fluid_input_ports": json.dumps(
                [self._port_metadata(port, product) for port in product.input_ports],
                sort_keys=True,
            ),
        }

    def _set_entity_properties(
        self,
        urn: str,
        custom_properties: Dict[str, str],
        external_url: str,
    ) -> None:
        """Set custom properties and externalUrl via the REST Entities API.

        These fields are not available on the GraphQL mutation inputs.
        """
        aspect: Dict[str, Any] = {}
        if custom_properties:
            aspect["customProperties"] = custom_properties
        if external_url:
            aspect["externalUrl"] = external_url
        if not aspect:
            return
        self._request_json(
            "POST",
            "/entities",
            params={"action": "ingest"},
            json_body={
                "entity": {
                    "value": {
                        "com.linkedin.metadata.snapshot.DataProductSnapshot": {
                            "urn": urn,
                            "aspects": [
                                {
                                    "com.linkedin.common.DataProductProperties": aspect,
                                },
                            ],
                        }
                    }
                }
            },
        )

    def _port_metadata(self, port: CatalogPort, product: CatalogProduct) -> Dict[str, Any]:
        return {
            "id": port.id,
            "name": port.name,
            "direction": port.direction,
            "platform": port.platform,
            "location": port.location,
            "links": port.links,
            "custom": port.custom,
            "contracts": [ref.id for ref in contract_refs_for_port(product, port)],
        }

    def _resolve_output_assets(self, product: CatalogProduct) -> Tuple[List[str], List[str]]:
        resource_urns: List[str] = []
        warnings: List[str] = []

        for port in product.output_ports:
            overrides = [
                (member.external_ref or member.id)
                for member in member_refs_for_port(product, port)
                if (member.external_ref or member.id)
            ]
            if overrides:
                for ref in overrides:
                    if ref not in resource_urns:
                        resource_urns.append(ref)
                continue

            inferred = self._infer_asset_urn(port)
            if inferred:
                resource_urns.append(inferred)
            else:
                warnings.append(
                    f"Could not resolve DataHub asset URN for output port '{port.id}'"
                )

        return resource_urns, warnings

    def _infer_asset_urn(self, port: CatalogPort) -> Optional[str]:
        location = port.location if isinstance(port.location, dict) else {}
        platform = (port.platform or "").lower()
        env = self.environment

        if platform in {"gcp", "bigquery"}:
            project = location.get("project")
            dataset = location.get("dataset")
            table = location.get("table") or location.get("view")
            if project and dataset and table:
                return (
                    "urn:li:dataset:(urn:li:dataPlatform:bigquery,"
                    f"{project}.{dataset}.{table},{env})"
                )

        if platform == "snowflake":
            parts = [
                location.get("account"),
                location.get("database"),
                location.get("schema"),
                location.get("table") or location.get("view"),
            ]
            if parts[1] and parts[2] and parts[3]:
                identifier = ".".join(str(part) for part in parts if part)
                return (
                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
                    f"{identifier},{env})"
                )

        if platform == "redshift":
            parts = [
                location.get("cluster"),
                location.get("database"),
                location.get("schema"),
                location.get("table") or location.get("view"),
            ]
            if parts[1] and parts[2] and parts[3]:
                identifier = ".".join(str(part) for part in parts if part)
                return (
                    "urn:li:dataset:(urn:li:dataPlatform:redshift,"
                    f"{identifier},{env})"
                )

        return None

    def _build_data_product_urn(self, product_id: str) -> str:
        return f"urn:li:dataProduct:{slugify(product_id)}"

    def _normalize_product_urn(self, product_id: str) -> str:
        return product_id if str(product_id).startswith("urn:li:") else self._build_data_product_urn(product_id)

    def _resolve_domain_urn(self, domain: str) -> str:
        return self.domain_urn_map.get(domain) or self.domain_urn_map.get(
            slugify(domain)
        ) or f"urn:li:domain:{slugify(domain)}"

    def _resolve_owner_urn(self, product: CatalogProduct) -> Optional[str]:
        for key in (
            product.owner.id,
            product.owner.team,
            slugify(product.owner.team),
            product.owner.email,
        ):
            if key and key in self.owner_urn_map:
                return self.owner_urn_map[key]
        return None

    @staticmethod
    def _create_mutation() -> str:
        return """
        mutation CreateDataProduct($input: CreateDataProductInput!) {
          createDataProduct(input: $input)
        }
        """

    @staticmethod
    def _update_mutation() -> str:
        return """
        mutation UpdateDataProduct($urn: String!, $input: UpdateDataProductInput!) {
          updateDataProduct(urn: $urn, input: $input)
        }
        """

    @staticmethod
    def _membership_mutation() -> str:
        return """
        mutation BatchSetDataProduct($dataProductUrn: String!, $resourceUrns: [String!]!) {
          batchSetDataProduct(
            input: {
              dataProductUrn: $dataProductUrn
              resourceUrns: $resourceUrns
            }
          )
        }
        """

    @staticmethod
    def _add_owner_mutation() -> str:
        return """
        mutation AddOwner($input: AddOwnerInput!) {
          addOwner(input: $input)
        }
        """

    @staticmethod
    def _batch_add_tags_mutation() -> str:
        return """
        mutation BatchAddTags($input: BatchAddTagsInput!) {
          batchAddTags(input: $input)
        }
        """
