"""Native Collibra publish integration."""

from __future__ import annotations

import io
import json
import logging
import os
import time
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


class CollibraProvider(BaseMarketplaceProvider):
    """Publish FLUID contracts as Collibra data product graphs."""

    name = "collibra"
    DEFAULT_ENDPOINT = "https://collibra.example.com"
    DEFAULT_ASSET_TYPES = {
        "product": "Data Product",
        "port": "Data Product Port",
        "contract": "Data Contract",
    }
    DEFAULT_RELATIONS = {
        "output_port": "exposes data as",
        "input_port": "consumes data through",
        "implements": "implements",
        "governs": "governs",
    }

    def __init__(
        self,
        *,
        endpoint: Optional[str] = None,
        base_url: Optional[str] = None,
        token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        auth: Optional[Dict[str, Any]] = None,
        data_product_catalog_domain_map: Optional[Dict[str, str]] = None,
        default_catalog_domain: Optional[str] = None,
        asset_type_names: Optional[Dict[str, str]] = None,
        relation_type_names: Optional[Dict[str, str]] = None,
        logger: Optional[logging.Logger] = None,
        timeout: float = 30.0,
        poll_interval: float = 0.2,
        max_polls: int = 10,
        **kwargs: Any,
    ) -> None:
        auth_config = dict(auth or {})
        self._credential_adapter = CatalogCredentialAdapter("collibra")
        endpoint_value = (
            endpoint
            or base_url
            or os.getenv("COLLIBRA_ENDPOINT")
            or os.getenv("COLLIBRA_BASE_URL")
            or self.DEFAULT_ENDPOINT
        )
        token_value = self._credential_adapter.get_credential(
            "token",
            required=False,
            cli_value=token or auth_config.get("token"),
        )
        username_value = self._credential_adapter.get_credential(
            "username",
            required=False,
            cli_value=username or auth_config.get("username"),
        )
        password_value = self._credential_adapter.get_credential(
            "password",
            required=False,
            cli_value=password or auth_config.get("password"),
        )
        if token_value:
            auth_config = {"type": "bearer", "token": token_value}
        elif username_value or password_value:
            auth_config = {
                "type": "basic",
                "username": username_value or "",
                "password": password_value or "",
            }
        super().__init__(
            endpoint=endpoint_value,
            auth=auth_config,
            logger=logger,
            timeout=timeout,
            **kwargs,
        )
        self.domain_map = dict(
            data_product_catalog_domain_map or kwargs.get("data_product_catalog_domain_map") or {}
        )
        self.default_catalog_domain = default_catalog_domain or kwargs.get("default_catalog_domain") or ""
        self.asset_type_names = dict(self.DEFAULT_ASSET_TYPES)
        self.asset_type_names.update(asset_type_names or kwargs.get("asset_type_names") or {})
        self.relation_type_names = dict(self.DEFAULT_RELATIONS)
        self.relation_type_names.update(relation_type_names or kwargs.get("relation_type_names") or {})
        self.poll_interval = poll_interval
        self.max_polls = max_polls
        self.token = token_value or ""
        self.username = username_value or ""
        self.password = password_value or ""
        self.auth_mode = "bearer" if self.token else "basic" if self.username and self.password else "none"

    def _publish_product(self, product: CatalogProduct, **kw: Any) -> Dict[str, Any]:
        dry_run = bool(kw.get("dry_run", False))
        payload, warnings = self._build_import_payload(product)

        if dry_run:
            return {
                "dry_run": True,
                "asset_id": product.id,
                "warnings": warnings,
                "payload": payload,
            }

        job = self._submit_import_job(payload)
        job_id = str(job.get("id") or job.get("jobId") or "")
        job_result = self._wait_for_job(job_id) if job_id else job
        status = str(job_result.get("status") or job_result.get("state") or "").upper()
        if status and status not in {"COMPLETED", "SUCCESS", "FINISHED"}:
            raise ProviderError(f"Collibra import job failed: {job_result}")
        return {
            "success": True,
            "asset_id": product.id,
            "job": job_result,
            "warnings": warnings,
        }

    def get_product(self, product_id: str) -> Dict[str, Any]:
        external_id = self._product_external_id(product_id)
        data = self._request_json(
            "GET",
            "/rest/2.0/assets",
            params={"externalEntityId": external_id, "limit": 1},
        )
        results = data.get("results", data.get("items", []))
        if not results:
            raise ProviderError(f"Collibra data product not found: {product_id}")
        return results[0]

    def list_products(self) -> List[Dict[str, Any]]:
        # The Collibra FindAssetsRequest API uses typePublicIds (not
        # typeName) to filter by asset type.
        data = self._request_json(
            "GET",
            "/rest/2.0/assets",
            params={"typePublicId": self.asset_type_names["product"], "limit": 100},
        )
        return data.get("results", data.get("items", []))

    def health_check(self) -> bool:
        try:
            self._ensure_auth()
            self._request("GET", "/rest/2.0/assets", params={"limit": 1})
            return True
        except Exception as exc:
            self._log.warning("Collibra health check failed: %s", exc)
            return False

    def ping(self) -> Dict[str, Any]:
        """Verify connectivity and return endpoint info for auth probes."""
        self._ensure_auth()
        self._request("GET", "/rest/2.0/assets", params={"limit": 1})
        return {"endpoint": self.endpoint, "auth_mode": self.auth_mode}

    def _build_import_payload(self, product: CatalogProduct) -> Tuple[Dict[str, Any], List[str]]:
        warnings: List[str] = []
        assets: List[Dict[str, Any]] = []
        relations: List[Dict[str, Any]] = []
        domain_ref = self._resolve_catalog_domain(product.domain)
        product_external_id = self._product_external_id(product.id)

        assets.append(
            self._asset(
                external_id=product_external_id,
                name=product.name,
                asset_type=self.asset_type_names["product"],
                domain_ref=domain_ref,
                attributes={
                    "description": product.description,
                    "status": product.status,
                    "domain": product.domain,
                    "layer": product.layer,
                    "version": product.version,
                    "tags": product.tags,
                    "links": product.links,
                    "custom": product.custom,
                    "ownerTeam": product.owner.team,
                    "ownerEmail": product.owner.email,
                },
            )
        )

        for port in product.output_ports:
            port_external_id = self._output_port_external_id(product.id, port.id)
            assets.append(
                self._asset(
                    external_id=port_external_id,
                    name=port.name,
                    asset_type=self.asset_type_names["port"],
                    domain_ref=domain_ref,
                    attributes=self._port_attributes(port),
                )
            )
            relations.append(
                self._relation(
                    relation_type=self.relation_type_names["output_port"],
                    source_external_id=product_external_id,
                    target_external_id=port_external_id,
                )
            )

            physical_ref = self._resolve_physical_asset_ref(product, port)
            if physical_ref:
                relations.append(
                    self._relation(
                        relation_type=self.relation_type_names["implements"],
                        source_external_id=port_external_id,
                        target_external_id=physical_ref,
                    )
                )
            else:
                warnings.append(
                    f"Could not resolve Collibra implementation asset for output port '{port.id}'"
                )

            for contract_ref in contract_refs_for_port(product, port):
                contract_external_id = self._contract_external_id(contract_ref.id)
                assets.append(
                    self._asset(
                        external_id=contract_external_id,
                        name=contract_ref.name or contract_ref.id,
                        asset_type=self.asset_type_names["contract"],
                        domain_ref=domain_ref,
                        attributes={
                            "format": contract_ref.format,
                            "url": contract_ref.url,
                            "portId": port.id,
                            "metadata": contract_ref.metadata,
                        },
                    )
                )
                relations.append(
                    self._relation(
                        relation_type=self.relation_type_names["governs"],
                        source_external_id=contract_external_id,
                        target_external_id=port_external_id,
                    )
                )

        for port in product.input_ports:
            port_external_id = self._input_port_external_id(product.id, port.id)
            assets.append(
                self._asset(
                    external_id=port_external_id,
                    name=port.name,
                    asset_type=self.asset_type_names["port"],
                    domain_ref=domain_ref,
                    attributes=self._port_attributes(port),
                )
            )
            relations.append(
                self._relation(
                    relation_type=self.relation_type_names["input_port"],
                    source_external_id=product_external_id,
                    target_external_id=port_external_id,
                )
            )

            physical_ref = self._resolve_physical_asset_ref(product, port)
            if physical_ref:
                relations.append(
                    self._relation(
                        relation_type=self.relation_type_names["implements"],
                        source_external_id=port_external_id,
                        target_external_id=physical_ref,
                    )
                )

        payload = {"operation": "UPSERT", "assets": assets, "relations": relations}
        return payload, warnings

    def _port_attributes(self, port: CatalogPort) -> Dict[str, Any]:
        return {
            "direction": port.direction,
            "description": port.description,
            "kind": port.kind,
            "platform": port.platform,
            "location": port.location,
            "schema": port.schema,
            "links": port.links,
            "custom": port.custom,
            "status": port.status,
            "sensitivity": port.sensitivity,
            "containsPii": port.contains_pii,
        }

    def _resolve_physical_asset_ref(self, product: CatalogProduct, port: CatalogPort) -> Optional[str]:
        overrides = [
            (member.external_ref or member.id)
            for member in member_refs_for_port(product, port)
            if (member.external_ref or member.id)
        ]
        if overrides:
            return overrides[0]

        location = port.location if isinstance(port.location, dict) else {}
        platform = (port.platform or "").lower()
        if platform in {"gcp", "bigquery"}:
            project = location.get("project")
            dataset = location.get("dataset")
            table = location.get("table") or location.get("view")
            if project and dataset and table:
                return f"bigquery:{project}.{dataset}.{table}"

        if platform == "snowflake":
            parts = [
                location.get("account"),
                location.get("database"),
                location.get("schema"),
                location.get("table") or location.get("view"),
            ]
            if parts[1] and parts[2] and parts[3]:
                return "snowflake:" + ".".join(str(part) for part in parts if part)

        if platform == "redshift":
            parts = [
                location.get("cluster"),
                location.get("database"),
                location.get("schema"),
                location.get("table") or location.get("view"),
            ]
            if parts[1] and parts[2] and parts[3]:
                return "redshift:" + ".".join(str(part) for part in parts if part)

        return None

    def _submit_import_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Submit an import job via the Collibra Import API v2.

        The official endpoint is ``POST /rest/2.0/import/json-job`` and
        expects a multipart form-data request with the JSON payload
        uploaded as a ``file`` part.
        """
        json_bytes = json.dumps(payload).encode("utf-8")
        url = f"{self.endpoint}/rest/2.0/import/json-job"
        headers = self._headers()
        # Remove Content-Type so requests can set multipart boundary
        headers.pop("Content-Type", None)
        try:
            response = self._session().post(
                url,
                files={"file": ("import.json", io.BytesIO(json_bytes), "application/json")},
                headers=headers,
                timeout=self.timeout,
            )
        except Exception as exc:
            raise ProviderError(f"collibra: Import job submission failed: {exc}") from exc

        if response.status_code >= 400:
            body = getattr(response, "text", "")[:1000]
            raise ProviderError(f"collibra: HTTP {response.status_code} for import job: {body}")

        try:
            return response.json()
        except Exception:
            return {}

    def _wait_for_job(self, job_id: str) -> Dict[str, Any]:
        """Poll import job status via the Core Jobs API."""
        latest: Dict[str, Any] = {}
        for _ in range(self.max_polls):
            latest = self._request_json("GET", f"/jobs/{job_id}")
            status = str(latest.get("status") or latest.get("state") or "").upper()
            if status in {"COMPLETED", "SUCCESS", "FINISHED", "FAILED", "ERROR"}:
                return latest
            time.sleep(self.poll_interval)
        return latest

    def _resolve_catalog_domain(self, domain: str) -> Dict[str, str]:
        value = self.domain_map.get(domain) or self.domain_map.get(slugify(domain)) or self.default_catalog_domain
        if not value:
            value = slugify(domain)
        if str(value).startswith("id:"):
            return {"id": str(value)[3:]}
        return {"name": str(value)}

    @staticmethod
    def _asset(
        *,
        external_id: str,
        name: str,
        asset_type: str,
        domain_ref: Dict[str, str],
        attributes: Dict[str, Any],
    ) -> Dict[str, Any]:
        # Collibra Import API v2 expects attributes in the format:
        # {"AttributeName": [{"value": "..."}]}
        formatted_attrs: Dict[str, List[Dict[str, Any]]] = {}
        for key, value in attributes.items():
            if value is None:
                continue
            if isinstance(value, (dict, list)):
                formatted_attrs[key] = [{"value": json.dumps(value)}]
            elif isinstance(value, bool):
                formatted_attrs[key] = [{"value": str(value).lower()}]
            else:
                formatted_attrs[key] = [{"value": str(value)}]
        asset: Dict[str, Any] = {
            "resourceType": "Asset",
            "identifier": {
                "externalSystemId": "fluid",
                "externalEntityId": external_id,
            },
            "name": name,
            "type": {"name": asset_type},
            "attributes": formatted_attrs,
            "domain": domain_ref,
        }
        return asset

    @staticmethod
    def _relation(
        *,
        relation_type: str,
        source_external_id: str,
        target_external_id: str,
    ) -> Dict[str, Any]:
        relation_id = f"{relation_type}:{source_external_id}->{target_external_id}"
        return {
            "resourceType": "Relation",
            "identifier": {
                "externalSystemId": "fluid",
                "externalEntityId": relation_id,
            },
            "type": {"name": relation_type},
            "source": {
                "externalSystemId": "fluid",
                "externalEntityId": source_external_id,
            },
            "target": {
                "externalEntityId": target_external_id,
            },
        }

    @staticmethod
    def _product_external_id(product_id: str) -> str:
        return f"fluid:{product_id}"

    @staticmethod
    def _output_port_external_id(product_id: str, port_id: str) -> str:
        return f"fluid:{product_id}:output:{port_id}"

    @staticmethod
    def _input_port_external_id(product_id: str, port_id: str) -> str:
        return f"fluid:{product_id}:input:{port_id}"

    @staticmethod
    def _contract_external_id(contract_id: str) -> str:
        return f"fluid:{contract_id}"

    def _ensure_auth(self) -> None:
        if self.auth_mode in {"bearer", "basic"}:
            return
        raise ProviderError(
            "Collibra credentials are required.\n"
            "Preferred: fluid auth set --provider collibra --key token\n"
            "Fallback: fluid auth set --provider collibra --key username and --key password\n"
            "Then verify with: fluid auth status --provider collibra"
        )
