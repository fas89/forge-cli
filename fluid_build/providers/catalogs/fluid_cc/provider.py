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

"""Native FLUID Command Center integration with resilience patterns.

Publishes FLUID contracts as assets to the FLUID Command Center API.
Reads the raw FLUID dict directly — no flat-asset intermediate — so
multi-port contracts round-trip losslessly through the ``metadata`` payload.

Includes:
- Circuit breaker for fault tolerance
- Retry with exponential backoff
- Health checking before operations
- Upsert logic (create or update)
- Comprehensive error handling
"""

import asyncio
import time
from typing import Any, Dict, List, Optional

import httpx

from ...common import CircuitBreaker, get_auth_headers, metrics_collector
from ..base import BaseCatalogProvider, CatalogTarget, PublishResult


class FluidCommandCenterProvider(BaseCatalogProvider):
    """Native integration with FLUID Command Center."""

    name = "fluid_cc"

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

        self.circuit_breaker = CircuitBreaker(
            failure_threshold=config.get("circuit_breaker_threshold", 3),
            recovery_timeout=config.get("circuit_breaker_timeout", 60),
            expected_exception=httpx.HTTPError,
        )

    async def publish(
        self, fluid: Dict[str, Any], target: CatalogTarget
    ) -> PublishResult:
        """Publish a FLUID contract with retry logic and upsert."""
        start_time = time.time()
        metrics_collector.record_publish_request(self.name)

        if not await self.health_check():
            metrics_collector.record_publish_failure(self.name, "health_check_failed")
            return PublishResult(
                success=False,
                catalog_id=self.name,
                asset_id=target.contract_id,
                error="Catalog health check failed - endpoint not accessible",
            )

        is_valid, error_msg = self.validate_target(fluid, target)
        if not is_valid:
            metrics_collector.record_validation_error(error_msg)
            return PublishResult(
                success=False,
                catalog_id=self.name,
                asset_id=target.contract_id,
                error=f"Validation failed: {error_msg}",
            )

        for attempt in range(self.max_retries):
            try:
                result = await self.circuit_breaker.call(
                    self._publish_impl, fluid, target
                )
                latency = time.time() - start_time
                metrics_collector.record_publish_success(self.name, latency)

                cb_state = self.circuit_breaker.get_state()
                metrics_collector.update_circuit_breaker_stats(
                    self.name,
                    cb_state["state"],
                    cb_state["failure_count"],
                    cb_state["success_count"],
                )

                self.logger.info(
                    f"✅ Published {target.contract_name} to Command Center "
                    f"(attempt {attempt + 1}/{self.max_retries}, {latency:.2f}s)"
                )
                return result

            except Exception as e:
                if attempt < self.max_retries - 1:
                    delay = self.config.get("retry_delay", 1.0) * (2**attempt)
                    error_details = str(e)
                    if hasattr(e, "response"):
                        try:
                            error_details = f"{e} - Response: {e.response.text}"
                        except Exception:
                            pass
                    self.logger.warning(
                        f"Publish failed (attempt {attempt + 1}/{self.max_retries}), "
                        f"retrying in {delay}s: {error_details}"
                    )
                    await asyncio.sleep(delay)
                else:
                    error_msg = str(e)
                    if hasattr(e, "response"):
                        try:
                            error_msg = f"{e} - Response: {e.response.text}"
                        except Exception:
                            pass
                    self.logger.error(
                        f"❌ Publish failed after {self.max_retries} attempts: {error_msg}"
                    )
                    metrics_collector.record_publish_failure(
                        self.name, str(type(e).__name__)
                    )
                    return PublishResult(
                        success=False,
                        catalog_id=self.name,
                        asset_id=target.contract_id,
                        error=error_msg,
                    )

    async def _publish_impl(
        self, fluid: Dict[str, Any], target: CatalogTarget
    ) -> PublishResult:
        """Internal publish implementation (wrapped by circuit breaker)."""
        asset_data = _build_cc_asset_payload(fluid, target)

        if target.contract_yaml:
            import hashlib

            asset_data["contract_yaml"] = target.contract_yaml
            asset_data["contract_hash"] = hashlib.sha256(
                target.contract_yaml.encode("utf-8")
            ).hexdigest()

        headers = get_auth_headers(self.endpoint, self.auth)

        import json as json_lib

        self.logger.info(
            f"Sending asset_data with {len(asset_data.get('metadata', {}))} metadata keys"
        )
        self.logger.debug(
            f"Full asset_data: {json_lib.dumps(asset_data, indent=2, default=str)}"
        )

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            existing = await self._find_by_contract_id(
                client, headers, target.contract_id
            )

            if existing:
                self.logger.info(
                    f"Updating existing asset: {existing['id']} for contract {target.contract_id}"
                )
                response = await client.patch(
                    f"{self.endpoint}/api/v1/assets/{existing['id']}",
                    json=asset_data,
                    headers=headers,
                )
            else:
                self.logger.info(
                    f"Creating new asset for contract: {target.contract_id}"
                )
                response = await client.post(
                    f"{self.endpoint}/api/v1/assets",
                    json=asset_data,
                    headers=headers,
                )

            response.raise_for_status()
            result_data = response.json()

            return PublishResult(
                success=True,
                catalog_id=self.name,
                asset_id=result_data["id"],
                catalog_url=f"{self.endpoint}/assets/{result_data['id']}",
                details={
                    "operation": "update" if existing else "create",
                    "api_asset_id": result_data["id"],
                    "contract_id": target.contract_id,
                },
            )

    async def _find_by_contract_id(
        self, client: httpx.AsyncClient, headers: Dict[str, str], contract_id: str
    ) -> Optional[Dict[str, Any]]:
        """Find asset by fluid_contract_id in metadata (enables upsert)."""
        try:
            response = await client.get(
                f"{self.endpoint}/api/v1/assets",
                params={"fluid_contract_id": contract_id, "limit": 1},
                headers=headers,
                timeout=10.0,
            )
            response.raise_for_status()

            results = response.json()
            assets = results.get("items", results.get("assets", []))
            if assets:
                return assets[0]

            # Fallback: text search with client-side metadata check
            response = await client.get(
                f"{self.endpoint}/api/v1/assets",
                params={"q": contract_id, "limit": 10},
                headers=headers,
                timeout=10.0,
            )
            response.raise_for_status()

            results = response.json()
            assets = results.get("items", results.get("assets", []))
            for asset in assets:
                metadata = asset.get("metadata", {})
                if metadata.get("fluid_contract_id") == contract_id:
                    return asset

            return None

        except Exception as e:
            self.logger.warning(f"Error searching for existing asset: {e}")
            return None

    async def update(
        self, fluid: Dict[str, Any], target: CatalogTarget
    ) -> PublishResult:
        """Update existing asset (delegates to publish for upsert logic)."""
        return await self.publish(fluid, target)

    async def verify(self, contract_id: str) -> bool:
        """Verify asset exists in catalog."""
        try:
            headers = get_auth_headers(self.endpoint, self.auth)
            async with httpx.AsyncClient(timeout=10.0) as client:
                existing = await self._find_by_contract_id(client, headers, contract_id)
                return existing is not None
        except Exception as e:
            self.logger.error(f"Verification failed: {e}")
            return False

    async def health_check(self) -> bool:
        """Check if Command Center API is accessible."""
        try:
            headers = get_auth_headers(self.endpoint, self.auth)
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(
                    f"{self.endpoint}/api/v1/assets",
                    params={"limit": 1},
                    headers=headers,
                )
                return response.status_code == 200
        except Exception as e:
            self.logger.warning(f"Health check failed: {e}")
            return False


# ---------------------------------------------------------------------------
# Payload builder — kept module-level so the publish path stays a thin
# wrapper around a pure function (easy to unit-test independently).
# ---------------------------------------------------------------------------


def _build_cc_asset_payload(
    fluid: Dict[str, Any], target: CatalogTarget
) -> Dict[str, Any]:
    """Flatten a FLUID contract into the Command Center asset payload.

    Multi-port contracts survive as ``metadata.exposes`` (a compact
    per-port summary) rather than being collapsed to the first port.
    """
    metadata = fluid.get("metadata") or {}
    owner = metadata.get("owner") or fluid.get("owner") or {}
    exposes = fluid.get("exposes") or []

    tags = metadata.get("tags") or fluid.get("tags") or []
    version = fluid.get("version") or metadata.get("version") or "1.0.0"
    description = fluid.get("description") or metadata.get("description") or ""
    domain = fluid.get("domain") or metadata.get("domain") or "general"
    layer = metadata.get("layer") or "Bronze"
    kind = fluid.get("kind") or "DataProduct"

    first_expose = exposes[0] if exposes else {}
    first_binding = first_expose.get("binding") or {}
    first_platform = first_binding.get("platform", "unknown")
    first_location = first_binding.get("location") or {}
    first_sensitivity = first_expose.get("sensitivity", "internal")
    first_schema = (first_expose.get("contract") or {}).get("schema")

    port_summary: List[Dict[str, Any]] = []
    for expose in exposes:
        binding = expose.get("binding") or {}
        contract_spec = expose.get("contract") or {}
        port_summary.append(
            {
                "id": expose.get("id"),
                "platform": binding.get("platform"),
                "location": binding.get("location"),
                "sensitivity": expose.get("sensitivity"),
                "schema_fields": len(contract_spec.get("schema") or []),
            }
        )

    return {
        "name": target.contract_name,
        "description": description,
        "type": kind.lower(),
        "owner_id": "placeholder",  # backend replaces from auth token
        "tags": list(tags),
        "version": version,
        "is_public": first_sensitivity in ("public", "internal"),
        "metadata": {
            "fluid_contract_id": target.contract_id,
            "domain": domain,
            "layer": layer,
            "platform": first_platform,
            "location": first_location,
            "schema": first_schema,
            "owner": owner.get("team", owner.get("name", "unknown")),
            "owner_email": owner.get("email", ""),
            "sensitivity": first_sensitivity,
            "exposes": port_summary,
        },
    }
