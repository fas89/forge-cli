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
Data Mesh Manager (Entropy Data) Provider — orchestrator.

Publishes FLUID contracts as data products **and** data contracts to the
Entropy Data / Data Mesh Manager REST API. Pure mapping logic lives in
:mod:`._mappers`; this module owns orchestration, HTTP, and the public
provider surface.

API reference : https://api.entropy-data.com/swagger/index.html
Docs          : https://docs.datamesh-manager.com/dataproducts

Authentication
--------------
All calls require the ``x-api-key`` header.
Generate one at: Profile → Organization → Settings → API Keys.

Environment Variables
---------------------
DMM_API_KEY   (required)  API key for Entropy Data.
DMM_API_URL   (optional)  Base URL, default ``https://api.entropy-data.com``.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from fluid_build.providers.base import BaseProvider, ProviderError

from . import _mappers

if TYPE_CHECKING:
    import requests as requests_typing

    RequestsSession = requests_typing.Session
    RequestsResponse = requests_typing.Response
else:
    RequestsSession = Any
    RequestsResponse = Any

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    REQUESTS_AVAILABLE = True
except ImportError:  # pragma: no cover
    requests = None  # type: ignore[assignment]
    HTTPAdapter = None  # type: ignore[assignment,misc]
    Retry = None  # type: ignore[assignment,misc]
    REQUESTS_AVAILABLE = False

LOG = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_API_URL = "https://api.entropy-data.com"
_TIMEOUT = 30  # seconds

# Re-exported from :mod:`._mappers` so external callers that imported these
# module-level dicts continue to work after the split.
_STATUS_MAP = _mappers.STATUS_MAP
_PROVIDER_TYPE_MAP = _mappers.PROVIDER_TYPE_MAP


# ---------------------------------------------------------------------------
# Provider
# ---------------------------------------------------------------------------


class DataMeshManagerProvider(BaseProvider):
    """Publish FLUID contracts to **Entropy Data / Data Mesh Manager**.

    The provider maps a FLUID contract to the Entropy Data
    ``PUT /api/dataproducts/{id}`` shape and, optionally, creates
    a companion data contract via ``PUT /api/datacontracts/{id}``.

    It also auto-creates teams when they don't exist yet.
    """

    name: str = "datamesh-manager"
    DATA_PRODUCT_SPEC_DPS = _mappers.DATA_PRODUCT_SPEC_DPS
    DATA_PRODUCT_SPEC_ODPS = _mappers.DATA_PRODUCT_SPEC_ODPS
    CONTRACT_FORMAT_ODCS = "odcs"
    CONTRACT_FORMAT_DCS = "dcs"

    # ---- lifecycle --------------------------------------------------------

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        api_url: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.name = "datamesh-manager"
        self._log = logger or LOG

        self.api_key = api_key or os.getenv("DMM_API_KEY", "")
        self.api_url = (api_url or os.getenv("DMM_API_URL", _DEFAULT_API_URL)).rstrip("/")

        if not REQUESTS_AVAILABLE:
            raise ProviderError(
                "The 'requests' library is required for the Data Mesh Manager provider.\n"
                "Install it with:  pip install requests"
            )

        self._session_instance: Optional[RequestsSession] = None

    # ---- BaseProvider abstract methods ------------------------------------

    def plan(
        self, contract: Any, out: Any = None, fmt: str = "yaml", **kw: Any
    ) -> List[Dict[str, Any]]:
        """Return a preview of what *apply* would PUT to Entropy Data."""
        data_product_specification: Optional[str] = kw.get("data_product_specification")
        provider_hint: Optional[str] = kw.get("provider_hint")
        contracts = contract if isinstance(contract, list) else [contract]
        actions: List[Dict[str, Any]] = []
        for c in contracts:
            dp = self._to_data_product(
                c,
                data_product_specification=self._resolve_data_product_specification(
                    data_product_specification,
                    provider_hint=provider_hint,
                    fluid=c,
                ),
            )
            actions.append(
                {
                    "action": "PUT",
                    "url": f"{self.api_url}/api/dataproducts/{dp['id']}",
                    "payload": dp,
                }
            )
        return actions

    def apply(self, contract: Any, out: Any = None, fmt: str = "yaml", **kw: Any) -> Dict[str, Any]:
        """Publish one or many FLUID contracts as data products.

        Keyword Args
        -------------
        dry_run : bool
            Preview the API call without sending it.
        team_id : str | None
            Override the team id derived from the contract owner.
        create_team : bool
            Auto-create the team if it doesn't exist (default True).
        publish_contract : bool
            Also publish a companion data contract (default False).
        contract_format : str
            ``"odcs"`` (default) or ``"dcs"`` for the companion data contract.
        """
        dry_run: bool = kw.get("dry_run", False)
        team_id: Optional[str] = kw.get("team_id")
        create_team: bool = kw.get("create_team", True)
        publish_contract_flag: bool = kw.get("publish_contract", False)
        contract_format: str = kw.get("contract_format", self.CONTRACT_FORMAT_ODCS)
        provider_hint: Optional[str] = kw.get("provider_hint")
        data_product_specification: Optional[str] = kw.get("data_product_specification")
        validate_generated_contracts: bool = kw.get("validate_generated_contracts", False)
        validation_mode: str = kw.get("validation_mode", "warn")

        self._require_api_key()

        contracts = contract if isinstance(contract, list) else [contract]
        results: List[Dict[str, Any]] = []

        for c in contracts:
            result = self._publish_one(
                c,
                dry_run=dry_run,
                team_id_override=team_id,
                create_team=create_team,
                publish_contract=publish_contract_flag,
                contract_format=contract_format,
                data_product_specification=self._resolve_data_product_specification(
                    data_product_specification,
                    provider_hint=provider_hint,
                    fluid=c,
                ),
                validate_generated_contracts=validate_generated_contracts,
                validation_mode=validation_mode,
            )
            results.append(result)

        if len(results) == 1:
            return results[0]
        return {"published": len(results), "results": results}

    def capabilities(self) -> Dict[str, bool]:
        return {
            "plan": True,
            "apply": True,
            "export": False,
            "validate_contract": False,
            "verify": True,
        }

    # ---- extra public methods ---------------------------------------------

    def verify(self, product_id: str) -> Dict[str, Any]:
        """GET a data product by *product_id*.  Returns the JSON body."""
        self._require_api_key()
        resp = self._request("GET", f"/api/dataproducts/{product_id}")
        return resp.json()

    def delete(self, product_id: str) -> bool:
        """DELETE a data product.  Returns True on success."""
        self._require_api_key()
        resp = self._request("DELETE", f"/api/dataproducts/{product_id}")
        return resp.status_code in (200, 204)

    def list_products(self) -> List[Dict[str, Any]]:
        """GET all data products."""
        self._require_api_key()
        resp = self._request("GET", "/api/dataproducts")
        return resp.json()

    def list_teams(self) -> List[Dict[str, Any]]:
        """GET all teams."""
        self._require_api_key()
        resp = self._request("GET", "/api/teams")
        return resp.json()

    def publish_data_contract(
        self,
        fluid: Mapping[str, Any],
        product_id: Optional[str] = None,
        *,
        fmt: str = "odcs",
    ) -> Dict[str, Any]:
        """Publish a FLUID contract as a data contract to Entropy Data."""
        self._require_api_key()
        pid = product_id or self._extract_id(fluid)
        return self._publish_data_contract_internal(fluid, pid, fmt=fmt)

    def publish_test_results(
        self,
        report: Any,
        *,
        publish_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        """POST test results to ``/api/test-results``.

        Compatible with the Entropy Data test-results endpoint used by
        DCCLI's ``--publish`` flag.
        """
        self._require_api_key()

        url = publish_url or f"{self.api_url}/api/test-results"

        issues = getattr(report, "issues", [])
        results: List[Dict[str, Any]] = []
        for issue in issues:
            results.append(
                {
                    "check": getattr(issue, "category", "unknown"),
                    "severity": getattr(issue, "severity", "info"),
                    "message": getattr(issue, "message", ""),
                    "path": getattr(issue, "path", ""),
                    "result": "failed" if getattr(issue, "severity", "") == "error" else "passed",
                }
            )

        if not results:
            results.append(
                {
                    "check": "all",
                    "severity": "info",
                    "message": "All checks passed",
                    "path": "",
                    "result": "passed",
                }
            )

        payload: Dict[str, Any] = {
            "dataContractId": getattr(report, "contract_id", "unknown"),
            "dataContractVersion": getattr(report, "contract_version", "1.0.0"),
            "result": "passed" if getattr(report, "is_valid", lambda: True)() else "failed",
            "timestamp": getattr(report, "validation_time", datetime.utcnow()).isoformat(),
            "duration": getattr(report, "duration", 0.0),
            "checks": {
                "passed": getattr(report, "checks_passed", 0),
                "failed": getattr(report, "checks_failed", 0),
            },
            "results": results,
        }

        self._log.debug("POST %s — %d result(s)", url, len(results))

        try:
            resp = self._session().request(
                "POST",
                url,
                headers=self._headers(),
                json=payload,
                timeout=_TIMEOUT,
            )
        except Exception as exc:
            raise ProviderError(f"Failed to publish test results to {url}: {exc}") from exc

        if resp.status_code >= 400:
            body = resp.text[:500]
            raise ProviderError(f"Test results publish failed (HTTP {resp.status_code}): {body}")

        self._log.info("Published test results to %s (HTTP %s)", url, resp.status_code)
        return {
            "success": True,
            "status_code": resp.status_code,
            "url": url,
        }

    # ---- publish pipeline -------------------------------------------------

    def _publish_one(
        self,
        fluid: Mapping[str, Any],
        *,
        dry_run: bool = False,
        team_id_override: Optional[str] = None,
        create_team: bool = True,
        publish_contract: bool = False,
        contract_format: str = "odcs",
        data_product_specification: Optional[str] = None,
        validate_generated_contracts: bool = False,
        validation_mode: str = "warn",
    ) -> Dict[str, Any]:
        dp = self._to_data_product(
            fluid,
            data_product_specification=data_product_specification,
        )
        product_id = dp.get("id") or self._extract_id(fluid)
        is_odps_payload = self._is_odps_payload(dp)

        tid = team_id_override or self._derive_team_id(fluid)
        if is_odps_payload:
            team_obj = dp.get("team")
            if not isinstance(team_obj, dict):
                team_obj = {}
            team_obj.setdefault("name", tid)
            dp["team"] = team_obj
        else:
            dp["teamId"] = tid

        # Wire contract references on output ports when publishing companion contracts.
        # IDs must match per-expose publish ids: ``{product_id}.{expose_id}``.
        if publish_contract:
            for port in dp.get("outputPorts", []):
                if is_odps_payload:
                    expose_ref = port.get("name") or port.get("id")
                    if expose_ref:
                        port["contractId"] = f"{product_id}.{expose_ref}"
                else:
                    if not port.get("dataContractId") and port.get("id"):
                        port["dataContractId"] = f"{product_id}.{port['id']}"

        if dry_run:
            result: Dict[str, Any] = {
                "dry_run": True,
                "method": "PUT",
                "url": f"{self.api_url}/api/dataproducts/{product_id}",
                "payload": dp,
            }
            if publish_contract:
                result["odcs_contracts"] = self._preview_odcs_per_expose(fluid, product_id)
            return result

        if create_team:
            self._ensure_team(fluid, tid)

        resp = self._request("PUT", f"/api/dataproducts/{product_id}", json_body=dp)
        self._log.info("Published data product %s (%s)", product_id, resp.status_code)

        result = {
            "success": True,
            "product_id": product_id,
            "team_id": tid,
            "status_code": resp.status_code,
            "url": f"{self.api_url}/dataproducts/{product_id}",
        }

        if publish_contract:
            odcs_results = self._publish_odcs_per_expose(
                fluid,
                product_id,
                validate_generated_contracts=validate_generated_contracts,
                validation_mode=validation_mode,
            )
            result["odcs_contracts"] = odcs_results

        return result

    # ---- ODCS per-expose publishing ---------------------------------------

    def _preview_odcs_per_expose(
        self, fluid: Mapping[str, Any], product_id: str
    ) -> List[Dict[str, Any]]:
        """Return the ODCS payloads that *_publish_odcs_per_expose* would PUT."""
        try:
            from fluid_build.providers.odcs import OdcsProvider  # lazy import
        except ImportError as exc:
            self._log.warning("OdcsProvider not available — cannot preview ODCS contracts: %s", exc)
            return []

        odcs_prov = OdcsProvider()
        previews: List[Dict[str, Any]] = []
        for expose in fluid.get("exposes", []):
            if not isinstance(expose, dict):
                continue
            expose_id = expose.get("exposeId") or expose.get("id")
            if not expose_id:
                continue
            contract_id = f"{product_id}.{expose_id}"
            try:
                odcs_body = odcs_prov.render(fluid, expose_id=expose_id)
            except Exception as exc:
                self._log.warning("Could not generate ODCS preview for %s: %s", expose_id, exc)
                continue
            previews.append(
                {
                    "method": "PUT",
                    "url": f"{self.api_url}/api/datacontracts/{contract_id}",
                    "payload": odcs_body,
                }
            )
        return previews

    def _publish_odcs_per_expose(
        self,
        fluid: Mapping[str, Any],
        product_id: str,
        *,
        validate_generated_contracts: bool = False,
        validation_mode: str = "warn",
    ) -> List[Dict[str, Any]]:
        """Publish one ODCS data contract for every expose port."""
        try:
            from fluid_build.providers.odcs import OdcsProvider  # lazy import
        except ImportError as exc:
            raise ProviderError(
                "OdcsProvider is required to publish ODCS contracts.\n"
                "Ensure fluid_build.providers.odcs is installed."
            ) from exc

        odcs_prov = OdcsProvider()
        results: List[Dict[str, Any]] = []

        for expose in fluid.get("exposes", []):
            if not isinstance(expose, dict):
                continue
            expose_id = expose.get("exposeId") or expose.get("id")
            if not expose_id:
                self._log.warning("Expose missing exposeId/id — skipping ODCS contract publish")
                continue

            contract_id = f"{product_id}.{expose_id}"

            try:
                odcs_body = odcs_prov.render(fluid, expose_id=expose_id)
            except Exception as exc:
                self._log.error(
                    "Failed to generate ODCS contract for expose '%s': %s", expose_id, exc
                )
                results.append(
                    {
                        "contract_id": contract_id,
                        "expose_id": expose_id,
                        "success": False,
                        "error": str(exc),
                        "error_type": "RENDER_FAILED",
                    }
                )
                continue

            payload_stats = self._summarize_odcs_payload(odcs_body)
            self._log.info(
                (
                    "Prepared ODCS contract %s for expose '%s' "
                    "(schema_objects=%s, properties=%s, servers=%s, sla_properties=%s)"
                ),
                contract_id,
                expose_id,
                payload_stats["schema_objects"],
                payload_stats["schema_properties"],
                payload_stats["servers"],
                payload_stats["sla_properties"],
            )

            validation_error = None
            is_valid: Optional[bool] = None
            if validate_generated_contracts:
                is_valid, validation_error = self._validate_generated_odcs_contract(
                    odcs_prov, odcs_body
                )
                if is_valid is False:
                    self._log.warning(
                        "Generated ODCS contract failed local validation for expose '%s': %s",
                        expose_id,
                        validation_error,
                    )
                    if validation_mode == "strict":
                        results.append(
                            {
                                "contract_id": contract_id,
                                "expose_id": expose_id,
                                "success": False,
                                "valid": False,
                                "validation_error": validation_error,
                                "error_type": "VALIDATION_FAILED",
                            }
                        )
                        continue

            try:
                resp = self._request(
                    "PUT", f"/api/datacontracts/{contract_id}", json_body=odcs_body
                )
                self._log.info(
                    "Published ODCS contract %s (HTTP %s)", contract_id, resp.status_code
                )
                entry: Dict[str, Any] = {
                    "contract_id": contract_id,
                    "expose_id": expose_id,
                    "success": True,
                    "status_code": resp.status_code,
                    "url": f"{self.api_url}/datacontracts/{contract_id}",
                }
                if is_valid is not None:
                    entry["valid"] = is_valid
                if validation_error:
                    entry["validation_error"] = validation_error
                entry.update(payload_stats)
                results.append(entry)
            except ProviderError as exc:
                self._log.error("HTTP error publishing ODCS contract %s: %s", contract_id, exc)
                entry = {
                    "contract_id": contract_id,
                    "expose_id": expose_id,
                    "success": False,
                    "error": str(exc),
                    "error_type": "HTTP_FAILED",
                }
                if is_valid is not None:
                    entry["valid"] = is_valid
                if validation_error:
                    entry["validation_error"] = validation_error
                entry.update(payload_stats)
                results.append(entry)

        success_count = len([r for r in results if r.get("success")])
        failed_count = len(results) - success_count
        self._log.info(
            "ODCS publish summary for %s: %s succeeded, %s failed",
            product_id,
            success_count,
            failed_count,
        )

        return results

    def _validate_generated_odcs_contract(
        self, odcs_provider: Any, odcs_body: Mapping[str, Any]
    ) -> tuple[bool, Optional[str]]:
        """Validate rendered ODCS payload and return (is_valid, error_message)."""
        try:
            if hasattr(odcs_provider, "validate_contract"):
                odcs_provider.validate_contract(odcs_body)
            else:
                odcs_provider._validate_odcs(odcs_body)
            return True, None
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)

    # ---- data contracts ---------------------------------------------------

    def _publish_data_contract_internal(
        self,
        fluid: Mapping[str, Any],
        product_id: str,
        *,
        fmt: str = "odcs",
    ) -> Dict[str, Any]:
        """Publish a companion data contract to ``PUT /api/datacontracts/{id}``."""
        if fmt == self.CONTRACT_FORMAT_DCS:
            dc = self._build_data_contract_dcs(fluid, product_id)
        else:
            dc = self._build_data_contract_odcs(fluid, product_id)

        contract_id = dc["id"]
        resp = self._request("PUT", f"/api/datacontracts/{contract_id}", json_body=dc)
        self._log.info(
            "Published data contract %s (format=%s, HTTP %s)",
            contract_id,
            fmt,
            resp.status_code,
        )
        return {
            "contract_id": contract_id,
            "format": fmt,
            "status_code": resp.status_code,
            "url": f"{self.api_url}/datacontracts/{contract_id}",
        }

    # ---- team management --------------------------------------------------

    def _ensure_team(self, fluid: Mapping[str, Any], team_id: str) -> None:
        """Create team via ``PUT /api/teams/{id}`` if it doesn't exist."""
        try:
            resp = self._session().get(
                f"{self.api_url}/api/teams/{team_id}",
                headers=self._headers(),
                timeout=_TIMEOUT,
            )
            if resp.status_code == 200:
                self._log.debug("Team already exists: %s", team_id)
                return
        except Exception:
            pass  # proceed to create

        owner = fluid.get("owner", fluid.get("metadata", {}).get("owner", {}))
        team: Dict[str, Any] = {
            "id": team_id,
            "name": owner.get("name") or owner.get("team") or team_id,
            # ``type`` is required by Entropy CE 2.0.x — older versions
            # accept and ignore it, so this is safe across versions.
            "type": owner.get("team_type", "internal"),
        }
        if owner.get("email"):
            team["contactEmail"] = owner["email"]

        try:
            resp = self._request("PUT", f"/api/teams/{team_id}", json_body=team)
            self._log.info("Created/updated team %s (%s)", team_id, resp.status_code)
        except ProviderError as exc:
            self._log.warning("Could not create team %s: %s", team_id, exc)

    # ---- HTTP helpers -----------------------------------------------------

    def _session(self) -> RequestsSession:
        if self._session_instance is None:
            s = requests.Session()
            retry = Retry(
                total=3,
                backoff_factor=0.5,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET", "PUT", "DELETE"],
            )
            adapter = HTTPAdapter(max_retries=retry)
            s.mount("https://", adapter)
            s.mount("http://", adapter)
            self._session_instance = s
        return self._session_instance

    def _headers(self) -> Dict[str, str]:
        return {
            "x-api-key": self.api_key,
            "content-type": "application/json",
            "accept": "application/json",
        }

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Any = None,
    ) -> RequestsResponse:
        url = f"{self.api_url}{path}"
        self._log.debug("%s %s", method, url)
        try:
            resp = self._session().request(
                method,
                url,
                headers=self._headers(),
                json=json_body,
                timeout=_TIMEOUT,
            )
        except requests.ConnectionError as exc:
            raise ProviderError(f"Connection failed: {url} — {exc}") from exc
        except requests.Timeout as exc:
            raise ProviderError(f"Request timed out: {url}") from exc
        except requests.RequestException as exc:
            raise ProviderError(f"HTTP request failed: {url} — {exc}") from exc

        if resp.status_code >= 400:
            body = resp.text[:500]
            raise ProviderError(
                f"Entropy Data API error {resp.status_code} on {method} {path}: {body}"
            )
        return resp

    def _require_api_key(self) -> None:
        if not self.api_key:
            raise ProviderError(
                "DMM_API_KEY environment variable is required.\n"
                "Generate one at: https://app.entropy-data.com "
                "-> Organization -> Settings -> API Keys"
            )

    # ---- pure-helper delegates (preserve test surface) --------------------
    #
    # Each method below was a pure helper inline in this file before the
    # _mappers.py extraction. We keep the class-level surface (instance and
    # static methods) so that tests calling
    # ``DataMeshManagerProvider._extract_provider(section)`` or
    # ``provider._map_output_ports(fluid)`` continue to work unchanged.

    @staticmethod
    def _extract_id(fluid: Mapping[str, Any]) -> str:
        return _mappers.extract_id(fluid)

    @staticmethod
    def _derive_team_id(fluid: Mapping[str, Any]) -> str:
        return _mappers.derive_team_id(fluid)

    @staticmethod
    def _extract_provider(section: Mapping[str, Any]) -> str:
        return _mappers.extract_provider(section)

    @staticmethod
    def _resolve_location(section: Mapping[str, Any], provider: str) -> str:
        return _mappers.resolve_location(section, provider)

    @staticmethod
    def _build_server_object(section: Mapping[str, Any], provider: str) -> Dict[str, Any]:
        return _mappers.build_server_object(section, provider)

    @staticmethod
    def _extract_links(fluid: Mapping[str, Any]) -> Dict[str, str]:
        return _mappers.extract_links(fluid)

    @staticmethod
    def _extract_custom(fluid: Mapping[str, Any]) -> Dict[str, Any]:
        return _mappers.extract_custom(fluid)

    @staticmethod
    def _odcs_logical_type(fluid_type: str) -> str:
        return _mappers.odcs_logical_type(fluid_type)

    @staticmethod
    def _is_odps_spec(value: Optional[str]) -> bool:
        return _mappers.is_odps_spec(value)

    @staticmethod
    def _is_odps_payload(payload: Mapping[str, Any]) -> bool:
        return _mappers.is_odps_payload(payload)

    @staticmethod
    def _summarize_odcs_payload(odcs_body: Mapping[str, Any]) -> Dict[str, int]:
        return _mappers.summarize_odcs_payload(odcs_body)

    def _resolve_data_product_specification(
        self,
        value: Optional[str],
        *,
        provider_hint: Optional[str] = None,
        fluid: Optional[Mapping[str, Any]] = None,
    ) -> str:
        return _mappers.resolve_data_product_specification(
            value, provider_hint=provider_hint, fluid=fluid
        )

    def _map_input_ports(self, fluid: Mapping[str, Any]) -> List[Dict[str, Any]]:
        return _mappers.map_input_ports(fluid)

    def _map_output_ports(
        self, fluid: Mapping[str, Any], product_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        return _mappers.map_output_ports(fluid, product_id=product_id)

    def _to_data_product(
        self,
        fluid: Mapping[str, Any],
        *,
        data_product_specification: Optional[str] = None,
    ) -> Dict[str, Any]:
        return _mappers.to_data_product(
            fluid, data_product_specification=data_product_specification
        )

    def _to_data_product_odps(self, fluid: Mapping[str, Any]) -> Dict[str, Any]:
        return _mappers.to_data_product_odps(fluid)

    @staticmethod
    def _normalize_fluid_for_odps_standard(fluid: Mapping[str, Any]) -> Dict[str, Any]:
        return _mappers.normalize_fluid_for_odps_standard(fluid)

    def _build_data_contract_odcs(
        self, fluid: Mapping[str, Any], product_id: str
    ) -> Dict[str, Any]:
        return _mappers.build_data_contract_odcs(fluid, product_id)

    def _build_data_contract_dcs(
        self, fluid: Mapping[str, Any], product_id: str
    ) -> Dict[str, Any]:
        return _mappers.build_data_contract_dcs(fluid, product_id)
