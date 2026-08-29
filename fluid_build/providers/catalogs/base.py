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

"""Base catalog provider for marketplace integrations.

The primary API is :meth:`BaseCatalogProvider.publish`, which accepts a raw
FLUID contract dict plus a thin :class:`CatalogTarget` descriptor. The old
:class:`CatalogAsset` DTO is kept as a deprecated builder for callers that
still construct one by hand.
"""

import logging
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class CatalogTarget:
    """Thin publish descriptor — the FLUID dict is the payload, this carries
    identity + optional per-catalog hints.

    Providers are expected to read structural information (exposes, schema,
    owner, tags, etc.) from the FLUID dict passed alongside this descriptor.
    The descriptor itself only names the target and offers a couple of
    convenience fields that would otherwise force every provider to
    re-derive them.
    """

    contract_id: str
    contract_name: str = ""
    contract_yaml: Optional[str] = None
    catalog_hints: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_fluid(
        cls,
        fluid: Dict[str, Any],
        *,
        contract_yaml: Optional[str] = None,
        catalog_hints: Optional[Dict[str, Any]] = None,
    ) -> "CatalogTarget":
        """Build a target descriptor from a FLUID dict.

        ``contract_id`` falls back to the top-level ``name`` and then to
        ``metadata.name`` so a FLUID document that omits the id (rare, but
        legal) still produces a usable descriptor.
        """
        metadata = fluid.get("metadata") or {}
        contract_id = (
            fluid.get("id")
            or fluid.get("name")
            or metadata.get("name")
            or "unknown"
        )
        contract_name = (
            fluid.get("name")
            or metadata.get("name")
            or fluid.get("id")
            or "unknown"
        )
        return cls(
            contract_id=str(contract_id),
            contract_name=str(contract_name),
            contract_yaml=contract_yaml,
            catalog_hints=dict(catalog_hints or {}),
        )


@dataclass
class CatalogAsset:
    """DEPRECATED: flat asset DTO retained for one release for back-compat.

    New code should call :meth:`BaseCatalogProvider.publish` with the raw
    FLUID dict + a :class:`CatalogTarget`. This class remains so external
    callers that build a CatalogAsset by hand continue to work through the
    :meth:`BaseCatalogProvider.publish_asset` shim.
    """

    id: str
    name: str
    description: str
    type: str
    domain: str
    owner: str
    owner_email: str
    layer: str
    tags: List[str]
    version: str
    platform: str
    location: Dict[str, Any]
    schema: Optional[List[Dict[str, Any]]] = None
    sensitivity: str = "internal"
    contract_yaml: Optional[str] = None
    # The full FLUID dict — set by :meth:`BaseCatalogProvider.map_contract_to_asset`
    # so the deprecated ``publish_asset`` shim can round-trip the original
    # payload rather than re-synthesising it from the flat fields.
    raw_contract: Optional[Dict[str, Any]] = None


@dataclass
class PublishResult:
    """Result of a catalog publish operation."""

    success: bool
    catalog_id: str
    asset_id: str
    catalog_url: Optional[str] = None
    error: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)


class BaseCatalogProvider(ABC):
    """Base class for all catalog integrations.

    Subclasses implement :meth:`publish`, :meth:`update`, :meth:`verify`.
    The base offers a default :meth:`validate_target` and back-compat shims
    (``publish_asset``, ``update_asset``, ``map_contract_to_asset``,
    ``validate_asset``) for the deprecated CatalogAsset flow.
    """

    name: str = "base"

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.endpoint = config.get("endpoint", config.get("url", ""))
        self.auth = config.get("auth", {})
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.max_retries = config.get("max_retries", 3)
        self.timeout = config.get("timeout", 30.0)

    # -- primary API --------------------------------------------------------

    @abstractmethod
    async def publish(
        self, fluid: Dict[str, Any], target: CatalogTarget
    ) -> PublishResult:
        """Publish a FLUID contract to the catalog.

        Args:
            fluid: The raw FLUID contract dict — providers should treat this
                as the source of truth for schema, output ports, metadata,
                owner, etc.
            target: Identity + per-catalog hints for this publish call.

        Returns:
            PublishResult with success/failure details.
        """
        ...

    @abstractmethod
    async def update(
        self, fluid: Dict[str, Any], target: CatalogTarget
    ) -> PublishResult:
        """Update an existing asset in the catalog.

        Same contract as :meth:`publish`; most providers treat PUT as
        idempotent and simply delegate to publish.
        """
        ...

    @abstractmethod
    async def verify(self, contract_id: str) -> bool:
        """Verify an asset with this contract id exists in the catalog."""
        ...

    async def delete(self, contract_id: str) -> bool:
        """Delete an asset from the catalog (optional)."""
        self.logger.warning(f"Delete not implemented for {self.name}")
        return False

    def validate_target(
        self, fluid: Dict[str, Any], target: CatalogTarget
    ) -> Tuple[bool, Optional[str]]:
        """Validate a (fluid, target) pair before publishing."""
        if not target.contract_id:
            return False, "contract id is required"
        if not target.contract_name:
            return False, "contract name is required"
        metadata = fluid.get("metadata") or {}
        owner = metadata.get("owner") or fluid.get("owner") or {}
        if not (owner.get("team") or owner.get("name")):
            return False, "contract owner is required"
        domain = fluid.get("domain") or metadata.get("domain")
        if not domain:
            return False, "contract domain is required"
        return True, None

    async def health_check(self) -> bool:
        """Check whether the catalog endpoint is reachable."""
        try:
            return True
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return False

    # -- deprecated CatalogAsset shim --------------------------------------

    def map_contract_to_asset(self, contract: Dict[str, Any]) -> CatalogAsset:
        """DEPRECATED: build a flat CatalogAsset from a FLUID contract.

        Prefer :meth:`CatalogTarget.from_fluid` and :meth:`publish` — the
        flat asset representation loses multi-port detail and forces every
        provider to reconstruct the FLUID dict.
        """
        warnings.warn(
            "map_contract_to_asset() and CatalogAsset are deprecated; "
            "call publish(fluid, CatalogTarget.from_fluid(fluid)) instead. "
            "This shim will be removed in a future release.",
            DeprecationWarning,
            stacklevel=2,
        )
        return _build_catalog_asset_from_fluid(contract)

    def validate_asset(
        self, asset: CatalogAsset
    ) -> Tuple[bool, Optional[str]]:
        """DEPRECATED: validate a flat CatalogAsset."""
        if not asset.name:
            return False, "Asset name is required"
        if not asset.id:
            return False, "Asset ID is required"
        if not asset.owner:
            return False, "Asset owner is required"
        if not asset.domain:
            return False, "Asset domain is required"
        return True, None

    async def publish_asset(self, asset: CatalogAsset) -> PublishResult:
        """DEPRECATED: publish via the flat CatalogAsset DTO.

        Reconstructs a ``(fluid, target)`` pair from the asset and forwards
        to :meth:`publish`. Emits a DeprecationWarning.
        """
        warnings.warn(
            "publish_asset(CatalogAsset) is deprecated; "
            "call publish(fluid, target) with the raw FLUID dict instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        fluid = asset.raw_contract if asset.raw_contract else _minimal_fluid_from_asset(asset)
        target = CatalogTarget(
            contract_id=asset.id,
            contract_name=asset.name,
            contract_yaml=asset.contract_yaml,
        )
        return await self.publish(fluid, target)

    async def update_asset(self, asset: CatalogAsset) -> PublishResult:
        """DEPRECATED: update via the flat CatalogAsset DTO."""
        warnings.warn(
            "update_asset(CatalogAsset) is deprecated; "
            "call update(fluid, target) with the raw FLUID dict instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        fluid = asset.raw_contract if asset.raw_contract else _minimal_fluid_from_asset(asset)
        target = CatalogTarget(
            contract_id=asset.id,
            contract_name=asset.name,
            contract_yaml=asset.contract_yaml,
        )
        return await self.update(fluid, target)


# ---------------------------------------------------------------------------
# Module-level helpers used by both the deprecated CatalogAsset shim and
# any provider that still wants a flat summary for logging / rendering.
# ---------------------------------------------------------------------------


def _build_catalog_asset_from_fluid(contract: Dict[str, Any]) -> CatalogAsset:
    """Flatten a FLUID contract into the legacy :class:`CatalogAsset` shape.

    Kept module-private so it can be reused by test helpers and the
    deprecation shim without going through the public warning path.
    """
    metadata = contract.get("metadata") or {}
    owner = metadata.get("owner") or contract.get("owner") or {}
    exposes = contract.get("exposes") or []

    platform = "unknown"
    location: Dict[str, Any] = {}
    schema: Optional[List[Dict[str, Any]]] = None
    sensitivity = "internal"

    if exposes:
        first_expose = exposes[0]
        binding = first_expose.get("binding") or {}
        platform = binding.get("platform", "unknown")
        location = binding.get("location") or {}
        contract_spec = first_expose.get("contract") or {}
        schema = contract_spec.get("schema")
        sensitivity = first_expose.get("sensitivity", "internal")

    return CatalogAsset(
        id=contract.get("id", contract.get("name", "unknown")),
        name=contract.get("name") or contract.get("id") or "unknown",
        description=contract.get("description", ""),
        type=(contract.get("kind") or "DataProduct").lower(),
        domain=contract.get("domain") or metadata.get("domain") or "general",
        owner=owner.get("team", owner.get("name", "unknown")),
        owner_email=owner.get("email", ""),
        layer=metadata.get("layer", "Bronze"),
        tags=metadata.get("tags", []),
        version=contract.get("version", "1.0.0"),
        platform=platform,
        location=location,
        schema=schema,
        sensitivity=sensitivity,
        raw_contract=dict(contract),
    )


def _minimal_fluid_from_asset(asset: CatalogAsset) -> Dict[str, Any]:
    """Reconstruct a minimal FLUID dict from a flat CatalogAsset.

    Used only when a deprecated caller invokes :meth:`publish_asset` with an
    asset whose ``raw_contract`` was never populated (older external
    integrations, hand-built assets in tests). Not lossless — the whole
    reason for the redesign is to avoid this path.
    """
    fluid: Dict[str, Any] = {
        "id": asset.id,
        "name": asset.name,
        "description": asset.description,
        "kind": (asset.type or "DataProduct").title().replace(" ", ""),
        "metadata": {
            "name": asset.name,
            "description": asset.description,
            "domain": asset.domain,
            "version": asset.version,
            "tags": list(asset.tags or []),
            "layer": asset.layer,
            "status": "active",
            "owner": {
                "team": asset.owner,
                "email": asset.owner_email,
            },
        },
        "owner": {
            "team": asset.owner,
            "email": asset.owner_email,
        },
        "domain": asset.domain,
    }
    if asset.location or asset.platform != "unknown":
        expose: Dict[str, Any] = {
            "id": asset.id,
            "binding": {"platform": asset.platform},
        }
        if asset.location:
            expose["binding"]["location"] = asset.location
        if asset.schema is not None:
            expose["contract"] = {"schema": asset.schema}
        if asset.sensitivity:
            expose["sensitivity"] = asset.sensitivity
        fluid["exposes"] = [expose]
    return fluid
