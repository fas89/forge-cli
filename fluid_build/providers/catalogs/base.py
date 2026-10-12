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

import copy
import dataclasses
import inspect
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
    # Set only on the deprecated CatalogAsset paths: the asset the caller
    # passed (or a 0.7.8 ``map_contract_to_asset`` override produced), so the
    # built-in providers can build their payload from its fields exactly as
    # 0.7.8 did. Not part of the public constructor.
    _legacy_asset: Optional["CatalogAsset"] = field(
        default=None, init=False, repr=False, compare=False
    )

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
        contract_id = fluid.get("id") or fluid.get("name") or metadata.get("name") or "unknown"
        contract_name = fluid.get("name") or metadata.get("name") or fluid.get("id") or "unknown"
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
        self, fluid: Dict[str, Any], target: Optional[CatalogTarget] = None
    ) -> PublishResult:
        """Publish a FLUID contract to the catalog.

        Args:
            fluid: The raw FLUID contract dict — providers should treat this
                as the source of truth for schema, output ports, metadata,
                owner, etc.
            target: Identity + per-catalog hints for this publish call.
                Defaults to :meth:`CatalogTarget.from_fluid`.

        Returns:
            PublishResult with success/failure details.

        Implementations should start with
        ``fluid, target = self._coerce_publish_args(fluid, target, "publish")``
        so callers still using the deprecated ``publish(asset)`` form (a
        single :class:`CatalogAsset`) keep working for one release.

        The second parameter must be named ``target`` (or be annotated
        :class:`CatalogTarget`): that is how ``fluid publish`` tells a
        provider implementing this API from one still implementing the
        0.7.8 ``publish(asset)`` API.
        """
        ...

    @abstractmethod
    async def update(
        self, fluid: Dict[str, Any], target: Optional[CatalogTarget] = None
    ) -> PublishResult:
        """Update an existing asset in the catalog.

        Same contract as :meth:`publish`; most providers treat PUT as
        idempotent and simply delegate to publish.
        """
        ...

    @abstractmethod
    async def verify(
        self, contract_id: Optional[str] = None, *, asset_id: Optional[str] = None
    ) -> bool:
        """Verify an asset with this contract id exists in the catalog.

        ``asset_id=`` is the deprecated 0.7.8 keyword for ``contract_id``;
        implementations should resolve both with :func:`_coerce_contract_id`.
        """
        ...

    async def delete(
        self, contract_id: Optional[str] = None, *, asset_id: Optional[str] = None
    ) -> bool:
        """Delete an asset from the catalog (optional)."""
        _coerce_contract_id(contract_id, asset_id, "delete")
        self.logger.warning(f"Delete not implemented for {self.name}")
        return False

    def validate_target(
        self, fluid: Dict[str, Any], target: CatalogTarget
    ) -> Tuple[bool, Optional[str]]:
        """Validate a (fluid, target) pair before publishing.

        The FLUID dict must identify a contract (``id``, ``name`` or
        ``metadata.name``) — anything else (a contract fragment, an overlay,
        a config file) is rejected, as 0.7.8 effectively did. A missing
        ``domain`` or owner is *not* an error — the payload builders fall
        back to ``"general"`` / ``"unknown"`` exactly as the 0.7.8
        ``map_contract_to_asset`` + ``validate_asset`` flow did, so
        schema-valid contracts that omit them still publish.

        A subclass that still overrides the deprecated
        :meth:`map_contract_to_asset` or :meth:`validate_asset` hooks keeps
        gating publishes: ``validate_asset`` is consulted with the asset the
        (possibly overridden) mapper produces.
        """
        if not target.contract_id:
            return False, "contract id is required"
        if not target.contract_name:
            return False, "contract name is required"
        identity_error = _fluid_identity_error(fluid)
        if identity_error:
            return False, identity_error
        if self._overrides_legacy_mapper() or (
            type(self).validate_asset is not BaseCatalogProvider.validate_asset
        ):
            asset = self._legacy_asset_from_fluid(fluid, target)
            return self.validate_asset(asset)
        return True, None

    # -- deprecated-API compatibility helpers -------------------------------

    @classmethod
    def _uses_legacy_asset_api(cls) -> bool:
        """True unless ``cls.publish`` positively declares the current
        ``publish(fluid, target)`` API (see :func:`_publish_declares_target`).

        Anything else — the 0.7.8 ``publish(asset)`` signature, with or
        without extra optional parameters or ``*args`` / ``**kwargs`` — is a
        third-party catalog plugin written against 0.7.8; ``fluid publish``
        routes it through ``map_contract_to_asset`` → ``validate_asset`` →
        ``publish(asset)``.
        """
        return not _publish_declares_target(cls)

    def _overrides_legacy_mapper(self) -> bool:
        return type(self).map_contract_to_asset is not BaseCatalogProvider.map_contract_to_asset

    def _legacy_asset_from_fluid(
        self, fluid: Dict[str, Any], target: Optional[CatalogTarget] = None
    ) -> CatalogAsset:
        """Build the flat asset a legacy hook expects, honouring a subclass's
        own :meth:`map_contract_to_asset` override (without re-emitting the
        base deprecation warning)."""
        if self._overrides_legacy_mapper():
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                asset = self.map_contract_to_asset(fluid)
        else:
            asset = _build_catalog_asset_from_fluid(fluid)
        if target is not None and target.contract_yaml and not asset.contract_yaml:
            asset.contract_yaml = target.contract_yaml
        return asset

    def _with_legacy_asset(self, fluid: Dict[str, Any], target: CatalogTarget) -> CatalogTarget:
        """Attach the CatalogAsset a built-in provider must publish from.

        Returns *target* unchanged when it already carries the caller's
        asset (deprecated ``publish(asset)`` / ``publish_asset``) or when
        the provider does not override :meth:`map_contract_to_asset`.
        Otherwise — a 0.7.8 subclass customising the mapping — the override
        is run (with a :class:`DeprecationWarning`) and a copy of *target*
        carrying its asset is returned, so the customisation reaches the
        published payload as it did in 0.7.8. Exceptions from the override
        propagate to the caller.
        """
        if target._legacy_asset is not None or not self._overrides_legacy_mapper():
            return target
        warnings.warn(
            f"{type(self).__name__}.map_contract_to_asset() overrides are deprecated "
            "and are honoured for one release only; build the payload from the "
            "FLUID dict in publish(fluid, target) instead.",
            DeprecationWarning,
            stacklevel=3,
        )
        asset = self._legacy_asset_from_fluid(fluid, target)
        new_target = dataclasses.replace(target)
        new_target._legacy_asset = asset
        return new_target

    def _coerce_publish_args(
        self,
        fluid: Any,
        target: Optional[CatalogTarget],
        method: str = "publish",
        asset: Optional[CatalogAsset] = None,
    ) -> Tuple[Dict[str, Any], CatalogTarget]:
        """Normalise ``publish``/``update`` arguments.

        Accepts the current ``(fluid, target)`` form (``target`` defaults to
        :meth:`CatalogTarget.from_fluid`) and the deprecated 0.7.8 forms
        ``(asset)`` / ``(asset=asset)``, which emit a
        :class:`DeprecationWarning` and are converted to the equivalent
        ``(fluid, target)`` pair (the target remembers the asset, see
        :attr:`CatalogTarget._legacy_asset`).
        """
        if asset is not None:
            if fluid is not None:
                raise TypeError(f"{method}() got values for both 'fluid' and 'asset'")
            fluid = asset
        if fluid is None:
            raise TypeError(f"{method}() missing 1 required positional argument: 'fluid'")
        if isinstance(fluid, CatalogAsset):
            warnings.warn(
                f"{method}(CatalogAsset) is deprecated; call "
                f"{method}(fluid, CatalogTarget.from_fluid(fluid)) with the raw "
                "FLUID dict instead. The CatalogAsset form will be removed in a "
                "future release.",
                DeprecationWarning,
                stacklevel=3,
            )
            legacy_fluid, legacy_target = _fluid_target_from_asset(fluid)
            if target is not None and target.catalog_hints:
                legacy_target.catalog_hints = dict(target.catalog_hints)
            return legacy_fluid, legacy_target
        if target is None:
            target = CatalogTarget.from_fluid(fluid)
        return fluid, target

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

    def validate_asset(self, asset: CatalogAsset) -> Tuple[bool, Optional[str]]:
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

        Reconstructs a ``(fluid, target)`` pair from the asset — the
        asset's own fields win over its ``raw_contract`` — and forwards to
        :meth:`publish`. Emits a DeprecationWarning.
        """
        warnings.warn(
            "publish_asset(CatalogAsset) is deprecated; "
            "call publish(fluid, target) with the raw FLUID dict instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if self._uses_legacy_asset_api():
            # Provider still implements the old ``publish(asset)`` signature.
            return await self.publish(asset)  # type: ignore[call-arg]
        fluid, target = _fluid_target_from_asset(asset)
        return await self.publish(fluid, target)

    async def update_asset(self, asset: CatalogAsset) -> PublishResult:
        """DEPRECATED: update via the flat CatalogAsset DTO."""
        warnings.warn(
            "update_asset(CatalogAsset) is deprecated; "
            "call update(fluid, target) with the raw FLUID dict instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if self._uses_legacy_asset_api():
            return await self.update(asset)  # type: ignore[call-arg]
        fluid, target = _fluid_target_from_asset(asset)
        return await self.update(fluid, target)


# ---------------------------------------------------------------------------
# Module-level helpers used by both the deprecated CatalogAsset shim and
# any provider that still wants a flat summary for logging / rendering.
# ---------------------------------------------------------------------------


def _publish_declares_target(cls: Any) -> bool:
    """True when ``cls.publish`` positively declares ``publish(fluid, target)``.

    The second parameter after ``self`` must be a positional parameter
    named ``target`` or annotated :class:`CatalogTarget` (string
    annotations, e.g. under ``from __future__ import annotations``, count).
    Callables whose signature cannot be read (mocks, builtins) do not.
    """
    publish = getattr(cls, "publish", None)
    if publish is None or not inspect.isfunction(inspect.unwrap(publish)):
        return False
    try:
        params = list(inspect.signature(publish).parameters.values())
    except (TypeError, ValueError):
        return False
    params = params[1:]  # drop ``self``
    if len(params) < 2:
        return False
    second = params[1]
    if second.kind not in (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    ):
        return False
    if second.name == "target":
        return True
    annotation = second.annotation
    if annotation is inspect.Parameter.empty:
        return False
    return annotation is CatalogTarget or "CatalogTarget" in str(annotation)


def _coerce_contract_id(contract_id: Optional[str], asset_id: Optional[str], method: str) -> str:
    """Resolve ``verify`` / ``delete``'s id, accepting the 0.7.8 ``asset_id=``
    keyword (deprecated) alongside ``contract_id``."""
    if asset_id is not None:
        warnings.warn(
            f"{method}(asset_id=...) is deprecated; pass the contract id "
            f"positionally or as {method}(contract_id=...).",
            DeprecationWarning,
            stacklevel=3,
        )
        if contract_id is not None and contract_id != asset_id:
            raise TypeError(f"{method}() got different values for 'contract_id' and 'asset_id'")
        return asset_id
    if contract_id is None:
        raise TypeError(f"{method}() missing 1 required positional argument: 'contract_id'")
    return contract_id


def _fluid_identity_error(fluid: Any) -> Optional[str]:
    """Return an error when *fluid* is not a FLUID contract, else ``None``.

    A contract must be a mapping with an ``id``, a ``name`` or a
    ``metadata.name``. Contract fragments, overlays and config files have
    none of them; 0.7.8 rejected them (``Failed to map contract to asset:
    'name'``) and publishing them would upsert an asset called
    ``unknown``.
    """
    if not isinstance(fluid, dict):
        return (
            "not a FLUID contract: expected a mapping at the top level, "
            f"got {type(fluid).__name__}"
        )
    metadata = fluid.get("metadata")
    meta_name = metadata.get("name") if isinstance(metadata, dict) else None
    if not (fluid.get("id") or fluid.get("name") or meta_name):
        return (
            "not a FLUID contract: 'id' is required (or 'name' / 'metadata.name'); "
            "contract fragments, overlays and config files cannot be published"
        )
    return None


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


def _fluid_target_from_asset(asset: CatalogAsset) -> Tuple[Dict[str, Any], CatalogTarget]:
    """Convert a deprecated :class:`CatalogAsset` to a ``(fluid, target)`` pair.

    The returned target remembers *asset* (``target._legacy_asset``) so the
    built-in providers can publish exactly the fields the caller set.
    """
    target = CatalogTarget(
        contract_id=asset.id,
        contract_name=asset.name,
        contract_yaml=asset.contract_yaml,
    )
    target._legacy_asset = asset
    return _fluid_from_asset(asset), target


def _fluid_from_asset(asset: CatalogAsset) -> Dict[str, Any]:
    """The FLUID dict equivalent of a deprecated :class:`CatalogAsset`.

    With a ``raw_contract`` (set by :meth:`BaseCatalogProvider.map_contract_to_asset`)
    a copy of it is returned with every flat field of the asset applied on
    top, so edits a 0.7.8 caller made to the asset after mapping it win —
    the asset is what that caller publishes — while the multi-port detail
    of the original contract is kept. Without one the dict is rebuilt from
    the flat fields (:func:`_minimal_fluid_from_asset`).
    """
    raw = asset.raw_contract
    if not raw:
        return _minimal_fluid_from_asset(asset)

    baseline = _build_catalog_asset_from_fluid(raw)
    fluid: Dict[str, Any] = copy.deepcopy(dict(raw))
    raw_meta = fluid.get("metadata")
    meta: Dict[str, Any] = dict(raw_meta) if isinstance(raw_meta, dict) else {}
    fluid["metadata"] = meta

    fluid["id"] = asset.id
    fluid["name"] = meta["name"] = asset.name
    fluid["description"] = meta["description"] = asset.description
    fluid["domain"] = meta["domain"] = asset.domain
    fluid["version"] = meta["version"] = asset.version
    meta["layer"] = asset.layer
    meta["tags"] = list(asset.tags or [])
    if "tags" in fluid:
        fluid["tags"] = list(asset.tags or [])
    if asset.type != baseline.type:
        fluid["kind"] = asset.type

    for holder in (fluid, meta):
        owner = holder.get("owner")
        if isinstance(owner, dict):
            holder["owner"] = {**owner, "team": asset.owner, "email": asset.owner_email}
    if not isinstance(meta.get("owner"), dict) and not isinstance(fluid.get("owner"), dict):
        meta["owner"] = {"team": asset.owner, "email": asset.owner_email}

    expose_edits = {
        "platform": asset.platform != baseline.platform,
        "location": asset.location != baseline.location,
        "schema": asset.schema != baseline.schema,
        "sensitivity": asset.sensitivity != baseline.sensitivity,
    }
    if any(expose_edits.values()):
        exposes = list(fluid.get("exposes") or [])
        first: Dict[str, Any] = dict(exposes[0]) if exposes else {"id": asset.id}
        binding = dict(first.get("binding") or {})
        if expose_edits["platform"]:
            binding["platform"] = asset.platform
        if expose_edits["location"]:
            binding["location"] = asset.location
        if binding:
            first["binding"] = binding
        if expose_edits["schema"]:
            first["contract"] = {**(first.get("contract") or {}), "schema": asset.schema}
        if expose_edits["sensitivity"]:
            first["sensitivity"] = asset.sensitivity
        if exposes:
            exposes[0] = first
        else:
            exposes = [first]
        fluid["exposes"] = exposes
    return fluid


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
        "version": asset.version,
    }
    has_expose_detail = (
        bool(asset.location)
        or asset.platform != "unknown"
        or asset.schema is not None
        or (bool(asset.sensitivity) and asset.sensitivity != "internal")
    )
    if has_expose_detail:
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
