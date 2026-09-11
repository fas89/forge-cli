# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Product top-level fields: apiVersion, kind, id, name, version, status,
description, domain, tenant, tags, productCreatedTs, customProperties,
authoritativeDefinitions, and the v1.1.0 ``type``/``deprecated``/``context``/
``synonyms``.

The Bitol ODPS spec requires ``apiVersion``, ``kind``, ``id`` (``status`` was
also required before v1.1.0; we always emit it).

Lossless by construction — the whole top-level document (minus the sections
owned by sibling mappers) is stashed verbatim on import and used as the
export base layer; see :mod:`fluid_build.providers.odcs.mappers.base`.

``apiVersion`` on export: explicit (``ExportCtx.options["api_version"]``) >
imported document's own > :data:`DEFAULT_API_VERSION`.

Product ``type`` (``sourceAligned`` / ``aggregate`` / ``consumerAligned`` …)
is a first-class field from v1.1.0. Fresh exports targeting ≥ v1.1.0 emit it
natively from ``metadata.product_type``; fresh v1.0.0 exports keep the legacy
``customProperties: [{property: type, value: …}]`` encoding so existing
consumers see no change.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict, Optional

from fluid_build.providers.base import ProviderError

from ..validation import DEFAULT_API_VERSION, api_version_tuple, normalize_api_version
from .base import (
    ExportCtx,
    ImportCtx,
    fluid_id,
    get_metadata_passthrough,
    get_stash,
    metadata_passthrough,
    overlay_native,
    stash,
)
from .types import bitol_to_fluid_status, fluid_to_bitol_status

KIND = "DataProduct"

# Top-level keys owned by sibling mappers — never stashed here.
SECTION_KEYS = frozenset({"outputPorts", "inputPorts", "team", "support", "managementPorts"})

_NATIVE_TYPE_MIN = (1, 1, 0)


# ----- ODPS → FLUID --------------------------------------------------------


def _project_product(odps: Mapping[str, Any]) -> Dict[str, Any]:
    """Pure projection of top-level ODPS scalars into FLUID ``metadata``."""
    metadata: Dict[str, Any] = {
        "version": odps.get("version", "1.0.0"),
        "name": odps.get("name", odps.get("id")),
        "status": bitol_to_fluid_status(odps.get("status", "draft")),
    }
    description = odps.get("description")
    if isinstance(description, Mapping):
        if description.get("purpose"):
            metadata["description"] = description["purpose"]
    elif isinstance(description, str) and description:
        metadata["description"] = description
    for key in ("domain", "tenant"):
        if odps.get(key):
            metadata[key] = odps[key]
    if odps.get("tags"):
        metadata["tags"] = list(odps["tags"])
    if odps.get("type"):
        metadata["product_type"] = odps["type"]
    if "deprecated" in odps:
        metadata["deprecated"] = bool(odps["deprecated"])
    return metadata


def to_fluid(ctx: ImportCtx) -> None:
    odps = ctx.odps
    fluid = ctx.fluid

    metadata = fluid.setdefault("metadata", {})
    metadata.update(_project_product(odps))

    fluid.setdefault("contract", {})["id"] = odps.get("id")
    fluid.setdefault("exposes", [])
    fluid.setdefault("expects", [])

    stash(
        metadata_passthrough(fluid),
        {k: v for k, v in odps.items() if k not in SECTION_KEYS},
    )


# ----- FLUID → ODPS --------------------------------------------------------


def _product_type(fluid: Mapping[str, Any]) -> Optional[str]:
    metadata = fluid.get("metadata") or {}
    return (
        metadata.get("product_type")
        or metadata.get("type")
        or fluid.get("product_type")
        or fluid.get("type")
        or None
    )


def _native_product(
    fluid: Mapping[str, Any],
    metadata: Mapping[str, Any],
    product_id: Optional[str],
    *,
    native_type: bool,
) -> Dict[str, Any]:
    """Total projection of FLUID-native top-level fields to ODPS keys."""
    tags = metadata.get("tags") or fluid.get("tags")
    return {
        "id": product_id,
        "name": metadata.get("name") or fluid.get("name") or None,
        "version": str(metadata.get("version", "1.0.0")),
        "status": fluid_to_bitol_status(metadata.get("status", "draft")),
        "description": (
            {"purpose": metadata["description"]} if metadata.get("description") else None
        ),
        "domain": metadata.get("domain") or None,
        "tenant": metadata.get("tenant") or None,
        "tags": list(tags) if tags else None,
        "type": _product_type(fluid) if native_type else None,
        "deprecated": metadata.get("deprecated") if "deprecated" in metadata else None,
    }


def _resolve_api_version(ctx: ExportCtx, st: Optional[Mapping[str, Any]]) -> str:
    explicit = ctx.options.get("api_version")
    if explicit:
        return normalize_api_version(explicit) or DEFAULT_API_VERSION
    if st and st.get("apiVersion"):
        return str(st["apiVersion"])
    return DEFAULT_API_VERSION


def to_odps(ctx: ExportCtx) -> None:
    fluid = ctx.fluid
    odps = ctx.odps
    metadata = fluid.get("metadata") or {}

    product_id = fluid_id(fluid)
    if not product_id:
        raise ProviderError("Contract missing required 'id' field for ODPS export")
    if not (metadata.get("name") or fluid.get("name")):
        raise ProviderError(
            "Contract missing required 'name' field. "
            "Set it at the contract root or under metadata.name."
        )

    st = get_stash(get_metadata_passthrough(fluid))
    api_version = _resolve_api_version(ctx, st)
    native_type = api_version_tuple(api_version) >= _NATIVE_TYPE_MIN

    now = _native_product(fluid, metadata, product_id, native_type=native_type)

    if st is not None:
        then_meta = _project_product(st)
        then = _native_product({"metadata": then_meta}, then_meta, st.get("id"), native_type=native_type)
        merged = overlay_native(st, then, now)
        if (
            now["description"] != then["description"]
            and now["description"] is not None
            and isinstance(st.get("description"), Mapping)
        ):
            merged["description"] = {**st["description"], **now["description"]}
    else:
        merged = {k: v for k, v in now.items() if v is not None}
        if not native_type:
            # Legacy (< v1.1.0) encoding of the product type.
            product_type = _product_type(fluid)
            if product_type:
                merged["customProperties"] = [{"property": "type", "value": product_type}]
        if ctx.options.get("include_custom_properties", True) is False:
            merged.pop("customProperties", None)

    merged["apiVersion"] = api_version
    merged["kind"] = KIND

    head = ("apiVersion", "kind", "id", "name", "version", "status")
    for key in head:
        if key in merged:
            odps[key] = merged[key]
    for key, value in merged.items():
        if key not in head:
            odps[key] = value
