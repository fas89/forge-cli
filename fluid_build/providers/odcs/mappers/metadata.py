# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Contract-level metadata mapper.

Owns the top-level ODCS scalars and descriptive blocks (``apiVersion``,
``kind``, ``id``, ``version``, ``status``, ``name``, ``description``, ``tags``,
``domain``, ``tenant``, ``dataProduct``, ``customProperties``,
``authoritativeDefinitions``, ``support``, ``price``, ``roles``,
``slaDefaultElement``, ``contractCreatedTs``, and the v3.2.0 ``context``).
The section keys owned by sibling mappers (``schema``, ``servers``,
``slaProperties``, ``team``, ``quality``) are excluded from this mapper's
verbatim stash.

``apiVersion`` on export resolves in this order:

1. ``ExportCtx.options["api_version"]`` — explicit (CLI / constructor / env).
2. The imported document's own ``apiVersion`` (verbatim stash) — a v3.0.0
   contract re-exports as v3.0.0, a v3.2.0 one as v3.2.0.
3. :data:`~fluid_build.providers.odcs.validation.DEFAULT_API_VERSION`.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict, Optional

from fluid_build.providers.base import ProviderError

from ..validation import DEFAULT_API_VERSION, normalize_api_version
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
from .types import fluid_to_odcs_status, odcs_to_fluid_status

ODCS_KIND = "DataContract"

# Top-level keys owned by sibling mappers — never stashed here.
SECTION_KEYS = frozenset({"schema", "servers", "slaProperties", "team", "quality"})


# ----- ODCS → FLUID --------------------------------------------------------


def _project_metadata(odcs: Mapping[str, Any]) -> Dict[str, Any]:
    """Pure projection of the top-level ODCS scalars into FLUID ``metadata``."""
    metadata: Dict[str, Any] = {
        "version": odcs.get("version", "1.0.0"),
        "name": odcs.get("name", odcs.get("id")),
        "status": odcs_to_fluid_status(odcs.get("status", "active")),
    }
    description = odcs.get("description")
    if isinstance(description, Mapping):
        if description.get("purpose"):
            metadata["description"] = description["purpose"]
    elif isinstance(description, str) and description:
        metadata["description"] = description
    if odcs.get("tags"):
        metadata["tags"] = list(odcs["tags"])
    for key in ("domain", "tenant", "dataProduct"):
        if odcs.get(key):
            metadata[key] = odcs[key]
    return metadata


def to_fluid(ctx: ImportCtx) -> None:
    odcs = ctx.odcs
    fluid = ctx.fluid

    metadata = fluid.setdefault("metadata", {})
    metadata.update(_project_metadata(odcs))

    fluid.setdefault("contract", {})["id"] = odcs.get("id")
    fluid.setdefault("exposes", [])
    fluid.setdefault("expects", [])

    stash(
        metadata_passthrough(fluid),
        {k: v for k, v in odcs.items() if k not in SECTION_KEYS},
    )


# ----- FLUID → ODCS --------------------------------------------------------


def _native_metadata(
    metadata: Mapping[str, Any], contract_id: Optional[str], raw_status: Any
) -> Dict[str, Any]:
    """Total projection of FLUID-native top-level fields to ODCS keys."""
    tags = metadata.get("tags")
    return {
        "version": metadata.get("version", "1.0.0"),
        "id": contract_id,
        "status": fluid_to_odcs_status(raw_status),
        "name": metadata.get("name") or None,
        "description": (
            {"purpose": metadata["description"]} if metadata.get("description") else None
        ),
        "tags": list(tags) if tags else None,
        "domain": metadata.get("domain") or None,
        "tenant": metadata.get("tenant") or None,
        "dataProduct": metadata.get("dataProduct") or None,
    }


def _resolve_api_version(ctx: ExportCtx, st: Optional[Mapping[str, Any]]) -> str:
    explicit = ctx.options.get("api_version")
    if explicit:
        return normalize_api_version(explicit) or DEFAULT_API_VERSION
    if st and st.get("apiVersion"):
        return str(st["apiVersion"])
    return DEFAULT_API_VERSION


def to_odcs(ctx: ExportCtx) -> None:
    fluid = ctx.fluid
    odcs = ctx.odcs
    metadata = fluid.get("metadata") or {}

    contract_id = fluid.get("_scoped_id") or fluid_id(fluid)
    if not contract_id:
        raise ProviderError(
            "Contract missing required 'id' field. "
            "Expected one of: fluid['id'], fluid['contract']['id'], or fluid['metadata']['id']"
        )
    raw_status = fluid.get("_scoped_status") or metadata.get("status", "active")

    st = get_stash(get_metadata_passthrough(fluid))
    now = _native_metadata(metadata, contract_id, raw_status)

    if st is not None:
        then = _native_metadata(
            _project_metadata(st), st.get("id"), _project_metadata(st)["status"]
        )
        merged = overlay_native(st, then, now)
        # ``description`` is an object in ODCS but a string in FLUID: an edit
        # to the purpose must not discard the stashed limitations/usage.
        if (
            now["description"] != then["description"]
            and now["description"] is not None
            and isinstance(st.get("description"), Mapping)
        ):
            merged["description"] = {**st["description"], **now["description"]}
    else:
        merged = {k: v for k, v in now.items() if v is not None}

    merged["apiVersion"] = _resolve_api_version(ctx, st)
    merged["kind"] = ODCS_KIND

    # Canonical head ordering for readable YAML; remaining keys keep their
    # stashed order (dict comparison is order-insensitive either way).
    head = ("version", "apiVersion", "kind", "id", "status", "name")
    for key in head:
        if key in merged:
            odcs[key] = merged[key]
    for key, value in merged.items():
        if key not in head:
            odcs[key] = value
