# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Schema mapper.

ODCS ``schema`` is an array of :class:`SchemaObject` whose ``properties[]``
carry the field definitions. Each SchemaObject becomes one FLUID expose; each
SchemaProperty becomes one FLUID field.

Lossless by construction (see :mod:`.base`): on import every SchemaObject and
SchemaProperty is stashed verbatim and only the FLUID-native fields are
projected out; on export the stash is the base layer and native fields are
overlaid only where they changed. Every other ODCS field — ``physicalType``,
``relationships``, ``transform*``, ``enum``, ``semanticType``, ``synonyms``,
``context``, ``deprecated``, ``id`` and whatever the next spec version adds —
round-trips with no per-field code here.

Fresh FLUID (no stash) is synthesised: ``physicalType`` from the provider's
type table, ``quality`` auto-checks via :mod:`.quality`.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict, List, Optional

from .base import (
    ExportCtx,
    ImportCtx,
    expose_passthrough,
    field_passthrough,
    get_expose_passthrough,
    get_field_passthrough,
    get_stash,
    overlay_native,
    stash,
)
from .types import (
    fluid_to_logical,
    fluid_to_physical,
    logical_to_fluid,
    physical_type_to_platform,
)

_PK_TAGS = ("primary-key", "primaryKey")


# ----- ODCS → FLUID --------------------------------------------------------


def to_fluid(ctx: ImportCtx) -> None:
    schema = ctx.odcs.get("schema") or []
    if not isinstance(schema, list):
        return
    exposes = ctx.fluid.setdefault("exposes", [])
    for schema_object in schema:
        if isinstance(schema_object, Mapping):
            exposes.append(_object_to_expose(schema_object, ctx.odcs))


def _project_object(schema_object: Mapping[str, Any], odcs: Mapping[str, Any]) -> Dict[str, Any]:
    """Pure FLUID projection of a SchemaObject (no stash, no properties)."""
    expose_id = schema_object.get("name") or odcs.get("id") or "default"
    expose: Dict[str, Any] = {
        "id": expose_id,
        "exposeId": expose_id,
        "version": odcs.get("version", "1.0.0"),
    }
    if schema_object.get("description"):
        expose["description"] = schema_object["description"]
    if schema_object.get("physicalType"):
        binding: Dict[str, Any] = {
            "platform": physical_type_to_platform(schema_object["physicalType"]),
        }
        if schema_object.get("physicalName"):
            binding["physical_name"] = schema_object["physicalName"]
        expose["binding"] = binding
    return expose


def _object_to_expose(schema_object: Mapping[str, Any], odcs: Mapping[str, Any]) -> Dict[str, Any]:
    expose = _project_object(schema_object, odcs)
    expose["contract"] = {
        "schema": [
            _property_to_field(p)
            for p in schema_object.get("properties") or []
            if isinstance(p, Mapping)
        ]
    }
    # Properties are stashed per-field; keep the object stash free of them.
    stash(
        expose_passthrough(expose),
        {k: v for k, v in schema_object.items() if k != "properties"},
    )
    return expose


def _project_property(prop: Mapping[str, Any]) -> Dict[str, Any]:
    """Pure FLUID projection of a SchemaProperty (presence-preserving)."""
    fld: Dict[str, Any] = {
        "name": prop.get("name", "unknown"),
        "type": logical_to_fluid(prop.get("logicalType", "string")),
    }
    if prop.get("description"):
        fld["description"] = prop["description"]
    if prop.get("classification"):
        fld["classification"] = prop["classification"]

    # required (v3.x); legacy isNullable accepted
    if "required" in prop:
        fld["required"] = bool(prop["required"])
    elif "isNullable" in prop:
        fld["required"] = not bool(prop["isNullable"])
    else:
        fld["required"] = False

    if "tags" in prop:
        fld["tags"] = list(prop["tags"] or [])

    # ``quality`` is always present on an imported field: the verbatim list,
    # or ``[]`` meaning "the source had none — don't synthesise on export".
    quality = prop.get("quality")
    fld["quality"] = list(quality) if isinstance(quality, list) else []
    return fld


def _property_to_field(prop: Mapping[str, Any]) -> Dict[str, Any]:
    fld = _project_property(prop)
    stash(field_passthrough(fld), prop)
    return fld


# ----- FLUID → ODCS --------------------------------------------------------


def to_odcs(ctx: ExportCtx) -> None:
    """Emit ``odcs.schema`` from FLUID exposes (always an array)."""
    ctx.odcs["schema"] = list(_extract_schema(ctx.fluid))


def _extract_schema(fluid: Mapping[str, Any]) -> List[Dict[str, Any]]:
    odcs_schema: List[Dict[str, Any]] = []
    for expose in fluid.get("exposes") or []:
        if not isinstance(expose, Mapping):
            continue

        # Field list — FLUID 0.7.x at expose.contract.schema, 0.5.x at expose.schema.fields
        contract_schema: List[Mapping[str, Any]] = []
        contract = expose.get("contract")
        if isinstance(contract, Mapping) and isinstance(contract.get("schema"), list):
            contract_schema = contract["schema"]
        else:
            schema_obj = expose.get("schema")
            if isinstance(schema_obj, Mapping) and isinstance(schema_obj.get("fields"), list):
                contract_schema = schema_obj["fields"]
        if not contract_schema:
            continue

        provider = _expose_provider(expose)
        properties = [
            _field_to_property(fld, provider) for fld in contract_schema if isinstance(fld, Mapping)
        ]
        if not properties:
            continue

        expose_id = expose.get("exposeId") or expose.get("id") or "dataset"
        st = get_stash(get_expose_passthrough(expose))
        if st is not None:
            then = _native_object(_project_object(st, {"id": expose_id}))
            now = _native_object(expose)
            schema_object = overlay_native(st, then, now)
            schema_object["properties"] = properties
        else:
            schema_object = {
                "name": expose_id,
                "logicalType": "object",
                "physicalType": _platform_to_physical_type(provider),
                "properties": properties,
            }
            if expose.get("description"):
                schema_object["description"] = expose["description"]
        odcs_schema.append(schema_object)
    return odcs_schema


def _native_object(expose: Mapping[str, Any]) -> Dict[str, Any]:
    """Total projection of an expose's FLUID-native fields to ODCS keys."""
    return {
        "name": expose.get("exposeId") or expose.get("id"),
        "description": expose.get("description") or None,
    }


def _native_property(fld: Mapping[str, Any]) -> Dict[str, Any]:
    """Total projection of a field's FLUID-native fields to ODCS keys."""
    tags = fld.get("tags")
    quality = fld.get("quality")
    return {
        "name": fld.get("name", "unknown"),
        "logicalType": fluid_to_logical(fld.get("type", "string")),
        "description": fld.get("description") or None,
        "classification": fld.get("classification") or None,
        "required": bool(fld.get("required")),
        "tags": list(tags) if tags is not None else None,
        "quality": list(quality) if isinstance(quality, list) else None,
        "primaryKey": True if tags and any(t in _PK_TAGS for t in tags) else None,
    }


def _field_to_property(fld: Mapping[str, Any], provider: Optional[str]) -> Dict[str, Any]:
    st = get_stash(get_field_passthrough(fld))
    if st is not None:
        then = _native_property(_project_property(st))
        now = _native_property(fld)
        return overlay_native(st, then, now)

    # ---- fresh FLUID: synthesise ---------------------------------------
    prop: Dict[str, Any] = {
        "name": fld.get("name", "unknown"),
        "logicalType": fluid_to_logical(fld.get("type", "string")),
    }
    if provider:
        phys = fluid_to_physical(fld.get("type", "string"), provider)
        if phys:
            prop["physicalType"] = phys
    if fld.get("description"):
        prop["description"] = fld["description"]
    # Downstream consumers of fresh exports expect an explicit ``required``.
    prop["required"] = bool(fld.get("required"))
    if fld.get("classification"):
        prop["classification"] = fld["classification"]
    if fld.get("tags"):
        prop["tags"] = list(fld["tags"])

    from . import quality as quality_mapper  # local import to avoid cycle

    qchecks = quality_mapper.to_odcs_property(fld)
    if qchecks:
        prop["quality"] = qchecks

    if fld.get("tags") and any(t in _PK_TAGS for t in fld["tags"]):
        prop["primaryKey"] = True
    return prop


def _expose_provider(expose: Mapping[str, Any]) -> Optional[str]:
    binding = expose.get("binding")
    if isinstance(binding, Mapping):
        prov = binding.get("platform") or binding.get("provider")
        if prov:
            return prov
    return expose.get("provider")


def _platform_to_physical_type(provider: Optional[str]) -> str:
    if provider and provider.lower() == "kafka":
        return "topic"
    return "table"
