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

import copy
import logging
from collections.abc import Mapping
from typing import Any, Dict, List, Optional

from .base import (
    ExportCtx,
    ImportCtx,
    expose_passthrough,
    field_passthrough,
    get_expose_passthrough,
    get_field_passthrough,
    get_metadata_passthrough,
    get_stash,
    metadata_passthrough,
    overlay_native,
    stash,
    target_api_version,
)
from .servers import binding_for_object
from .types import fluid_to_logical, fluid_to_physical, logical_to_fluid

LOG = logging.getLogger(__name__)

_PK_TAGS = ("primary-key", "primaryKey")

# Contract-level pass-through flag: the imported document had no ``schema``.
SCHEMA_ABSENT_KEY = "schema_absent"


# ----- ODCS → FLUID --------------------------------------------------------


def to_fluid(ctx: ImportCtx) -> None:
    if "schema" not in ctx.odcs:
        # Recorded so re-export does not synthesise ``schema: []``.
        metadata_passthrough(ctx.fluid)[SCHEMA_ABSENT_KEY] = True
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
        expose["description"] = copy.deepcopy(schema_object["description"])
    # ``physicalType`` (table/view/topic) says what the object is, not which
    # platform hosts it — the binding comes from the servers, and only when
    # they identify one unambiguously.
    binding = binding_for_object(schema_object, odcs.get("servers") or [])
    if binding:
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
    # Properties are stashed per-field; keep the object stash free of them,
    # but record that the key was present (as ``properties: []``) so an
    # object with an explicit empty list round-trips exactly.
    stash(
        expose_passthrough(expose),
        {
            k: ([] if k == "properties" else v)
            for k, v in schema_object.items()
            if k != "properties" or isinstance(v, list)
        },
    )
    return expose


def _project_property(prop: Mapping[str, Any]) -> Dict[str, Any]:
    """Pure FLUID projection of a SchemaProperty (presence-preserving)."""
    fld: Dict[str, Any] = {
        "name": prop.get("name", "unknown"),
        "type": logical_to_fluid(prop.get("logicalType", "string")),
    }
    if prop.get("description"):
        fld["description"] = copy.deepcopy(prop["description"])
    if prop.get("classification"):
        fld["classification"] = copy.deepcopy(prop["classification"])

    # required (v3.x); legacy isNullable accepted
    if "required" in prop:
        fld["required"] = bool(prop["required"])
    elif "isNullable" in prop:
        fld["required"] = not bool(prop["isNullable"])
    else:
        fld["required"] = False

    if "tags" in prop:
        fld["tags"] = copy.deepcopy(list(prop["tags"] or []))

    # ``quality`` is always present on an imported field: the verbatim list,
    # or ``[]`` meaning "the source had none — don't synthesise on export".
    quality = prop.get("quality")
    fld["quality"] = copy.deepcopy(list(quality)) if isinstance(quality, list) else []
    return fld


def _property_to_field(prop: Mapping[str, Any]) -> Dict[str, Any]:
    fld = _project_property(prop)
    stash(field_passthrough(fld), prop)
    return fld


# ----- FLUID → ODCS --------------------------------------------------------


def to_odcs(ctx: ExportCtx) -> None:
    """Emit ``odcs.schema`` from FLUID exposes (an array).

    An imported document that had no ``schema`` key gets none back unless
    FLUID now produces SchemaObjects.
    """
    schema = list(
        _extract_schema(
            ctx.fluid,
            include_quality=ctx.options.get("include_quality_checks", True),
            api_version=target_api_version(ctx),
        )
    )
    if not schema and get_metadata_passthrough(ctx.fluid).get(SCHEMA_ABSENT_KEY):
        return
    ctx.odcs["schema"] = schema


def _extract_schema(
    fluid: Mapping[str, Any],
    include_quality: bool = True,
    api_version: Optional[str] = None,
) -> List[Dict[str, Any]]:
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

        # An imported SchemaObject is re-emitted even without properties
        # (they are optional in ODCS); fresh exposes without fields are
        # skipped as before.
        st = get_stash(get_expose_passthrough(expose))
        if not contract_schema and st is None:
            continue

        provider = _expose_provider(expose)
        properties = [
            _field_to_property(
                fld, provider, include_quality=include_quality, api_version=api_version
            )
            for fld in contract_schema
            if isinstance(fld, Mapping)
        ]
        if not properties and st is None:
            continue

        expose_id = expose.get("exposeId") or expose.get("id") or "dataset"
        if st is not None:
            then = _native_object(_project_object(st, {"id": expose_id}))
            now = _native_object(expose)
            schema_object = overlay_native(st, then, now)
            if properties or "properties" in st:
                schema_object["properties"] = properties
            else:
                schema_object.pop("properties", None)
            if not include_quality:
                # Object-level quality is replayed from the stash only.
                schema_object.pop("quality", None)
        else:
            schema_object = {
                "name": expose_id,
                "logicalType": "object",
                "physicalType": _platform_to_physical_type(provider),
                "properties": properties,
            }
            if expose.get("description"):
                schema_object["description"] = copy.deepcopy(expose["description"])
        odcs_schema.append(schema_object)
    return odcs_schema


def _native_object(expose: Mapping[str, Any]) -> Dict[str, Any]:
    """Total projection of an expose's FLUID-native fields to ODCS keys."""
    return {
        "name": expose.get("exposeId") or expose.get("id"),
        "description": expose.get("description") or None,
    }


def _native_property(fld: Mapping[str, Any], api_version: Optional[str] = None) -> Dict[str, Any]:
    """Total projection of a field's FLUID-native fields to ODCS keys."""
    tags = fld.get("tags")
    quality = fld.get("quality")
    return {
        "name": fld.get("name", "unknown"),
        "logicalType": fluid_to_logical(fld.get("type", "string"), api_version),
        "description": fld.get("description") or None,
        "classification": fld.get("classification") or None,
        "required": bool(fld.get("required")),
        "tags": list(tags) if tags is not None else None,
        "quality": list(quality) if isinstance(quality, list) else None,
        "primaryKey": True if tags and any(t in _PK_TAGS for t in tags) else None,
    }


# Property keys that are only valid alongside one particular logicalType.
_TYPE_BOUND_KEYS = {"items": "array", "properties": "object", "map": "map"}

# Most general ODCS v3.2.0 ``map`` shape — FLUID has no way to say more.
_DEFAULT_MAP_SHAPE = {"key": {"logicalType": "string"}, "value": {"logicalType": "string"}}


def _field_to_property(
    fld: Mapping[str, Any],
    provider: Optional[str],
    include_quality: bool = True,
    api_version: Optional[str] = None,
) -> Dict[str, Any]:
    st = get_stash(get_field_passthrough(fld))
    if st is not None:
        then = _native_property(_project_property(st), api_version)
        now = _native_property(fld, api_version)
        prop = overlay_native(st, then, now)
        if now["logicalType"] != then["logicalType"]:
            _drop_stale_type_details(prop, fld, provider, api_version)
        if not include_quality:
            _strip_quality(prop)
        return prop

    # ---- fresh FLUID: synthesise ---------------------------------------
    prop = {
        "name": fld.get("name", "unknown"),
        "logicalType": fluid_to_logical(fld.get("type", "string"), api_version),
    }
    if prop["logicalType"] == "map":
        # ODCS v3.2.0 requires the key/value shape alongside ``map``.
        prop["map"] = copy.deepcopy(_DEFAULT_MAP_SHAPE)
    if provider:
        phys = fluid_to_physical(fld.get("type", "string"), provider, api_version)
        if phys:
            prop["physicalType"] = phys
    if fld.get("description"):
        prop["description"] = copy.deepcopy(fld["description"])
    # Downstream consumers of fresh exports expect an explicit ``required``.
    prop["required"] = bool(fld.get("required"))
    if fld.get("classification"):
        prop["classification"] = copy.deepcopy(fld["classification"])
    if fld.get("tags"):
        prop["tags"] = list(fld["tags"])

    if include_quality:
        from . import quality as quality_mapper  # local import to avoid cycle

        qchecks = quality_mapper.to_odcs_property(fld)
        if qchecks:
            prop["quality"] = qchecks

    if fld.get("tags") and any(t in _PK_TAGS for t in fld["tags"]):
        prop["primaryKey"] = True
    return prop


def _strip_quality(prop: Dict[str, Any]) -> None:
    """Remove ``quality`` from a replayed property and everything nested in it
    (object ``properties``, array ``items``, map ``key``/``value``)."""
    prop.pop("quality", None)
    props = prop.get("properties")
    nested: List[Any] = list(props) if isinstance(props, list) else []
    if isinstance(prop.get("items"), dict):
        nested.append(prop["items"])
    if isinstance(prop.get("map"), dict):
        nested.extend(v for k, v in prop["map"].items() if k in ("key", "value"))
    for child in nested:
        if isinstance(child, dict):
            _strip_quality(child)


def _drop_stale_type_details(
    prop: Dict[str, Any],
    fld: Mapping[str, Any],
    provider: Optional[str],
    api_version: Optional[str],
) -> None:
    """The FLUID type of an imported field changed: drop stashed type details.

    ``physicalType`` and ``logicalTypeOptions`` describe the *old* type (the
    schema rejects options that don't fit the new one), so they are removed
    — ``physicalType`` is re-synthesised when the platform is known —
    together with any ``items`` / ``properties`` / ``map`` block the new
    logicalType does not allow.
    """
    dropped = [k for k in ("physicalType", "logicalTypeOptions") if k in prop]
    prop.pop("physicalType", None)
    prop.pop("logicalTypeOptions", None)
    for key, logical in _TYPE_BOUND_KEYS.items():
        if key in prop and prop.get("logicalType") != logical:
            prop.pop(key)
            dropped.append(key)
    if provider:
        phys = fluid_to_physical(fld.get("type", "string"), provider, api_version)
        if phys:
            prop["physicalType"] = phys
    if prop.get("logicalType") == "map" and "map" not in prop:
        prop["map"] = copy.deepcopy(_DEFAULT_MAP_SHAPE)
    if dropped:
        LOG.warning(
            "ODCS export: type of field %r changed to %r; dropped stale %s",
            fld.get("name"),
            prop.get("logicalType"),
            ", ".join(dropped),
        )


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
