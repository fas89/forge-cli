# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Downgrade replayed ODCS v3.2.0 content for an older target ``apiVersion``.

The section mappers replay an imported document's verbatim stash. When that
document declared v3.2.0 (or newer) and the export targets an older version
(an explicit ``api_version`` / ``ODCS_API_VERSION`` pin), the replayed content
can carry constructs the declared version does not allow. This last export
step rewrites them so the output validates against the version it declares,
the way the ODPS side handles its v1.1.0-only keys:

- ``logicalType: map`` → ``object`` and ``vector`` → ``array``; the ``map``
  shape and vector ``logicalTypeOptions`` move to ``customProperties``;
- v3.2.0-only keys on the contract (``context``), SchemaObjects
  (``context``, ``synonyms``, ``deprecated``) and properties (``enum``,
  ``semanticType``, ``synonyms``, ``deprecated``) move to the element's
  ``customProperties``; a relationship ``id`` moves to the relationship's;
- keys the older version has no place for are dropped: ``vendor`` on custom
  properties, ``customProperties`` / ``authoritativeDefinitions`` on SLA
  entries;
- servers are checked against the target schema: a ``${VAR}`` string port
  and any key the server type does not allow move to the server's
  ``customProperties`` (a numeric string port becomes an integer); an engine
  type the target lacks, or a typed server left without a key its type
  requires, becomes ``custom`` with the original type kept as the
  ``serverType`` custom property.

Every change is logged as one warning. Fresh FLUID never needs this: the
mappers already gate v3.2.0-only output on the target version.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping, MutableMapping
from typing import Any, Dict, List, Optional, Set

from ..validation import api_version_tuple, load_schema
from .base import ExportCtx, ImportCtx, get_metadata_passthrough, get_stash

_V320 = (3, 2, 0)

_TOP_LEVEL_KEYS = ("context",)
_OBJECT_KEYS = ("context", "synonyms", "deprecated")
_PROPERTY_KEYS = ("enum", "semanticType", "synonyms", "deprecated")
_SLA_DROP_KEYS = ("customProperties", "authoritativeDefinitions")


def to_fluid(ctx: ImportCtx) -> None:  # pragma: no cover - export-only step
    return None


def to_odcs(ctx: ExportCtx) -> None:
    target = ctx.odcs.get("apiVersion")
    target_t = api_version_tuple(target)
    if not target_t or target_t >= _V320:
        return
    st = get_stash(get_metadata_passthrough(ctx.fluid))
    source = st.get("apiVersion") if st else None
    source_t = api_version_tuple(source)
    if not source_t or source_t < _V320:
        return
    changes = downgrade_v320(ctx.odcs, str(target))
    if changes:
        ctx.logger.warning(
            "ODCS export at %s of a %s contract: downgraded v3.2.0-only content (%s)",
            target,
            source,
            "; ".join(changes),
        )


def downgrade_v320(doc: MutableMapping[str, Any], target: str) -> List[str]:
    """Rewrite ``doc`` in place so it is valid for ``target`` (< v3.2.0).

    Returns a description of every change made.
    """
    changes: List[str] = []
    _move_to_custom(doc, _TOP_LEVEL_KEYS, "contract", changes)
    for i, obj in enumerate(doc.get("schema") or []):
        if isinstance(obj, MutableMapping):
            _downgrade_element(obj, f"schema[{i}]", _OBJECT_KEYS, changes)
    for i, entry in enumerate(doc.get("slaProperties") or []):
        if isinstance(entry, MutableMapping):
            dropped = [k for k in _SLA_DROP_KEYS if entry.pop(k, None) is not None]
            if dropped:
                changes.append(f"slaProperties[{i}]: dropped {', '.join(dropped)}")
    _downgrade_servers(doc, target, changes)
    _strip_custom_property_vendor(doc, "", changes)
    return changes


# ----- schema elements -------------------------------------------------------


def _downgrade_element(
    el: MutableMapping[str, Any], where: str, keys: Any, changes: List[str]
) -> None:
    logical = el.get("logicalType")
    if logical == "map":
        el["logicalType"] = "object"
        moved = ["map"] if "map" in el else []
        if "logicalTypeOptions" in el:
            moved.append("logicalTypeOptions")
        _move_to_custom(el, moved, where, changes, note=False)
        changes.append(f"{where}: logicalType map -> object")
    elif logical == "vector":
        el["logicalType"] = "array"
        _move_to_custom(el, ["logicalTypeOptions"], where, changes, note=False)
        changes.append(f"{where}: logicalType vector -> array")
    _move_to_custom(el, keys, where, changes)
    for j, rel in enumerate(el.get("relationships") or []):
        if isinstance(rel, MutableMapping):
            _move_to_custom(rel, ("id",), f"{where}.relationships[{j}]", changes)
    for j, prop in enumerate(el.get("properties") or []):
        if isinstance(prop, MutableMapping):
            _downgrade_element(prop, f"{where}.properties[{j}]", _PROPERTY_KEYS, changes)
    items = el.get("items")
    if isinstance(items, MutableMapping):
        _downgrade_element(items, f"{where}.items", _PROPERTY_KEYS, changes)


def _move_to_custom(
    el: MutableMapping[str, Any],
    keys: Any,
    where: str,
    changes: List[str],
    *,
    note: bool = True,
) -> None:
    """Move ``keys`` of ``el`` into its ``customProperties`` list.

    An existing custom property of the same name wins (its value is kept and
    the moved value dropped); ``None`` values are dropped.
    """
    present = [k for k in keys if k in el]
    if not present:
        return
    custom = el.get("customProperties")
    custom_list: List[Any] = list(custom) if isinstance(custom, list) else []
    names = {c.get("property") for c in custom_list if isinstance(c, Mapping)}
    for key in present:
        value = el.pop(key)
        if key not in names and value is not None:
            custom_list.append({"property": key, "value": copy.deepcopy(value)})
            names.add(key)
    if custom_list:
        el["customProperties"] = custom_list
    if note:
        changes.append(f"{where}: {', '.join(present)} -> customProperties")


def _strip_custom_property_vendor(node: Any, where: str, changes: List[str]) -> None:
    if isinstance(node, MutableMapping):
        for key, value in node.items():
            path = f"{where}.{key}" if where else str(key)
            if key == "customProperties" and isinstance(value, list):
                hit = False
                for entry in value:
                    if isinstance(entry, MutableMapping) and "vendor" in entry:
                        entry.pop("vendor")
                        hit = True
                if hit:
                    changes.append(f"{path}: dropped vendor")
                continue  # custom property values are free-form
            _strip_custom_property_vendor(value, path, changes)
    elif isinstance(node, list):
        for i, value in enumerate(node):
            _strip_custom_property_vendor(value, f"{where}[{i}]", changes)


# ----- servers ---------------------------------------------------------------


def _downgrade_servers(doc: MutableMapping[str, Any], target: str, changes: List[str]) -> None:
    servers = doc.get("servers")
    if not isinstance(servers, list) or not servers:
        return
    try:
        schema = load_schema(target)
    except Exception:  # unknown target: nothing to check against
        schema = None
    if not schema:
        return
    defs = schema.get("$defs") or {}
    server_def = defs.get("Server") or {}
    base_keys: Set[str] = set((server_def.get("properties") or {}).keys())
    type_enum = ((server_def.get("properties") or {}).get("type") or {}).get("enum") or []
    sources = defs.get("ServerSource") or {}
    by_type: Dict[str, Mapping[str, Any]] = {}
    for cond in server_def.get("allOf") or []:
        const = (((cond.get("if") or {}).get("properties") or {}).get("type") or {}).get("const")
        ref = (cond.get("then") or {}).get("$ref") or ""
        source = sources.get(ref.rsplit("/", 1)[-1]) if ref else None
        if const and isinstance(source, Mapping):
            by_type[const] = source

    def fit(server: MutableMapping[str, Any], where: str) -> Mapping[str, Any]:
        source = by_type.get(str(server.get("type"))) or {}
        source_props: Mapping[str, Any] = source.get("properties") or {}
        _coerce_port(server, source_props.get("port"), where, changes)
        allowed = base_keys | set(source_props)
        _move_to_custom(server, [k for k in server if k not in allowed], where, changes)
        return source

    for i, server in enumerate(servers):
        if not isinstance(server, MutableMapping):
            continue
        where = f"servers[{i}]"
        stype = server.get("type")
        if stype is not None and type_enum and stype not in type_enum:
            _to_custom(server, where, changes)
        source = fit(server, where)
        missing = [k for k in source.get("required") or [] if k not in server]
        if missing and server.get("type") != "custom":
            # A required key could not be kept (e.g. a ``${VAR}`` port):
            # the typed server cannot be expressed, the custom one can.
            _to_custom(server, where, changes)
            fit(server, where)


def _to_custom(server: MutableMapping[str, Any], where: str, changes: List[str]) -> None:
    """Turn ``server`` into a ``custom`` one, keeping its type as ``serverType``."""
    stype = server.get("type")
    server["type"] = "custom"
    custom = server.get("customProperties")
    custom_list: List[Any] = list(custom) if isinstance(custom, list) else []
    if not any(isinstance(c, Mapping) and c.get("property") == "serverType" for c in custom_list):
        custom_list.append({"property": "serverType", "value": stype})
    server["customProperties"] = custom_list
    changes.append(f"{where}: type {stype} -> custom")


def _coerce_port(
    server: MutableMapping[str, Any],
    port_schema: Optional[Mapping[str, Any]],
    where: str,
    changes: List[str],
) -> None:
    """v3.2.0 allows a string port (``${DB_PORT}``); older versions an integer."""
    port = server.get("port")
    if not isinstance(port, str) or not isinstance(port_schema, Mapping):
        return
    if port_schema.get("type") != "integer":
        return
    if port.strip().isdigit():
        server["port"] = int(port.strip())
        changes.append(f"{where}: port {port!r} -> {server['port']}")
    else:
        _move_to_custom(server, ("port",), where, changes)
