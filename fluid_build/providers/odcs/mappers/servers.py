# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""ODCS servers ↔ FLUID expects / exposes binding.

ODCS ``servers[]`` carry one entry per source/sink. FLUID models the same
information in two places: each ``expose.binding`` (where data is produced)
and each ``expect`` (where data is consumed). On export we read from both;
on import every server becomes an ``expects[]`` entry, and an expose's
``binding`` is derived from the server that unambiguously backs its
SchemaObject (see :func:`binding_for_object`).

Round-trip: the imported ``servers`` list is stashed verbatim. On export it
is replayed as-is unless the FLUID ``expects`` (or a derived expose
``binding``) changed since import; then only the edited servers are
rebuilt, keeping every stashed key FLUID does not model.
"""

from __future__ import annotations

import copy
import re
from collections.abc import Mapping
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .base import (
    ExportCtx,
    ImportCtx,
    get_expose_passthrough,
    get_metadata_passthrough,
    get_stash,
    metadata_passthrough,
    overlay_native,
    target_api_version,
)
from .types import provider_to_server_type, server_type_to_provider

# Binding-level FLUID keys that ODCS carries on the server itself. FLUID's
# schema requires ``binding.format`` and forbids it in ``binding.location``,
# so the binding is the only place to find it.
_BINDING_LEVEL_KEYS = ("format", "delimiter")

# Server keys that are not type-specific — kept when a server's type changes.
_SERVER_COMMON_KEYS = frozenset(
    {"id", "server", "description", "environment", "roles", "customProperties"}
)

# Characters outside the v3.1.0 ``StableId`` pattern (``^[A-Za-z0-9_-]+$``,
# which also satisfies the looser v3.2.0 pattern).
_STABLE_ID_INVALID = re.compile(r"[^A-Za-z0-9_-]")

# ----- ODCS → FLUID --------------------------------------------------------


def to_fluid(ctx: ImportCtx) -> None:
    if "servers" not in ctx.odcs:
        return
    servers = ctx.odcs.get("servers")
    if not isinstance(servers, list):
        return
    # Verbatim pass-through so re-export reproduces the exact same servers
    # list — including an explicit empty one.
    metadata_passthrough(ctx.fluid)["servers"] = [
        copy.deepcopy(dict(s)) for s in servers if isinstance(s, Mapping)
    ]
    expects = ctx.fluid.setdefault("expects", [])
    for server in servers:
        if not isinstance(server, Mapping):
            continue
        expect = _server_to_expect(server)
        if expect:
            expects.append(expect)


def _server_id(server: Mapping[str, Any]) -> Optional[str]:
    # ODCS canonical id lives at ``server`` (v3.1.0); ``name`` is accepted
    # for back-compat with legacy fixtures, then ``id``.
    return server.get("server") or server.get("name") or server.get("id")


def _server_to_expect(server: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    server_type = server.get("type")
    if not server_type:
        return None
    expect: Dict[str, Any] = {
        "id": _server_id(server) or "dependency",
        "provider": server_type_to_provider(server_type),
    }
    location = _location_from_server(server)
    if location:
        expect["location"] = location
    return expect


def _location_from_server(server: Mapping[str, Any]) -> Dict[str, Any]:
    location: Dict[str, Any] = {}
    for key in (
        "project",
        "dataset",
        "table",
        "account",
        "database",
        "schema",
        "bucket",
        "path",
        "region",
        "host",
        "port",
        "format",
        # ODCS v3.2.0 IcebergServer
        "catalog",
        "catalogUrl",
        "namespace",
        "warehouse",
    ):
        if key in server:
            location[key] = copy.deepcopy(server[key])
    return location


def _select_server(
    schema_object: Mapping[str, Any], servers: Sequence[Any]
) -> Optional[Tuple[int, Mapping[str, Any]]]:
    """The server that unambiguously backs ``schema_object``, if any.

    Either the single typed server whose ``server`` id equals the object's
    ``name``, or — when no server is named after it — the document's only
    typed server. Anything else is ambiguous and yields ``None``.
    """
    typed = [
        (i, s) for i, s in enumerate(servers or []) if isinstance(s, Mapping) and s.get("type")
    ]
    name = schema_object.get("name")
    named = [(i, s) for i, s in typed if name and _server_id(s) == name]
    if len(named) == 1:
        return named[0]
    if not named and len(typed) == 1:
        return typed[0]
    return None


def _platform_for_server_type(server_type: str) -> str:
    """FLUID ``binding.platform`` for an ODCS ``Server.type``.

    Uses the usual provider name (``bigquery`` → ``gcp``) unless that mapping
    is lossy (``redshift`` → ``aws`` → ``s3``), in which case the server type
    itself is the platform.
    """
    provider = server_type_to_provider(server_type)
    if provider_to_server_type(provider, "v3.2.0") != str(server_type).lower():
        return str(server_type)
    return provider


def _binding_from_server(server: Mapping[str, Any], schema_object: Mapping[str, Any]):
    binding: Dict[str, Any] = {"platform": _platform_for_server_type(server["type"])}
    location = _location_from_server(server)
    fmt = location.pop("format", None)
    if fmt is not None:
        binding["format"] = fmt
    if location:
        binding["location"] = location
    if schema_object.get("physicalName"):
        binding["physical_name"] = schema_object["physicalName"]
    return binding


def binding_for_object(
    schema_object: Mapping[str, Any], servers: Sequence[Any]
) -> Optional[Dict[str, Any]]:
    """FLUID ``binding`` for a SchemaObject, derived from the ODCS servers.

    ``SchemaObject.physicalType`` says *what* the object is (table, view,
    topic), not *where* it lives, so it is never used to guess a platform.
    Returns ``None`` when no server unambiguously backs the object.
    """
    selected = _select_server(schema_object, servers)
    if selected is None:
        return None
    return _binding_from_server(selected[1], schema_object)


# ----- FLUID → ODCS --------------------------------------------------------


def to_odcs(ctx: ExportCtx) -> None:
    """Emit ``servers`` from FLUID exposes/expects.

    ``servers`` is optional in every ODCS version we vendor. Precedence:

    1. Imported doc that had a ``servers`` list — replayed verbatim (empty
       list included) when FLUID's ``expects`` and derived expose bindings
       are unchanged since import; otherwise a three-way merge rebuilds only
       the edited servers, drops servers whose expect was removed (unless
       an expose binding still selects them) and adds servers for new
       expects.
    2. Imported doc that had *no* ``servers`` — emit none unless FLUID added
       expects/bindings since, so the round-trip stays lossless and we never
       synthesise a server that the source platform can't validate.
    3. Fresh FLUID — synthesise from bindings.
    """
    api_version = target_api_version(ctx)
    pt = get_metadata_passthrough(ctx.fluid)
    stashed = pt.get("servers")
    if isinstance(stashed, list):
        ctx.odcs["servers"] = _merge_servers(
            ctx.fluid, [s for s in stashed if isinstance(s, Mapping)], api_version
        )
        return
    if get_stash(pt) is not None:
        servers = _merge_servers(ctx.fluid, [], api_version)
        if servers:
            ctx.odcs["servers"] = servers
        return

    servers: List[Dict[str, Any]] = []
    for expose in ctx.fluid.get("exposes") or []:
        s = _expose_to_server(expose, api_version)
        if s:
            servers.append(s)
    for expect in ctx.fluid.get("expects") or []:
        s = _expect_to_server(expect, api_version)
        if s:
            servers.append(s)
    ctx.odcs["servers"] = servers


def _merge_servers(
    fluid: Mapping[str, Any], stashed: List[Mapping[str, Any]], api_version: str
) -> List[Dict[str, Any]]:
    """Three-way merge of the stashed servers with the current FLUID."""
    then_expects = [_server_to_expect(s) for s in stashed]
    now_expects = [e for e in fluid.get("expects") or [] if isinstance(e, Mapping)]

    binding_edits = []
    # Stashed servers an expose binding still selects: they stay even when
    # the expects projected from them are removed.
    bound = set()
    for expose in fluid.get("exposes") or []:
        if not isinstance(expose, Mapping):
            continue
        obj_stash = get_stash(get_expose_passthrough(expose))
        selected = _select_server(obj_stash, stashed) if obj_stash is not None else None
        then_b = _binding_from_server(selected[1], obj_stash) if selected else None
        now_b = expose.get("binding") if isinstance(expose.get("binding"), Mapping) else None
        if selected and now_b is not None:
            bound.add(selected[0])
        if now_b != then_b:
            binding_edits.append((selected[0] if selected else None, then_b, expose))

    if now_expects == [e for e in then_expects if e is not None] and not binding_edits:
        return [copy.deepcopy(dict(s)) for s in stashed]

    pending: Dict[Any, List[Mapping[str, Any]]] = {}
    for expect in now_expects:
        pending.setdefault(expect.get("id"), []).append(expect)
    consumed = set()
    out: List[Dict[str, Any]] = []
    out_pos: Dict[int, int] = {}
    for i, (server, then_e) in enumerate(zip(stashed, then_expects)):
        if then_e is None:
            # Type-less server: FLUID has no projection of it to edit.
            out_pos[i] = len(out)
            out.append(copy.deepcopy(dict(server)))
            continue
        bucket = pending.get(then_e["id"])
        if not bucket:
            if i in bound:
                # Its expect was removed, but an expose binding still selects
                # it: keep it (a binding edit is merged onto it below).
                out_pos[i] = len(out)
                out.append(copy.deepcopy(dict(server)))
            continue  # otherwise its expect was removed in FLUID
        now_e = bucket.pop(0)
        consumed.add(id(now_e))
        if now_e == then_e:
            merged: Optional[Dict[str, Any]] = copy.deepcopy(dict(server))
        else:
            same_provider = now_e.get("provider") == then_e.get("provider")
            override = server.get("type") if same_provider else None
            merged = _merge_server(
                server,
                _expect_to_server(then_e, api_version, override),
                _expect_to_server(now_e, api_version, override),
            )
        if merged is not None:
            out_pos[i] = len(out)
            out.append(merged)

    for expect in now_expects:
        if id(expect) not in consumed:
            s = _expect_to_server(expect, api_version)
            if s:
                out.append(s)

    for index, then_b, expose in binding_edits:
        now_b = expose.get("binding")
        if not isinstance(now_b, Mapping):
            continue  # binding removed; the server is still owned by its expect
        if then_b is not None and index is not None and index in out_pos:
            pos = out_pos[index]
            same_platform = now_b.get("platform") == then_b.get("platform")
            override = stashed[index].get("type") if same_platform else None
            merged = _merge_server(
                out[pos],
                _expose_to_server({**expose, "binding": then_b}, api_version, override),
                _expose_to_server(expose, api_version, override),
            )
            if merged is not None:
                out[pos] = merged
        else:
            s = _expose_to_server(expose, api_version)
            if s:
                out.append(s)
    return out


def _merge_server(
    stashed: Mapping[str, Any],
    then_srv: Optional[Mapping[str, Any]],
    now_srv: Optional[Mapping[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Overlay the FLUID edit (``now_srv`` − ``then_srv``) onto a stashed server."""
    if now_srv is None:
        return None
    if then_srv is None or then_srv.get("type") != now_srv.get("type"):
        # Type changed: type-specific stashed keys no longer apply.
        out = {k: copy.deepcopy(v) for k, v in stashed.items() if k in _SERVER_COMMON_KEYS}
        out.update({k: copy.deepcopy(v) for k, v in now_srv.items() if k not in ("id", "server")})
        out.setdefault("server", now_srv.get("server"))
        return out
    keys = (set(then_srv) | set(now_srv)) - {"id", "server", "type"}
    return overlay_native(stashed, then_srv, now_srv, keys=keys)


def _stable_id(value: Any) -> Optional[str]:
    """Sanitise ``value`` to the ODCS ``StableId`` pattern (``None`` if empty)."""
    if value is None:
        return None
    sid = _STABLE_ID_INVALID.sub("_", str(value))
    return sid or None


def _build_server(
    entity: Mapping[str, Any],
    entity_id: str,
    api_version: Optional[str],
    provider_override: Optional[str],
) -> Optional[Dict[str, Any]]:
    binding = entity.get("binding") if isinstance(entity.get("binding"), Mapping) else None
    provider = None
    location: Optional[Mapping[str, Any]] = None
    if binding:
        provider = binding.get("platform") or binding.get("provider")
        location = binding.get("location") if isinstance(binding.get("location"), Mapping) else None
    if not provider:
        provider = entity.get("provider")
    if not provider:
        return None
    if provider_override:
        provider = provider_override

    server: Dict[str, Any] = {}
    sid = _stable_id(entity_id)
    if sid:
        server["id"] = sid
    server["server"] = entity_id
    server["type"] = provider_to_server_type(str(provider), api_version)

    if not isinstance(location, Mapping):
        loc = entity.get("location")
        location = loc if isinstance(loc, Mapping) else None
    merged: Dict[str, Any] = dict(location) if isinstance(location, Mapping) else {}
    if binding:
        # Location values win; binding-level keys are the fallback.
        for key in _BINDING_LEVEL_KEYS:
            if key in binding and key not in merged:
                merged[key] = binding[key]
    if merged:
        details_provider = "custom" if server["type"] == "custom" else str(provider)
        server.update(copy.deepcopy(_server_details_from_location(merged, details_provider)))
    return server


def _expose_to_server(
    expose: Any,
    api_version: Optional[str] = None,
    provider_override: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if not isinstance(expose, Mapping):
        return None
    expose_id = expose.get("exposeId") or expose.get("id") or "default"
    return _build_server(expose, expose_id, api_version, provider_override)


def _expect_to_server(
    expect: Any,
    api_version: Optional[str] = None,
    provider_override: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if not isinstance(expect, Mapping):
        return None
    expect_id = expect.get("id") or "dependency"
    return _build_server(expect, expect_id, api_version, provider_override)


def _server_details_from_location(location: Mapping[str, Any], provider: str) -> Dict[str, Any]:
    """Map FLUID binding/location → ODCS server-type-specific allowed fields.

    ODCS servers use ``unevaluatedProperties: false`` — emitting any field
    outside the allowed list for the server's type breaks validation. This
    table mirrors the ``$defs/ServerSource/<TypeServer>.properties`` keys
    (v3.1.0 set plus the v3.2.0 engines); FLUID fields with no ODCS
    counterpart are quietly dropped here and surface elsewhere via expose
    pass-through if needed.
    """
    p = provider.lower()
    details: Dict[str, Any] = {}

    if p in ("gcp", "bigquery"):
        for key in ("project", "dataset"):
            if key in location:
                details[key] = location[key]
    elif p == "snowflake":
        for key in ("host", "port", "account", "database", "schema", "warehouse"):
            if key in location:
                details[key] = location[key]
    elif p in ("aws", "s3"):
        # ODCS S3Server: location (URI), endpointUrl, format, delimiter
        loc_val = location.get("location") or _build_s3_uri(
            location.get("bucket"), location.get("path") or location.get("key")
        )
        if loc_val:
            details["location"] = loc_val
        for key in ("endpointUrl", "format", "delimiter"):
            if key in location:
                details[key] = location[key]
    elif p == "kafka":
        host = location.get("host") or location.get("account")
        if host:
            details["host"] = host
        if "format" in location:
            details["format"] = location["format"]
    elif p in ("postgres", "postgresql"):
        for key in ("host", "port", "database", "schema"):
            if key in location:
                details[key] = location[key]
    elif p == "mysql":
        for key in ("host", "port", "database"):
            if key in location:
                details[key] = location[key]
    elif p == "databricks":
        for key in ("host", "catalog", "schema"):
            if key in location:
                details[key] = location[key]
    elif p == "redshift":
        for key in ("host", "database", "schema", "region", "account"):
            if key in location:
                details[key] = location[key]
    elif p == "local":
        for key in ("path", "format"):
            if key in location:
                details[key] = location[key]
    elif p in ("athena",):
        for key in ("stagingDir", "schema", "catalog", "regionName"):
            if key in location:
                details[key] = location[key]
    # ---- ODCS v3.2.0 engines --------------------------------------------
    elif p == "iceberg":
        for key in ("catalog", "catalogUrl", "namespace", "warehouse"):
            if key in location:
                details[key] = location[key]
    elif p in ("hana", "sap-hana", "saphana"):
        for key in ("host", "port", "database", "schema"):
            if key in location:
                details[key] = location[key]
    elif p == "teradata":
        for key in ("host", "port", "database"):
            if key in location:
                details[key] = location[key]
    elif p == "exasol":
        for key in ("host", "port", "schema"):
            if key in location:
                details[key] = location[key]
    elif p in (
        "ingres",
        "vectorwise",
        "actian",
        "versant",
        "zen",
        "btrieve",
        "poet",
        "fastobjects",
    ):
        # Actian family (Ingres / Vectorwise / Versant / Zen) and Poet /
        # FastObjects all share the {host, port, database} shape.
        for key in ("host", "port", "database"):
            if key in location:
                details[key] = location[key]
    else:
        # Custom / unknown provider → ODCS server.type=custom which accepts
        # the union of all server-type fields (``CustomServer.properties``,
        # identical in v3.1.0 and v3.2.0 apart from v3.2.0's ``encoding``).
        custom_keys = {
            "account",
            "catalog",
            "database",
            "dataset",
            "delimiter",
            "endpointUrl",
            "format",
            "host",
            "location",
            "path",
            "port",
            "project",
            "region",
            "regionName",
            "schema",
            "serviceName",
            "stagingDir",
            "warehouse",
            "stream",
        }
        for key, value in location.items():
            if key in custom_keys:
                details[key] = value
    return details


def _build_s3_uri(bucket: Optional[str], path: Optional[str]) -> Optional[str]:
    if not bucket:
        return None
    if path:
        return f"s3://{bucket.rstrip('/')}/{path.lstrip('/')}"
    return f"s3://{bucket}"
