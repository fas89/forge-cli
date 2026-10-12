# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Input / output port mapping.

Each FLUID expose becomes one ``outputPorts[]`` entry; each FLUID expect
becomes one ``inputPorts[]`` entry. For *fresh* FLUID, ``contractId`` follows
the convention ``{productId}.{portName}`` (matches
:mod:`fluid_build.providers.datamesh_manager.datamesh_manager`) and equals the
``id`` of the per-port ODCS contract the provider emits as a sibling file.

Lossless by construction: every port is stashed verbatim on import
(``<port>.odps_passthrough._verbatim``) and used as the export base layer, so
``contractId``, ``type``, ``sbom``, ``inputContracts``, ``customProperties``,
``authoritativeDefinitions`` and the v1.1.0 ``id`` / ``deprecated`` /
``context`` / ``synonyms`` all round-trip untouched. Only ``name``,
``version``, ``description`` and ``tags`` are FLUID-native and overlaid when
edited. ``expose.odps_passthrough.contract_id`` is additionally exposed as a
convenience for callers.

ODPS v1.1.0 relaxed ``OutputPort``/``InputPort`` ``required`` to just
``name`` — ``version`` and ``contractId`` may be absent on import and are
preserved absent on re-export. Fresh exports always emit both.

Fresh output ports (no stash) keep the legacy ``OdpsStandardProvider``
surface: ``expose.contract_id`` overrides the ``contractId`` convention
(set it to ``{exposeId}_contract`` to keep the id ``fluid odps-bitol export``
minted in 0.7.8),
``ODPS_DEFAULT_PORT_VERSION`` (``ExportCtx.options["default_port_version"]``)
is the default ``version``, and ``customProperties`` carry ``server``
(``expose.location``), ``environment``, ``containsPii``, ``autoApprove`` and
``status``.

Ports may share a ``name`` and differ only in ``version`` (the upstream Bitol
examples do). Such ports get distinct FLUID ids on import (``name@version``),
recorded as ``odps_passthrough.fluid_id`` so re-export maps them back to the
original name.

A product rendered at an ODPS version below v1.1.0 cannot carry the v1.1.0
port keys (``id``/``deprecated``/``context``/``synonyms``); they are moved to
``customProperties`` (see :func:`.base.downgrade_v110_keys`).
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from typing import Any, Dict, Iterable, List, Optional, Set

from ..validation import api_version_tuple
from .base import (
    ExportCtx,
    ImportCtx,
    contract_id_for_port,
    downgrade_v110_keys,
    expect_passthrough,
    expose_passthrough,
    fluid_id,
    get_expect_passthrough,
    get_expose_passthrough,
    get_stash,
    overlay_native,
    stash,
)
from .types import provider_to_port_type

# ----- FLUID → ODPS --------------------------------------------------------


DEFAULT_PORT_VERSION = "1"

# Port keys that only exist from ODPS v1.1.0.
_V110_PORT_KEYS = ("id", "deprecated", "context", "synonyms")


def to_odps(ctx: ExportCtx) -> None:
    fluid = ctx.fluid
    product_id = fluid_id(fluid) or "product"
    default_version = str(ctx.options.get("default_port_version") or DEFAULT_PORT_VERSION)

    output_ports: List[Dict[str, Any]] = []
    for expose in fluid.get("exposes") or []:
        if isinstance(expose, Mapping):
            port = _expose_to_output_port(
                expose, product_id, fluid=fluid, default_version=default_version
            )
            if port:
                output_ports.append(port)

    input_ports: List[Dict[str, Any]] = []
    for expect in fluid.get("expects") or []:
        if isinstance(expect, Mapping):
            port = _expect_to_input_port(expect, product_id)
            if port:
                input_ports.append(port)

    if not _declares_v110(ctx.odps.get("apiVersion")):
        for port in output_ports:
            _downgrade_port(port, ctx, default_version=default_version)
        for port in input_ports:
            _downgrade_port(port, ctx, default_version=default_version)
            port.setdefault(
                "contractId", contract_id_for_port(product_id, f"input.{port.get('name')}")
            )

    if output_ports:
        ctx.odps["outputPorts"] = output_ports
    if input_ports:
        ctx.odps["inputPorts"] = input_ports


def _declares_v110(api_version: Any) -> bool:
    if not api_version:
        return True  # product mapper not run — leave the ports untouched
    return api_version_tuple(str(api_version)) >= (1, 1, 0)


def _downgrade_port(port: Dict[str, Any], ctx: ExportCtx, *, default_version: str) -> None:
    downgrade_v110_keys(port, _V110_PORT_KEYS, ctx.logger, where=f"port {port.get('name')!r}")
    # ``version`` is required on v1.0.0 ports.
    port.setdefault("version", default_version)


def _native_output(expose: Mapping[str, Any], default_version: Any = 1) -> Dict[str, Any]:
    """Total projection of an expose's FLUID-native fields to port keys."""
    tags = expose.get("tags")
    return {
        "name": expose.get("exposeId") or expose.get("id"),
        "version": str(expose.get("version", default_version)),
        "description": expose.get("description") or None,
        "tags": list(tags) if tags else None,
    }


def _expose_to_output_port(
    expose: Mapping[str, Any],
    product_id: str,
    *,
    fluid: Optional[Mapping[str, Any]] = None,
    default_version: Any = DEFAULT_PORT_VERSION,
) -> Optional[Dict[str, Any]]:
    name = expose.get("exposeId") or expose.get("id")
    if not name:
        return None

    pt = get_expose_passthrough(expose)
    st = get_stash(pt)
    if st is not None:
        now = _native_output(expose)
        then = _native_output(_project_output_port(st, fluid_name=pt.get("fluid_id")))
        return overlay_native(st, then, now)

    # ---- fresh FLUID: synthesise ---------------------------------------
    now = _native_output(expose, default_version)
    port: Dict[str, Any] = {
        "name": name,
        "version": now["version"],
        # ``odps_passthrough.contract_id``: the original ODCS id of an expose
        # imported from an ODCS-only directory.
        "contractId": expose.get("contract_id")
        or pt.get("contract_id")
        or contract_id_for_port(product_id, name),
    }
    if now["description"]:
        port["description"] = now["description"]
    port_type = _port_type_from_expose(expose)
    if port_type:
        port["type"] = port_type
    if now["tags"]:
        port["tags"] = now["tags"]
    port["customProperties"] = _port_custom_properties(expose, fluid or {})
    return port


def _port_custom_properties(
    expose: Mapping[str, Any], fluid: Mapping[str, Any]
) -> List[Dict[str, Any]]:
    """Legacy ``OdpsStandardProvider`` port custom properties."""
    metadata = fluid.get("metadata") if isinstance(fluid.get("metadata"), Mapping) else {}
    props: List[Dict[str, Any]] = []
    location = expose.get("location")
    if isinstance(location, Mapping) and location:
        props.append({"property": "server", "value": dict(location)})
    environment = metadata.get("environment")
    if environment:
        props.append({"property": "environment", "value": environment})
    contains_pii = _contains_pii(expose)
    props.append({"property": "containsPii", "value": contains_pii})
    props.append({"property": "autoApprove", "value": _auto_approve(fluid, contains_pii)})
    status = expose.get("status") or metadata.get("status") or "active"
    props.append({"property": "status", "value": status})
    return props


def _mentions_pii(value: Any) -> bool:
    text = str(value or "").lower()
    return "pii" in text or "sensitive" in text


def _expose_fields(expose: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
    schema = expose.get("schema")
    if isinstance(schema, Mapping) and isinstance(schema.get("fields"), list):
        yield from (f for f in schema["fields"] if isinstance(f, Mapping))
    contract = expose.get("contract")
    if isinstance(contract, Mapping) and isinstance(contract.get("schema"), list):
        yield from (f for f in contract["schema"] if isinstance(f, Mapping))


def _contains_pii(expose: Mapping[str, Any]) -> bool:
    for fld in _expose_fields(expose):
        if fld.get("pii") is True:
            return True
        if _mentions_pii(fld.get("classification")) or _mentions_pii(fld.get("sensitivity")):
            return True
    return "pii" in str(expose.get("classification") or "").lower()


def _auto_approve(fluid: Mapping[str, Any], contains_pii: bool) -> bool:
    if contains_pii:
        return False  # never auto-approve access to PII
    access_control = fluid.get("access_control")
    if isinstance(access_control, Mapping) and access_control.get("auto_approve") is not None:
        return bool(access_control["auto_approve"])
    return False  # safe default


def _native_input(expect: Mapping[str, Any]) -> Dict[str, Any]:
    tags = expect.get("tags")
    return {
        "name": expect.get("id"),
        "version": str(expect.get("version", 1)),
        "tags": list(tags) if tags else None,
    }


def _expect_to_input_port(expect: Mapping[str, Any], product_id: str) -> Optional[Dict[str, Any]]:
    name = expect.get("id")
    if not name:
        return None

    pt = get_expect_passthrough(expect)
    st = get_stash(pt)
    now = _native_input(expect)
    if st is not None:
        then = _native_input(_project_input_port(st, fluid_name=pt.get("fluid_id")))
        return overlay_native(st, then, now)

    # Fresh: prefer an explicit contractId, else mint one consistent with the
    # product (Bitol input ports must reference *some* contract).
    port: Dict[str, Any] = {
        "name": name,
        "version": now["version"],
        "contractId": expect.get("contractId") or contract_id_for_port(product_id, f"input.{name}"),
    }
    if now["tags"]:
        port["tags"] = now["tags"]
    return port


def _port_type_from_expose(expose: Mapping[str, Any]) -> Optional[str]:
    binding = expose.get("binding")
    if isinstance(binding, Mapping):
        platform = binding.get("platform") or binding.get("provider")
        if platform:
            return provider_to_port_type(platform)
    provider = expose.get("provider")
    if provider:
        return provider_to_port_type(provider)
    return None


# ----- ODPS → FLUID --------------------------------------------------------


def to_fluid(ctx: ImportCtx) -> None:
    """Skeleton expose/expect generation.

    The full ODCS-resolution step (which actually populates each expose's
    schema/quality/qos) lives in the provider's ``import_contract`` method —
    it needs the :class:`ContractResolver` instance. Here we just stub each
    port with its identifying fields and stash it verbatim, so the resolver
    step can fill in the rest.
    """
    odps = ctx.odps
    fluid = ctx.fluid

    outputs = [p for p in odps.get("outputPorts") or [] if isinstance(p, Mapping)]
    exposes = fluid.setdefault("exposes", [])
    for port, fluid_name in zip(outputs, _fluid_names(outputs, "output")):
        exposes.append(_output_port_to_expose_stub(port, fluid_name))

    inputs = [p for p in odps.get("inputPorts") or [] if isinstance(p, Mapping)]
    expects = fluid.setdefault("expects", [])
    for port, fluid_name in zip(inputs, _fluid_names(inputs, "input")):
        expects.append(_input_port_to_expect_stub(port, fluid_name))


def _fluid_names(ports: List[Mapping[str, Any]], default: str) -> List[Optional[str]]:
    """Distinct FLUID ids for ports sharing a name (``name@version``).

    ``None`` means "use the port name as is".
    """
    names = [str(p.get("name") or default) for p in ports]
    counts = Counter(names)
    used: Set[str] = {n for n in names if counts[n] == 1}
    out: List[Optional[str]] = []
    for port, name in zip(ports, names):
        if counts[name] == 1:
            out.append(None)
            continue
        version = port.get("version")
        candidate = f"{name}@{version}" if version not in (None, "") else f"{name}#1"
        n = 1
        while candidate in used:
            n += 1
            candidate = f"{name}#{n}"
        used.add(candidate)
        out.append(candidate)
    return out


def _project_output_port(
    port: Mapping[str, Any], fluid_name: Optional[str] = None
) -> Dict[str, Any]:
    """Pure FLUID projection of an output port (no stash)."""
    name = fluid_name or port.get("name") or "output"
    stub: Dict[str, Any] = {
        "id": name,
        "exposeId": name,
        "version": str(port.get("version", 1)),
    }
    if port.get("description"):
        stub["description"] = port["description"]
    if port.get("tags"):
        stub["tags"] = list(port["tags"])
    return stub


def _output_port_to_expose_stub(
    port: Mapping[str, Any], fluid_name: Optional[str] = None
) -> Dict[str, Any]:
    stub = _project_output_port(port, fluid_name)
    pt = expose_passthrough(stub)
    if port.get("contractId"):
        pt["contract_id"] = port["contractId"]
    if fluid_name:
        pt["fluid_id"] = fluid_name
    stash(pt, port)
    return stub


def _project_input_port(
    port: Mapping[str, Any], fluid_name: Optional[str] = None
) -> Dict[str, Any]:
    name = fluid_name or port.get("name") or "input"
    stub: Dict[str, Any] = {
        "id": name,
        "version": str(port.get("version", 1)),
    }
    if port.get("contractId"):
        stub["contractId"] = port["contractId"]
    if port.get("tags"):
        stub["tags"] = list(port["tags"])
    return stub


def _input_port_to_expect_stub(
    port: Mapping[str, Any], fluid_name: Optional[str] = None
) -> Dict[str, Any]:
    stub = _project_input_port(port, fluid_name)
    pt = expect_passthrough(stub)
    if fluid_name:
        pt["fluid_id"] = fluid_name
    stash(pt, port)
    return stub
