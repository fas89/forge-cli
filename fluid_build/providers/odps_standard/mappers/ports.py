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
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict, List, Optional

from .base import (
    ExportCtx,
    ImportCtx,
    contract_id_for_port,
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


def to_odps(ctx: ExportCtx) -> None:
    fluid = ctx.fluid
    product_id = fluid_id(fluid) or "product"

    output_ports: List[Dict[str, Any]] = []
    for expose in fluid.get("exposes") or []:
        if isinstance(expose, Mapping):
            port = _expose_to_output_port(expose, product_id)
            if port:
                output_ports.append(port)
    if output_ports:
        ctx.odps["outputPorts"] = output_ports

    input_ports: List[Dict[str, Any]] = []
    for expect in fluid.get("expects") or []:
        if isinstance(expect, Mapping):
            port = _expect_to_input_port(expect, product_id)
            if port:
                input_ports.append(port)
    if input_ports:
        ctx.odps["inputPorts"] = input_ports


def _native_output(expose: Mapping[str, Any]) -> Dict[str, Any]:
    """Total projection of an expose's FLUID-native fields to port keys."""
    tags = expose.get("tags")
    return {
        "name": expose.get("exposeId") or expose.get("id"),
        "version": str(expose.get("version", 1)),
        "description": expose.get("description") or None,
        "tags": list(tags) if tags else None,
    }


def _expose_to_output_port(
    expose: Mapping[str, Any], product_id: str
) -> Optional[Dict[str, Any]]:
    name = expose.get("exposeId") or expose.get("id")
    if not name:
        return None

    st = get_stash(get_expose_passthrough(expose))
    now = _native_output(expose)
    if st is not None:
        then = _native_output(_project_output_port(st))
        return overlay_native(st, then, now)

    # ---- fresh FLUID: synthesise ---------------------------------------
    port: Dict[str, Any] = {
        "name": name,
        "version": now["version"],
        "contractId": contract_id_for_port(product_id, name),
    }
    if now["description"]:
        port["description"] = now["description"]
    port_type = _port_type_from_expose(expose)
    if port_type:
        port["type"] = port_type
    if now["tags"]:
        port["tags"] = now["tags"]
    return port


def _native_input(expect: Mapping[str, Any]) -> Dict[str, Any]:
    tags = expect.get("tags")
    return {
        "name": expect.get("id"),
        "version": str(expect.get("version", 1)),
        "tags": list(tags) if tags else None,
    }


def _expect_to_input_port(
    expect: Mapping[str, Any], product_id: str
) -> Optional[Dict[str, Any]]:
    name = expect.get("id")
    if not name:
        return None

    st = get_stash(get_expect_passthrough(expect))
    now = _native_input(expect)
    if st is not None:
        then = _native_input(_project_input_port(st))
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

    exposes = fluid.setdefault("exposes", [])
    for port in odps.get("outputPorts") or []:
        if isinstance(port, Mapping):
            exposes.append(_output_port_to_expose_stub(port))

    expects = fluid.setdefault("expects", [])
    for port in odps.get("inputPorts") or []:
        if isinstance(port, Mapping):
            expects.append(_input_port_to_expect_stub(port))


def _project_output_port(port: Mapping[str, Any]) -> Dict[str, Any]:
    """Pure FLUID projection of an output port (no stash)."""
    name = port.get("name") or "output"
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


def _output_port_to_expose_stub(port: Mapping[str, Any]) -> Dict[str, Any]:
    stub = _project_output_port(port)
    pt = expose_passthrough(stub)
    if port.get("contractId"):
        pt["contract_id"] = port["contractId"]
    stash(pt, port)
    return stub


def _project_input_port(port: Mapping[str, Any]) -> Dict[str, Any]:
    name = port.get("name") or "input"
    stub: Dict[str, Any] = {
        "id": name,
        "version": str(port.get("version", 1)),
    }
    if port.get("contractId"):
        stub["contractId"] = port["contractId"]
    if port.get("tags"):
        stub["tags"] = list(port["tags"])
    return stub


def _input_port_to_expect_stub(port: Mapping[str, Any]) -> Dict[str, Any]:
    stub = _project_input_port(port)
    stash(expect_passthrough(stub), port)
    return stub
