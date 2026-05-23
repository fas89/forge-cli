# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Input / output port mapping.

Each FLUID expose becomes one ``outputPorts[]`` entry; each FLUID expect
becomes one ``inputPorts[]`` entry. ``contractId`` follows the convention
``{productId}.{portName}`` (matches :mod:`fluid_build.providers.datamesh_manager.datamesh_manager`)
and equals the ``id`` of the per-port ODCS contract the provider emits as a
sibling file.

Per-port pass-throughs flow through ``expose.odps_passthrough.*`` /
``expect.odps_passthrough.*`` for round-trip:

  - ``sbom``, ``input_contracts``  (output-port only)
  - ``custom_properties``, ``authoritative_definitions``, ``tags``
  - ``port_type``                  (overrides the provider-derived type)
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
)
from .types import provider_to_port_type

NAME = "ports"


# ----- FLUID → ODPS --------------------------------------------------------


def to_odps(ctx: ExportCtx) -> None:
    fluid = ctx.fluid
    product_id = fluid_id(fluid) or "product"

    output_ports = []
    for expose in fluid.get("exposes") or []:
        if not isinstance(expose, Mapping):
            continue
        port = _expose_to_output_port(expose, product_id)
        if port:
            output_ports.append(port)
    if output_ports:
        ctx.odps["outputPorts"] = output_ports

    input_ports = []
    for expect in fluid.get("expects") or []:
        if not isinstance(expect, Mapping):
            continue
        port = _expect_to_input_port(expect, product_id)
        if port:
            input_ports.append(port)
    if input_ports:
        ctx.odps["inputPorts"] = input_ports


def _expose_to_output_port(
    expose: Mapping[str, Any], product_id: str
) -> Optional[Dict[str, Any]]:
    name = expose.get("exposeId") or expose.get("id")
    if not name:
        return None

    port: Dict[str, Any] = {
        "name": name,
        "version": str(expose.get("version", 1)),
        "contractId": contract_id_for_port(product_id, name),
    }
    if expose.get("description"):
        port["description"] = expose["description"]

    pt = get_expose_passthrough(expose)
    port_type = pt.get("port_type") or _port_type_from_expose(expose)
    if port_type:
        port["type"] = port_type

    if "sbom" in pt:
        port["sbom"] = list(pt["sbom"])
    if "input_contracts" in pt:
        port["inputContracts"] = list(pt["input_contracts"])

    if expose.get("tags"):
        port["tags"] = list(expose["tags"])
    elif "tags" in pt:
        port["tags"] = list(pt["tags"])
    if "custom_properties" in pt:
        port["customProperties"] = list(pt["custom_properties"])
    if "authoritative_definitions" in pt:
        port["authoritativeDefinitions"] = list(pt["authoritative_definitions"])
    return port


def _expect_to_input_port(
    expect: Mapping[str, Any], product_id: str
) -> Optional[Dict[str, Any]]:
    name = expect.get("id")
    if not name:
        return None

    pt = get_expect_passthrough(expect)

    # Prefer an explicit contractId (set when imported); otherwise mint one
    # consistent with the product. Bitol input ports must reference an
    # existing contract — for a fresh FLUID we synthesise a placeholder.
    contract_id = (
        expect.get("contractId")
        or pt.get("contract_id")
        or contract_id_for_port(product_id, f"input.{name}")
    )

    port: Dict[str, Any] = {
        "name": name,
        "version": str(expect.get("version", pt.get("version", 1))),
        "contractId": contract_id,
    }
    if expect.get("tags"):
        port["tags"] = list(expect["tags"])
    elif "tags" in pt:
        port["tags"] = list(pt["tags"])
    if "custom_properties" in pt:
        port["customProperties"] = list(pt["custom_properties"])
    if "authoritative_definitions" in pt:
        port["authoritativeDefinitions"] = list(pt["authoritative_definitions"])
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


# ----- ODPS → FLUID (Phase 3) ---------------------------------------------


def to_fluid(ctx: ImportCtx) -> None:
    """Skeleton expose/expect generation.

    The full ODCS-resolution step (which actually populates each expose's
    schema/quality/qos) lives in the provider's ``import_contract`` method —
    it needs the :class:`ContractResolver` instance. Here we just stub each
    port with its identifying fields and pass-through metadata, so the
    resolver step can fill in the rest.
    """
    odps = ctx.odps
    fluid = ctx.fluid

    exposes = fluid.setdefault("exposes", [])
    for port in odps.get("outputPorts") or []:
        if not isinstance(port, Mapping):
            continue
        exposes.append(_output_port_to_expose_stub(port))

    expects = fluid.setdefault("expects", [])
    for port in odps.get("inputPorts") or []:
        if not isinstance(port, Mapping):
            continue
        expects.append(_input_port_to_expect_stub(port))


def _output_port_to_expose_stub(port: Mapping[str, Any]) -> Dict[str, Any]:
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

    pt = expose_passthrough(stub)
    pt["contract_id"] = port.get("contractId")
    if port.get("type"):
        pt["port_type"] = port["type"]
    if port.get("sbom"):
        pt["sbom"] = list(port["sbom"])
    if port.get("inputContracts"):
        pt["input_contracts"] = list(port["inputContracts"])
    if port.get("customProperties"):
        pt["custom_properties"] = list(port["customProperties"])
    if port.get("authoritativeDefinitions"):
        pt["authoritative_definitions"] = list(port["authoritativeDefinitions"])
    return stub


def _input_port_to_expect_stub(port: Mapping[str, Any]) -> Dict[str, Any]:
    name = port.get("name") or "input"
    stub: Dict[str, Any] = {
        "id": name,
        "version": str(port.get("version", 1)),
        "contractId": port.get("contractId"),
    }
    if port.get("tags"):
        stub["tags"] = list(port["tags"])
    pt = expect_passthrough(stub)
    if port.get("customProperties"):
        pt["custom_properties"] = list(port["customProperties"])
    if port.get("authoritativeDefinitions"):
        pt["authoritative_definitions"] = list(port["authoritativeDefinitions"])
    return stub
