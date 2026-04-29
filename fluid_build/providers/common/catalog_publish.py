"""Shared helpers for marketplace-style catalog publishing."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any, Dict, List

from fluid_build.providers.catalogs.base import (
    BaseCatalogProvider,
    CatalogAssetRef,
    CatalogContractRef,
    CatalogPort,
    CatalogProduct,
)


class _CatalogProductMapper(BaseCatalogProvider):
    """Utility-only mapper for reusing the normalized publish model."""

    name = "catalog-product-mapper"

    async def publish(self, product: CatalogProduct):  # pragma: no cover - utility only
        raise NotImplementedError

    async def update(self, product: CatalogProduct):  # pragma: no cover - utility only
        raise NotImplementedError

    async def verify(self, asset_id: str) -> bool:  # pragma: no cover - utility only
        return False


_PRODUCT_MAPPER = _CatalogProductMapper({})


def map_contract_to_product(contract: Mapping[str, Any]) -> CatalogProduct:
    """Normalize a FLUID contract to the shared product graph."""

    return _PRODUCT_MAPPER.map_contract_to_product(dict(contract))


def product_to_contract(product: CatalogProduct) -> Dict[str, Any]:
    """Best-effort fallback reconstruction of a FLUID contract."""

    metadata: Dict[str, Any] = {
        "name": product.name,
        "description": product.description,
        "domain": product.domain,
        "version": product.version,
        "layer": product.layer,
        "status": product.status,
        "tags": list(product.tags),
        "custom": dict(product.custom),
    }
    fluid: Dict[str, Any] = {
        "id": product.id,
        "name": product.name,
        "description": product.description,
        "kind": product.kind,
        "domain": product.domain,
        "version": product.version,
        "metadata": metadata,
        "owner": {
            "team": product.owner.team,
            "email": product.owner.email,
            "name": product.owner.name,
            "id": product.owner.id,
        },
    }

    if product.links:
        fluid["links"] = dict(product.links)

    if product.input_ports:
        fluid["expects"] = [_port_to_contract(port, output=False) for port in product.input_ports]

    if product.output_ports:
        fluid["exposes"] = [_port_to_contract(port, output=True) for port in product.output_ports]

    if product.members:
        fluid["members"] = [_member_to_contract(member) for member in product.members]

    if product.contracts:
        fluid["contracts"] = [_contract_to_dict(ref) for ref in product.contracts]

    return fluid


def slugify(value: str) -> str:
    """Build a deterministic ASCII slug."""

    text = re.sub(r"[^a-zA-Z0-9]+", "-", str(value or "").strip()).strip("-").lower()
    return text or "unknown"


def member_refs_for_port(product: CatalogProduct, port: CatalogPort) -> List[CatalogAssetRef]:
    """Return member refs that explicitly correspond to *port*."""

    matches: List[CatalogAssetRef] = []
    for member in product.members:
        metadata = member.metadata if isinstance(member.metadata, Mapping) else {}
        linked_ids = {
            _normalize_string(member.id),
            _normalize_string(metadata.get("port_id")),
            _normalize_string(metadata.get("portId")),
            _normalize_string(metadata.get("expose_id")),
            _normalize_string(metadata.get("exposeId")),
            _normalize_string(metadata.get("output_port_id")),
            _normalize_string(metadata.get("outputPortId")),
        }
        if _normalize_string(port.id) in linked_ids:
            matches.append(member)
    return matches


def contract_refs_for_port(product: CatalogProduct, port: CatalogPort) -> List[CatalogContractRef]:
    """Return all known contract refs linked to *port*."""

    refs: List[CatalogContractRef] = []
    if port.contract_ref:
        refs.append(port.contract_ref)

    for ref in product.contracts:
        if _normalize_string(ref.port_id) == _normalize_string(port.id):
            refs.append(ref)
        elif _normalize_string(ref.id).endswith(f".{_normalize_string(port.id)}"):
            refs.append(ref)

    seen: set[str] = set()
    deduped: List[CatalogContractRef] = []
    for ref in refs:
        if ref.id in seen:
            continue
        seen.add(ref.id)
        deduped.append(ref)
    return deduped


def _port_to_contract(port: CatalogPort, *, output: bool) -> Dict[str, Any]:
    section: Dict[str, Any] = {
        "id": port.id,
        "name": port.name,
        "description": port.description,
        "kind": port.kind,
        "tags": list(port.tags),
        "links": dict(port.links),
        "custom": dict(port.custom),
        "status": port.status,
        "sensitivity": port.sensitivity,
    }
    if output:
        section["exposeId"] = port.id
    if port.platform and port.platform != "unknown":
        section["provider"] = port.platform
    binding: Dict[str, Any] = {}
    if port.platform and port.platform != "unknown":
        binding["platform"] = port.platform
    if port.location not in (None, {}, []):
        binding["location"] = port.location
    if isinstance(port.custom, Mapping) and port.custom.get("format") is not None:
        binding["format"] = port.custom["format"]
    if binding:
        section["binding"] = binding
    if port.schema:
        section["contract"] = {"schema": port.schema}
    if port.source_system_id:
        section["productId"] = port.source_system_id
    return section


def _member_to_contract(member: CatalogAssetRef) -> Dict[str, Any]:
    return {
        "id": member.id,
        "name": member.name,
        "kind": member.kind,
        "relation": member.relation,
        "platform": member.platform,
        "location": member.location,
        "external_ref": member.external_ref,
        "metadata": dict(member.metadata),
    }


def _contract_to_dict(ref: CatalogContractRef) -> Dict[str, Any]:
    return {
        "id": ref.id,
        "name": ref.name,
        "port_id": ref.port_id,
        "format": ref.format,
        "url": ref.url,
        "kind": ref.kind,
        "metadata": dict(ref.metadata),
    }


def _normalize_string(value: Any) -> str:
    return str(value or "").strip().lower()
