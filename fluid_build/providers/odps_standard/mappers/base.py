# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Section mapper protocol + pass-through namespace for the Bitol ODPS provider.

Mirrors the design of :mod:`fluid_build.providers.odcs.mappers.base` so the
two providers share the same modular vocabulary — including the verbatim
stash + native overlay round-trip pattern (:func:`stash`, :func:`get_stash`,
:func:`overlay_native` are re-exported from there). Per-level pass-through
namespaces:

- contract-level → ``metadata.odps_passthrough.*``
- per-expose    → ``expose.odps_passthrough.*``
- per-expect    → ``expect.odps_passthrough.*``
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

from fluid_build.providers.odcs.mappers.base import (  # noqa: F401
    STASH_KEY,
    get_stash,
    overlay_native,
    stash,
)

PASSTHROUGH_KEY = "odps_passthrough"


def metadata_passthrough(fluid: MutableMapping[str, Any]) -> Dict[str, Any]:
    metadata = fluid.setdefault("metadata", {})
    if not isinstance(metadata, dict):
        metadata = dict(metadata)
        fluid["metadata"] = metadata
    return metadata.setdefault(PASSTHROUGH_KEY, {})


def get_metadata_passthrough(fluid: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = fluid.get("metadata") or {}
    if not isinstance(metadata, Mapping):
        return {}
    pt = metadata.get(PASSTHROUGH_KEY)
    return pt if isinstance(pt, Mapping) else {}


def expose_passthrough(expose: MutableMapping[str, Any]) -> Dict[str, Any]:
    return expose.setdefault(PASSTHROUGH_KEY, {})


def get_expose_passthrough(expose: Mapping[str, Any]) -> Mapping[str, Any]:
    pt = expose.get(PASSTHROUGH_KEY)
    return pt if isinstance(pt, Mapping) else {}


def expect_passthrough(expect: MutableMapping[str, Any]) -> Dict[str, Any]:
    return expect.setdefault(PASSTHROUGH_KEY, {})


def get_expect_passthrough(expect: Mapping[str, Any]) -> Mapping[str, Any]:
    pt = expect.get(PASSTHROUGH_KEY)
    return pt if isinstance(pt, Mapping) else {}


@dataclass
class ImportCtx:
    odps: Mapping[str, Any]
    fluid: MutableMapping[str, Any]
    logger: logging.Logger
    options: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExportCtx:
    fluid: Mapping[str, Any]
    odps: MutableMapping[str, Any]
    logger: logging.Logger
    options: Dict[str, Any] = field(default_factory=dict)


def fluid_id(fluid: Mapping[str, Any]) -> Optional[str]:
    if "id" in fluid:
        return fluid["id"]
    contract = fluid.get("contract")
    if isinstance(contract, Mapping) and contract.get("id"):
        return contract["id"]
    metadata = fluid.get("metadata")
    if isinstance(metadata, Mapping) and metadata.get("id"):
        return metadata["id"]
    return None


def downgrade_v110_keys(
    doc: MutableMapping[str, Any],
    keys: Iterable[str],
    logger: logging.Logger,
    *,
    where: str,
) -> None:
    """Make ``doc`` (a product or port) valid for an ODPS version below v1.1.0.

    Each v1.1.0-only key in ``keys`` is moved into the v1.0.0-compatible
    ``customProperties`` list as ``{property: <key>, value: <value>}`` (the
    legacy encoding of the product ``type``) unless an entry with that
    property already exists. The v1.1.0-only ``id``/``vendor`` keys of
    custom-property entries are dropped. Every change is logged as a warning.
    """
    moved: List[str] = []
    custom = doc.get("customProperties")
    custom_list: List[Any] = list(custom) if isinstance(custom, list) else []
    present = {c.get("property") for c in custom_list if isinstance(c, Mapping)}
    for key in keys:
        if key not in doc:
            continue
        value = doc.pop(key)
        moved.append(key)
        if key not in present and value is not None:
            custom_list.append({"property": key, "value": value})
            present.add(key)
    stripped = False
    cleaned: List[Any] = []
    for entry in custom_list:
        if isinstance(entry, Mapping) and ("id" in entry or "vendor" in entry):
            entry = {k: v for k, v in entry.items() if k not in ("id", "vendor")}
            stripped = True
        cleaned.append(entry)
    if (moved or stripped) and (cleaned or "customProperties" in doc):
        doc["customProperties"] = cleaned
    if moved:
        logger.warning(
            "ODPS < v1.1.0 has no %s on %s; moved to customProperties",
            ", ".join(moved),
            where,
        )
    if stripped:
        logger.warning(
            "ODPS < v1.1.0 custom properties have no id/vendor; dropped them on %s", where
        )


def conform_to_schema(
    doc: MutableMapping[str, Any],
    schema: Mapping[str, Any],
    logger: logging.Logger,
) -> None:
    """Make ``doc`` fit ``schema`` (an ODPS JSON Schema) by removing what it
    does not allow — used to render a product below v1.1.0.

    What is allowed is read from the schema itself, not from hand lists, so
    every nested object is covered (ports, management ports, SBOM entries,
    input contracts, support channels, team members, custom properties, …):

    - a key an object's schema does not list (``additionalProperties:
      false``) is moved into that object's ``customProperties`` as
      ``{property: <key>, value: <value>}`` when the object allows
      ``customProperties`` (and has no entry of that name yet), else it is
      dropped;
    - an array entry still missing a key its schema requires (an SBOM entry
      without ``url``, say) is dropped.

    Every change is logged as a warning. A document that already fits is
    left untouched.
    """
    _conform(doc, schema, schema, "", logger)


def _resolve_ref(node: Any, root: Mapping[str, Any]) -> Mapping[str, Any]:
    seen = 0
    while isinstance(node, Mapping) and isinstance(node.get("$ref"), str) and seen < 32:
        ref = node["$ref"]
        if not ref.startswith("#/"):
            return {}
        target: Any = root
        for part in ref[2:].split("/"):
            target = target.get(part) if isinstance(target, Mapping) else None
        node = target
        seen += 1
    return node if isinstance(node, Mapping) else {}


def _conform(value: Any, node: Any, root: Mapping[str, Any], where: str, logger) -> None:
    node = _resolve_ref(node, root)
    if isinstance(value, MutableMapping):
        props = node.get("properties")
        if not isinstance(props, Mapping):
            return
        if node.get("additionalProperties") is False:
            _strip_unknown_keys(value, props, where or "the product", logger)
        for key, sub in list(value.items()):
            if key in props:
                _conform(sub, props[key], root, f"{where}.{key}" if where else key, logger)
    elif isinstance(value, list):
        items = _resolve_ref(node.get("items"), root)
        if not items:
            return
        required = [r for r in items.get("required") or [] if isinstance(r, str)]
        kept: List[Any] = []
        for index, item in enumerate(value):
            item_where = f"{where}[{index}]"
            _conform(item, items, root, item_where, logger)
            missing = [r for r in required if isinstance(item, Mapping) and r not in item]
            if missing:
                logger.warning(
                    "ODPS < v1.1.0 requires %s on %s; dropped that entry",
                    ", ".join(missing),
                    item_where,
                )
                continue
            kept.append(item)
        if len(kept) != len(value):
            value[:] = kept


def _strip_unknown_keys(
    obj: MutableMapping[str, Any],
    props: Mapping[str, Any],
    where: str,
    logger: logging.Logger,
) -> None:
    unknown = [k for k in obj if k not in props]
    if not unknown:
        return
    moved: List[str] = []
    dropped: List[str] = []
    custom: Optional[List[Any]] = None
    if "customProperties" in props:
        existing = obj.get("customProperties")
        custom = list(existing) if isinstance(existing, list) else []
    present = {c.get("property") for c in custom or [] if isinstance(c, Mapping)}
    for key in unknown:
        value = obj.pop(key)
        if custom is not None and key not in present and value is not None:
            custom.append({"property": key, "value": value})
            present.add(key)
            moved.append(key)
        else:
            dropped.append(key)
    if moved and custom is not None:
        obj["customProperties"] = custom
    if moved:
        logger.warning(
            "ODPS < v1.1.0 has no %s on %s; moved to customProperties", ", ".join(moved), where
        )
    if dropped:
        logger.warning("ODPS < v1.1.0 has no %s on %s; dropped", ", ".join(dropped), where)


def contract_id_for_port(product_id: str, port_name: str) -> str:
    """Convention used by export + datamesh-manager: ``{productId}.{portName}``.

    The OdcsProvider's per-port render guarantees the emitted ODCS contract's
    ``id`` matches this string exactly, which is the linking invariant for
    Bitol ODPS fragments mode.
    """
    return f"{product_id}.{port_name}"
