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


def contract_id_for_port(product_id: str, port_name: str) -> str:
    """Convention used by export + datamesh-manager: ``{productId}.{portName}``.

    The OdcsProvider's per-port render guarantees the emitted ODCS contract's
    ``id`` matches this string exactly, which is the linking invariant for
    Bitol ODPS fragments mode.
    """
    return f"{product_id}.{port_name}"
