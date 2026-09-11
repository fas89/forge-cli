# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Section mapper protocol, contexts, and the round-trip pass-through namespace.

Every ODCS section maps to one module under :mod:`fluid_build.providers.odcs.mappers`
exposing a pair of pure functions: :func:`to_fluid` (ODCS → FLUID) and
:func:`to_odcs` (FLUID → ODCS).

Lossless round-trip is achieved with a **verbatim stash + native overlay**
pattern rather than per-field pass-through lists:

- On import, each ODCS section (top-level doc, schema object, property) is
  stashed *verbatim* under ``<bucket>[STASH_KEY]`` and only the FLUID-native
  fields are projected out.
- On export, the stash is the base layer; native fields are overlaid **only
  where they differ from what the stash would have projected**
  (:func:`overlay_native`). Untouched fields — including ones added by a
  newer spec version, explicit ``false`` booleans, and empty lists — pass
  through byte-for-byte with zero per-field code.
- A section with no stash (fresh FLUID) is synthesised as before.

Pass-through data lives under a single key per level so the round-trip
surface is auditable in one place.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Optional

PASSTHROUGH_KEY = "odcs_passthrough"
STASH_KEY = "_verbatim"


def metadata_passthrough(fluid: MutableMapping[str, Any]) -> Dict[str, Any]:
    """Get or create the contract-level pass-through bucket under ``metadata``."""
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
    """Get or create an expose's pass-through bucket."""
    return expose.setdefault(PASSTHROUGH_KEY, {})


def get_expose_passthrough(expose: Mapping[str, Any]) -> Mapping[str, Any]:
    pt = expose.get(PASSTHROUGH_KEY)
    return pt if isinstance(pt, Mapping) else {}


def field_passthrough(fld: MutableMapping[str, Any]) -> Dict[str, Any]:
    """Get or create a field's pass-through bucket."""
    return fld.setdefault(PASSTHROUGH_KEY, {})


def get_field_passthrough(fld: Mapping[str, Any]) -> Mapping[str, Any]:
    pt = fld.get(PASSTHROUGH_KEY)
    return pt if isinstance(pt, Mapping) else {}


# ---- verbatim stash + native overlay ---------------------------------------


def stash(bucket: MutableMapping[str, Any], section: Mapping[str, Any]) -> None:
    """Record ``section`` verbatim in ``bucket`` for lossless re-export."""
    bucket[STASH_KEY] = dict(section)


def get_stash(bucket: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
    """The verbatim section stashed on import, or ``None`` for fresh FLUID."""
    s = bucket.get(STASH_KEY)
    return s if isinstance(s, Mapping) else None


def overlay_native(
    base: Mapping[str, Any],
    native_then: Mapping[str, Any],
    native_now: Mapping[str, Any],
    *,
    keys: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Three-way merge: ``base`` + (``native_now`` − ``native_then``).

    ``native_then`` is what the FLUID projection of ``base`` maps back to;
    ``native_now`` is the same projection of the *current* FLUID. Only keys
    whose value actually changed are written over ``base`` — so a value the
    user never touched keeps its exact original form (absent stays absent,
    ``false`` stays ``false``, ``[]`` stays ``[]``), while a genuine edit in
    FLUID-land wins. A key edited to ``None`` is removed.

    Both projections must be *total* (always emit every native key, using
    ``None`` for "not set") so the comparison is meaningful.
    """
    out = dict(base)
    for k in keys if keys is not None else (set(native_then) | set(native_now)):
        then_v = native_then.get(k)
        now_v = native_now.get(k)
        if now_v == then_v:
            continue
        if now_v is None:
            out.pop(k, None)
        else:
            out[k] = now_v
    return out


@dataclass
class ImportCtx:
    """Mutable context threaded through ODCS → FLUID mappers."""

    odcs: Mapping[str, Any]
    fluid: MutableMapping[str, Any]
    logger: logging.Logger
    options: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExportCtx:
    """Mutable context threaded through FLUID → ODCS mappers.

    ``options["api_version"]`` — when set, the explicit ODCS version to emit
    (CLI flag / constructor / env). When ``None`` the metadata mapper
    preserves an imported document's own ``apiVersion`` and otherwise falls
    back to :data:`fluid_build.providers.odcs.validation.DEFAULT_API_VERSION`.
    """

    fluid: Mapping[str, Any]
    odcs: MutableMapping[str, Any]
    logger: logging.Logger
    options: Dict[str, Any] = field(default_factory=dict)


def fluid_id(fluid: Mapping[str, Any]) -> Optional[str]:
    """Resolve a FLUID contract's id from any of the supported locations."""
    if "id" in fluid:
        return fluid["id"]
    contract = fluid.get("contract")
    if isinstance(contract, Mapping) and contract.get("id"):
        return contract["id"]
    metadata = fluid.get("metadata")
    if isinstance(metadata, Mapping) and metadata.get("id"):
        return metadata["id"]
    return None
