# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""SLA mapper: ODCS ``slaProperties[]`` ↔ FLUID expose ``qos``.

The full ODCS list is preserved verbatim under
``metadata.odcs_passthrough.sla_properties`` so that an ODCS → FLUID → ODCS
round-trip is lossless. The first/only expose's ``qos`` block is also
populated (availability, freshnessSLO, labels) so the FLUID view is
meaningful in isolation. On export the stash is replayed unless that
``qos`` changed since import; an edit rewrites only the entries it touches.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Optional

from .base import (
    ExportCtx,
    ImportCtx,
    get_expose_passthrough,
    get_metadata_passthrough,
    get_stash,
    metadata_passthrough,
)

# ----- ODCS → FLUID --------------------------------------------------------


def to_fluid(ctx: ImportCtx) -> None:
    sla_props = ctx.odcs.get("slaProperties")
    if not isinstance(sla_props, list):
        return

    # Verbatim pass-through (canonical source of truth on re-export) — an
    # explicit empty list included.
    metadata_passthrough(ctx.fluid)["sla_properties"] = [
        copy.deepcopy(dict(p)) for p in sla_props if isinstance(p, Mapping)
    ]
    if not sla_props:
        return

    exposes = ctx.fluid.get("exposes") or []
    target = exposes[0] if exposes else None
    if not isinstance(target, dict):
        return

    qos: Dict[str, Any] = target.get("qos") or {}
    labels: Dict[str, Any] = qos.get("labels") or {}

    for prop in sla_props:
        if not isinstance(prop, Mapping):
            continue
        _apply_sla_to_qos(prop, qos, labels)

    if labels:
        qos["labels"] = labels
    if qos:
        target["qos"] = qos


def _project_qos(sla_props: Sequence[Any]) -> Optional[Dict[str, Any]]:
    """The ``qos`` block an import projects from ``slaProperties`` (or ``None``)."""
    qos: Dict[str, Any] = {}
    labels: Dict[str, Any] = {}
    for prop in sla_props:
        if isinstance(prop, Mapping):
            _apply_sla_to_qos(prop, qos, labels)
    if labels:
        qos["labels"] = labels
    return qos or None


def _apply_sla_to_qos(prop: Mapping[str, Any], qos: Dict[str, Any], labels: Dict[str, Any]) -> None:
    name = prop.get("property")
    value = copy.deepcopy(prop.get("value"))
    unit = prop.get("unit")
    if not name:
        return
    if name == "availability":
        try:
            fraction = float(value)
            qos["availability"] = f"{fraction * 100:g}%" if fraction <= 1 else f"{fraction:g}%"
        except (TypeError, ValueError):
            qos["availability"] = str(value)
    elif name in _FRESHNESS_PROPERTIES:
        qos.setdefault("freshnessSLO", str(value))
    elif name.startswith("label:"):
        labels[name[len("label:") :]] = value
    else:
        labels[_label_key(name, unit)] = value


_FRESHNESS_PROPERTIES = ("interval", "frequency", "latency", "retention")


def _label_key(name: str, unit: Any) -> str:
    if name.startswith("label:"):
        return name[len("label:") :]
    return f"{name}:{unit}" if unit else name


# ----- FLUID → ODCS --------------------------------------------------------


def to_odcs(ctx: ExportCtx) -> None:
    if not ctx.options.get("include_sla", True):
        return

    # Imported contract: three-way merge with the verbatim pass-through.
    raw = get_metadata_passthrough(ctx.fluid).get("sla_properties")
    if isinstance(raw, list):
        stashed = [p for p in raw if isinstance(p, Mapping)]
        merged = _merge_sla(ctx.fluid, stashed)
        if merged is not None:
            ctx.odcs["slaProperties"] = merged
        return

    properties = list(_build_sla_properties(ctx.fluid))
    if properties:
        ctx.odcs["slaProperties"] = properties


def _merge_sla(
    fluid: Mapping[str, Any], stashed: List[Mapping[str, Any]]
) -> Optional[List[Dict[str, Any]]]:
    """Replay the stash unless ``exposes[0].qos`` changed; then apply the edit.

    Only the keys FLUID projects (``availability``, ``freshnessSLO``,
    ``labels``) are compared; each edit rewrites, removes or appends just
    the ``slaProperties`` entry it came from, so units, elements and other
    unmodelled entries survive.
    """
    out = [copy.deepcopy(dict(p)) for p in stashed]
    exposes = fluid.get("exposes") or []
    target = exposes[0] if exposes else None
    if not isinstance(target, Mapping) or get_stash(get_expose_passthrough(target)) is None:
        # The import projected qos onto an imported expose only; with none
        # to compare against, the stash is the only source of truth.
        return out
    then = _project_qos(stashed) or {}
    now = target.get("qos") if isinstance(target.get("qos"), Mapping) else {}
    if not stashed and not now:
        return out
    if _qos_view(now) == _qos_view(then):
        return out

    # availability
    if now.get("availability") != then.get("availability"):
        idx = _find(out, lambda p: p.get("property") == "availability")
        parsed: Dict[str, Any] = {}
        if now.get("availability") is not None:
            _qos_to_sla({"availability": now["availability"]}, parsed)
        _apply_edit(out, idx, "availability", parsed.get("availability"))

    # freshnessSLO — the first interval/frequency/latency/retention entry
    now_f = now.get("freshnessSLO") or now.get("freshness_slo")
    if now_f != then.get("freshnessSLO"):
        idx = _find(out, lambda p: p.get("property") in _FRESHNESS_PROPERTIES)
        prop_name = out[idx]["property"] if idx is not None else "interval"
        _apply_edit(out, idx, prop_name, str(now_f) if now_f else None)

    # labels — keyed like the import projection; the last entry wins there
    then_l = then.get("labels") or {}
    now_l = now.get("labels") if isinstance(now.get("labels"), Mapping) else {}
    for key in list(then_l) + [k for k in now_l if k not in then_l]:
        if now_l.get(key) == then_l.get(key) and (key in now_l) == (key in then_l):
            continue
        idx = _find(
            out,
            lambda p, key=key: p.get("property")
            and p.get("property") != "availability"
            and p.get("property") not in _FRESHNESS_PROPERTIES
            and _label_key(p["property"], p.get("unit")) == key,
            last=True,
        )
        prop_name = out[idx]["property"] if idx is not None else f"label:{key}"
        _apply_edit(out, idx, prop_name, now_l.get(key) if key in now_l else None)
    return out


def _qos_view(qos: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "availability": qos.get("availability"),
        "freshnessSLO": qos.get("freshnessSLO") or qos.get("freshness_slo"),
        "labels": dict(qos.get("labels") or {}) or None,
    }


def _find(items: List[Dict[str, Any]], pred, last: bool = False) -> Optional[int]:
    hits = [i for i, p in enumerate(items) if pred(p)]
    if not hits:
        return None
    return hits[-1] if last else hits[0]


def _apply_edit(items: List[Dict[str, Any]], idx: Optional[int], name: str, value: Any) -> None:
    """Set (``idx``), remove (``value is None``) or append an SLA entry."""
    if value is None:
        if idx is not None:
            items.pop(idx)
        return
    if idx is not None:
        items[idx]["value"] = _coerce_like(items[idx].get("value"), value)
    else:
        items.append({"property": name, "value": copy.deepcopy(value)})


def _coerce_like(old: Any, new: Any) -> Any:
    """Keep the stashed value's numeric type when the edit is a numeric string."""
    if isinstance(new, str) and isinstance(old, (int, float)) and not isinstance(old, bool):
        try:
            number = float(new)
        except ValueError:
            return new
        return int(number) if isinstance(old, int) and number.is_integer() else number
    return copy.deepcopy(new)


def _build_sla_properties(fluid: Mapping[str, Any]) -> Sequence[Dict[str, Any]]:
    sla_values: Dict[str, Any] = {}
    for expose in fluid.get("exposes", []):
        if not isinstance(expose, Mapping):
            continue
        qos = expose.get("qos") or {}
        if not isinstance(qos, Mapping):
            continue
        _qos_to_sla(qos, sla_values)

    metadata = fluid.get("metadata") or {}
    if isinstance(metadata, Mapping):
        if metadata.get("update_frequency") and "interval" not in sla_values:
            sla_values["interval"] = metadata["update_frequency"]
        if metadata.get("availability") and "availability" not in sla_values:
            try:
                a = float(metadata["availability"])
                sla_values["availability"] = a / 100 if a > 1 else a
            except (TypeError, ValueError):
                pass
        if metadata.get("quality_threshold"):
            try:
                sla_values["completenessKpi"] = float(metadata["quality_threshold"])
            except (TypeError, ValueError):
                pass

    out: List[Dict[str, Any]] = []
    for prop, value in sla_values.items():
        if value is None:
            continue
        out.append({"property": prop, "value": copy.deepcopy(value)})
    return out


def _qos_to_sla(qos: Mapping[str, Any], sla_values: Dict[str, Any]) -> None:
    avail = qos.get("availability")
    if avail is not None and "availability" not in sla_values:
        try:
            s = str(avail).strip()
            if s.endswith("%"):
                sla_values["availability"] = float(s.rstrip("%")) / 100
            else:
                parsed = float(s)
                sla_values["availability"] = parsed / 100 if parsed > 1 else parsed
        except (TypeError, ValueError):
            sla_values["availability"] = str(avail)

    freshness = qos.get("freshnessSLO") or qos.get("freshness_slo")
    if freshness and "interval" not in sla_values:
        sla_values["interval"] = str(freshness)

    labels = qos.get("labels") or {}
    if isinstance(labels, Mapping):
        for k, v in labels.items():
            sla_values.setdefault(f"label:{k}", v)
