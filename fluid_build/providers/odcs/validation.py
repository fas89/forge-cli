# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""ODCS JSON Schema validation + round-trip diff utility."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Dict, List, Optional

from fluid_build.providers.base import ProviderError

LOG = logging.getLogger(__name__)

_SCHEMA_FILENAME = "odcs-schema-v3.1.0.json"


def load_schema() -> Optional[Dict[str, Any]]:
    """Load the vendored ODCS v3.1.0 JSON Schema. Returns None if missing."""
    schema_path = Path(__file__).parent / _SCHEMA_FILENAME
    if not schema_path.exists():
        LOG.warning("ODCS schema not found: %s", schema_path)
        return None
    try:
        with open(schema_path) as f:
            return json.load(f)
    except Exception as exc:  # pragma: no cover - defensive
        LOG.error("Failed to load ODCS schema: %s", exc)
        return None


def validate(odcs: Mapping[str, Any], schema: Mapping[str, Any]) -> None:
    """Validate an ODCS contract against the v3.1.0 JSON Schema.

    Raises :class:`ProviderError` on validation failure. Silently no-ops when
    the optional :mod:`jsonschema` dependency is not installed.
    """
    try:
        import jsonschema
    except ImportError:
        LOG.warning("jsonschema not installed, skipping ODCS validation")
        return
    try:
        jsonschema.validate(instance=odcs, schema=schema)
    except jsonschema.ValidationError as exc:
        raise ProviderError(f"ODCS validation failed: {exc.message}") from exc


def roundtrip_check(odcs: Mapping[str, Any], reconstructed: Mapping[str, Any]) -> Dict[str, Any]:
    """Compare an original ODCS dict to one rebuilt via ``import → export``.

    Returns a structured diff::

        {
            "equal": bool,
            "missing": [".path.to.lost.field", ...],
            "extra":   [".path.added.by.export", ...],
            "changed": [{"path": ".x", "old": ..., "new": ...}, ...]
        }

    Used by tests and the forge ground-truth guard. Pure; no I/O.
    """
    missing: List[str] = []
    extra: List[str] = []
    changed: List[Dict[str, Any]] = []
    _diff("", odcs, reconstructed, missing, extra, changed)
    return {
        "equal": not (missing or extra or changed),
        "missing": missing,
        "extra": extra,
        "changed": changed,
    }


def _diff(
    path: str,
    a: Any,
    b: Any,
    missing: List[str],
    extra: List[str],
    changed: List[Dict[str, Any]],
) -> None:
    if isinstance(a, Mapping) and isinstance(b, Mapping):
        a_keys = set(a.keys())
        b_keys = set(b.keys())
        for k in a_keys - b_keys:
            missing.append(f"{path}.{k}".lstrip("."))
        for k in b_keys - a_keys:
            extra.append(f"{path}.{k}".lstrip("."))
        for k in a_keys & b_keys:
            _diff(f"{path}.{k}", a[k], b[k], missing, extra, changed)
        return
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            changed.append({"path": path, "old": f"len={len(a)}", "new": f"len={len(b)}"})
            return
        for i, (x, y) in enumerate(zip(a, b)):
            _diff(f"{path}[{i}]", x, y, missing, extra, changed)
        return
    if a != b:
        changed.append({"path": path, "old": a, "new": b})
