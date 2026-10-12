# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Bitol ODPS JSON Schema validation (version-aware) + round-trip diff.

Two ODPS schema versions are vendored side by side:

- ``v1.1.0`` — current upstream. Additive over v1.0.0: optional top-level
  ``type`` / ``deprecated`` / ``context`` / ``synonyms``; ``status`` no
  longer required; per-element ``id``; ``deprecated``/``context``/``synonyms``
  on ports; ``vendor`` on custom properties; SBOM ``url`` optional.
- ``v1.0.0`` — previous. Kept so a product that *declares* ``v1.0.0`` is
  validated against exactly the spec it claims.

Mirrors :mod:`fluid_build.providers.odcs.validation`; see there for the
version-resolution rules. Emit default stays at :data:`DEFAULT_API_VERSION`;
opt into the newer version with ``ODPS_API_VERSION`` or
``BitolOdpsProvider(api_version=...)``.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from fluid_build.providers.base import ProviderError

# Reuse the deep-diff implementation and the version normaliser from the odcs
# validation module — the two providers share the same notion of "structural
# equality" and the same ``vX.Y.Z`` spelling rules.
from fluid_build.providers.odcs.validation import (  # noqa: F401
    api_version_tuple,
    normalize_api_version,
    roundtrip_check,
)

LOG = logging.getLogger(__name__)

SUPPORTED_API_VERSIONS: Tuple[str, ...] = ("v1.1.0", "v1.0.0")  # newest first
DEFAULT_API_VERSION = "v1.0.0"  # conservative emit default

_SCHEMA_DIR = Path(__file__).parent / "schemas"
_SCHEMA_FILES: Dict[str, str] = {v: f"odps-product-{v}.json" for v in SUPPORTED_API_VERSIONS}
_SCHEMA_CACHE: Dict[str, Optional[Dict[str, Any]]] = {}


def resolve_schema_version(doc: Mapping[str, Any]) -> str:
    """Declared+vendored ``apiVersion`` → that; otherwise newest vendored."""
    declared = normalize_api_version(doc.get("apiVersion") if isinstance(doc, Mapping) else None)
    if declared in _SCHEMA_FILES:
        return declared
    return SUPPORTED_API_VERSIONS[0]


def load_schema(api_version: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Load a vendored ODPS JSON Schema (default :data:`DEFAULT_API_VERSION`)."""
    version = normalize_api_version(api_version) or DEFAULT_API_VERSION
    if version not in _SCHEMA_FILES:
        raise ProviderError(
            f"Unsupported ODPS apiVersion {version!r}; "
            f"vendored: {', '.join(SUPPORTED_API_VERSIONS)}"
        )
    if version in _SCHEMA_CACHE:
        return _SCHEMA_CACHE[version]

    schema_path = _SCHEMA_DIR / _SCHEMA_FILES[version]
    schema: Optional[Dict[str, Any]] = None
    if not schema_path.exists():
        LOG.warning("ODPS schema not found: %s", schema_path)
    else:
        try:
            with open(schema_path) as f:
                schema = json.load(f)
        except Exception as exc:  # pragma: no cover - defensive
            LOG.error("Failed to load ODPS schema %s: %s", schema_path, exc)
    _SCHEMA_CACHE[version] = schema
    return schema


def validate(odps: Mapping[str, Any], schema: Optional[Mapping[str, Any]] = None) -> None:
    """Validate an ODPS product against the JSON Schema.

    When ``schema`` is omitted the version is resolved from the document's
    ``apiVersion`` so a v1.1.0 product is never judged by the v1.0.0 schema.
    """
    try:
        import jsonschema
    except ImportError:
        LOG.warning("jsonschema not installed, skipping ODPS validation")
        return
    if schema is None:
        schema = load_schema(resolve_schema_version(odps))
        if schema is None:
            LOG.warning("ODPS schema not available, skipping validation")
            return
    try:
        jsonschema.validate(instance=odps, schema=schema)
    except jsonschema.ValidationError as exc:
        raise ProviderError(f"ODPS validation failed: {exc.message}") from exc
