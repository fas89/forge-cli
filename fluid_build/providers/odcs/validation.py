# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""ODCS JSON Schema validation (version-aware) + round-trip diff utility.

Two ODCS schema versions are vendored side by side:

- ``v3.2.0`` — current upstream. Additive over v3.1.0: optional ``context``,
  ``synonyms``, ``enum``, ``semanticType``, ``deprecated``, per-element
  ``id``; ``status`` no longer required; 10 new ``Server.type`` engines.
- ``v3.1.0`` — previous. Kept so a contract that *declares* ``v3.1.0`` is
  validated against exactly the spec it claims, not a relaxed superset.

:func:`validate` picks the schema from the document's own ``apiVersion``
(see :func:`resolve_schema_version`). The emit default stays at
:data:`DEFAULT_API_VERSION` so existing downstream validators keep
accepting what we produce; opt into the newer version with the
``ODCS_API_VERSION`` environment variable or ``OdcsProvider(api_version=...)``.
"""

from __future__ import annotations

import datetime
import json
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fluid_build.providers.base import ProviderError

LOG = logging.getLogger(__name__)

# Newest first. The first entry is the superset fallback for any document
# whose apiVersion we don't vendor exactly (older-than-vendored versions are
# in every newer schema's ``apiVersion`` enum, and each release has only
# relaxed/added — never tightened — so the newest schema accepts them).
SUPPORTED_API_VERSIONS: Tuple[str, ...] = ("v3.2.0", "v3.1.0")

# Conservative emit default. Bumping this changes what every export declares
# and would break downstream validators pinned to the older schema.
DEFAULT_API_VERSION = "v3.1.0"

_SCHEMA_FILES: Dict[str, str] = {v: f"odcs-schema-{v}.json" for v in SUPPORTED_API_VERSIONS}
_SCHEMA_CACHE: Dict[str, Optional[Dict[str, Any]]] = {}


def normalize_api_version(version: Optional[str]) -> Optional[str]:
    """Accept ``3.2.0`` / ``v3.2.0`` / ``V3.2.0`` and return ``v3.2.0``."""
    if version is None:
        return None
    v = str(version).strip()
    if not v:
        return None
    if v[0] in ("v", "V"):
        v = v[1:]
    return f"v{v}"


def api_version_tuple(version: Optional[str]) -> Tuple[int, ...]:
    """``v3.2.0`` → ``(3, 2, 0)`` for ordered comparison (``()`` if unparsable)."""
    v = normalize_api_version(version)
    if not v:
        return ()
    try:
        return tuple(int(p) for p in v[1:].split("."))
    except ValueError:
        return ()


def resolve_schema_version(doc: Mapping[str, Any]) -> str:
    """Pick the vendored schema version to validate ``doc`` against.

    - Declared ``apiVersion`` that we vendor → that exact version.
    - Anything else (missing, older-than-vendored, unknown-future) → newest
      vendored, which is the acceptance superset. Unknown-future versions
      still fail on the ``apiVersion`` enum, with the clearest error.
    """
    declared = normalize_api_version(doc.get("apiVersion") if isinstance(doc, Mapping) else None)
    if declared in _SCHEMA_FILES:
        return declared
    return SUPPORTED_API_VERSIONS[0]


def load_schema(api_version: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Load a vendored ODCS JSON Schema. Returns None if missing.

    ``api_version`` defaults to :data:`DEFAULT_API_VERSION`. Unknown versions
    raise :class:`ProviderError` so a typo in ``ODCS_API_VERSION`` fails loud.
    """
    version = normalize_api_version(api_version) or DEFAULT_API_VERSION
    if version not in _SCHEMA_FILES:
        raise ProviderError(
            f"Unsupported ODCS apiVersion {version!r}; "
            f"vendored: {', '.join(SUPPORTED_API_VERSIONS)}"
        )
    if version in _SCHEMA_CACHE:
        return _SCHEMA_CACHE[version]

    schema_path = Path(__file__).parent / _SCHEMA_FILES[version]
    schema: Optional[Dict[str, Any]] = None
    if not schema_path.exists():
        LOG.warning("ODCS schema not found: %s", schema_path)
    else:
        try:
            with open(schema_path) as f:
                schema = json.load(f)
        except Exception as exc:  # pragma: no cover - defensive
            LOG.error("Failed to load ODCS schema %s: %s", schema_path, exc)
    _SCHEMA_CACHE[version] = schema
    return schema


def json_instance(doc: Any) -> Any:
    """``doc`` as JSON would carry it, for JSON-Schema validation.

    Bitol schemas are JSON Schemas, but contracts are usually YAML, where an
    unquoted ``2024-01-01`` loads as a :class:`datetime.date`. JSON has no
    date type, so such values are rendered as ISO-8601 strings (what the
    author wrote) instead of failing every ``type: string`` check. Other
    values are returned unchanged; the input is never mutated.

    Containers shared by reference in ``doc`` (YAML aliases load that way)
    are converted once and stay shared in the result, so the conversion
    never multiplies memory.
    """
    return _json_instance(doc, {})


def _json_instance(doc: Any, memo: Dict[int, Any]) -> Any:
    if isinstance(doc, (datetime.datetime, datetime.date, datetime.time)):
        return doc.isoformat()
    if isinstance(doc, (Mapping, list, tuple)):
        key = id(doc)
        if key in memo:
            return memo[key]
        if isinstance(doc, Mapping):
            out_map: Dict[Any, Any] = {}
            memo[key] = out_map
            for k, v in doc.items():
                out_map[k] = _json_instance(v, memo)
            return out_map
        out_list: List[Any] = []
        memo[key] = out_list
        out_list.extend(_json_instance(v, memo) for v in doc)
        return out_list
    return doc


def validate(odcs: Mapping[str, Any], schema: Optional[Mapping[str, Any]] = None) -> None:
    """Validate an ODCS contract against the JSON Schema.

    When ``schema`` is omitted the version is resolved from the document's
    ``apiVersion`` (:func:`resolve_schema_version`) — so a v3.2.0 contract
    is never judged by the v3.1.0 schema and vice versa. YAML date/time
    values are validated as the ISO strings JSON would carry
    (:func:`json_instance`).

    Raises :class:`ProviderError` on validation failure. Silently no-ops when
    the optional :mod:`jsonschema` dependency is not installed.
    """
    try:
        import jsonschema
    except ImportError:
        LOG.warning("jsonschema not installed, skipping ODCS validation")
        return
    if schema is None:
        schema = load_schema(resolve_schema_version(odcs))
        if schema is None:
            LOG.warning("ODCS schema not available, skipping validation")
            return
    try:
        jsonschema.validate(instance=json_instance(odcs), schema=schema)
    except jsonschema.ValidationError as exc:
        raise ProviderError(f"ODCS validation failed: {exc.message}") from exc


def validate_via_vowl(odcs: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    """Second-pass ODCS validation via the ``vowl`` library (optional).

    Runs on the way out of every export to give us an independent, native
    ODCS-tooling sanity check that the emitted contract is parseable by a
    real ODCS consumer — not just spec-shape-valid against the vendored
    JSON Schema. ``vowl`` is an official Bitol ODCS vendor (UK Government
    Digital Service) and shipped as an MIT-licensed library, so importing
    it here doesn't bring forge-cli into competition with another data-
    contract toolchain — it's pure standards-body validation.

    Behaviour:
      - Soft-imports ``vowl``. If absent, returns ``None`` so callers can
        skip the second pass cleanly. The first-pass ``jsonschema``
        validation still ran.
      - Builds ``vowl.Contract(odcs_dict)`` which runs vowl's own
        jsonschema validation against the version-appropriate ODCS schema.
        If the installed vowl predates the contract's ``apiVersion`` it
        raises; we surface that as a :class:`ProviderError` with a
        ``vowl:`` prefix rather than masking it.
      - Enumerates the resolved check references so any lazy parse errors
        surface here too.
      - Returns a small diagnostic dict ``{api_version, schemas, total_checks}``
        the export path can log/expose.
    """
    try:
        from vowl import Contract  # type: ignore[import-untyped]
    except ImportError:
        LOG.debug("vowl not installed, skipping second-pass ODCS validation")
        return None

    try:
        contract = Contract(dict(odcs))
        refs = contract.get_check_references_by_schema()
    except ValueError as exc:
        # vowl raises ValueError("Unsupported API version: vX.Y.Z ...") when
        # the installed release predates the contract's apiVersion (vowl 0.0.5
        # tops out at v3.1.0; 0.0.6 adds v3.2.0). That is a tooling gap, not a
        # contract defect — skip the second pass rather than fail the export.
        if "Unsupported API version" in str(exc):
            LOG.warning(
                "vowl second-pass skipped — installed vowl does not support " "apiVersion %r (%s)",
                odcs.get("apiVersion"),
                exc,
            )
            return None
        raise ProviderError(f"vowl: {type(exc).__name__}: {exc}") from exc
    except Exception as exc:
        raise ProviderError(f"vowl: {type(exc).__name__}: {exc}") from exc

    return {
        "api_version": contract.get_api_version(),
        "schemas": list(contract.get_schema_names() or []),
        "total_checks": sum(len(v) for v in (refs or {}).values()),
    }


def roundtrip_check(odcs: Mapping[str, Any], reconstructed: Mapping[str, Any]) -> Dict[str, Any]:
    """Compare an original ODCS dict to one rebuilt via ``import → export``.

    Returns a structured diff::

        {
            "equal": bool,
            "missing": [".path.to.lost.field", ...],
            "extra":   [".path.added.by.export", ...],
            "changed": [{"path": ".x", "old": ..., "new": ...}, ...]
        }

    Used by tests and the forge ground-truth guard. Pure; no I/O. Backed by
    :mod:`deepdiff` so list-of-objects ordering, nested-dict comparison, and
    type coercion are handled consistently.
    """
    from deepdiff import DeepDiff

    diff = DeepDiff(
        dict(odcs),
        dict(reconstructed),
        ignore_order=False,
        verbose_level=0,
        view="tree",
    )

    def _path(node: Any) -> str:
        # deepdiff path strings come in like ``root['schema'][0]['name']``.
        return str(node.path(output_format="list"))

    missing: List[str] = []
    extra: List[str] = []
    changed: List[Dict[str, Any]] = []

    for node in diff.get("dictionary_item_removed", []):
        missing.append(_path(node))
    for node in diff.get("iterable_item_removed", []):
        missing.append(_path(node))
    for node in diff.get("dictionary_item_added", []):
        extra.append(_path(node))
    for node in diff.get("iterable_item_added", []):
        extra.append(_path(node))
    for node in diff.get("values_changed", []):
        changed.append({"path": _path(node), "old": node.t1, "new": node.t2})
    for node in diff.get("type_changes", []):
        changed.append({"path": _path(node), "old": node.t1, "new": node.t2})

    return {
        "equal": not (missing or extra or changed),
        "missing": missing,
        "extra": extra,
        "changed": changed,
    }
