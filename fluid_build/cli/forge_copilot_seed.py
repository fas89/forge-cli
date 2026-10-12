# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Pre-processor for ``fluid forge --seed-from <path>``.

Turns a user-supplied ODCS contract, Bitol ODPS product, or directory
bundle into a structural FLUID skeleton the copilot can use as ground truth.
The skeleton's per-expose schema and qos are the truth — the LLM is expected
to fill in builds (with their execution) and the non-ground-truth parts of
each expose (kind, binding, semantics) only.

Returns a :class:`SeedResult` with:
  - ``fluid``: the imported FLUID dict (multi-expose where applicable).
  - ``shape``: ``"odcs-file" | "odps-file" | "directory" | "odcs-only-directory"``.
  - ``provenance``: an audit trail of every resolved contract source
    (paths/URLs) so downstream consumers (system prompt, ground-truth
    guard) can trace what came from where.
  - ``ground_truth_paths``: dotted paths into the FLUID dict whose values
    the LLM must not mutate (filled in by Phase 7's runtime guard).

The prompt block and the guard both work on a FLUID-0.7.2-valid *view* of
the seed (see :func:`format_ground_truth_prompt_block`), so a model that
copies the ground truth verbatim passes strict schema validation and the
guard alike.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from fluid_build.providers.base import ProviderError
from fluid_build.providers.odcs import OdcsProvider
from fluid_build.providers.odcs.io import read_input
from fluid_build.providers.odps_standard import BitolOdpsProvider
from fluid_build.providers.odps_standard.resolver import ContractNotFound, ContractResolver

LOG = logging.getLogger(__name__)


@dataclass
class SeedResult:
    """The outcome of a ``--seed-from`` invocation."""

    fluid: Dict[str, Any]
    shape: str
    provenance: List[Dict[str, Any]] = field(default_factory=list)
    ground_truth_paths: List[str] = field(default_factory=list)


@dataclass
class SeedOptions:
    """Runtime options that steer the copilot's seed hand-off.

    Constructed once at the CLI boundary from ``args.seed_from`` /
    ``args.seed_no_remote`` and threaded through the copilot pipeline into
    :func:`generate_copilot_artifacts`. ``None`` means "no seed" and the
    runtime behaves exactly as it did before Phase 7 landed.
    """

    seed_from: Union[str, Path]
    allow_remote: bool = True

    @classmethod
    def from_args(cls, args: Any) -> "Optional[SeedOptions]":
        """Build ``SeedOptions`` from an argparse ``Namespace``.

        Returns ``None`` when ``--seed-from`` wasn't passed, so callers can
        thread the result through unconditionally.
        """
        seed_from = getattr(args, "seed_from", None)
        if not seed_from:
            return None
        return cls(
            seed_from=seed_from,
            allow_remote=not bool(getattr(args, "seed_no_remote", False)),
        )


_PASSTHROUGH_KEYS = frozenset({"odcs_passthrough", "odps_passthrough"})


def _strip_passthrough(value: Any) -> Any:
    """Drop round-trip stash buckets (``*_passthrough``) recursively.

    The stash is lossless-export machinery — opaque bytes the LLM should
    neither see nor be asked to reproduce. Ground truth is the FLUID-native
    projection only.
    """
    if isinstance(value, Mapping):
        return {k: _strip_passthrough(v) for k, v in value.items() if k not in _PASSTHROUGH_KEYS}
    if isinstance(value, list):
        return [_strip_passthrough(v) for v in value]
    return value


# ---- FLUID 0.7.2 ground-truth view ----------------------------------------
#
# The ODCS/ODPS importers produce a FLUID-shaped dict that still carries
# Bitol-only keys: per-field ``quality`` (``[]`` when the source had none) and
# ``classification``, an expose ``id`` next to ``exposeId``, qos values that
# are not ISO-8601 (ODCS ``latency: 4`` becomes ``freshnessSLO: '4'``), label
# values that are not strings, and so on. The copilot validates its output
# against the strict FLUID 0.7.2 schema, so presenting those keys as
# AUTHORITATIVE ground truth made strict validation and the ground-truth guard
# block each other. Instead the prompt block and :func:`diff_against_seed`
# share one FLUID-0.7.2-valid view of the seed: keys the schema forbids are
# dropped (or mapped: ``classification`` becomes ``sensitivity`` when the
# value is a FLUID sensitivity level) and values that fail their sub-schema
# are left out. The raw values stay in ``seed.fluid`` (and its
# ``*_passthrough`` buckets), so ODCS export stays lossless.

GROUND_TRUTH_FLUID_VERSION = "0.7.2"

_ROOT_DEF_PREFIX = "_fluid_root_"


@lru_cache(maxsize=1)
def _schema_document() -> Dict[str, Any]:
    """The FLUID 0.7.2 schema, re-rooted so every fragment is a ``$defs`` entry.

    Root-level ``properties`` are aliased as ``$defs/_fluid_root_<key>`` so a
    single ``{"$defs": ..., "$ref": pointer}`` wrapper validates any fragment
    without the root's ``required``/``additionalProperties``.
    """
    schema_path = (
        Path(__file__).resolve().parent.parent
        / "schemas"
        / f"fluid-schema-{GROUND_TRUTH_FLUID_VERSION}.json"
    )
    with open(schema_path, encoding="utf-8") as fh:
        root = json.load(fh)
    defs = dict(root.get("$defs") or {})
    for key, sub in (root.get("properties") or {}).items():
        defs[f"{_ROOT_DEF_PREFIX}{key}"] = sub
    return {"$schema": root.get("$schema"), "$defs": defs}


def _resolve_pointer(pointer: str) -> Any:
    node: Any = _schema_document()
    for part in pointer.lstrip("#/").split("/"):
        node = node[part]
    return node


@lru_cache(maxsize=None)
def _validator(pointer: str) -> Any:
    import jsonschema

    doc = _schema_document()
    cls = jsonschema.validators.validator_for(doc)
    return cls(
        {"$schema": doc["$schema"], "$defs": doc["$defs"], "$ref": pointer},
        format_checker=cls.FORMAT_CHECKER,
    )


def _is_valid(pointer: str, value: Any) -> bool:
    return bool(_validator(pointer).is_valid(value))


def _json_safe(value: Any) -> Any:
    """Normalise YAML-native values (dates, datetimes, ...) to JSON-safe ones.

    Applied to both sides of the diff and to the prompt block, so a seed with
    an unquoted ``2024-01-01`` neither crashes ``json.dumps`` nor mismatches
    the string an LLM returns for it.
    """
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


_DROP = object()


def _coerce_for_key(key: str, value: Any) -> Any:
    """Map common near-misses onto their FLUID shape before validation."""
    if key == "tags" and isinstance(value, list):
        kept: List[str] = []
        for tag in value:
            if isinstance(tag, str) and _is_valid("#/$defs/tags/items", tag) and tag not in kept:
                kept.append(tag)
        if value and not kept:
            return _DROP
        return kept
    if key == "labels" and isinstance(value, Mapping):
        return {str(k): v if isinstance(v, str) else json.dumps(v) for k, v in value.items()}
    return value


def _object_view(value: Any, pointer: str) -> Optional[Dict[str, Any]]:
    """Keep only the keys of ``value`` the object schema at ``pointer``
    declares, and only when their value validates against that key's schema."""
    if not isinstance(value, Mapping):
        return None
    properties = _resolve_pointer(pointer).get("properties") or {}
    out: Dict[str, Any] = {}
    for key, item in value.items():
        if key not in properties:
            continue
        item = _coerce_for_key(key, item)
        if item is _DROP:
            continue
        if _is_valid(f"{pointer}/properties/{key}", item):
            out[key] = item
    return out


def _column_view(fld: Any) -> Any:
    if not isinstance(fld, Mapping):
        return fld
    out = _object_view(fld, "#/$defs/column") or {}
    if "sensitivity" not in out:
        classification = fld.get("classification")
        if isinstance(classification, str):
            level = classification.strip().lower()
            if _is_valid("#/$defs/sensitivityLevel", level):
                out["sensitivity"] = level
    return out


def _schema_view(schema: Any) -> Any:
    if isinstance(schema, list):
        return [_column_view(fld) for fld in schema]
    return schema


def _qos_view(qos: Any) -> Optional[Dict[str, Any]]:
    return _object_view(qos, "#/$defs/exposeQoS") or None


def _ground_truth_view_at(path: str, value: Any) -> Any:
    """The FLUID-0.7.2 view of the value found at a ground-truth ``path``."""
    value = _json_safe(_strip_passthrough(value))
    if path.endswith(".contract.schema"):
        return _schema_view(value)
    if path.endswith(".qos"):
        return _qos_view(value)
    return value


def _root_field_view(key: str, value: Any) -> Any:
    value = _coerce_for_key(key, _json_safe(value))
    if value is _DROP or value is None:
        return _DROP
    return value if _is_valid(f"#/$defs/{_ROOT_DEF_PREFIX}{key}", value) else _DROP


def _effective_ground_truth_paths(seed: "SeedResult") -> List[str]:
    """Ground-truth paths whose FLUID view has something to enforce."""
    return [
        path
        for path in seed.ground_truth_paths
        if _ground_truth_view_at(path, resolve_at_path(seed.fluid, path)) is not None
    ]


def format_ground_truth_prompt_block(seed: "SeedResult") -> Dict[str, Any]:
    """Return the JSON-serialisable block the copilot prompt injects when a
    seed is present.

    ``ground_truth`` is a FLUID-0.7.2-valid fragment the LLM can copy
    verbatim into its contract: root ``id``/``name``/``description``/
    ``domain``/``tags``, ``metadata.owner`` when the seed names one, and per
    expose ``exposeId``, ``contract.schema`` and ``qos``. It is exactly the
    view :func:`diff_against_seed` enforces. ``*_passthrough`` stash buckets,
    ``odcs_*``/``odps_*`` keys and any other key the 0.7.2 schema does not
    allow at that position are left out.
    """
    fluid = seed.fluid
    metadata = fluid.get("metadata") or {}
    ground_truth: Dict[str, Any] = {}

    root_id = (fluid.get("contract") or {}).get("id") or fluid.get("id")
    for key, value in (
        ("id", root_id),
        ("name", metadata.get("name", fluid.get("name"))),
        ("description", metadata.get("description", fluid.get("description"))),
        ("domain", metadata.get("domain", fluid.get("domain"))),
        ("tags", metadata.get("tags", fluid.get("tags"))),
    ):
        view = _root_field_view(key, value)
        if view is not _DROP:
            ground_truth[key] = view

    owner_source = fluid.get("owner")
    if not isinstance(owner_source, Mapping):
        owner_source = metadata.get("owner")
    owner = _object_view(
        _json_safe(owner_source), f"#/$defs/{_ROOT_DEF_PREFIX}metadata/properties/owner"
    )
    if owner:
        ground_truth["metadata"] = {"owner": owner}

    exposes: List[Dict[str, Any]] = []
    for i, expose in enumerate(fluid.get("exposes") or []):
        if not isinstance(expose, Mapping):
            continue
        prefix = f"exposes[{i}]"
        view_expose: Dict[str, Any] = {"exposeId": expose.get("exposeId") or expose.get("id")}
        contract = expose.get("contract")
        raw_schema = contract.get("schema") if isinstance(contract, Mapping) else None
        schema = _ground_truth_view_at(f"{prefix}.contract.schema", raw_schema)
        view_expose["contract"] = {"schema": schema if isinstance(schema, list) else []}
        qos = _ground_truth_view_at(f"{prefix}.qos", expose.get("qos"))
        if qos:
            view_expose["qos"] = qos
        exposes.append(view_expose)
    ground_truth["exposes"] = exposes

    return _json_safe(
        {
            "shape": seed.shape,
            "fluidVersion": GROUND_TRUTH_FLUID_VERSION,
            "ground_truth": ground_truth,
            "ground_truth_paths": _effective_ground_truth_paths(seed),
            "provenance": list(seed.provenance),
        }
    )


GROUND_TRUTH_ERROR_PREFIX = "ground_truth violation at "
_GROUND_TRUTH_OVERFLOW_SUFFIX = "more mutated paths"


def is_ground_truth_error(message: Any) -> bool:
    """True for the repair-feedback strings :func:`format_mismatch_report` emits."""
    if not isinstance(message, str):
        return False
    return message.startswith(GROUND_TRUTH_ERROR_PREFIX) or (
        message.startswith("... and ") and message.endswith(_GROUND_TRUTH_OVERFLOW_SUFFIX)
    )


def format_mismatch_report(mismatches: List[Dict[str, Any]]) -> List[str]:
    """Render a ``diff_against_seed`` result as human-readable repair
    feedback strings, one per mismatch. Fed back to the LLM via the existing
    ``previous_errors`` slot in the repair-loop prompt.
    """
    lines: List[str] = []
    for m in mismatches[:10]:  # cap for prompt-size sanity
        seed_val = json.dumps(_json_safe(m.get("seed")))[:120]
        cand_val = json.dumps(_json_safe(m.get("candidate")))[:120]
        lines.append(
            f"{GROUND_TRUTH_ERROR_PREFIX}{m.get('path')}: "
            f"seed had {seed_val} but you returned {cand_val}"
        )
    if len(mismatches) > 10:
        lines.append(f"... and {len(mismatches) - 10} {_GROUND_TRUTH_OVERFLOW_SUFFIX}")
    return lines


SHAPE_ODCS_FILE = "odcs-file"
SHAPE_ODPS_FILE = "odps-file"
SHAPE_DIRECTORY = "directory"
SHAPE_ODCS_ONLY_DIRECTORY = "odcs-only-directory"

_SEED_FILE_SUFFIXES = (".yaml", ".yml", ".json")
_BITOL_INPUT_HINT = (
    "--seed-from expects Bitol input: export a FLUID contract first "
    "(`fluid odcs export` / `fluid odps-bitol export`) and pass the resulting "
    "*.odcs.yaml, *.odps.yaml, or bundle directory."
)


def _is_fluid_document(data: Any) -> bool:
    return isinstance(data, Mapping) and "fluidVersion" in data


def _reject_fluid_document(path: Path, data: Any) -> None:
    if _is_fluid_document(data):
        raise ProviderError(
            f"--seed-from: {path} is a FLUID contract (fluidVersion "
            f"{data.get('fluidVersion')!r}), not a Bitol ODCS/ODPS document. " + _BITOL_INPUT_HINT
        )


def load_seed(seed_from: Union[str, Path], *, allow_remote: bool = True) -> SeedResult:
    """Detect the input shape and run the matching importer.

    Detection order:
      1. Directory             → Bitol bundle (one ODPS doc plus its ODCS
         files) or an ODCS-only directory. FLUID contracts inside the
         directory are ignored with a warning.
      2. ``*.odcs.{yaml,yml,json}`` filename → OdcsProvider.import_contract.
      3. ``*.odps.{yaml,yml,json}`` filename → BitolOdpsProvider.import_contract.
      4. Sniff the file's ``kind`` field: ``DataContract`` → ODCS,
         ``DataProduct`` → ODPS.

    A FLUID contract (any document carrying ``fluidVersion``) is rejected,
    every ODPS output port must resolve to its ODCS contract, and a seed with
    no ground truth at all is an error: a seed that silently enforces nothing
    would defeat the point of ``--seed-from``.
    """
    path = Path(seed_from)
    if not path.exists():
        raise ProviderError(f"--seed-from path not found: {path}")

    if path.is_dir():
        return _load_directory(path, allow_remote=allow_remote)

    name_lower = path.name.lower()
    named_odcs = name_lower.endswith((".odcs.yaml", ".odcs.yml", ".odcs.json"))
    named_odps = name_lower.endswith((".odps.yaml", ".odps.yml", ".odps.json"))

    sniffed: Any
    if named_odcs or named_odps:
        try:
            sniffed = read_input(path)
        except Exception:  # let the importer report parse errors as before
            sniffed = None
    else:
        sniffed = read_input(path)
    _reject_fluid_document(path, sniffed)

    if named_odcs:
        return _load_odcs_file(path)

    if named_odps:
        return _load_odps_file(path, allow_remote=allow_remote)

    # Last-resort: sniff `kind`
    kind = sniffed.get("kind") if isinstance(sniffed, Mapping) else None
    if kind == "DataContract":
        return _load_odcs_file(path)
    if kind == "DataProduct":
        return _load_odps_file(path, allow_remote=allow_remote)

    raise ProviderError(
        f"--seed-from: cannot determine input type of {path}. "
        "Expected a directory, *.odcs.yaml, *.odps.yaml, or a Bitol document "
        "with kind: DataContract (ODCS) or kind: DataProduct (ODPS)."
    )


# ---- shape-specific loaders -----------------------------------------------


def _load_odcs_file(path: Path) -> SeedResult:
    provider = OdcsProvider()
    fluid = provider.import_contract(path)
    provenance = [{"shape": SHAPE_ODCS_FILE, "source": "local", "origin": str(path)}]
    ground_truth = _ground_truth_paths_for(fluid)
    _check_ground_truth(path, fluid, ground_truth)
    return SeedResult(
        fluid=fluid,
        shape=SHAPE_ODCS_FILE,
        provenance=provenance,
        ground_truth_paths=ground_truth,
    )


def _import_odps_requiring_outputs(
    provider: BitolOdpsProvider,
    odps_path: Path,
    *,
    base_path: Path,
    allow_remote: bool,
    resolver: ContractResolver,
) -> Dict[str, Any]:
    """Import an ODPS doc, requiring every *output* port to resolve.

    Input ports stay lenient inside the provider (upstream contracts often
    live elsewhere). An unresolved output port would leave that expose with
    no schema ground truth, so it is a hard error here.
    """
    try:
        return provider.import_contract(
            odps_path,
            base_path=base_path,
            allow_remote=allow_remote,
            resolver=resolver,
            lenient=False,
        )
    except ContractNotFound as exc:
        raise ProviderError(
            f"--seed-from: output port contract {exc.contract_id!r} could not be "
            f"resolved, so its schema cannot be used as ground truth. {exc}"
        ) from exc


def _load_odps_file(path: Path, *, allow_remote: bool) -> SeedResult:
    provider = BitolOdpsProvider()
    provider.strict_validation = False  # seed mode tolerates non-strict contracts
    resolver = ContractResolver(
        base_path=path.parent,
        allow_remote=allow_remote,
        odcs_provider=provider._odcs,
    )
    fluid = _import_odps_requiring_outputs(
        provider, path, base_path=path.parent, allow_remote=allow_remote, resolver=resolver
    )
    provenance = [{"shape": SHAPE_ODPS_FILE, "source": "local", "origin": str(path)}]
    provenance.extend(_resolver_provenance(resolver))
    ground_truth = _ground_truth_paths_for(fluid)
    _check_ground_truth(path, fluid, ground_truth)
    return SeedResult(
        fluid=fluid,
        shape=SHAPE_ODPS_FILE,
        provenance=provenance,
        ground_truth_paths=ground_truth,
    )


def _scan_directory(root: Path) -> Tuple[List[Path], List[Path], List[Path]]:
    """Classify every YAML/JSON file under ``root`` (recursively).

    Mirrors :meth:`BitolOdpsProvider.import_directory`'s scan, except that
    FLUID contracts (which also say ``kind: DataProduct``) are set aside
    instead of being taken for the bundle's ODPS doc.
    """
    odps_files: List[Path] = []
    odcs_files: List[Path] = []
    fluid_files: List[Path] = []
    for child in sorted(root.rglob("*")):
        if not child.is_file() or child.suffix.lower() not in _SEED_FILE_SUFFIXES:
            continue
        try:
            data = read_input(child)
        except Exception:
            continue
        if not isinstance(data, Mapping):
            continue
        if _is_fluid_document(data):
            fluid_files.append(child)
            continue
        kind = data.get("kind")
        if kind == "DataProduct":
            odps_files.append(child)
        elif kind == "DataContract":
            odcs_files.append(child)
    return odps_files, odcs_files, fluid_files


def _load_directory(path: Path, *, allow_remote: bool) -> SeedResult:
    """Either a full bundle (ODPS + ODCS) or an ODCS-only directory."""
    odps_files, odcs_files, fluid_files = _scan_directory(path)
    if fluid_files:
        LOG.warning(
            "--seed-from: ignoring FLUID contract(s) in %s: %s",
            path,
            ", ".join(str(f) for f in fluid_files),
        )
    if len(odps_files) > 1:
        raise ProviderError(
            f"--seed-from: found {len(odps_files)} ODPS docs in {path}; "
            "expected exactly one (or zero for ODCS-only mode). "
            f"Found: {[str(p) for p in odps_files]}"
        )

    provider = BitolOdpsProvider()
    provider.strict_validation = False
    provenance: List[Dict[str, Any]]
    if not odps_files:
        if not odcs_files:
            skipped = f" ({len(fluid_files)} FLUID contract(s) ignored)" if fluid_files else ""
            raise ProviderError(
                f"--seed-from: {path} contains no Bitol ODPS or ODCS documents{skipped}. "
                + _BITOL_INPUT_HINT
            )
        shape = SHAPE_ODCS_ONLY_DIRECTORY
        fluid = provider._import_odcs_only(path, odcs_files)
        provenance = [{"shape": shape, "source": "local", "origin": str(path)}]
        provenance.extend({"source": "local", "origin": str(f)} for f in odcs_files)
    else:
        shape = SHAPE_DIRECTORY
        resolver = ContractResolver(
            base_path=path,
            allow_remote=allow_remote,
            odcs_provider=provider._odcs,
            additional_files=odcs_files,
        )
        resolver.index_directory(path)
        fluid = _import_odps_requiring_outputs(
            provider, odps_files[0], base_path=path, allow_remote=allow_remote, resolver=resolver
        )
        provenance = [
            {"shape": shape, "source": "local", "origin": str(path)},
            {"source": "local", "origin": str(odps_files[0])},
        ]
        # Every contract the resolver actually consumed: nested directories,
        # .yml/.json files and remote URLs included.
        provenance.extend(_resolver_provenance(resolver))
    ground_truth = _ground_truth_paths_for(fluid)
    _check_ground_truth(path, fluid, ground_truth)
    return SeedResult(
        fluid=fluid,
        shape=shape,
        provenance=provenance,
        ground_truth_paths=ground_truth,
    )


def _resolver_provenance(resolver: ContractResolver) -> List[Dict[str, Any]]:
    return [
        {
            "contract_id": resolved.contract_id,
            "source": resolved.source,
            "origin": resolved.origin,
        }
        for resolved in resolver._cache.values()
    ]


def _check_ground_truth(path: Path, fluid: Mapping[str, Any], ground_truth: List[str]) -> None:
    """Refuse a seed with nothing to enforce; warn about unprotected exposes."""
    if not ground_truth:
        raise ProviderError(
            f"--seed-from: {path} produced no ground truth (no output port or "
            "schema object with a contract), so there is nothing to preserve."
        )
    for expose in fluid.get("exposes") or []:
        if isinstance(expose, Mapping) and not isinstance(expose.get("contract"), Mapping):
            LOG.warning(
                "--seed-from: expose %r in %s has no contract; its schema is not "
                "protected as ground truth",
                expose.get("exposeId") or expose.get("id"),
                path,
            )


# ---- ground-truth path enumeration ----------------------------------------


def _ground_truth_paths_for(fluid: Mapping[str, Any]) -> List[str]:
    """Enumerate dotted paths inside ``fluid`` the LLM must not mutate.

    Currently: every ``exposes[i].contract.schema`` and ``exposes[i].qos``,
    plus ``exposes[i].contract.relationships`` and
    ``exposes[i].contract.odcs_quality``. The runtime guard (Phase 7
    finishing touches) reads this list and diffs the LLM's output against
    the seed values at each path.
    """
    paths: List[str] = []
    for i, expose in enumerate(fluid.get("exposes") or []):
        if not isinstance(expose, Mapping):
            continue
        prefix = f"exposes[{i}]"
        if isinstance(expose.get("contract"), Mapping):
            paths.append(f"{prefix}.contract.schema")
            if "relationships" in expose["contract"]:
                paths.append(f"{prefix}.contract.relationships")
            if "odcs_quality" in expose["contract"]:
                paths.append(f"{prefix}.contract.odcs_quality")
        if "qos" in expose:
            paths.append(f"{prefix}.qos")
    return paths


_PATH_TOKEN_RE = re.compile(r"[^.\[\]]+|\[\d+\]")


def resolve_at_path(fluid: Mapping[str, Any], path: str) -> Any:
    """Look up the value at a dotted/indexed path like ``exposes[0].contract.schema``.

    Returns ``None`` for anything missing, including an index past the end of
    a list (the candidate has fewer exposes than the seed) or an index into a
    non-list, so the guard reports a mismatch instead of raising.
    """
    cursor: Any = fluid
    for token in _PATH_TOKEN_RE.findall(path):
        if token.startswith("[") and token.endswith("]"):
            idx = int(token[1:-1])
            if not isinstance(cursor, (list, tuple)) or idx >= len(cursor):
                return None
            cursor = cursor[idx]
        else:
            if isinstance(cursor, Mapping):
                cursor = cursor.get(token)
            else:
                cursor = getattr(cursor, token, None)
        if cursor is None:
            return None
    return cursor


def diff_against_seed(seed: SeedResult, candidate: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Compare ground-truth values in ``candidate`` against the seed.

    Returns a list of ``{"path", "seed", "candidate"}`` entries for paths
    where the candidate's value differs from the seed. The runtime guard
    raises (or triggers a repair loop) when this list is non-empty.

    Both sides go through the same FLUID-0.7.2 view the prompt shows (see
    :func:`format_ground_truth_prompt_block`): ``*_passthrough`` stash buckets
    and keys the 0.7.2 schema forbids (ODCS ``quality``, ``classification``,
    ...) are ignored, and values are JSON-normalised. A candidate that copies
    the prompt's ground truth verbatim, or the raw seed, is therefore clean,
    and one that drops round-trip bookkeeping is not a violation.
    """
    mismatches: List[Dict[str, Any]] = []
    for path in seed.ground_truth_paths:
        seed_value = _ground_truth_view_at(path, resolve_at_path(seed.fluid, path))
        if seed_value is None:
            continue  # nothing FLUID-expressible to enforce at this path
        cand_value = _ground_truth_view_at(path, resolve_at_path(candidate, path))
        if seed_value != cand_value:
            mismatches.append({"path": path, "seed": seed_value, "candidate": cand_value})
    return mismatches
