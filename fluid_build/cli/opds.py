# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# fluid_build/cli/opds.py
"""
ODPS / OPDS CLI Commands — two specifications under one user-facing command.

Specifications selected via ``--spec``:

- ``bitol-1.0.0`` (**default**) — Bitol Open Data Product Standard v1.0.0.
  Emits 1 ODPS doc + N sibling ``<contractId>.odcs.yaml`` files (canonical
  Bitol fragments layout). Backed by :class:`BitolOdpsProvider`.
- ``odpi-4.1`` — Open Data Product Initiative v4.1 (Linux Foundation).
  Emits a single JSON document. Backed by the legacy ``odps`` provider.

The legacy ``--version 4.1`` flag still works as a deprecated alias for
``--spec odpi-4.1``.

Machine-readable payloads (JSON/YAML written to stdout) are emitted raw via
``sys.stdout.write`` so pipelines receive exactly the serialised document;
human-facing status lines keep going through the Rich console.

Usage:
    fluid opds export <contract> [--spec bitol-1.0.0|bitol-1.1.0|odpi-4.1]
                                 [--out FILE | --out-dir DIR] [-f yaml|json]
    fluid opds import <path>    [--spec bitol-1.0.0] [--no-remote] [--lenient] [-o OUT]
    fluid opds validate <file>  [--spec ...]   (auto-detects Bitol vs ODPI when omitted)
    fluid opds info             [--spec ...]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Dict, List, Mapping, Optional

from fluid_build.cli.console import cprint
from fluid_build.cli.console import error as console_error

LOG = logging.getLogger("fluid.cli.opds")

# ODPS specification versions and their schema URLs
ODPS_VERSIONS = {
    "4.1": {
        "spec_url": "https://github.com/Open-Data-Product-Initiative/v4.1",
        "schema_url": "https://github.com/Open-Data-Product-Initiative/v4.1/blob/main/source/schema/odps.json",
        "schema_url_raw": "https://raw.githubusercontent.com/Open-Data-Product-Initiative/v4.1/main/source/schema/odps.json",
        "description": "ODPS v4.1 - Current stable version with full JSON Schema",
        "status": "stable",
        "default": True,
    },
    # Future versions can be added here
    # "5.0": {
    #     "spec_url": "https://github.com/Open-Data-Product-Initiative/v5.0",
    #     "schema_url": "...",
    #     "description": "ODPS v5.0 - Next generation",
    #     "status": "draft",
    #     "default": False
    # }
}

DEFAULT_VERSION = next(v for v, info in ODPS_VERSIONS.items() if info.get("default", False))


# Specification selector — the user-facing ``--spec`` flag dispatches between
# the Bitol Open Data Product Standard (v1.0.0 / v1.1.0) and the legacy Open
# Data Product Initiative v4.1. The default stays on bitol-1.0.0 so existing
# pipelines keep producing the document shape they validate against; opt into
# v1.1.0 explicitly. Imports accept either Bitol version (the validator picks
# the schema from the document's own apiVersion).
SPEC_BITOL_1_0_0 = "bitol-1.0.0"
SPEC_BITOL_1_1_0 = "bitol-1.1.0"
SPEC_ODPI_4_1 = "odpi-4.1"
DEFAULT_SPEC = SPEC_BITOL_1_0_0
SUPPORTED_SPECS = (SPEC_BITOL_1_0_0, SPEC_BITOL_1_1_0, SPEC_ODPI_4_1)
BITOL_SPEC_API_VERSIONS = {SPEC_BITOL_1_0_0: "v1.0.0", SPEC_BITOL_1_1_0: "v1.1.0"}
BITOL_SPEC_URL = "https://github.com/bitol-io/open-data-product-standard"
BITOL_SPECS: Dict[str, Dict[str, Any]] = {
    SPEC_BITOL_1_0_0: {
        "apiVersion": "v1.0.0",
        "spec_url": BITOL_SPEC_URL,
        "description": (
            "Bitol Open Data Product Standard v1.0.0 - 1 ODPS doc + N sibling ODCS contracts"
        ),
        "status": "stable",
        "default": True,
    },
    SPEC_BITOL_1_1_0: {
        "apiVersion": "v1.1.0",
        "spec_url": BITOL_SPEC_URL,
        "description": (
            "Bitol Open Data Product Standard v1.1.0 - additive over v1.0.0 "
            "(type, deprecated, context, synonyms)"
        ),
        "status": "stable",
        "default": False,
    },
}

_FORMAT_BY_SUFFIX = {".json": "json", ".yaml": "yaml", ".yml": "yaml"}


def _write_stdout(payload: str) -> None:
    """Write a machine-readable payload to stdout verbatim (never through Rich).

    Rich would hard-wrap long lines at the console width and treat
    ``[a-z0-9_]``-style substrings as markup, corrupting YAML/JSON.
    """
    sys.stdout.write(payload if payload.endswith("\n") else payload + "\n")
    sys.stdout.flush()


def _warn(logger: logging.Logger, msg: str) -> None:
    """Human-facing warning on stderr (keeps stdout clean for payloads)."""
    logger.warning(msg)
    print(f"⚠ {msg}", file=sys.stderr)


def _str_arg(args: argparse.Namespace, name: str) -> Optional[str]:
    """Return a string CLI arg, or None when unset (tolerates mock namespaces)."""
    value = getattr(args, name, None)
    return value if isinstance(value, str) else None


def resolve_spec(args: argparse.Namespace) -> str:
    """Resolve the active ``--spec`` from CLI args.

    Precedence: ``--spec`` > legacy ``--version 4.1`` > default.
    Emits a deprecation warning when ``--version`` is the only signal.
    """
    spec = getattr(args, "spec", None)
    if isinstance(spec, str) and spec in SUPPORTED_SPECS:
        return spec
    legacy_version = getattr(args, "version", None)
    if isinstance(legacy_version, str) and legacy_version in ODPS_VERSIONS:
        if legacy_version == "4.1":
            LOG.warning(
                "--version 4.1 is deprecated; use --spec odpi-4.1 instead",
            )
            return SPEC_ODPI_4_1
    return DEFAULT_SPEC


def get_version_info(version: str) -> Dict[str, Any]:
    """Get information about a specific ODPS version."""
    if version not in ODPS_VERSIONS:
        available = ", ".join(ODPS_VERSIONS.keys())
        raise ValueError(f"Unsupported ODPS version: {version}. Available: {available}")
    return ODPS_VERSIONS[version]


def cmd_opds_export(args: argparse.Namespace, logger: logging.Logger) -> int:
    """Export FLUID contract — dispatched by ``--spec``."""
    from fluid_build.cli.bootstrap import load_contract_with_overlay

    spec = resolve_spec(args)
    try:
        contract = load_contract_with_overlay(args.contract, getattr(args, "env", None), logger)
    except Exception as e:
        logger.error("contract_load_failed", extra={"error": str(e)})
        console_error(f"Error loading contract: {e}")
        return 1

    if spec in BITOL_SPEC_API_VERSIONS:
        # Only an explicit Bitol ``--spec`` pins the emitted apiVersion. When
        # it is omitted the provider's own chain applies: ODPS_API_VERSION env
        # > the imported document's source apiVersion > default v1.0.0.
        explicit = _str_arg(args, "spec") in BITOL_SPEC_API_VERSIONS
        return _export_bitol(
            args,
            contract,
            logger,
            api_version=BITOL_SPEC_API_VERSIONS[spec] if explicit else None,
        )
    if spec == SPEC_ODPI_4_1:
        return _export_odpi_v41(args, contract, logger)
    console_error(f"Unsupported --spec {spec!r}; supported: {SUPPORTED_SPECS}")
    return 2


def _unsafe_file_stem(name: Any) -> Optional[str]:
    """Return why ``name`` cannot be used as a file stem, or None when it is safe."""
    if not isinstance(name, str) or not name.strip():
        return "it is empty"
    if PurePosixPath(name).is_absolute() or PureWindowsPath(name).is_absolute():
        return "it is an absolute path"
    if "/" in name or "\\" in name:
        return "it contains a path separator"
    if ".." in name:
        return "it contains '..'"
    return None


def _resolve_bitol_format(explicit_fmt: Optional[str], out_file: Optional[Path]) -> str:
    """Explicit ``-f`` wins; otherwise infer from the ``--out`` suffix; default yaml."""
    if explicit_fmt:
        return explicit_fmt.lower()
    if out_file is not None:
        return _FORMAT_BY_SUFFIX.get(out_file.suffix.lower(), "yaml")
    return "yaml"


def _non_strict_problems(bundle: Mapping[str, Any]) -> List[str]:
    """Run every export-time check and collect failures instead of raising.

    Mirrors what strict mode enforces: the ODPS product schema, each per-port
    ODCS schema, and the optional ``vowl`` second pass.
    """
    from fluid_build.providers.odcs import validation as odcs_validation
    from fluid_build.providers.odps_standard import validation as odps_validation

    problems: List[str] = []
    try:
        odps_validation.validate(bundle.get("product") or {})
    except Exception as exc:  # noqa: BLE001 - reported, never raised
        problems.append(f"ODPS product: {exc}")
    for contract_id, odcs in (bundle.get("contracts") or {}).items():
        try:
            odcs_validation.validate(odcs)
        except Exception as exc:  # noqa: BLE001
            problems.append(f"ODCS contract {contract_id}: {exc}")
        try:
            odcs_validation.validate_via_vowl(odcs)
        except Exception as exc:  # noqa: BLE001
            problems.append(f"ODCS contract {contract_id}: {exc}")
    return problems


def _export_bitol(
    args: argparse.Namespace,
    contract: Dict[str, Any],
    logger: logging.Logger,
    *,
    api_version: Optional[str],
) -> int:
    """Bitol ODPS export — 1 ODPS doc + N sibling ODCS contracts.

    ``api_version=None`` lets :class:`BitolOdpsProvider` resolve the version
    itself (``ODPS_API_VERSION`` > imported source apiVersion > v1.0.0).
    """
    from fluid_build.providers.odps_standard import BitolOdpsProvider
    from fluid_build.providers.odps_standard.io import write_output

    out = _str_arg(args, "out") or "-"
    out_dir = _str_arg(args, "out_dir")
    validate_strict = getattr(args, "validate_strict", True)
    out_file = Path(out) if out != "-" else None

    if out_file is not None and out_dir:
        console_error(
            "--out and --out-dir are mutually exclusive: use --out FILE for the product "
            "doc (per-port ODCS contracts are written next to it) or --out-dir DIR."
        )
        return 2

    fmt = _resolve_bitol_format(_str_arg(args, "format"), out_file)

    try:
        provider = BitolOdpsProvider(api_version=api_version)
        provider.strict_validation = bool(validate_strict)
        # Render in memory first so every target path can be checked before
        # anything touches the filesystem.
        bundle = provider.render(contract, out=None, out_dir=None, fmt=fmt)
    except Exception as e:
        logger.error("opds_export_failed", extra={"error": str(e), "api_version": api_version})
        console_error(f"Error exporting to Bitol ODPS: {e}")
        return 1

    product = bundle.get("product") or {}
    contracts = bundle.get("contracts") or {}
    written_version = product.get("apiVersion") or getattr(provider, "api_version", "")

    if not validate_strict:
        for problem in _non_strict_problems(bundle):
            _warn(logger, f"validation (non-strict): {problem}")

    if out_file is None and not out_dir:
        # Dump product doc to stdout. Use JSON for stdout regardless of fmt
        # so the output is unambiguous in pipelines.
        _write_stdout(json.dumps(product, indent=2, ensure_ascii=False))
        return 0

    stems = [("contractId", contract_id) for contract_id in contracts]
    if out_dir:
        # The provider names the product file after its id (or name).
        stems.append(("product id", product.get("id") or product.get("name") or "product"))
    for label, stem in stems:
        reason = _unsafe_file_stem(stem)
        if reason:
            console_error(
                f"Refusing to write Bitol ODPS export: {label} {stem!r} cannot be used "
                f"as a file name because {reason}."
            )
            return 1

    if out_dir:
        try:
            # Canonical layout, written by the provider itself.
            provider._write_bundle(product, contracts, Path(out_dir), fmt)
        except Exception as e:
            logger.error("opds_export_write_failed", extra={"error": str(e)})
            console_error(f"Error writing Bitol ODPS export: {e}")
            return 1
        cprint(
            f"✓ Exported Bitol ODPS {written_version}: 1 product + "
            f"{len(contracts)} ODCS contract(s) → {out_dir}",
            soft_wrap=True,
            markup=False,
        )
        return 0

    # --out FILE: the product goes to FILE and every per-port ODCS contract is
    # written next to it as <contractId>.odcs.<ext>, so the bundle stays
    # importable (the resolver probes exactly these sibling names).
    ext = out_file.suffix.lstrip(".").lower()
    if _FORMAT_BY_SUFFIX.get(f".{ext}") != fmt:
        ext = fmt
    contract_paths = [out_file.parent / f"{contract_id}.odcs.{ext}" for contract_id in contracts]
    try:
        write_output(dict(product), out_file, fmt)
        for contract_path, odcs in zip(contract_paths, contracts.values()):
            write_output(dict(odcs), contract_path, fmt)
    except Exception as e:
        logger.error("opds_export_write_failed", extra={"error": str(e)})
        console_error(f"Error writing Bitol ODPS export: {e}")
        return 1

    cprint(
        f"✓ Exported Bitol ODPS {written_version}: 1 product + "
        f"{len(contract_paths)} ODCS contract(s)"
    )
    # Paths are printed unwrapped and without markup parsing so they stay
    # copy-pasteable.
    cprint(f"  product:  {out_file}", soft_wrap=True, markup=False)
    for contract_path in contract_paths:
        cprint(f"  contract: {contract_path}", soft_wrap=True, markup=False)
    return 0


def _export_odpi_v41(
    args: argparse.Namespace, contract: Dict[str, Any], logger: logging.Logger
) -> int:
    """Legacy ODPI v4.1 export path (single JSON document)."""
    from fluid_build.cli.bootstrap import build_provider

    # --out-dir and -f/--format only mean something for the Bitol bundle
    # layout; ODPI v4.1 is always a single JSON document. Refuse them instead
    # of silently writing JSON somewhere else.
    if _str_arg(args, "out_dir"):
        console_error(
            "--out-dir is not supported with --spec odpi-4.1 (ODPI v4.1 is a single JSON "
            "document): use --out FILE, or omit it to print to stdout."
        )
        return 2
    explicit_fmt = _str_arg(args, "format")
    if explicit_fmt and explicit_fmt.lower() != "json":
        console_error(
            f"--format {explicit_fmt} is not supported with --spec odpi-4.1: ODPI v4.1 is "
            "always written as JSON (omit -f/--format or use -f json)."
        )
        return 2

    version_info = get_version_info("4.1")
    try:
        provider = build_provider("odps", None, None, logger)
    except Exception as e:
        logger.error("provider_build_failed", extra={"error": str(e)})
        console_error(f"Error building ODPI provider: {e}")
        return 1

    provider.opds_version = "4.1"
    provider.opds_spec_url = version_info["spec_url"]
    provider.opds_schema_url = version_info["schema_url"]

    try:
        result = provider.render(contract, out=getattr(args, "out", "-"), fmt="opds")
        out_path = getattr(args, "out", "-")
        if out_path == "-":
            if getattr(args, "pretty", True):
                _write_stdout(json.dumps(result, indent=2, ensure_ascii=False))
            else:
                _write_stdout(json.dumps(result, ensure_ascii=False))
        else:
            cprint(f"✓ Exported to ODPI v4.1: {out_path}", soft_wrap=True, markup=False)
            cprint(f"  Specification: {version_info['spec_url']}")
        return 0
    except Exception as e:
        logger.error("opds_export_failed", extra={"error": str(e)})
        console_error(f"Error exporting to ODPI: {e}")
        return 1


def cmd_opds_import(args: argparse.Namespace, logger: logging.Logger) -> int:
    """Import an ODPS / ODCS file or directory → one FLUID contract skeleton.

    The *inputs* are schema-validated; the emitted FLUID is a skeleton that
    may need edits (fluidVersion/kind/id/name, expose kind, binding
    format/location) before ``fluid validate`` / ``plan`` accept it.

    Input dispatch:
      - Path ends in ``.odcs.yaml``/``.odcs.yml``/``.odcs.json`` →
        :meth:`OdcsProvider.import_contract` (single-expose FLUID).
      - Path is a file with ``kind: DataProduct`` (or ``.odps.yaml``) →
        :meth:`BitolOdpsProvider.import_contract` with resolver.
      - Path is a directory → :meth:`BitolOdpsProvider.import_directory`.

    ``--spec odpi-4.1`` is reserved for the legacy provider, which is
    export-only and rejects import with a clear error.
    """
    spec = resolve_spec(args)
    if spec == SPEC_ODPI_4_1:
        console_error(
            "ODPI v4.1 is export-only; import is supported for the Bitol specs "
            f"({', '.join(BITOL_SPEC_API_VERSIONS)})"
        )
        return 2

    in_path = Path(args.path)
    if not in_path.exists():
        console_error(f"Input path not found: {in_path}")
        return 1

    allow_remote = not getattr(args, "no_remote", False)
    lenient = bool(getattr(args, "lenient", False))
    out = getattr(args, "out", None)
    out_file = (
        Path(out) if isinstance(out, (str, os.PathLike)) and str(out) not in ("", "-") else None
    )
    # Same rule as export: an explicit -f/--format wins, else the -o suffix
    # decides (.json → json, .yaml/.yml → yaml), else yaml.
    fmt = _resolve_bitol_format(_str_arg(args, "format"), out_file)

    try:
        fluid = _dispatch_import(in_path, allow_remote=allow_remote, lenient=lenient)
    except Exception as e:
        logger.error("opds_import_failed", extra={"error": str(e), "path": str(in_path)})
        console_error(f"Error importing: {e}")
        return 1

    if out_file is not None:
        out_path = out_file
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if fmt == "json":
            out_path.write_text(json.dumps(fluid, indent=2, ensure_ascii=False))
        else:
            import yaml

            out_path.write_text(yaml.dump(fluid, default_flow_style=False, sort_keys=False))
        # Paths are printed unwrapped and without markup parsing so they stay
        # copy-pasteable.
        cprint(f"✓ Imported {in_path} → {out_path}", soft_wrap=True, markup=False)
    else:
        if fmt == "json":
            _write_stdout(json.dumps(fluid, indent=2, ensure_ascii=False))
        else:
            import yaml

            _write_stdout(yaml.dump(fluid, default_flow_style=False, sort_keys=False))
    return 0


def _dispatch_import(path: Path, *, allow_remote: bool, lenient: bool) -> Dict[str, Any]:
    """Choose the right import method based on the input path's shape/content."""
    if path.is_dir():
        from fluid_build.providers.odps_standard import BitolOdpsProvider

        return BitolOdpsProvider().import_directory(
            path, allow_remote=allow_remote, lenient=lenient
        )

    # Single-file dispatch
    suffixes = {s.lower() for s in path.suffixes}
    name_lower = path.name.lower()

    if ".odcs" in suffixes or name_lower.endswith((".odcs.yaml", ".odcs.yml", ".odcs.json")):
        from fluid_build.providers.odcs import OdcsProvider

        return OdcsProvider().import_contract(path, lenient=lenient)

    if ".odps" in suffixes or name_lower.endswith((".odps.yaml", ".odps.yml", ".odps.json")):
        from fluid_build.providers.odps_standard import BitolOdpsProvider

        return BitolOdpsProvider().import_contract(path, allow_remote=allow_remote, lenient=lenient)

    # Last resort: sniff the file's ``kind`` field
    from fluid_build.providers.odcs.io import read_input

    data = read_input(path)
    if isinstance(data, dict) and "fluidVersion" in data:
        # FLUID contracts also declare ``kind: DataProduct``.
        raise ValueError(
            f"{path} is a FLUID contract, not a Bitol ODPS/ODCS document; "
            "use `fluid opds export` to turn it into ODPS."
        )
    kind = data.get("kind") if isinstance(data, dict) else None
    if kind == "DataProduct":
        from fluid_build.providers.odps_standard import BitolOdpsProvider

        return BitolOdpsProvider().import_contract(path, allow_remote=allow_remote, lenient=lenient)
    if kind == "DataContract":
        from fluid_build.providers.odcs import OdcsProvider

        return OdcsProvider().import_contract(path, lenient=lenient)

    raise ValueError(
        f"Cannot determine input type for {path}: "
        f"expected a directory, *.odps.yaml, *.odcs.yaml, or a file with "
        f"kind: DataProduct or kind: DataContract"
    )


def _load_document(path: Path) -> Any:
    """Load a YAML or JSON document.

    ``.yaml``/``.yml`` files are parsed as YAML. Anything else is parsed as
    JSON first (so JSON errors read exactly as before); when that fails a
    YAML parse is attempted and accepted only if it yields a mapping.
    """
    import yaml

    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() in (".yaml", ".yml"):
        data = yaml.safe_load(text)
        if not isinstance(data, dict):
            raise ValueError(f"{path} did not parse as a mapping")
        return data
    try:
        return json.loads(text)
    except json.JSONDecodeError as json_err:
        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError:
            raise json_err from None
        if isinstance(data, dict):
            return data
        raise json_err from None


def _is_bitol_document(doc: Any) -> bool:
    return isinstance(doc, dict) and "apiVersion" in doc and doc.get("kind") == "DataProduct"


def _validate_bitol(
    path: Path,
    doc: Any,
    logger: logging.Logger,
    *,
    spec: Optional[str],
) -> int:
    """Validate a Bitol ODPS product with :meth:`BitolOdpsProvider.validate_product`.

    The schema follows the document's own ``apiVersion`` unless an explicit
    Bitol ``--spec`` pins it.
    """
    from fluid_build.providers.base import ProviderError
    from fluid_build.providers.odps_standard import BitolOdpsProvider
    from fluid_build.providers.odps_standard import validation as odps_validation

    if not isinstance(doc, dict):
        console_error(f"✗ Bitol ODPS validation failed: {path} is not a mapping")
        return 1

    if spec in BITOL_SPEC_API_VERSIONS:
        schema_version = BITOL_SPEC_API_VERSIONS[spec]
    else:
        schema_version = odps_validation.resolve_schema_version(doc)

    logger.info(
        "opds_validate_start",
        extra={"file": str(path), "spec": spec or "auto", "schema_version": schema_version},
    )
    try:
        if spec in BITOL_SPEC_API_VERSIONS:
            odps_validation.validate(doc, odps_validation.load_schema(schema_version))
        else:
            BitolOdpsProvider().validate_product(doc)
    except ProviderError as e:
        console_error(f"✗ Bitol ODPS validation failed (schema {schema_version})")
        console_error(f"  - {e}")
        logger.error("opds_invalid", extra={"file": str(path), "error": str(e)})
        return 1

    cprint(f"✓ Bitol ODPS file is valid (schema {schema_version})")
    cprint(f"  Data Product: {doc.get('id')}")
    cprint(f"  Name: {doc.get('name')}")
    cprint(f"  apiVersion: {doc.get('apiVersion')}")
    cprint(f"  Specification: {BITOL_SPEC_URL}")
    logger.info("opds_valid", extra={"file": str(path), "schema_version": schema_version})
    return 0


def cmd_opds_validate(args: argparse.Namespace, logger: logging.Logger) -> int:
    """
    Validate an ODPS file (YAML or JSON) against the specification schema.

    Args:
        args.file: Path to the ODPS file (YAML or JSON)
        args.spec: ``bitol-1.0.0`` / ``bitol-1.1.0`` / ``odpi-4.1``; when
            omitted the document is auto-detected — ``apiVersion`` plus
            ``kind: DataProduct`` means Bitol, anything else is ODPI v4.1.
        args.version: Legacy ODPI version selector (forces the ODPI path)
        args.full_schema: Use full JSON schema validation (default: true)

    Returns:
        0 if valid, non-zero if invalid
    """
    spec = _str_arg(args, "spec")
    if spec is not None and spec not in SUPPORTED_SPECS:
        spec = None
    legacy_version = _str_arg(args, "version")
    version = legacy_version or DEFAULT_VERSION
    version_info = get_version_info(version)
    use_full_schema = getattr(args, "full_schema", True)

    # Load OPDS file
    try:
        path = Path(args.file)
        if not path.exists():
            console_error(f"Error: File not found: {args.file}")
            return 1

        opds_data = _load_document(path)
    except Exception as e:
        console_error(f"Error loading OPDS file: {e}")
        return 1

    if spec in BITOL_SPEC_API_VERSIONS or (
        spec is None and legacy_version is None and _is_bitol_document(opds_data)
    ):
        return _validate_bitol(path, opds_data, logger, spec=spec)

    logger.info(
        "opds_validate_start",
        extra={
            "file": args.file,
            "version": version,
            "schema": version_info["schema_url"],
            "full_validation": use_full_schema,
        },
    )

    try:
        # Check if this is a wrapped format (from render())
        if "artifacts" in opds_data and isinstance(opds_data["artifacts"], dict):
            logger.info("opds_validate_unwrap", extra={"format": "wrapped"})
            opds_data = opds_data["artifacts"]
    except Exception as e:
        console_error(f"Error loading OPDS file: {e}")
        return 1

    # Use comprehensive validator if available
    try:
        from fluid_build.providers.odps.validator import validate_opds_structure

        result = validate_opds_structure(
            opds_data,
            version=version,
            use_full_schema=use_full_schema,
            schema_url=version_info.get("schema_url_raw"),
        )

        if not result["valid"]:
            console_error(
                f"✗ OPDS validation failed ({result.get('validation_type', 'unknown')} validation)"
            )
            if result.get("errors"):
                console_error("\nErrors:")
                for error in result["errors"]:
                    console_error(f"  - {error}")
            logger.error("opds_invalid", extra=result)
            return 1

        # Validation successful
        validation_type = result.get("validation_type", "basic")
        cprint(f"✓ OPDS file is valid (v{version} {validation_type} validation)")
        cprint(f"  Data Product: {opds_data.get('dataProductId')}")
        cprint(f"  Name: {opds_data.get('dataProductName')}")
        cprint(f"  Schema Reference: {version_info['schema_url']}")

        if result.get("warnings"):
            cprint("\nWarnings:")
            for warning in result["warnings"]:
                cprint(f"  ⚠ {warning}")

        if validation_type == "full_schema":
            cprint("\n✓ Validated against official JSON Schema")
            cprint(f"  {version_info.get('schema_url_raw', version_info['schema_url'])}")

        logger.info(
            "opds_valid",
            extra={"file": args.file, "version": version, "validation_type": validation_type},
        )
        return 0

    except ImportError:
        logger.warning("opds_validator_unavailable", extra={"message": "Using basic validation"})
        # Fall back to basic validation
        pass

    # Basic validation fallback
    required_fields = ["dataProductId", "dataProductName", "dataProductDescription"]
    missing = [f for f in required_fields if f not in opds_data]

    if missing:
        console_error(f"✗ OPDS validation failed: Missing required fields: {', '.join(missing)}")
        logger.error("opds_invalid", extra={"missing_fields": missing})
        return 1

    # Check version metadata if present
    if "version" in opds_data:
        cprint(f"  OPDS version in file: {opds_data['version']}")

    cprint(f"✓ OPDS file is valid (v{version} basic validation)")
    cprint(f"  Data Product: {opds_data.get('dataProductId')}")
    cprint(f"  Name: {opds_data.get('dataProductName')}")
    cprint(f"  Schema Reference: {version_info['schema_url']}")

    logger.info("opds_valid", extra={"file": args.file, "version": version})
    return 0


def cmd_opds_info(args: argparse.Namespace, logger: logging.Logger) -> int:
    """
    Display information about OPDS specification versions.

    Args:
        args.spec: Optional specification (``bitol-1.0.0`` / ``bitol-1.1.0`` /
            ``odpi-4.1``) to show info for
        args.version: Optional specific ODPI version to show info for (legacy)
        args.json: Output in JSON format

    Returns:
        0 on success
    """
    spec = _str_arg(args, "spec")
    if spec in BITOL_SPECS:
        spec_info = BITOL_SPECS[spec]
        if getattr(args, "json", False):
            _write_stdout(json.dumps({spec: spec_info}, indent=2))
        else:
            cprint(f"Bitol ODPS {spec_info['apiVersion']} (--spec {spec})")
            cprint("=" * 60)
            cprint(f"Description:  {spec_info['description']}")
            cprint(f"Status:       {spec_info['status']}")
            cprint(f"Spec URL:     {spec_info['spec_url']}")
            cprint(f"Schema:       vendored odps-product-{spec_info['apiVersion']}.json")
            if spec_info.get("default"):
                cprint("Default:      Yes (fluid odps export)")
        return 0

    version = "4.1" if spec == SPEC_ODPI_4_1 else getattr(args, "version", None)
    if version:
        # Show info for specific version
        try:
            version_info = get_version_info(version)

            if getattr(args, "json", False):
                _write_stdout(json.dumps({version: version_info}, indent=2))
            else:
                cprint(f"OPDS Version {version}")
                cprint("=" * 60)
                cprint(f"Description:  {version_info['description']}")
                cprint(f"Status:       {version_info['status']}")
                cprint(f"Spec URL:     {version_info['spec_url']}")
                cprint(f"Schema URL:   {version_info['schema_url']}")
                if version_info.get("default"):
                    cprint("Default:      Yes")
        except ValueError as e:
            console_error(str(e))
            return 1
    else:
        # Show all versions
        if getattr(args, "json", False):
            _write_stdout(json.dumps(ODPS_VERSIONS, indent=2))
        else:
            cprint("ODPS (Open Data Product Specification) Versions")
            cprint("=" * 60)
            cprint()

            for version, info in ODPS_VERSIONS.items():
                default_marker = " [DEFAULT]" if info.get("default") else ""
                status_marker = f" ({info['status'].upper()})" if info["status"] != "stable" else ""
                cprint(f"Version {version}{default_marker}{status_marker}")
                cprint(f"  {info['description']}")
                cprint(f"  Spec:   {info['spec_url']}")
                cprint(f"  Schema: {info['schema_url']}")
                cprint()

            cprint("Bitol ODPS (Open Data Product Standard) Specifications")
            cprint("=" * 60)
            cprint()
            for spec_name, spec_info in BITOL_SPECS.items():
                default_marker = " (default for export)" if spec_info.get("default") else ""
                cprint(f"--spec {spec_name}{default_marker}")
                cprint(f"  {spec_info['description']}")
                cprint(f"  Spec:   {spec_info['spec_url']}")
                cprint()

            cprint("Usage:")
            cprint(f"  fluid odps export contract.yaml --out-dir out/      # {SPEC_BITOL_1_0_0}")
            cprint("  fluid odps export contract.yaml --spec odpi-4.1 --out output.json")
            cprint("  fluid odps validate output.json                    # auto-detects the spec")
            cprint("  fluid odps info --spec odpi-4.1")

    return 0


def register(subparsers: argparse._SubParsersAction) -> None:
    """Register ODPS commands with the CLI."""

    # Main ODPS command group. ``opds`` is registered as an alias so users
    # can spell the command either way; both go through the --spec dispatcher.
    odps = subparsers.add_parser(
        "odps",
        aliases=["opds"],
        help="Bitol ODPS v1.0.0 (default) or ODPI v4.1 — export, import, validate",
        description="""
        Work with the ODPS family of standards.

        Two specifications are supported, dispatched via --spec:
          bitol-1.0.0  Bitol Open Data Product Standard v1.0.0 (default).
          bitol-1.1.0  Bitol Open Data Product Standard v1.1.0.
                       Bidirectional. Emits 1 ODPS doc + N sibling ODCS
                       contracts. Import accepts a file, a directory, or
                       a lone ODCS file.
          odpi-4.1     Open Data Product Initiative v4.1 (Linux Foundation).
                       Export-only single JSON document.

        validate auto-detects Bitol vs ODPI when --spec is omitted.

        Official Specifications:
          Bitol ODPS: https://github.com/bitol-io/open-data-product-standard
          ODPI v4.1:  https://github.com/Open-Data-Product-Initiative/v4.1
        """,
    )

    odps_sub = odps.add_subparsers(dest="odps_command", help="ODPS operations")

    # odps export
    export = odps_sub.add_parser(
        "export",
        help="Export FLUID contract to ODPS format",
        description="""
        Export a FLUID contract to ODPS.

        Bitol specs (default): --out-dir DIR writes the ODPS doc plus one
        <contractId>.odcs.<fmt> file per output port; --out FILE writes the
        ODPS doc to FILE and the ODCS files next to it; with neither, the
        ODPS doc is printed to stdout as JSON. With --spec odpi-4.1 a single
        ODPI v4.1 JSON document is written to --out (default: stdout).
        """,
    )
    export.add_argument("contract", help="Path to FLUID contract file (YAML/JSON)")
    export.add_argument(
        "--spec",
        default=None,
        choices=list(SUPPORTED_SPECS),
        help=(
            f"Target specification (default: Bitol ODPS, {DEFAULT_SPEC} for fresh exports). "
            f"{SPEC_BITOL_1_0_0} / {SPEC_BITOL_1_1_0} emit 1 ODPS doc + N sibling "
            f"ODCS contracts declaring apiVersion v1.0.0 / v1.1.0; when omitted the "
            f"version follows ODPS_API_VERSION, else the imported document's own "
            f"apiVersion, else v1.0.0. "
            f"{SPEC_ODPI_4_1} emits a single ODPI v4.1 JSON document."
        ),
    )
    # Deprecated alias — kept for back-compat. Hidden from --help.
    export.add_argument(
        "--version",
        default=None,
        choices=list(ODPS_VERSIONS.keys()),
        help=argparse.SUPPRESS,
    )
    export.add_argument(
        "--out",
        default="-",
        help=(
            "Output file path, or '-' for stdout (default: stdout). For the Bitol specs "
            "the per-port ODCS contracts are written next to it as <contractId>.odcs.<fmt>."
        ),
    )
    export.add_argument(
        "--out-dir",
        dest="out_dir",
        default=None,
        help=(
            "For the Bitol specs: directory where the ODPS doc + per-port "
            "ODCS files are written. Mutually exclusive with --out (use one or the other). "
            "Rejected with --spec odpi-4.1."
        ),
    )
    export.add_argument(
        "--format",
        "-f",
        default=None,
        choices=["yaml", "json"],
        help=(
            "Bitol specs only: output format for file/dir writes (default: inferred from "
            "the --out suffix — .json → json, .yaml/.yml → yaml — else yaml). Stdout "
            "always uses JSON. ODPI v4.1 is always JSON; -f yaml is rejected with "
            "--spec odpi-4.1."
        ),
    )
    export.add_argument("--env", help="Environment name for overlay application")
    export.add_argument(
        "--validate-strict",
        dest="validate_strict",
        action="store_true",
        default=True,
        help="Validate the emitted ODPS + per-port ODCS docs against vendored schemas (default: true).",
    )
    export.add_argument(
        "--no-validate-strict",
        dest="validate_strict",
        action="store_false",
        help=(
            "Still validate, but report schema problems as warnings on stderr "
            "instead of failing the export."
        ),
    )
    export.add_argument(
        "--pretty",
        action="store_true",
        default=True,
        help="(ODPI only) Pretty-print JSON output (default: true)",
    )
    export.add_argument(
        "--compact",
        dest="pretty",
        action="store_false",
        help="(ODPI only) Compact JSON output (no indentation).",
    )
    export.set_defaults(func=cmd_opds_export)

    # odps import
    importp = odps_sub.add_parser(
        "import",
        help="Import an ODPS / ODCS file or directory → one FLUID contract",
        description=(
            "Import a Bitol ODPS product file, a directory containing the ODPS "
            "doc + sibling ODCS files (or just ODCS files), or a single ODCS "
            "contract file. The inputs are schema-validated; the output is one FLUID "
            "contract skeleton that may need edits (fluidVersion/kind/id/name, expose "
            "kind, binding format/location) before `fluid validate` / `fluid plan` pass."
        ),
    )
    importp.add_argument(
        "path",
        help="Input path — a .odps.yaml file, a directory, or a .odcs.yaml file.",
    )
    importp.add_argument(
        "--spec",
        default=None,
        choices=list(SUPPORTED_SPECS),
        help=(
            f"Specification (default: {DEFAULT_SPEC}). Either Bitol spec accepts "
            f"v1.0.0 and v1.1.0 documents on import; {SPEC_ODPI_4_1} is export-only."
        ),
    )
    importp.add_argument(
        "-o", "--out", default=None, help="Output FLUID file path (default: stdout)"
    )
    importp.add_argument(
        "-f",
        "--format",
        default=None,
        choices=["yaml", "json"],
        help=(
            "Output format (default: inferred from the -o suffix — .json → json, "
            ".yaml/.yml → yaml — else yaml; stdout defaults to yaml)."
        ),
    )
    importp.add_argument(
        "--no-remote",
        dest="no_remote",
        action="store_true",
        help="Disable http(s) fetch when resolving contractId references.",
    )
    importp.add_argument(
        "--lenient",
        action="store_true",
        help=(
            "Downgrade output-port resolution failures, and schema-invalid ODCS "
            "inputs, to warnings (input ports are always lenient)."
        ),
    )
    importp.set_defaults(func=cmd_opds_import)

    # odps validate
    validate = odps_sub.add_parser(
        "validate",
        help="Validate an ODPS file against specification",
        description="""
        Validate an ODPS file (YAML or JSON) against its specification schema.

        With --spec omitted the document is auto-detected: one declaring
        apiVersion and kind: DataProduct is validated as Bitol ODPS (schema
        chosen from its apiVersion); anything else as ODPI v4.1.
        """,
    )
    validate.add_argument("file", help="Path to ODPS file (YAML or JSON)")
    validate.add_argument(
        "--spec",
        default=None,
        choices=list(SUPPORTED_SPECS),
        help=(
            "Specification to validate against (default: auto-detect). An explicit "
            "Bitol spec pins the schema version."
        ),
    )
    validate.add_argument(
        "--version",
        default=None,
        choices=list(ODPS_VERSIONS.keys()),
        help=f"ODPI version to validate against (forces ODPI; default: {DEFAULT_VERSION})",
    )
    validate.add_argument(
        "--full-schema",
        action="store_true",
        default=True,
        help="Use full JSON schema validation (default: true, requires jsonschema library)",
    )
    validate.add_argument(
        "--no-full-schema",
        dest="full_schema",
        action="store_false",
        help="Skip full JSON schema validation, use basic validation only",
    )
    validate.set_defaults(func=cmd_opds_validate)

    # odps info
    info = odps_sub.add_parser(
        "info",
        help="Display ODPS specification version information",
        description="""
        Show information about supported ODPS specification versions.

        Displays specification URLs, schema references, and version status.
        """,
    )
    info.add_argument(
        "--spec",
        default=None,
        choices=list(SUPPORTED_SPECS),
        help="Show info for one specification only",
    )
    info.add_argument(
        "--version", choices=list(ODPS_VERSIONS.keys()), help="Show info for specific version only"
    )
    info.add_argument("--json", action="store_true", help="Output in JSON format")
    info.set_defaults(func=cmd_opds_info)
