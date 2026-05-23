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

Usage:
    fluid opds export <contract> [--spec bitol-1.0.0|odpi-4.1] [--out file] [--out-dir DIR]
    fluid opds import <path>    [--spec bitol-1.0.0] [--no-remote] [--lenient] [-o OUT]
    fluid opds validate <file>  [--spec ...]
    fluid opds info             [--spec ...]
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict

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
# the Bitol Open Data Product Standard v1.0.0 and the legacy Open Data Product
# Initiative v4.1.
SPEC_BITOL_1_0_0 = "bitol-1.0.0"
SPEC_ODPI_4_1 = "odpi-4.1"
DEFAULT_SPEC = SPEC_BITOL_1_0_0
SUPPORTED_SPECS = (SPEC_BITOL_1_0_0, SPEC_ODPI_4_1)


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

    if spec == SPEC_BITOL_1_0_0:
        return _export_bitol(args, contract, logger)
    if spec == SPEC_ODPI_4_1:
        return _export_odpi_v41(args, contract, logger)
    console_error(f"Unsupported --spec {spec!r}; supported: {SUPPORTED_SPECS}")
    return 2


def _export_bitol(
    args: argparse.Namespace, contract: Dict[str, Any], logger: logging.Logger
) -> int:
    """Bitol ODPS v1.0.0 export — 1 ODPS doc + N sibling ODCS contracts."""
    from fluid_build.providers.odps_standard import BitolOdpsProvider

    out = getattr(args, "out", "-")
    out_dir = getattr(args, "out_dir", None)
    validate_strict = getattr(args, "validate_strict", True)
    fmt = getattr(args, "format", "yaml") or "yaml"

    provider = BitolOdpsProvider()
    provider.strict_validation = bool(validate_strict)

    try:
        bundle = provider.render(
            contract,
            out=out if out and out != "-" and not out_dir else None,
            out_dir=out_dir,
            fmt=fmt,
        )
    except Exception as e:
        logger.error("opds_export_failed", extra={"error": str(e), "spec": SPEC_BITOL_1_0_0})
        console_error(f"Error exporting to Bitol ODPS: {e}")
        return 1

    if out == "-" and not out_dir:
        # Dump product doc to stdout. Use JSON for stdout regardless of fmt
        # so the output is unambiguous in pipelines.
        cprint(json.dumps(bundle["product"], indent=2, ensure_ascii=False))
    else:
        product_count = 1
        contract_count = len(bundle.get("contracts") or {})
        location = out_dir if out_dir else out
        cprint(
            f"✓ Exported Bitol ODPS v1.0.0: {product_count} product + "
            f"{contract_count} ODCS contract(s) → {location}"
        )
    return 0


def _export_odpi_v41(
    args: argparse.Namespace, contract: Dict[str, Any], logger: logging.Logger
) -> int:
    """Legacy ODPI v4.1 export path (single JSON document)."""
    from fluid_build.cli.bootstrap import build_provider

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
                cprint(json.dumps(result, indent=2, ensure_ascii=False))
            else:
                cprint(json.dumps(result, ensure_ascii=False))
        else:
            cprint(f"✓ Exported to ODPI v4.1: {out_path}")
            cprint(f"  Specification: {version_info['spec_url']}")
        return 0
    except Exception as e:
        logger.error("opds_export_failed", extra={"error": str(e)})
        console_error(f"Error exporting to ODPI: {e}")
        return 1


def cmd_opds_import(args: argparse.Namespace, logger: logging.Logger) -> int:
    """Import an ODPS / ODCS file or directory → one validated FLUID contract.

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
            "ODPI v4.1 is export-only; import is supported only for --spec bitol-1.0.0"
        )
        return 2

    in_path = Path(args.path)
    if not in_path.exists():
        console_error(f"Input path not found: {in_path}")
        return 1

    allow_remote = not getattr(args, "no_remote", False)
    lenient = bool(getattr(args, "lenient", False))
    out = getattr(args, "out", None)
    fmt = (getattr(args, "format", "yaml") or "yaml").lower()

    try:
        fluid = _dispatch_import(in_path, allow_remote=allow_remote, lenient=lenient)
    except Exception as e:
        logger.error("opds_import_failed", extra={"error": str(e), "path": str(in_path)})
        console_error(f"Error importing: {e}")
        return 1

    if out and out != "-":
        out_path = Path(out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if fmt == "json":
            out_path.write_text(json.dumps(fluid, indent=2, ensure_ascii=False))
        else:
            import yaml

            out_path.write_text(yaml.dump(fluid, default_flow_style=False, sort_keys=False))
        cprint(f"✓ Imported {in_path} → {out_path}")
    else:
        if fmt == "json":
            cprint(json.dumps(fluid, indent=2, ensure_ascii=False))
        else:
            import yaml

            cprint(yaml.dump(fluid, default_flow_style=False, sort_keys=False))
    return 0


def _dispatch_import(
    path: Path, *, allow_remote: bool, lenient: bool
) -> Dict[str, Any]:
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

        return OdcsProvider().import_contract(path)

    if ".odps" in suffixes or name_lower.endswith((".odps.yaml", ".odps.yml", ".odps.json")):
        from fluid_build.providers.odps_standard import BitolOdpsProvider

        return BitolOdpsProvider().import_contract(
            path, allow_remote=allow_remote, lenient=lenient
        )

    # Last resort: sniff the file's ``kind`` field
    from fluid_build.providers.odcs.io import read_input

    data = read_input(path)
    kind = data.get("kind") if isinstance(data, dict) else None
    if kind == "DataProduct":
        from fluid_build.providers.odps_standard import BitolOdpsProvider

        return BitolOdpsProvider().import_contract(
            path, allow_remote=allow_remote, lenient=lenient
        )
    if kind == "DataContract":
        from fluid_build.providers.odcs import OdcsProvider

        return OdcsProvider().import_contract(path)

    raise ValueError(
        f"Cannot determine input type for {path}: "
        f"expected a directory, *.odps.yaml, *.odcs.yaml, or a file with "
        f"kind: DataProduct or kind: DataContract"
    )


def cmd_opds_validate(args: argparse.Namespace, logger: logging.Logger) -> int:
    """
    Validate an OPDS file against the specification schema.

    Args:
        args.file: Path to OPDS JSON file
        args.version: OPDS specification version to validate against
        args.full_schema: Use full JSON schema validation (default: true)

    Returns:
        0 if valid, non-zero if invalid
    """
    version = getattr(args, "version", DEFAULT_VERSION)
    version_info = get_version_info(version)
    use_full_schema = getattr(args, "full_schema", True)

    logger.info(
        "opds_validate_start",
        extra={
            "file": args.file,
            "version": version,
            "schema": version_info["schema_url"],
            "full_validation": use_full_schema,
        },
    )

    # Load OPDS file
    try:
        path = Path(args.file)
        if not path.exists():
            console_error(f"Error: File not found: {args.file}")
            return 1

        with open(path, encoding="utf-8") as f:
            opds_data = json.load(f)

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
        args.version: Optional specific version to show info for
        args.json: Output in JSON format

    Returns:
        0 on success
    """
    if hasattr(args, "version") and args.version:
        # Show info for specific version
        try:
            version_info = get_version_info(args.version)

            if getattr(args, "json", False):
                cprint(json.dumps({args.version: version_info}, indent=2))
            else:
                cprint(f"OPDS Version {args.version}")
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
            cprint(json.dumps(ODPS_VERSIONS, indent=2))
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

            cprint("Usage:")
            cprint("  fluid odps export contract.yaml --version 4.1")
            cprint("  fluid odps validate output.json --version 4.1")

    return 0


def register(subparsers: argparse._SubParsersAction) -> None:
    """Register ODPS commands with the CLI."""

    # Main ODPS command group
    odps = subparsers.add_parser(
        "odps",
        help="Export and validate ODPS (Open Data Product Specification) format",
        description="""
        Work with ODPS (Open Data Product Specification) format.
        
        The Open Data Product Specification is a vendor-neutral, open-source standard
        for describing data products. This command supports exporting FLUID contracts
        to ODPS format and validating ODPS files.
        
        Official Specification: https://github.com/Open-Data-Product-Initiative
        """,
    )

    odps_sub = odps.add_subparsers(dest="odps_command", help="ODPS operations")

    # odps export
    export = odps_sub.add_parser(
        "export",
        help="Export FLUID contract to ODPS format",
        description="""
        Export a FLUID contract to ODPS (Open Data Product Specification) JSON format.
        
        Supports multiple OPDS specification versions for future compatibility.
        Output can be written to a file or stdout for pipeline integration.
        """,
    )
    export.add_argument("contract", help="Path to FLUID contract file (YAML/JSON)")
    export.add_argument(
        "--spec",
        default=None,
        choices=list(SUPPORTED_SPECS),
        help=(
            f"Target specification (default: {DEFAULT_SPEC}). "
            f"{SPEC_BITOL_1_0_0} emits 1 ODPS doc + N sibling ODCS contracts; "
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
        "--out", default="-", help="Output file path, or '-' for stdout (default: stdout)"
    )
    export.add_argument(
        "--out-dir",
        dest="out_dir",
        default=None,
        help=(
            f"For {SPEC_BITOL_1_0_0}: directory where the ODPS doc + per-port "
            "ODCS files are written. Mutually exclusive with --out (use one or the other)."
        ),
    )
    export.add_argument(
        "--format",
        "-f",
        default="yaml",
        choices=["yaml", "json"],
        help="Output format for file/dir writes (default: yaml). Stdout always uses JSON.",
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
        help="Downgrade schema validation to warnings.",
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
            "contract file. Always emits one validated FLUID contract."
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
        help=f"Specification (default: {DEFAULT_SPEC}). {SPEC_ODPI_4_1} is export-only.",
    )
    importp.add_argument(
        "-o", "--out", default=None, help="Output FLUID file path (default: stdout)"
    )
    importp.add_argument(
        "-f", "--format", default="yaml", choices=["yaml", "json"], help="Output format"
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
        help="Downgrade output-port resolution failures to warnings (input ports are always lenient).",
    )
    importp.set_defaults(func=cmd_opds_import)

    # odps validate
    validate = odps_sub.add_parser(
        "validate",
        help="Validate an ODPS file against specification",
        description="""
        Validate an ODPS JSON file against the official specification schema.
        
        Performs structural validation and checks required fields according to
        the ODPS specification version.
        """,
    )
    validate.add_argument("file", help="Path to ODPS JSON file")
    validate.add_argument(
        "--version",
        default=DEFAULT_VERSION,
        choices=list(ODPS_VERSIONS.keys()),
        help=f"ODPS version to validate against (default: {DEFAULT_VERSION})",
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
        "--version", choices=list(ODPS_VERSIONS.keys()), help="Show info for specific version only"
    )
    info.add_argument("--json", action="store_true", help="Output in JSON format")
    info.set_defaults(func=cmd_opds_info)
