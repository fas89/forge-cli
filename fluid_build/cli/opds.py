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

"""
ODPS CLI Commands — Bitol Open Data Product Standard v1.0.0.

Bidirectional. Export emits 1 ODPS doc + N sibling ``<contractId>.odcs.yaml``
files (the canonical Bitol fragments layout). Import accepts a Bitol ODPS
product file, a directory bundle, or a lone ODCS file.

Usage:
    fluid opds export <contract> [--out file] [--out-dir DIR]
    fluid opds import <path>     [--no-remote] [--lenient] [-o OUT]
    fluid opds validate <file>
    fluid opds info
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


# Bitol Open Data Product Standard v1.0.0 — the only supported spec.
SPEC_BITOL_1_0_0 = "bitol-1.0.0"
DEFAULT_SPEC = SPEC_BITOL_1_0_0
SUPPORTED_SPECS = (SPEC_BITOL_1_0_0,)

BITOL_SPEC_URL = "https://github.com/bitol-io/open-data-product-standard"
BITOL_SCHEMA_URL = (
    "https://raw.githubusercontent.com/bitol-io/open-data-product-standard/main/schema/odps.schema.json"
)


def resolve_spec(args: argparse.Namespace) -> str:
    """Resolve the active ``--spec`` from CLI args.

    Bitol ODPS v1.0.0 is the only supported spec; ``--spec`` is kept on the
    surface for forward-compatibility with future spec additions.
    """
    spec = getattr(args, "spec", None)
    if isinstance(spec, str) and spec in SUPPORTED_SPECS:
        return spec
    return DEFAULT_SPEC


def cmd_opds_export(args: argparse.Namespace, logger: logging.Logger) -> int:
    """Export a FLUID contract to Bitol ODPS v1.0.0."""
    from fluid_build.cli.bootstrap import load_contract_with_overlay
    from fluid_build.providers.odps_standard import BitolOdpsProvider

    try:
        contract = load_contract_with_overlay(args.contract, getattr(args, "env", None), logger)
    except Exception as e:
        logger.error("contract_load_failed", extra={"error": str(e)})
        console_error(f"Error loading contract: {e}")
        return 1

    out = getattr(args, "out", "-")
    out_dir = getattr(args, "out_dir", None)
    validate_strict = getattr(args, "validate_strict", True)
    fmt = (getattr(args, "format", "yaml") or "yaml").lower()

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
        # Stdout dump of the product doc — JSON for unambiguous pipeline use.
        cprint(json.dumps(bundle["product"], indent=2, ensure_ascii=False))
    else:
        contract_count = len(bundle.get("contracts") or {})
        location = out_dir if out_dir else out
        cprint(
            f"✓ Exported Bitol ODPS v1.0.0: 1 product + "
            f"{contract_count} ODCS contract(s) → {location}"
        )
    return 0


def cmd_opds_import(args: argparse.Namespace, logger: logging.Logger) -> int:
    """Import an ODPS / ODCS file or directory → one validated FLUID contract.

    Input dispatch:
      - Path ends in ``.odcs.yaml``/``.odcs.yml``/``.odcs.json`` →
        :meth:`OdcsProvider.import_contract` (single-expose FLUID).
      - Path is a file with ``kind: DataProduct`` (or ``.odps.yaml``) →
        :meth:`BitolOdpsProvider.import_contract` with resolver.
      - Path is a directory → :meth:`BitolOdpsProvider.import_directory`.
    """
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
    """Validate a Bitol ODPS v1.0.0 file against the vendored JSON Schema."""
    from fluid_build.providers.base import ProviderError
    from fluid_build.providers.odps_standard import BitolOdpsProvider
    from fluid_build.providers.odps_standard.io import read_input

    path = Path(args.file)
    if not path.exists():
        console_error(f"Error: File not found: {args.file}")
        return 1

    try:
        odps_data = read_input(path)
    except Exception as e:
        console_error(f"Error loading ODPS file: {e}")
        return 1

    if not isinstance(odps_data, dict):
        console_error(f"Error: {path} did not parse as a mapping")
        return 1

    try:
        BitolOdpsProvider().validate_product(odps_data)
    except ProviderError as e:
        console_error(f"✗ Bitol ODPS validation failed: {e}")
        logger.error("opds_invalid", extra={"file": str(path), "error": str(e)})
        return 1

    cprint(f"✓ Bitol ODPS v1.0.0 file is valid: {path}")
    cprint(f"  Data Product: {odps_data.get('id') or odps_data.get('name', '(unknown)')}")
    cprint(f"  Spec: {BITOL_SPEC_URL}")
    logger.info("opds_valid", extra={"file": str(path), "spec": SPEC_BITOL_1_0_0})
    return 0


def cmd_opds_info(args: argparse.Namespace, logger: logging.Logger) -> int:
    """Show Bitol Open Data Product Standard v1.0.0 information."""
    info = {
        "spec": SPEC_BITOL_1_0_0,
        "name": "Bitol Open Data Product Standard",
        "version": "1.0.0",
        "spec_url": BITOL_SPEC_URL,
        "schema_url": BITOL_SCHEMA_URL,
        "media_type": "application/odps+yaml;version=1.0.0",
        "license": "Apache-2.0",
    }

    if getattr(args, "json", False):
        cprint(json.dumps(info, indent=2))
        return 0

    cprint(f"{info['name']} v{info['version']}")
    cprint("=" * 60)
    cprint(f"Spec URL:   {info['spec_url']}")
    cprint(f"Schema URL: {info['schema_url']}")
    cprint(f"Media type: {info['media_type']}")
    cprint(f"License:    {info['license']}")
    return 0


def register(subparsers: argparse._SubParsersAction) -> None:
    """Register ODPS commands with the CLI."""

    # ``opds`` and ``odps`` are registered as aliases so users can spell the
    # command either way; both target Bitol ODPS v1.0.0.
    odps = subparsers.add_parser(
        "odps",
        aliases=["opds"],
        help="Bitol Open Data Product Standard v1.0.0 — export, import, validate",
        description=(
            "Work with the Bitol Open Data Product Standard v1.0.0. Bidirectional: "
            "export emits 1 ODPS doc + N sibling ODCS contracts; import accepts a "
            "file, a directory, or a lone ODCS file.\n\n"
            f"Specification: {BITOL_SPEC_URL}"
        ),
    )

    odps_sub = odps.add_subparsers(dest="odps_command", help="ODPS operations")

    # odps export
    export = odps_sub.add_parser(
        "export",
        help="Export FLUID contract to Bitol ODPS v1.0.0",
        description=(
            "Export a FLUID contract to Bitol Open Data Product Standard v1.0.0. "
            "Writes 1 ODPS product doc + N sibling ODCS contracts (one per output "
            "port). Output can be a single file (stdout or --out) or a directory "
            "bundle (--out-dir)."
        ),
    )
    export.add_argument("contract", help="Path to FLUID contract file (YAML/JSON)")
    export.add_argument(
        "--spec",
        default=None,
        choices=list(SUPPORTED_SPECS),
        help=f"Target specification (default: {DEFAULT_SPEC}).",
    )
    export.add_argument(
        "--out", default="-", help="Output file path, or '-' for stdout (default: stdout)"
    )
    export.add_argument(
        "--out-dir",
        dest="out_dir",
        default=None,
        help=(
            "Directory where the ODPS doc + per-port ODCS files are written. "
            "Mutually exclusive with --out."
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
        help="Validate emitted ODPS + per-port ODCS docs against vendored schemas (default: true).",
    )
    export.add_argument(
        "--no-validate-strict",
        dest="validate_strict",
        action="store_false",
        help="Downgrade schema validation to warnings.",
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
        help=f"Specification (default: {DEFAULT_SPEC}).",
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
        help="Validate a Bitol ODPS v1.0.0 file against the vendored JSON Schema",
        description=(
            "Validate an ODPS YAML/JSON file against the vendored Bitol ODPS "
            "v1.0.0 JSON Schema."
        ),
    )
    validate.add_argument("file", help="Path to ODPS YAML/JSON file")
    validate.set_defaults(func=cmd_opds_validate)

    # odps info
    info = odps_sub.add_parser(
        "info",
        help="Display Bitol Open Data Product Standard v1.0.0 metadata",
        description="Show specification URL, schema URL, media type, and license.",
    )
    info.add_argument("--json", action="store_true", help="Output in JSON format")
    info.set_defaults(func=cmd_opds_info)
