# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""``fluid contract`` subcommand surface.

Subcommands:

* ``apply-suggestion <suggestion-file>`` — merge a forge-generated
  ``<contract>.suggested.json`` (with per-field provenance annotations)
  into a target contract YAML/JSON. Hard-rejects any AI-provenance
  values that land on safety-critical paths.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict

import yaml

from ._common import CLIError
from ._forge_ai_guardrails import (
    GuardrailViolation,
    apply_suggestion,
    read_suggestion_file,
)

COMMAND = "contract"


def register(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(COMMAND, help="Inspect / mutate FLUID contracts")
    sub = p.add_subparsers(dest="contract_subcmd", required=True)

    apply_p = sub.add_parser(
        "apply-suggestion",
        help="Merge a *.suggested.json file into a target contract",
    )
    apply_p.add_argument("suggestion", help="Path to <contract>.suggested.json")
    apply_p.add_argument(
        "--target",
        required=True,
        help="Path to the target contract YAML/JSON to merge into",
    )
    apply_p.add_argument(
        "--accept-provenance",
        nargs="*",
        choices=["ai", "introspection", "template", "user"],
        default=None,
        help="Accept only fields with these provenance kinds (default: all)",
    )
    apply_p.add_argument(
        "--out",
        help="Output path. Default: overwrite --target after a one-line backup",
    )
    apply_p.set_defaults(cmd=COMMAND, func=_run_apply_suggestion)


def _load_contract(path: Path) -> Dict[str, Any]:
    body = path.read_text(encoding="utf-8")
    if path.suffix in (".yaml", ".yml"):
        return yaml.safe_load(body) or {}
    return json.loads(body)


def _dump_contract(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix in (".yaml", ".yml"):
        path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    else:
        path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")


def _run_apply_suggestion(args, logger: logging.Logger) -> int:
    suggestion_path = Path(args.suggestion)
    target_path = Path(args.target)

    if not suggestion_path.exists():
        raise CLIError(1, "suggestion_not_found", {"path": str(suggestion_path)})
    if not target_path.exists():
        raise CLIError(1, "contract_not_found", {"path": str(target_path)})

    try:
        suggestion = read_suggestion_file(suggestion_path)
    except Exception as exc:  # noqa: BLE001
        raise CLIError(1, "suggestion_parse_failed", {"error": str(exc)}) from exc

    contract = _load_contract(target_path)

    accept = tuple(args.accept_provenance) if args.accept_provenance else None
    try:
        merged = apply_suggestion(contract, suggestion, accept_provenance=accept)
    except GuardrailViolation as exc:
        # The guardrails reject AI-provenance values on safety-critical
        # paths. Surface the message verbatim — it tells the user
        # exactly which path was blocked and why.
        logger.error(str(exc))
        raise CLIError(1, "ai_guardrail_violation", {"reason": str(exc)}) from exc

    out_path = Path(args.out) if args.out else target_path
    if not args.out:
        backup = target_path.with_suffix(target_path.suffix + ".bak")
        backup.write_bytes(target_path.read_bytes())
        logger.info(f"Backup written: {backup}")
    _dump_contract(out_path, merged)
    logger.info(f"Merged {len(suggestion.fields)} field(s) → {out_path}")
    return 0
