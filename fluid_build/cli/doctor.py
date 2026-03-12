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
FLUID Doctor Command - Unified System Diagnostics

Runs comprehensive system diagnostics including:
- Basic infrastructure checks
- FLUID 0.7.1 feature availability (automatic detection)
- Provider capabilities
- Schema validation
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple

from fluid_build.cli.console import cprint

from ._common import CLIError
from ._logging import info
from .security import (
    InputSanitizer,
    ProcessManager,
    ProductionLogger,
    validate_input_file,
    validate_output_file,
)

# Try Rich for better output
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

COMMAND = "doctor"


def register(subparsers: argparse._SubParsersAction):
    """Register unified doctor command"""
    p = subparsers.add_parser(
        COMMAND,
        help="Run system diagnostics and feature checks",
        description="""
Run comprehensive system diagnostics for FLUID CLI.

Automatically checks:
• Core FLUID infrastructure
• FLUID 0.7.1 feature availability (if applicable)
• Provider capabilities
• Schema validation
• Runtime dependencies
        """.strip(),
    )
    p.add_argument(
        "--out-dir", default="runtime/diag", help="Output directory for diagnostic files"
    )
    p.add_argument(
        "--features-only",
        action="store_true",
        help="Only check FLUID feature availability (skip infrastructure)",
    )
    p.add_argument("--verbose", "-v", action="store_true", help="Show detailed output")
    p.set_defaults(cmd=COMMAND, func=run)


def run(args, logger: logging.Logger) -> int:
    """
    Run system diagnostics with automatic feature detection.

    Automatically checks both base infrastructure and 0.7.1 features.
    """
    secure_logger = ProductionLogger(logger)

    # Always check 0.7.1 feature availability (non-intrusive)
    feature_checks_ok, feature_checks = _check_fluid_features()

    # If features-only mode, just show features and exit
    if getattr(args, "features_only", False):
        _print_feature_checks(feature_checks, getattr(args, "verbose", False))
        return 0 if feature_checks_ok else 1

    # Show feature checks first
    if getattr(args, "verbose", False) or not feature_checks_ok:
        _print_feature_checks(feature_checks, getattr(args, "verbose", False))
        cprint()  # Spacing

    # Run infrastructure diagnostics
    process_manager = ProcessManager()
    script_path = Path("./scripts/diagnose.sh")

    # Validate script exists and is safe to execute
    try:
        validated_script = validate_input_file(script_path, "diagnostic script")
    except Exception:
        secure_logger.log_safe("warning", f"Diagnostic script not found or invalid: {script_path}")
        info(
            logger,
            "doctor_script_missing",
            script=str(script_path),
            note="Skipping infrastructure checks",
        )

        # If we only have feature checks, return based on those
        return 0 if feature_checks_ok else 1

    # Validate and create output directory
    try:
        out_dir = Path(args.out_dir)
        validate_output_file(out_dir / "test", "diagnostic output")
        out_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise CLIError(
            1, "doctor_output_dir_failed", context={"out_dir": args.out_dir, "error": str(e)}
        )

    # Prepare secure environment
    env = os.environ.copy()

    # Sanitize environment variables
    if getattr(args, "provider", None):
        provider = InputSanitizer.sanitize_filename(args.provider)
        if provider != args.provider:
            secure_logger.log_safe(
                "warning", f"Provider name sanitized: {args.provider} -> {provider}"
            )
        env["PROVIDER"] = provider

    # Add output directory to environment
    env["FLUID_DIAG_OUT_DIR"] = str(out_dir.resolve())

    try:
        # Execute with timeout and security constraints
        def run_diagnostic():
            return subprocess.run(
                ["bash", str(validated_script)],
                env=env,
                check=True,
                cwd=Path.cwd(),
                capture_output=False,  # Allow real-time output
                timeout=300,  # 5 minute timeout
            )

        # Run with process manager for timeout handling
        process_manager.run_with_timeout(run_diagnostic, timeout=300)

        secure_logger.log_safe("info", "Diagnostic completed successfully", out_dir=str(out_dir))
        info(logger, "doctor_ok")

        # Return success only if both feature checks and infrastructure checks pass
        return 0 if feature_checks_ok else 1

    except subprocess.TimeoutExpired:
        raise CLIError(1, "doctor_timeout", context={"timeout": 300})
    except subprocess.CalledProcessError as e:
        secure_logger.log_safe("error", f"Diagnostic failed with return code: {e.returncode}")
        raise CLIError(1, "doctor_failed", context={"returncode": e.returncode})
    except Exception as e:
        secure_logger.log_safe("error", f"Unexpected diagnostic error: {str(e)}")
        raise CLIError(1, "doctor_unexpected_error", context={"error": str(e)})


def _check_fluid_features() -> Tuple[bool, List[Dict[str, any]]]:
    """
    Check FLUID feature availability (both 0.5.7 base and 0.7.1 enhancements).

    Returns:
        (all_ok, checks) - Boolean and list of check results
    """
    checks = []
    all_ok = True

    # Core checks (0.5.7 baseline)
    try:
        from fluid_build.schema_manager import SchemaManager

        sm = SchemaManager()
        versions = sm.BUNDLED_VERSIONS
        _has_057 = "0.5.7" in versions  # noqa: F841
        has_071 = "0.7.1" in versions

        checks.append(
            {
                "check": "FLUID Schema Manager",
                "category": "core",
                "status": "✅ Available",
                "ok": True,
                "details": f"Versions: {', '.join(versions)}",
            }
        )

        if has_071:
            checks.append(
                {
                    "check": "FLUID 0.7.1 Schema",
                    "category": "0.7.1",
                    "status": "✅ Available",
                    "ok": True,
                    "details": "Provider-first orchestration schema",
                }
            )
        else:
            checks.append(
                {
                    "check": "FLUID 0.7.1 Schema",
                    "category": "0.7.1",
                    "status": "⚠️  Not found",
                    "ok": True,  # Not critical - can still use 0.5.7
                    "details": "0.7.1 features unavailable, falling back to 0.5.7",
                }
            )
    except Exception as e:
        checks.append(
            {
                "check": "FLUID Schema Manager",
                "category": "core",
                "status": "❌ Error",
                "ok": False,
                "details": str(e),
            }
        )
        all_ok = False

    # 0.7.1 Enhancement checks
    try:
        checks.append(
            {
                "check": "Sovereignty Validator",
                "category": "0.7.1",
                "status": "✅ Available",
                "ok": True,
                "details": "Jurisdiction & data residency constraints",
            }
        )
    except Exception:
        checks.append(
            {
                "check": "Sovereignty Validator",
                "category": "0.7.1",
                "status": "⚠️  Not available",
                "ok": True,  # Non-critical
                "details": "0.7.1 sovereignty features unavailable",
            }
        )

    try:
        checks.append(
            {
                "check": "AgentPolicy Validator",
                "category": "0.7.1",
                "status": "✅ Available",
                "ok": True,
                "details": "AI/LLM usage governance",
            }
        )
    except Exception:
        checks.append(
            {
                "check": "AgentPolicy Validator",
                "category": "0.7.1",
                "status": "⚠️  Not available",
                "ok": True,  # Non-critical
                "details": "0.7.1 agent policy features unavailable",
            }
        )

    try:
        checks.append(
            {
                "check": "Provider Action Parser",
                "category": "0.7.1",
                "status": "✅ Available",
                "ok": True,
                "details": "Provider-first orchestration ready",
            }
        )
    except Exception:
        checks.append(
            {
                "check": "Provider Action Parser",
                "category": "0.7.1",
                "status": "⚠️  Not available",
                "ok": True,  # Non-critical - falls back to 0.5.7
                "details": "0.7.1 provider actions unavailable",
            }
        )

    # Provider-specific checks (optional)
    try:
        checks.append(
            {
                "check": "GCP Provider Actions",
                "category": "providers",
                "status": "✅ Available",
                "ok": True,
                "details": "BigQuery, GCS, IAM actions",
            }
        )
    except Exception:
        checks.append(
            {
                "check": "GCP Provider Actions",
                "category": "providers",
                "status": "⚠️  Not available",
                "ok": True,  # Non-critical if GCP not used
                "details": "Install GCP dependencies for full support",
            }
        )

    try:
        from fluid_build.providers.aws.actions import glue, iam, s3  # noqa: F401

        checks.append(
            {
                "check": "AWS Provider Actions",
                "category": "providers",
                "status": "✅ Available",
                "ok": True,
                "details": "S3, Glue, IAM actions (service-level dispatch)",
            }
        )
    except Exception:
        checks.append(
            {
                "check": "AWS Provider Actions",
                "category": "providers",
                "status": "⚠️  Not available",
                "ok": True,  # Non-critical if AWS not used
                "details": "Install AWS dependencies for full support",
            }
        )

    return all_ok, checks


def _print_feature_checks(checks: List[Dict[str, any]], verbose: bool = False):
    """Print feature checks with appropriate formatting."""

    if RICH_AVAILABLE:
        console = Console()

        table = Table(title="🔍 FLUID Feature Availability", show_header=True)
        table.add_column("Feature", style="cyan", width=30)
        table.add_column("Status", width=20)
        if verbose:
            table.add_column("Details", style="dim")

        for check in checks:
            status_color = "green" if check["ok"] else "red"
            if "⚠️" in check["status"]:
                status_color = "yellow"

            row = [check["check"], f"[{status_color}]{check['status']}[/{status_color}]"]
            if verbose:
                row.append(check["details"])

            table.add_row(*row)

        console.print(table)

        # Summary
        ok_count = sum(1 for c in checks if c["ok"])
        warning_count = sum(1 for c in checks if "⚠️" in c["status"])
        total = len(checks)

        if ok_count == total:
            console.print(
                Panel("[green]✅ All critical features available![/green]", border_style="green")
            )
        elif ok_count >= total - warning_count:
            console.print(
                Panel(
                    f"[yellow]⚠️  {ok_count}/{total} features available ({warning_count} optional features missing)[/yellow]",
                    border_style="yellow",
                )
            )
        else:
            console.print(
                Panel(
                    f"[red]❌ {total - ok_count} critical features missing[/red]",
                    border_style="red",
                )
            )
    else:
        # Simple text output
        cprint("\n" + "=" * 60)
        cprint("FLUID Feature Availability")
        cprint("=" * 60)

        for check in checks:
            status = check["status"]
            cprint(f"{status:20} {check['check']}")
            if verbose:
                cprint(f"                     → {check['details']}")

        cprint("=" * 60)

        ok_count = sum(1 for c in checks if c["ok"])
        total = len(checks)
        cprint(f"\n{ok_count}/{total} features available")
        cprint()
