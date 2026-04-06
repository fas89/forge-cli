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

"""``fluid skills`` subcommand — manage the workspace industry skills file."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from fluid_build.cli.console import cprint
from fluid_build.cli.console import error as console_error
from fluid_build.cli.workspace_config import find_workspace_root

COMMAND = "skills"


def register(subparsers: argparse._SubParsersAction):
    """Register the skills command."""
    p = subparsers.add_parser(
        COMMAND,
        help="Manage industry skills for agentic project knowledge",
    )
    skills_sub = p.add_subparsers(dest="skills_action", help="Skills actions")

    skills_sub.add_parser(
        "update",
        help="Refresh the tools section of .fluid/skills.yaml to the latest CLI version",
    )

    skills_sub.add_parser(
        "show",
        help="Display the current industry skills file",
    )

    p.set_defaults(cmd=COMMAND, func=run)


def run(args, logger: logging.Logger) -> int:
    """Route skills subcommands."""
    action = getattr(args, "skills_action", None)
    if action == "update":
        return _update(logger)
    elif action == "show":
        return _show(logger)
    else:
        console_error("Usage: fluid skills <update|show>")
        return 1


def _update(logger: logging.Logger) -> int:
    """Refresh the tools section of .fluid/skills.yaml."""
    ws_root = find_workspace_root()
    if ws_root is None:
        console_error("Not inside a FLUID workspace. Run 'fluid init' first.")
        return 1

    skills_path = ws_root / ".fluid" / "skills.yaml"
    if not skills_path.exists():
        console_error("No .fluid/skills.yaml found. Run 'fluid init' to generate one.")
        return 1

    try:
        from fluid_build.cli.industry_skills import refresh_tools_section

        refresh_tools_section(skills_path)
    except Exception as e:
        console_error(f"Failed to update skills: {e}")
        return 1

    try:
        from rich.console import Console

        console = Console()
        import yaml

        with skills_path.open() as f:
            data = yaml.safe_load(f)
        ind = data.get("industry", {})
        console.print(f"[green]Updated .fluid/skills.yaml[/green]")
        console.print(
            f"[dim]Tools section refreshed to FLUID CLI v{data.get('_version', '?')}[/dim]"
        )
        if ind.get("label"):
            console.print(f"[dim]Industry section preserved: {ind['label']}[/dim]")
    except ImportError:
        cprint("Updated .fluid/skills.yaml")

    return 0


def _show(logger: logging.Logger) -> int:
    """Display the current skills file."""
    ws_root = find_workspace_root()
    if ws_root is None:
        console_error("Not inside a FLUID workspace.")
        return 1

    skills_path = ws_root / ".fluid" / "skills.yaml"
    if not skills_path.exists():
        console_error("No .fluid/skills.yaml found. Run 'fluid init' to generate one.")
        return 1

    try:
        from rich.console import Console
        from rich.syntax import Syntax

        console = Console()
        content = skills_path.read_text()
        console.print(Syntax(content, "yaml", theme="monokai", line_numbers=True))
    except ImportError:
        print(skills_path.read_text())

    return 0
