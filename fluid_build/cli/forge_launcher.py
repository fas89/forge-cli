# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Smart launcher menu for forge (worktree B prototype).

Run ``fluid`` on a TTY with no arguments and you get a friendly single-
screen menu that:

    1. Shows the current session status line
    2. Lists 6 core actions with keyboard shortcuts
    3. Routes your choice into the existing one-shot command handlers
    4. On `create`, enters a **scoped** copilot REPL that offers
       /edit, /retry, /back, /skip, /show, /doctor, /memory, /help,
       /abort slash commands during the interview

Everything else (validate, apply, doctor, etc.) remains a pure one-shot
command. This is the "hybrid" model — persistent state only where it
helps most (the adaptive copilot interview).
"""

from __future__ import annotations

import logging
import sys
from typing import List, Optional, Tuple

from .forge_session import ForgeSession
from .onboarding import is_first_run, run as run_onboarding
from .status_line import render_status_line

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.prompt import Prompt
    from rich.table import Table
    from rich.text import Text

    RICH_AVAILABLE = True
except ImportError:  # pragma: no cover
    Console = None  # type: ignore[assignment]
    Panel = None  # type: ignore[assignment]
    Prompt = None  # type: ignore[assignment]
    Table = None  # type: ignore[assignment]
    Text = None  # type: ignore[assignment]
    RICH_AVAILABLE = False

LOG = logging.getLogger("fluid.cli.forge_launcher")


# (key, label, description, handler)
# Handlers are callables that accept (session, console) and return an int exit code.
MenuItem = Tuple[str, str, str, "callable"]


def run(argv=None) -> int:
    """Show the launcher menu. Returns a process exit code."""
    if not _is_tty():
        print("fluid: no command given (and not a TTY). Try: fluid --help")
        return 0

    console = Console() if RICH_AVAILABLE else None

    if is_first_run():
        run_onboarding()

    session = ForgeSession.load()

    while True:
        _render(console, session)
        choice = _prompt(console)
        if choice in {"q", "quit", "exit", "x"}:
            if console:
                console.print("[cyan]bye 👋[/cyan]")
            else:
                print("bye")
            session.save()
            return 0
        item = _match(choice)
        if item is None:
            if console:
                console.print(f"[yellow]unknown choice: {choice!r}[/yellow]")
            else:
                print(f"unknown choice: {choice!r}")
            continue
        key, label, _desc, handler = item
        try:
            rc = handler(session, console)
            if rc is None:
                rc = 0
        except KeyboardInterrupt:
            if console:
                console.print("\n[yellow]⚠ cancelled[/yellow]")
            else:
                print("\n⚠ cancelled")
            continue
        except Exception as exc:  # noqa: BLE001
            LOG.debug("launcher handler failed", exc_info=True)
            if console:
                console.print(f"[red]✗ {label} failed: {exc}[/red]")
            else:
                print(f"✗ {label} failed: {exc}")


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _render(console, session: ForgeSession) -> None:
    if RICH_AVAILABLE and console is not None:
        header = Text.from_markup(
            "🔨 [bold magenta]FLUID Forge[/bold magenta]   "
            "[dim]pick an action below or type its key[/dim]"
        )
        console.print()
        console.print(Panel(header, border_style="cyan"))
        render_status_line(session, console)
        console.print()

        table = Table(show_header=False, box=None, padding=(0, 2))
        table.add_column("Key", style="bold magenta", no_wrap=True)
        table.add_column("Action", style="bold")
        table.add_column("Description", style="dim")
        for key, label, desc, _ in _MENU:
            table.add_row(f"[{key}]", label, desc)
        table.add_row("[q]", "quit", "leave the launcher")
        console.print(table)
    else:
        print("\n=== 🔨 FLUID Forge — Launcher ===")
        render_status_line(session, console)
        for key, label, desc, _ in _MENU:
            print(f"  [{key}]  {label:<10} {desc}")
        print("  [q]  quit        leave the launcher")


def _prompt(console) -> str:
    if RICH_AVAILABLE and console is not None and Prompt is not None:
        return Prompt.ask("\n[bold cyan]forge ›[/bold cyan]").strip().lower()
    try:
        return input("\nforge › ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        return "q"


def _match(choice: str) -> Optional[MenuItem]:
    if not choice:
        return None
    for item in _MENU:
        key, label, *_ = item
        if choice == key or choice == label or choice == label[:1]:
            return item
    return None


def _is_tty() -> bool:
    try:
        return sys.stdin.isatty() and sys.stdout.isatty()
    except Exception:  # noqa: BLE001
        return False


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


def _handle_create(session: ForgeSession, console) -> int:
    """Enter the scoped copilot-interview REPL."""
    from .interview_repl import run_scoped_interview

    return run_scoped_interview(session, console)


def _handle_validate(session: ForgeSession, console) -> int:
    if console:
        console.print(
            "[dim]→ run[/dim] [bold]fluid validate <contract.yaml>[/bold] "
            "[dim]from your shell. The launcher keeps other commands as[/dim] "
            "[bold]one-shot[/bold][dim] on purpose — this is the hybrid design.[/dim]"
        )
    else:
        print("→ run: fluid validate <contract.yaml>")
    return 0


def _handle_doctor(session: ForgeSession, console) -> int:
    """Invoke the existing doctor command in-process."""
    try:
        from types import SimpleNamespace

        from .doctor import run as run_doctor

        args = SimpleNamespace(
            out_dir="runtime/diag",
            features_only=True,
            extended=False,
            verbose=False,
            provider=session.provider,
        )
        return run_doctor(args, LOG)
    except Exception as exc:  # noqa: BLE001
        if console:
            console.print(f"[red]doctor failed: {exc}[/red]")
        else:
            print(f"doctor failed: {exc}")
        return 1


def _handle_memory(session: ForgeSession, console) -> int:
    snap = session.refresh_memory()
    if not snap:
        if console:
            console.print(
                "[dim](no copilot memory yet — run `create` with save-memory enabled)[/dim]"
            )
        else:
            print("(no copilot memory yet)")
        return 0
    if console:
        console.print(f"[bold]Copilot memory:[/bold]\n{snap}")
    else:
        print(f"Copilot memory:\n{snap}")
    return 0


def _handle_help(session: ForgeSession, console) -> int:
    try:
        from .help_v2 import print_help_v2

        print_help_v2(None, None)
    except Exception as exc:  # noqa: BLE001
        if console:
            console.print(f"[red]help failed: {exc}[/red]")
        else:
            print(f"help failed: {exc}")
        return 1
    return 0


def _handle_settings(session: ForgeSession, console) -> int:
    """Quick settings menu: change provider, toggle memory."""
    if console and RICH_AVAILABLE:
        console.print("\n[bold]Current settings[/bold]")
        for k, v in session.to_status_dict().items():
            console.print(f"  {k}: [magenta]{v}[/magenta]")
        raw = Prompt.ask(
            "\nWhat to change?",
            choices=["provider", "mode", "memory", "cancel"],
            default="cancel",
        )
    else:
        print("\nCurrent settings:", session.to_status_dict())
        raw = input("What to change? (provider/mode/memory/cancel): ").strip() or "cancel"

    if raw == "cancel":
        return 0
    if raw == "provider":
        new = input("Provider (local/gcp/snowflake/aws/azure/odps): ").strip() or "local"
        session.switch_provider(new)
    elif raw == "mode":
        new = input("Mode (copilot/template/agent/blueprint): ").strip() or "copilot"
        session.switch_mode(new)
    elif raw == "memory":
        session.memory_on = not session.memory_on
        session.save()
    return 0


_MENU: List[MenuItem] = [
    ("1", "create", "Build a new data product with the AI copilot", _handle_create),
    ("2", "validate", "Validate a contract YAML", _handle_validate),
    ("3", "doctor", "Run health & readiness checks", _handle_doctor),
    ("4", "memory", "Show copilot memory for this project", _handle_memory),
    ("5", "settings", "Change provider / mode / memory", _handle_settings),
    ("6", "help", "Show core commands", _handle_help),
]


__all__ = ["run"]
