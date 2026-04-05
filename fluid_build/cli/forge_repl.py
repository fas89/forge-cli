# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Persistent forge REPL (worktree A prototype).

Launched when the user runs ``fluid`` with no subcommand on a TTY:

    $ fluid
    forge › ▊

Features
--------
    • Slash commands (/help, /doctor, /memory, /providers, /templates, …)
    • Tab completion via prompt_toolkit (optional dep — falls back cleanly)
    • Persistent history in ``~/.fluid/repl_history``
    • Status line at the bottom showing provider / mode / memory / project
    • Inline help: type ``?`` at any prompt
    • Bare text ``→`` default action (launch copilot interview)
    • Existing ``fluid <cmd>`` one-shot paths are untouched
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional

from .forge_session import ForgeSession
from .onboarding import is_first_run, run as run_onboarding
from .slash_commands import dispatch, list_commands
from .status_line import format_status_text, render_status_line

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.text import Text

    RICH_AVAILABLE = True
except ImportError:  # pragma: no cover
    Console = None  # type: ignore[assignment]
    Panel = None  # type: ignore[assignment]
    Text = None  # type: ignore[assignment]
    RICH_AVAILABLE = False

try:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.completion import WordCompleter, Completer, Completion
    from prompt_toolkit.history import FileHistory
    from prompt_toolkit.formatted_text import FormattedText

    PROMPT_TOOLKIT_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dep
    PromptSession = None  # type: ignore[assignment]
    WordCompleter = None  # type: ignore[assignment]
    Completer = object  # type: ignore[assignment]
    Completion = None  # type: ignore[assignment]
    FileHistory = None  # type: ignore[assignment]
    FormattedText = None  # type: ignore[assignment]
    PROMPT_TOOLKIT_AVAILABLE = False

LOG = logging.getLogger("fluid.cli.forge_repl")

WELCOME_TAGLINE = (
    "Forge — Declarative data products with AI assistance.\n"
    "Type [bold magenta]/help[/bold magenta] for commands, "
    "[bold magenta]/exit[/bold magenta] to quit, "
    "or just describe what you want to build."
)


# ---------------------------------------------------------------------------
# Completer
# ---------------------------------------------------------------------------


if PROMPT_TOOLKIT_AVAILABLE:

    class ForgeCompleter(Completer):  # type: ignore[misc]
        """Context-aware completer for slash commands, templates, providers."""

        def __init__(self):
            self._static_providers = [
                "local",
                "gcp",
                "snowflake",
                "aws",
                "azure",
                "odps",
                "opds",
            ]
            self._slash_cache = None
            self._template_cache = None

        def _slash_names(self):
            if self._slash_cache is None:
                self._slash_cache = [f"/{s.name}" for s in list_commands(scope="repl")]
            return self._slash_cache

        def _templates(self):
            if self._template_cache is None:
                try:
                    from ..forge.core.registry import template_registry

                    self._template_cache = list(template_registry.list_available())
                except Exception:  # noqa: BLE001
                    self._template_cache = []
            return self._template_cache

        def get_completions(self, document, complete_event):  # noqa: D401
            text = document.text_before_cursor
            word = document.get_word_before_cursor(WORD=True)
            candidates: list = []

            if text.lstrip().startswith("/"):
                candidates = self._slash_names()
                # Also offer /config keys
                if text.strip().startswith("/config"):
                    candidates = ["provider", "mode", "memory"]
                if text.strip().startswith("/mode"):
                    candidates = ["copilot", "template", "agent", "blueprint"]
            else:
                # Top-level words: bare subcommand names + providers
                candidates = [
                    "create",
                    "validate",
                    "plan",
                    "apply",
                    "doctor",
                    "help",
                    "exit",
                ] + self._static_providers + self._templates()

            for c in candidates:
                if not word or c.startswith(word):
                    yield Completion(c, start_position=-len(word))


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run(argv=None) -> int:
    """Launch the forge REPL. Returns an exit code."""
    if not _is_tty():
        # Non-TTY invocation: defer to argparse help like the legacy path.
        print("fluid: no command given (and not a TTY). Try: fluid --help")
        return 0

    console = Console() if RICH_AVAILABLE else None

    # First-run onboarding — chain directly into the REPL afterwards.
    if is_first_run():
        run_onboarding()

    session = ForgeSession.load()
    _banner(console, session)

    if PROMPT_TOOLKIT_AVAILABLE:
        return _repl_ptk(session, console)
    return _repl_plain(session, console)


# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------


def _banner(console, session: ForgeSession) -> None:
    if RICH_AVAILABLE and console is not None:
        body = Text.from_markup(WELCOME_TAGLINE)
        console.print(Panel(body, title="🔨 forge", border_style="cyan"))
    else:
        print("\n=== 🔨 forge ===")
        print("Type /help for commands, /exit to quit.\n")
    render_status_line(session, console)


def _is_tty() -> bool:
    try:
        return sys.stdin.isatty() and sys.stdout.isatty()
    except Exception:  # noqa: BLE001
        return False


# ---------------------------------------------------------------------------
# prompt_toolkit loop
# ---------------------------------------------------------------------------


def _repl_ptk(session: ForgeSession, console) -> int:
    history_path = session.config_dir / "repl_history"
    session.config_dir.mkdir(parents=True, exist_ok=True)
    ptk_session = PromptSession(
        history=FileHistory(str(history_path)) if FileHistory else None,
        completer=ForgeCompleter(),
    )

    def bottom_toolbar():
        return FormattedText([("class:bottom-toolbar", format_status_text(session))])

    while True:
        try:
            raw = ptk_session.prompt(
                "forge › ",
                bottom_toolbar=bottom_toolbar,
            )
        except KeyboardInterrupt:
            if console:
                console.print("[yellow](Ctrl-C — type /exit to quit)[/yellow]")
            else:
                print("(Ctrl-C — type /exit to quit)")
            continue
        except EOFError:
            break
        if not raw.strip():
            continue
        session.record(raw.strip())
        if _handle_line(raw, session, console) is False:
            break
    session.save()
    if console:
        console.print("[cyan]bye 👋[/cyan]")
    else:
        print("bye")
    return 0


# ---------------------------------------------------------------------------
# Plain fallback loop
# ---------------------------------------------------------------------------


def _repl_plain(session: ForgeSession, console) -> int:
    if console:
        console.print(
            "[dim](prompt_toolkit not installed — tab completion and history disabled)[/dim]"
        )
    else:
        print("(install `prompt_toolkit` for tab completion and history)")
    while True:
        try:
            raw = input("forge › ")
        except (KeyboardInterrupt, EOFError):
            print()
            break
        if not raw.strip():
            continue
        session.record(raw.strip())
        if _handle_line(raw, session, console) is False:
            break
    session.save()
    print("bye")
    return 0


# ---------------------------------------------------------------------------
# Line dispatch
# ---------------------------------------------------------------------------


def _handle_line(raw: str, session: ForgeSession, console) -> bool:
    """Return False if the REPL should exit."""
    line = raw.strip()
    if line.startswith("/"):
        result = dispatch(line, session, console=console, scope="repl")
        if result.renderable is not None and console is not None:
            console.print(result.renderable)
        elif result.message:
            if console is not None:
                console.print(result.message)
            else:
                print(result.message)
        return result.continue_session

    # Bare subcommand shortcuts
    bare = line.split()[0].lower() if line else ""
    if bare in {"help", "?"}:
        return _handle_line("/help", session, console)
    if bare in {"exit", "quit"}:
        return False
    if bare == "doctor":
        return _handle_line("/doctor", session, console)
    if bare == "status":
        return _handle_line("/status", session, console)

    # Default action: launch a copilot interview (or stub if LLM not ready)
    _default_action(line, session, console)
    return True


def _default_action(line: str, session: ForgeSession, console) -> None:
    """Route bare text into the copilot interview flow.

    This delegates to the existing ``forge`` command via ``run_ai_copilot_mode``
    with a synthesized args object, keeping business logic out of the REPL.
    """
    if console and RICH_AVAILABLE:
        console.print(
            f"[dim]→ starting copilot interview with goal:[/dim] [bold]{line}[/bold]"
        )
    else:
        print(f"→ starting copilot interview with goal: {line}")

    try:
        from types import SimpleNamespace

        from . import forge as forge_cmd

        args = SimpleNamespace(
            mode="copilot",
            provider=session.provider,
            help=False,
            quickstart=False,
            interactive=True,
            non_interactive=False,
            dry_run=False,
            context=None,
            llm_provider=None,
            llm_model=None,
            llm_endpoint=None,
            discover=True,
            discovery_path=None,
            memory=session.memory_on,
            save_memory=False,
            show_memory=False,
            reset_memory=False,
            domain=None,
            template=None,
            blueprint=None,
            target_dir=None,
            agent=None,
            project_goal=line,
            _enable_copilot_recovery=True,
        )
        forge_cmd.run(args, LOG)
    except Exception as exc:  # noqa: BLE001
        LOG.debug("default_action failed", exc_info=True)
        if console and RICH_AVAILABLE:
            console.print(f"[yellow]⚠ copilot launch failed: {exc}[/yellow]")
            console.print(
                "[dim]Tip: run /doctor to check LLM readiness, "
                "or set ANTHROPIC_API_KEY / OPENAI_API_KEY.[/dim]"
            )
        else:
            print(f"⚠ copilot launch failed: {exc}")


__all__ = ["run"]
