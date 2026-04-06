# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Persistent forge REPL — the primary interactive surface for forge.

Run ``fluid`` with no arguments:

    $ fluid

    ╭────────────────── forge ──────────────────╮
    │  Describe what you want to build, or      │
    │  type /help for commands.                  │
    ╰──────────────────────────────────────────╯
    gcp · copilot · memory:on · my-project

    forge ›

"""

from __future__ import annotations

import logging
import os
import random
import sys
import time
from pathlib import Path
from typing import List, Optional

from .forge_session import ForgeSession
from .onboarding import is_first_run, run as run_onboarding
from .slash_commands import dispatch, list_commands

try:
    from rich.console import Console
    from rich.columns import Columns
    from rich.panel import Panel
    from rich.rule import Rule
    from rich.text import Text

    RICH_AVAILABLE = True
except ImportError:  # pragma: no cover
    Console = None  # type: ignore[assignment]
    Panel = None  # type: ignore[assignment]
    Text = None  # type: ignore[assignment]
    RICH_AVAILABLE = False

try:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.completion import Completer, Completion
    from prompt_toolkit.history import FileHistory
    from prompt_toolkit.formatted_text import HTML
    from prompt_toolkit.styles import Style as PTKStyle

    PROMPT_TOOLKIT_AVAILABLE = True
except ImportError:  # pragma: no cover
    PromptSession = None  # type: ignore[assignment]
    Completer = object  # type: ignore[assignment]
    Completion = None  # type: ignore[assignment]
    FileHistory = None  # type: ignore[assignment]
    HTML = None  # type: ignore[assignment]
    PTKStyle = None  # type: ignore[assignment]
    PROMPT_TOOLKIT_AVAILABLE = False

LOG = logging.getLogger("fluid.cli.forge_repl")

# ── Colour palette ────────────────────────────────────────────────────────
# All visual tokens reference this palette so the look is consistent.
C_BRAND = "magenta"
C_ACCENT = "cyan"
C_OK = "green"
C_WARN = "yellow"
C_DIM = "dim"
C_ERR = "red"

# ── Contextual tips ───────────────────────────────────────────────────────
TIPS: List[str] = [
    "Describe what you want to build in plain English — the copilot will guide you.",
    "Type [bold {a}]/doctor[/bold {a}] to check your LLM and provider readiness.",
    "Type [bold {a}]/templates[/bold {a}] to browse available project templates.",
    "Use [bold {a}]/config provider snowflake[/bold {a}] to switch providers on the fly.",
    "Run [bold {a}]/memory[/bold {a}] to see what the copilot remembers about this project.",
    "Press [bold]Tab[/bold] for auto-completion of commands and template names.",
    "Type [bold {a}]/mode template[/bold {a}] to switch from AI copilot to template mode.",
    "During the copilot interview, type [bold {a}]/edit 2[/bold {a}] to revise answer 2.",
    "During the copilot interview, type [bold {a}]/back[/bold {a}] to step back one question.",
    "Use [bold {a}]/history[/bold {a}] to see your recent commands.",
]


# ── Completer ─────────────────────────────────────────────────────────────

if PROMPT_TOOLKIT_AVAILABLE:

    class _ForgeCompleter(Completer):  # type: ignore[misc]

        def __init__(self) -> None:
            self._slash: Optional[List[str]] = None
            self._templates: Optional[List[str]] = None

        def _slash_names(self) -> List[str]:
            if self._slash is None:
                self._slash = [f"/{s.name}" for s in list_commands(scope="repl")]
            return self._slash

        def _template_names(self) -> List[str]:
            if self._templates is None:
                try:
                    from ..forge.core.registry import template_registry
                    self._templates = list(template_registry.list_available())
                except Exception:  # noqa: BLE001
                    self._templates = []
            return self._templates

        def get_completions(self, document, complete_event):
            text = document.text_before_cursor.lstrip()
            word = document.get_word_before_cursor(WORD=True)

            if text.startswith("/"):
                # Sub-arg completion
                parts = text.split()
                if len(parts) >= 2 and parts[0] in ("/config", "/mode"):
                    pool = {
                        "/config": ["provider", "mode", "memory"],
                        "/mode": ["copilot", "template", "agent", "blueprint"],
                    }.get(parts[0], [])
                else:
                    pool = self._slash_names()
            else:
                pool = self._template_names() + [
                    "create", "validate", "plan", "apply", "doctor",
                ]

            for c in pool:
                if not word or c.lower().startswith(word.lower()):
                    yield Completion(c, start_position=-len(word))


# ── Public entry point ────────────────────────────────────────────────────


def run(argv=None) -> int:
    """Launch the forge REPL. Returns an exit code."""
    if not _is_tty():
        print("fluid: no command given (and not a TTY). Try: fluid --help")
        return 0

    console = Console(highlight=False) if RICH_AVAILABLE else None

    # First-run onboarding
    if is_first_run():
        session = run_onboarding()
    else:
        session = ForgeSession.load()

    _welcome(console, session)

    if PROMPT_TOOLKIT_AVAILABLE:
        return _loop_ptk(session, console)
    return _loop_plain(session, console)


# ── Welcome ───────────────────────────────────────────────────────────────


def _welcome(console: Optional["Console"], session: ForgeSession) -> None:
    if not console or not RICH_AVAILABLE:
        print("\n  forge — type /help for commands, or describe what you want to build.\n")
        _print_status_plain(session)
        return

    # Compact branded header
    try:
        from fluid_build import __version__ as ver
    except Exception:  # noqa: BLE001
        ver = "?"

    header = Text()
    header.append(" forge", style=f"bold {C_BRAND}")
    header.append(f"  v{ver}", style=C_DIM)
    console.print()
    console.print(
        Panel(
            "[bold]Describe what you want to build[/bold], or type "
            f"[bold {C_ACCENT}]/help[/bold {C_ACCENT}] for commands.",
            title=header,
            title_align="left",
            border_style=C_ACCENT,
            padding=(0, 1),
        )
    )
    _print_status_rich(console, session)
    console.print()


def _print_status_rich(console: "Console", s: ForgeSession) -> None:
    parts = Text()
    provider = s.provider or "not set"
    parts.append(f"  {provider}", style=f"bold {C_BRAND}")
    parts.append(f"  ·  {s.mode}", style=C_ACCENT)
    mem = "on" if s.memory_on else "off"
    mem_style = C_OK if s.memory_on else C_DIM
    parts.append(f"  ·  memory:{mem}", style=mem_style)
    parts.append(f"  ·  {s.project_root.name}", style=C_DIM)
    console.print(parts)


def _print_status_plain(s: ForgeSession) -> None:
    provider = s.provider or "not set"
    mem = "on" if s.memory_on else "off"
    print(f"  {provider}  ·  {s.mode}  ·  memory:{mem}  ·  {s.project_root.name}")


# ── Tips ──────────────────────────────────────────────────────────────────

_tip_index = 0


def _maybe_tip(console: Optional["Console"], every: int = 4, command_count: int = 0) -> None:
    """Show a contextual tip every N commands."""
    global _tip_index
    if command_count > 0 and command_count % every == 0:
        tip = TIPS[_tip_index % len(TIPS)].format(a=C_ACCENT)
        _tip_index += 1
        if console and RICH_AVAILABLE:
            console.print(f"\n  [{C_DIM}]tip:[/{C_DIM}] {tip}")
        else:
            # Strip markup for plain text
            import re
            clean = re.sub(r"\[.*?\]", "", tip)
            print(f"\n  tip: {clean}")


# ── prompt_toolkit loop ──────────────────────────────────────────────────


def _loop_ptk(session: ForgeSession, console: Optional["Console"]) -> int:
    session.config_dir.mkdir(parents=True, exist_ok=True)
    history_path = session.config_dir / "repl_history"

    style = PTKStyle.from_dict({
        "bottom-toolbar": f"bg:ansibrightblack fg:ansiwhite",
        "prompt": f"bold fg:ansimagenta",
    }) if PTKStyle else None

    ptk = PromptSession(
        history=FileHistory(str(history_path)) if FileHistory else None,
        completer=_ForgeCompleter(),
        style=style,
    )

    cmd_count = 0
    while True:
        toolbar = _toolbar_text(session)
        try:
            raw = ptk.prompt(
                HTML("<prompt>forge</prompt> <b>›</b> "),
                bottom_toolbar=HTML(toolbar),
            )
        except KeyboardInterrupt:
            if console:
                console.print(f"  [{C_WARN}]Ctrl-C — type /exit to quit[/{C_WARN}]")
            continue
        except EOFError:
            break
        if not raw.strip():
            continue
        session.record(raw.strip())
        cmd_count += 1
        cont = _dispatch(raw, session, console)
        _maybe_tip(console, every=3, command_count=cmd_count)
        if cont is False:
            break

    session.save()
    _goodbye(console)
    return 0


def _toolbar_text(session: ForgeSession) -> str:
    provider = session.provider or "—"
    mem = "on" if session.memory_on else "off"
    return (
        f"  <b>{provider}</b> · {session.mode} · memory:{mem} · "
        f"{session.project_root.name}   "
        f"<i>/help</i> commands  <i>/exit</i> quit"
    )


# ── Plain fallback loop ─────────────────────────────────────────────────


def _loop_plain(session: ForgeSession, console: Optional["Console"]) -> int:
    if console:
        console.print(f"  [{C_DIM}]install prompt_toolkit for tab completion and history[/{C_DIM}]")
    cmd_count = 0
    while True:
        try:
            raw = input("forge › ")
        except (KeyboardInterrupt, EOFError):
            print()
            break
        if not raw.strip():
            continue
        session.record(raw.strip())
        cmd_count += 1
        cont = _dispatch(raw, session, console)
        _maybe_tip(console, every=3, command_count=cmd_count)
        if cont is False:
            break
    session.save()
    _goodbye(console)
    return 0


# ── Dispatch ─────────────────────────────────────────────────────────────


def _dispatch(raw: str, session: ForgeSession, console: Optional["Console"]) -> bool:
    """Route input. Return False to exit the REPL."""
    line = raw.strip()

    # Slash commands
    if line.startswith("/"):
        result = dispatch(line, session, console=console, scope="repl")
        _render_result(result, console)
        return result.continue_session

    # Bare word shortcuts (case-insensitive first token)
    first = line.split()[0].lower() if line else ""
    if first in {"help", "?"}:
        return _dispatch("/help", session, console)
    if first in {"exit", "quit", "q"}:
        return False
    if first == "doctor":
        return _dispatch("/doctor", session, console)
    if first == "status":
        return _dispatch("/status", session, console)
    if first == "clear":
        return _dispatch("/clear", session, console)
    if first == "memory":
        return _dispatch("/memory", session, console)
    if first == "templates":
        return _dispatch("/templates", session, console)
    if first == "providers":
        return _dispatch("/providers", session, console)

    # Default: launch copilot interview with this text as the project goal
    _run_copilot(line, session, console)
    return True


def _render_result(result, console) -> None:
    if result.renderable is not None and console is not None:
        console.print(result.renderable)
    elif result.message:
        if console is not None:
            console.print(result.message)
        else:
            print(result.message)


# ── Default action: copilot interview ────────────────────────────────────


def _run_copilot(goal: str, session: ForgeSession, console: Optional["Console"]) -> None:
    """Route bare text into a copilot session.

    Delegates to ``forge.run()`` with a synthesized args namespace so all
    existing business logic (discovery, interview, generation, validation)
    runs unchanged.
    """
    if console and RICH_AVAILABLE:
        console.print()
        console.print(Rule(style=C_DIM))
        console.print(
            f"  [{C_DIM}]copilot[/{C_DIM}]  [bold]{goal}[/bold]"
        )
        console.print(
            f"  [{C_DIM}]Type ? for help on any question, /back to go back, "
            f"/edit <n> to revise an answer.[/{C_DIM}]"
        )
        console.print(Rule(style=C_DIM))
        console.print()
    else:
        print(f"\n  copilot: {goal}\n")

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
            project_goal=goal,
            _enable_copilot_recovery=True,
        )
        forge_cmd.run(args, LOG)
    except KeyboardInterrupt:
        if console:
            console.print(f"\n  [{C_WARN}]interview cancelled[/{C_WARN}]")
        else:
            print("\n  interview cancelled")
    except Exception as exc:  # noqa: BLE001
        LOG.debug("copilot launch failed", exc_info=True)
        if console and RICH_AVAILABLE:
            console.print(f"\n  [{C_ERR}]copilot error: {exc}[/{C_ERR}]")
            console.print(
                f"  [{C_DIM}]Run /doctor to check LLM readiness. "
                f"Make sure ANTHROPIC_API_KEY or OPENAI_API_KEY is set.[/{C_DIM}]"
            )
        else:
            print(f"\n  copilot error: {exc}")
            print("  Run /doctor to check readiness.")


# ── Utilities ─────────────────────────────────────────────────────────────


def _goodbye(console: Optional["Console"]) -> None:
    if console and RICH_AVAILABLE:
        console.print(f"\n  [{C_ACCENT}]see you next time[/{C_ACCENT}]\n")
    else:
        print("\n  see you next time\n")


def _is_tty() -> bool:
    try:
        return sys.stdin.isatty() and sys.stdout.isatty()
    except Exception:  # noqa: BLE001
        return False


__all__ = ["run"]
