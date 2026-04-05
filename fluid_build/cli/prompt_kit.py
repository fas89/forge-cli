# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Session-aware prompt helpers.

Thin facade over ``forge_dialogs`` that adds three UX affordances:

    • ``?`` at any prompt prints an inline help hint.
    • ``/<cmd>`` at any prompt routes to the slash-command dispatcher.
    • A ``ForgeSession`` reference is threaded through so slash commands
      run against the current session state.

Existing callers of ``forge_dialogs.ask_dialog_question`` continue to work
unchanged — this module is additive.
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional

from .forge_session import ForgeSession
from .slash_commands import dispatch

try:
    from rich.console import Console

    RICH_AVAILABLE = True
except ImportError:  # pragma: no cover
    Console = None  # type: ignore[assignment]
    RICH_AVAILABLE = False

LOG = logging.getLogger("fluid.cli.prompt_kit")


class _Sentinel:
    pass


SLASH_HANDLED = _Sentinel()  # returned when slash command handled the input


def _print_help_hint(help_hint: Optional[str], console: Optional["Console"]) -> None:
    if not help_hint:
        msg = "(no extra help for this question)"
    else:
        msg = help_hint
    if RICH_AVAILABLE and console is not None:
        console.print(f"[dim]? {msg}[/dim]")
    else:
        print(f"? {msg}")


def _handle_meta_input(
    raw: str,
    session: ForgeSession,
    help_hint: Optional[str],
    console: Optional["Console"],
    scope: str,
) -> Optional[Any]:
    """Returns SLASH_HANDLED if the input was a meta-command, else None.

    The caller should loop and re-prompt when this returns SLASH_HANDLED.
    """
    raw = raw.strip()
    if raw == "?":
        _print_help_hint(help_hint, console)
        return SLASH_HANDLED
    if raw.startswith("/"):
        result = dispatch(raw, session, console=console, scope=scope)
        if result.renderable is not None and console is not None:
            console.print(result.renderable)
        elif result.message:
            if console is not None:
                console.print(result.message)
            else:
                print(result.message)
        if not result.continue_session:
            # Caller decides what to do; we propagate via a special marker.
            raise KeyboardInterrupt("slash command requested exit")
        return SLASH_HANDLED
    return None


def ask_text(
    session: ForgeSession,
    question: str,
    default: Optional[str] = None,
    help_hint: Optional[str] = None,
    console: Optional["Console"] = None,
    scope: str = "repl",
) -> str:
    """Ask a free-text question with ? and / meta-command support."""
    while True:
        prompt_suffix = f" [{default}]" if default else ""
        prompt = f"{question}{prompt_suffix} "
        if RICH_AVAILABLE and console is not None:
            console.print(f"[bold]{question}[/bold]", end=" ")
            if default:
                console.print(f"[dim][{default}][/dim]", end=" ")
            try:
                raw = input("› ")
            except EOFError:
                raw = ""
        else:
            try:
                raw = input(prompt)
            except EOFError:
                raw = ""
        handled = _handle_meta_input(raw, session, help_hint, console, scope)
        if handled is SLASH_HANDLED:
            continue
        return raw.strip() or (default or "")


def ask_choice(
    session: ForgeSession,
    question: str,
    options: List[str],
    default: Optional[str] = None,
    help_hint: Optional[str] = None,
    console: Optional["Console"] = None,
    scope: str = "repl",
) -> str:
    """Ask a single-choice question with flexible matching."""
    while True:
        if RICH_AVAILABLE and console is not None:
            console.print(f"[bold]{question}[/bold]")
            for i, opt in enumerate(options, 1):
                marker = "●" if opt == default else " "
                console.print(f"  [magenta]{i}[/magenta]. {marker} {opt}")
            try:
                raw = input("› ")
            except EOFError:
                raw = ""
        else:
            print(question)
            for i, opt in enumerate(options, 1):
                marker = "*" if opt == default else " "
                print(f"  {i}. {marker} {opt}")
            try:
                raw = input("> ")
            except EOFError:
                raw = ""
        handled = _handle_meta_input(raw, session, help_hint, console, scope)
        if handled is SLASH_HANDLED:
            continue
        raw = raw.strip()
        if not raw:
            return default or options[0]
        # Numeric selection
        if raw.isdigit():
            idx = int(raw) - 1
            if 0 <= idx < len(options):
                return options[idx]
        # Exact/prefix match
        for opt in options:
            if opt.lower() == raw.lower() or opt.lower().startswith(raw.lower()):
                return opt
        if console is not None:
            console.print(f"[yellow]no match — try again, or type ? for help[/yellow]")
        else:
            print("no match — try again, or type ? for help")


def ask_confirm(
    session: ForgeSession,
    question: str,
    default: bool = False,
    console: Optional["Console"] = None,
    scope: str = "repl",
) -> bool:
    suffix = " [Y/n]" if default else " [y/N]"
    while True:
        try:
            raw = input(f"{question}{suffix} ").strip().lower()
        except EOFError:
            raw = ""
        handled = _handle_meta_input(raw, session, None, console, scope)
        if handled is SLASH_HANDLED:
            continue
        if not raw:
            return default
        if raw in {"y", "yes"}:
            return True
        if raw in {"n", "no"}:
            return False


__all__ = ["ask_text", "ask_choice", "ask_confirm"]
