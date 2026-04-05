# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Central slash-command dispatcher for the forge REPL and in-interview UX.

Design goals:
    • A tiny registry (``@slash("name")``) so new commands are one-liners.
    • Pure delegation — handlers call into existing modules (doctor,
      forge_copilot_memory, capability matrix). No business logic lives here.
    • Works identically in the full REPL (worktree A) and the scoped
      interview REPL (worktree B).
    • Never raises: every handler returns a ``SlashResult`` so the caller
      can print the message and decide whether to keep looping.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

from .forge_session import ForgeSession

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

    RICH_AVAILABLE = True
except ImportError:  # pragma: no cover
    Console = None  # type: ignore[assignment]
    Panel = None  # type: ignore[assignment]
    Table = None  # type: ignore[assignment]
    RICH_AVAILABLE = False

LOG = logging.getLogger("fluid.cli.slash")


@dataclass
class SlashResult:
    message: str = ""
    continue_session: bool = True
    # Rich renderable; the caller prints this if present instead of ``message``.
    renderable: object = None


SlashHandler = Callable[["SlashContext"], SlashResult]


@dataclass
class SlashContext:
    session: ForgeSession
    args: List[str]
    console: Optional["Console"] = None
    scope: str = "repl"  # "repl" (full) or "interview" (scoped)


_REGISTRY: Dict[str, "SlashSpec"] = {}


@dataclass
class SlashSpec:
    name: str
    handler: SlashHandler
    help: str
    scope: str = "both"  # "repl", "interview", or "both"
    aliases: tuple = ()


def slash(name: str, help: str, scope: str = "both", aliases: tuple = ()) -> Callable:
    """Decorator to register a slash-command handler."""

    def _wrap(fn: SlashHandler) -> SlashHandler:
        spec = SlashSpec(name=name, handler=fn, help=help, scope=scope, aliases=aliases)
        _REGISTRY[name] = spec
        for alias in aliases:
            _REGISTRY[alias] = spec
        return fn

    return _wrap


def dispatch(line: str, session: ForgeSession, console=None, scope: str = "repl") -> SlashResult:
    """Parse ``/cmd arg1 arg2`` and dispatch.

    Returns a ``SlashResult`` telling the caller whether to keep looping.
    An unknown command yields a helpful suggestion, never an exception.
    """
    line = line.strip()
    if not line.startswith("/"):
        return SlashResult(message=f"not a slash command: {line}")
    parts = line[1:].split()
    if not parts:
        return SlashResult(message="empty slash command — try /help")
    name = parts[0].lower()
    args = parts[1:]
    spec = _REGISTRY.get(name)
    if spec is None:
        suggest = _suggest(name)
        hint = f" Did you mean {suggest}?" if suggest else ""
        return SlashResult(message=f"unknown command: /{name}.{hint} Type /help.")
    if spec.scope != "both" and spec.scope != scope:
        return SlashResult(
            message=f"/{name} is not available in {scope} mode (only {spec.scope})."
        )
    ctx = SlashContext(session=session, args=args, console=console, scope=scope)
    try:
        return spec.handler(ctx)
    except Exception as exc:  # noqa: BLE001
        LOG.exception("slash handler %s failed", name)
        return SlashResult(message=f"/{name} failed: {exc}")


def _suggest(name: str) -> Optional[str]:
    """Crude Levenshtein-free suggester."""
    best: Optional[str] = None
    best_score = 0
    for key in _REGISTRY:
        if key == name:
            continue
        score = sum(1 for a, b in zip(key, name) if a == b)
        if score > best_score:
            best = key
            best_score = score
    return f"/{best}" if best and best_score >= 2 else None


def list_commands(scope: str = "repl") -> List[SlashSpec]:
    seen = set()
    out: List[SlashSpec] = []
    for key, spec in _REGISTRY.items():
        if key != spec.name:  # skip alias entries
            continue
        if spec.scope != "both" and spec.scope != scope:
            continue
        if spec.name in seen:
            continue
        seen.add(spec.name)
        out.append(spec)
    return sorted(out, key=lambda s: s.name)


# ---------------------------------------------------------------------------
# Built-in handlers
# ---------------------------------------------------------------------------


@slash("help", "Show available slash commands", aliases=("?",))
def _cmd_help(ctx: SlashContext) -> SlashResult:
    cmds = list_commands(scope=ctx.scope)
    if RICH_AVAILABLE and ctx.console is not None:
        table = Table(title="Slash Commands", show_header=True, header_style="bold cyan")
        table.add_column("Command", style="magenta", no_wrap=True)
        table.add_column("Description", style="white")
        for spec in cmds:
            table.add_row(f"/{spec.name}", spec.help)
        return SlashResult(renderable=table)
    lines = ["Available slash commands:"]
    for spec in cmds:
        lines.append(f"  /{spec.name:<12} {spec.help}")
    return SlashResult(message="\n".join(lines))


@slash("exit", "Exit the forge REPL", aliases=("quit", "q"))
def _cmd_exit(ctx: SlashContext) -> SlashResult:
    return SlashResult(message="bye 👋", continue_session=False)


@slash("status", "Show current session status line")
def _cmd_status(ctx: SlashContext) -> SlashResult:
    from .status_line import render_status_line

    render_status_line(ctx.session, ctx.console)
    return SlashResult()


@slash("clear", "Clear the screen")
def _cmd_clear(ctx: SlashContext) -> SlashResult:
    if RICH_AVAILABLE and ctx.console is not None:
        ctx.console.clear()
    else:
        print("\033[2J\033[H", end="")
    return SlashResult()


@slash("history", "Show recent REPL history", scope="repl")
def _cmd_history(ctx: SlashContext) -> SlashResult:
    if not ctx.session.history:
        return SlashResult(message="(no history yet)")
    lines = [f"  {i+1:>3}. {entry}" for i, entry in enumerate(ctx.session.history[-20:])]
    return SlashResult(message="Recent commands:\n" + "\n".join(lines))


@slash("doctor", "Run quick health checks")
def _cmd_doctor(ctx: SlashContext) -> SlashResult:
    try:
        from .forge_copilot_llm_providers import check_llm_readiness

        readiness = check_llm_readiness()
        ok = getattr(readiness, "ready", False)
        detail = getattr(readiness, "detail", "") or getattr(readiness, "message", "")
        status = "✓ ready" if ok else "✗ not ready"
        msg = f"Copilot LLM: {status}"
        if detail:
            msg += f"\n  {detail}"
        return SlashResult(message=msg)
    except Exception as exc:  # noqa: BLE001
        return SlashResult(message=f"doctor failed: {exc}")


@slash("memory", "Show project-scoped copilot memory summary")
def _cmd_memory(ctx: SlashContext) -> SlashResult:
    snap = ctx.session.refresh_memory()
    if not snap:
        return SlashResult(
            message="(no copilot memory yet — run a copilot session with --save-memory)"
        )
    # snap may be a dict or a textual summary depending on the helper version.
    if isinstance(snap, dict):
        lines = ["Copilot memory (project-scoped):"]
        for key, value in list(snap.items())[:20]:
            lines.append(f"  {key}: {value}")
        return SlashResult(message="\n".join(lines))
    return SlashResult(message=f"Copilot memory:\n{snap}")


@slash("providers", "List supported infrastructure providers")
def _cmd_providers(ctx: SlashContext) -> SlashResult:
    providers = ["local", "gcp", "snowflake", "odps", "opds", "aws", "azure"]
    if RICH_AVAILABLE and ctx.console is not None:
        table = Table(title="Providers", header_style="bold cyan")
        table.add_column("Name", style="magenta")
        table.add_column("Active", style="green")
        for name in providers:
            active = "●" if ctx.session.provider == name else ""
            table.add_row(name, active)
        return SlashResult(renderable=table)
    active = ctx.session.provider or "(none)"
    return SlashResult(
        message="Providers: " + ", ".join(providers) + f"\nActive: {active}"
    )


@slash("templates", "List available forge templates")
def _cmd_templates(ctx: SlashContext) -> SlashResult:
    try:
        from ..forge.core.registry import template_registry

        names = template_registry.list_available()
    except Exception as exc:  # noqa: BLE001
        return SlashResult(message=f"template registry unavailable: {exc}")
    if not names:
        return SlashResult(message="(no templates registered)")
    if RICH_AVAILABLE and ctx.console is not None:
        table = Table(title="Templates", header_style="bold cyan")
        table.add_column("Name", style="magenta")
        table.add_column("Description", style="white")
        for name in names:
            tpl = template_registry.get(name)
            desc = ""
            if tpl:
                try:
                    desc = tpl.get_metadata().description or ""
                except Exception:  # noqa: BLE001
                    desc = ""
            table.add_row(name, desc)
        return SlashResult(renderable=table)
    return SlashResult(message="Templates: " + ", ".join(names))


@slash("mode", "Show or set the default forge mode (copilot|template|agent|blueprint)")
def _cmd_mode(ctx: SlashContext) -> SlashResult:
    if not ctx.args:
        return SlashResult(message=f"current mode: {ctx.session.mode}")
    new_mode = ctx.args[0].lower()
    if new_mode not in {"copilot", "template", "agent", "blueprint"}:
        return SlashResult(message=f"unknown mode: {new_mode}")
    ctx.session.switch_mode(new_mode)
    return SlashResult(message=f"mode switched to {new_mode}")


@slash("config", "Set a session config key, e.g. /config provider gcp")
def _cmd_config(ctx: SlashContext) -> SlashResult:
    if len(ctx.args) < 2:
        return SlashResult(
            message="usage: /config <key> <value>  (keys: provider, mode, memory)"
        )
    key, value = ctx.args[0], ctx.args[1]
    if key == "provider":
        ctx.session.switch_provider(value)
        return SlashResult(message=f"provider → {value}")
    if key == "mode":
        ctx.session.switch_mode(value)
        return SlashResult(message=f"mode → {value}")
    if key == "memory":
        ctx.session.memory_on = value.lower() in {"on", "true", "1", "yes"}
        ctx.session.save()
        return SlashResult(
            message=f"memory → {'on' if ctx.session.memory_on else 'off'}"
        )
    return SlashResult(message=f"unknown config key: {key}")


@slash("new", "Start a new copilot project (default action)")
def _cmd_new(ctx: SlashContext) -> SlashResult:
    return SlashResult(
        message="→ launching copilot interview... (hint: any bare text in the REPL does this)",
        continue_session=True,
    )


# ---- interview-scoped commands (used by worktree B; registered globally) ---


@slash(
    "show",
    "Show the current interview answers",
    scope="interview",
)
def _cmd_show(ctx: SlashContext) -> SlashResult:
    state = getattr(ctx.session, "interview_state", None)
    if not state:
        return SlashResult(message="(no active interview)")
    lines = ["Current answers:"]
    for i, (q, a) in enumerate(state.get("qa", []), 1):
        lines.append(f"  {i}. {q}\n     → {a}")
    return SlashResult(message="\n".join(lines))


@slash("edit", "Revise a previous interview answer: /edit <n>", scope="interview")
def _cmd_edit(ctx: SlashContext) -> SlashResult:
    if not ctx.args:
        return SlashResult(message="usage: /edit <question-number>")
    try:
        n = int(ctx.args[0])
    except ValueError:
        return SlashResult(message="question number must be an integer")
    state = getattr(ctx.session, "interview_state", None)
    if not state:
        return SlashResult(message="(no active interview)")
    state["edit_target"] = n
    return SlashResult(message=f"→ will revise answer {n} on the next prompt")


@slash("back", "Go back one question", scope="interview")
def _cmd_back(ctx: SlashContext) -> SlashResult:
    state = getattr(ctx.session, "interview_state", None)
    if not state:
        return SlashResult(message="(no active interview)")
    state["back"] = True
    return SlashResult(message="→ going back one question")


@slash("skip", "Skip the current question", scope="interview")
def _cmd_skip(ctx: SlashContext) -> SlashResult:
    state = getattr(ctx.session, "interview_state", None)
    if not state:
        return SlashResult(message="(no active interview)")
    state["skip"] = True
    return SlashResult(message="→ skipping this question")


@slash("retry", "Re-ask the current question", scope="interview")
def _cmd_retry(ctx: SlashContext) -> SlashResult:
    state = getattr(ctx.session, "interview_state", None)
    if not state:
        return SlashResult(message="(no active interview)")
    state["retry"] = True
    return SlashResult(message="→ re-asking the current question")


@slash("abort", "Abort the current interview", scope="interview")
def _cmd_abort(ctx: SlashContext) -> SlashResult:
    state = getattr(ctx.session, "interview_state", None)
    if state:
        state["abort"] = True
    return SlashResult(message="→ interview aborted", continue_session=False)


__all__ = [
    "SlashResult",
    "SlashContext",
    "slash",
    "dispatch",
    "list_commands",
]
