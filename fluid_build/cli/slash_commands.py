# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Slash-command registry and dispatcher for the forge REPL.

Every handler is a thin delegate to an existing module — no business
logic lives here.  Register a new command in three lines:

    @slash("foo", "Do a thing", category="session")
    def _cmd_foo(ctx: SlashContext) -> SlashResult:
        return SlashResult(message="done")
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from difflib import get_close_matches
from typing import Callable, Dict, List, Optional

from .forge_session import ForgeSession

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    from rich.rule import Rule

    RICH_AVAILABLE = True
except ImportError:  # pragma: no cover
    Console = None  # type: ignore[assignment]
    Panel = None  # type: ignore[assignment]
    Table = None  # type: ignore[assignment]
    Text = None  # type: ignore[assignment]
    RICH_AVAILABLE = False

LOG = logging.getLogger("fluid.cli.slash")


# ── Data structures ──────────────────────────────────────────────────────


@dataclass
class SlashResult:
    message: str = ""
    continue_session: bool = True
    renderable: object = None  # Rich renderable; printed instead of ``message``.


SlashHandler = Callable[["SlashContext"], SlashResult]


@dataclass
class SlashContext:
    session: ForgeSession
    args: List[str]
    console: Optional["Console"] = None
    scope: str = "repl"  # "repl" or "interview"


_REGISTRY: Dict[str, "SlashSpec"] = {}


@dataclass
class SlashSpec:
    name: str
    handler: SlashHandler
    help: str
    scope: str = "both"      # "repl", "interview", or "both"
    aliases: tuple = ()
    category: str = "general"
    usage: str = ""           # e.g. "/config provider gcp"


def slash(
    name: str,
    help: str,
    scope: str = "both",
    aliases: tuple = (),
    category: str = "general",
    usage: str = "",
) -> Callable:
    def _wrap(fn: SlashHandler) -> SlashHandler:
        spec = SlashSpec(
            name=name, handler=fn, help=help,
            scope=scope, aliases=aliases,
            category=category, usage=usage,
        )
        _REGISTRY[name] = spec
        for alias in aliases:
            _REGISTRY[alias] = spec
        return fn
    return _wrap


# ── Dispatcher ───────────────────────────────────────────────────────────


def dispatch(line: str, session: ForgeSession, console=None, scope: str = "repl") -> SlashResult:
    line = line.strip()
    if not line.startswith("/"):
        return SlashResult(message=f"not a slash command: {line}")
    parts = line[1:].split()
    if not parts:
        return SlashResult(message="empty command — type /help")
    name = parts[0].lower()
    args = parts[1:]
    spec = _REGISTRY.get(name)
    if spec is None:
        suggestion = _suggest(name)
        hint = f"  Did you mean [bold cyan]{suggestion}[/bold cyan]?" if suggestion else ""
        return SlashResult(message=f"[yellow]unknown command:[/yellow] /{name}{hint}")
    if spec.scope != "both" and spec.scope != scope:
        return SlashResult(
            message=f"[yellow]/{name} is not available in {scope} mode[/yellow]"
        )
    ctx = SlashContext(session=session, args=args, console=console, scope=scope)
    try:
        return spec.handler(ctx)
    except Exception as exc:  # noqa: BLE001
        LOG.exception("slash handler %s failed", name)
        return SlashResult(message=f"[red]/{name} failed: {exc}[/red]")


def _suggest(name: str) -> Optional[str]:
    names = [k for k, v in _REGISTRY.items() if k == v.name]
    matches = get_close_matches(name, names, n=1, cutoff=0.5)
    return f"/{matches[0]}" if matches else None


def list_commands(scope: str = "repl") -> List[SlashSpec]:
    seen = set()
    out: List[SlashSpec] = []
    for key, spec in _REGISTRY.items():
        if key != spec.name:
            continue
        if spec.scope != "both" and spec.scope != scope:
            continue
        if spec.name in seen:
            continue
        seen.add(spec.name)
        out.append(spec)
    return sorted(out, key=lambda s: (s.category, s.name))


# ── Built-in handlers ────────────────────────────────────────────────────
# Categories: navigation, explore, session, interview


@slash("help", "List commands (or /help <cmd> for details)", aliases=("?",), category="navigation")
def _cmd_help(ctx: SlashContext) -> SlashResult:
    # Drill into a specific command?
    if ctx.args:
        target = ctx.args[0].lstrip("/")
        spec = _REGISTRY.get(target)
        if spec:
            msg = f"  [bold cyan]/{spec.name}[/bold cyan]  {spec.help}"
            if spec.usage:
                msg += f"\n  [dim]usage: {spec.usage}[/dim]"
            if spec.aliases:
                msg += f"\n  [dim]aliases: {', '.join('/' + a for a in spec.aliases)}[/dim]"
            return SlashResult(message=msg)
        return SlashResult(message=f"[yellow]no command named /{target}[/yellow]")

    cmds = list_commands(scope=ctx.scope)
    if not RICH_AVAILABLE or ctx.console is None:
        lines = ["Commands:"]
        for spec in cmds:
            lines.append(f"  /{spec.name:<12} {spec.help}")
        return SlashResult(message="\n".join(lines))

    # Categorised Rich table
    table = Table(
        show_header=False, box=None, padding=(0, 2),
        title="[bold]Commands[/bold]", title_style="",
    )
    table.add_column("", style="bold cyan", no_wrap=True, min_width=14)
    table.add_column("", style="white")

    prev_cat = None
    for spec in cmds:
        if spec.category != prev_cat:
            if prev_cat is not None:
                table.add_row("", "")  # visual gap
            prev_cat = spec.category
        aliases = f" [dim]({', '.join('/' + a for a in spec.aliases)})[/dim]" if spec.aliases else ""
        table.add_row(f"/{spec.name}", f"{spec.help}{aliases}")

    return SlashResult(renderable=table)


@slash("exit", "Leave forge", aliases=("quit", "q"), category="navigation")
def _cmd_exit(ctx: SlashContext) -> SlashResult:
    return SlashResult(continue_session=False)


@slash("clear", "Clear the screen", category="navigation")
def _cmd_clear(ctx: SlashContext) -> SlashResult:
    if RICH_AVAILABLE and ctx.console is not None:
        ctx.console.clear()
    else:
        print("\033[2J\033[H", end="")
    return SlashResult()


@slash("status", "Show current session state", category="session")
def _cmd_status(ctx: SlashContext) -> SlashResult:
    s = ctx.session
    if RICH_AVAILABLE and ctx.console is not None:
        parts = Text()
        provider = s.provider or "not set"
        parts.append(f"  {provider}", style="bold magenta")
        parts.append(f"  ·  {s.mode}", style="cyan")
        mem = "on" if s.memory_on else "off"
        parts.append(f"  ·  memory:{mem}", style="green" if s.memory_on else "dim")
        parts.append(f"  ·  {s.project_root.name}", style="dim")
        return SlashResult(renderable=parts)
    provider = s.provider or "not set"
    mem = "on" if s.memory_on else "off"
    return SlashResult(
        message=f"  {provider}  ·  {s.mode}  ·  memory:{mem}  ·  {s.project_root.name}"
    )


@slash(
    "config", "Set a config key",
    category="session",
    usage="/config provider gcp  ·  /config mode template  ·  /config memory off",
)
def _cmd_config(ctx: SlashContext) -> SlashResult:
    if len(ctx.args) < 2:
        return SlashResult(
            message=(
                "  [bold]usage:[/bold]  /config provider <name>\n"
                "          /config mode <copilot|template|agent|blueprint>\n"
                "          /config memory <on|off>"
            )
        )
    key, value = ctx.args[0].lower(), ctx.args[1]
    if key == "provider":
        ctx.session.switch_provider(value)
        return SlashResult(message=f"  provider → [bold magenta]{value}[/bold magenta]")
    if key == "mode":
        ctx.session.switch_mode(value)
        return SlashResult(message=f"  mode → [bold cyan]{value}[/bold cyan]")
    if key == "memory":
        ctx.session.memory_on = value.lower() in {"on", "true", "1", "yes"}
        ctx.session.save()
        state = "on" if ctx.session.memory_on else "off"
        return SlashResult(message=f"  memory → [bold]{state}[/bold]")
    return SlashResult(message=f"  [yellow]unknown key: {key}[/yellow]")


@slash("mode", "Show or set mode", category="session", usage="/mode copilot")
def _cmd_mode(ctx: SlashContext) -> SlashResult:
    if not ctx.args:
        return SlashResult(message=f"  mode: [bold cyan]{ctx.session.mode}[/bold cyan]")
    new = ctx.args[0].lower()
    valid = {"copilot", "template", "agent", "blueprint"}
    if new not in valid:
        return SlashResult(message=f"  [yellow]unknown mode: {new}[/yellow]  (valid: {', '.join(sorted(valid))})")
    ctx.session.switch_mode(new)
    return SlashResult(message=f"  mode → [bold cyan]{new}[/bold cyan]")


@slash("history", "Show recent commands", scope="repl", category="session")
def _cmd_history(ctx: SlashContext) -> SlashResult:
    if not ctx.session.history:
        return SlashResult(message="  [dim](no history yet)[/dim]")
    recent = ctx.session.history[-15:]
    lines = [f"  [dim]{i:>3}[/dim]  {entry}" for i, entry in enumerate(recent, len(ctx.session.history) - len(recent) + 1)]
    return SlashResult(message="\n".join(lines))


@slash("doctor", "Check LLM and provider readiness", category="explore")
def _cmd_doctor(ctx: SlashContext) -> SlashResult:
    try:
        from .forge_copilot_llm_providers import check_llm_readiness
        readiness = check_llm_readiness()
        ok = getattr(readiness, "ready", False)
        detail = getattr(readiness, "detail", "") or getattr(readiness, "message", "")
        icon = "[green]✓[/green]" if ok else "[red]✗[/red]"
        status = "ready" if ok else "not configured"
        msg = f"  {icon} copilot LLM: [bold]{status}[/bold]"
        if detail:
            msg += f"\n  [dim]{detail}[/dim]"
        if not ok:
            msg += (
                "\n\n  [dim]Set one of these env vars:[/dim]\n"
                "    ANTHROPIC_API_KEY, OPENAI_API_KEY, GEMINI_API_KEY"
            )
        return SlashResult(message=msg)
    except Exception as exc:  # noqa: BLE001
        return SlashResult(message=f"  [red]doctor check failed: {exc}[/red]")


@slash("memory", "Show project copilot memory", category="explore")
def _cmd_memory(ctx: SlashContext) -> SlashResult:
    snap = ctx.session.refresh_memory()
    if not snap:
        return SlashResult(
            message="  [dim](no copilot memory for this project)[/dim]\n"
                    "  [dim]Run a copilot session with --save-memory to create one.[/dim]"
        )
    if isinstance(snap, dict):
        lines = ["  [bold]Copilot memory[/bold]"]
        for key, value in list(snap.items())[:15]:
            lines.append(f"    [cyan]{key}:[/cyan] {value}")
        return SlashResult(message="\n".join(lines))
    return SlashResult(message=f"  [bold]Copilot memory[/bold]\n{snap}")


@slash("providers", "List infrastructure providers", category="explore")
def _cmd_providers(ctx: SlashContext) -> SlashResult:
    providers = [
        ("local", "Filesystem"),
        ("gcp", "Google Cloud"),
        ("snowflake", "Snowflake"),
        ("aws", "AWS"),
        ("azure", "Azure"),
        ("odps", "MaxCompute"),
    ]
    active = ctx.session.provider
    if RICH_AVAILABLE and ctx.console is not None:
        table = Table(show_header=False, box=None, padding=(0, 2))
        table.add_column("", min_width=12)
        table.add_column("")
        table.add_column("")
        for name, desc in providers:
            marker = "[green bold]●[/green bold]" if name == active else "[dim]○[/dim]"
            style = "bold" if name == active else ""
            table.add_row(f"  {marker}", f"[{style}]{name}[/{style}]", f"[dim]{desc}[/dim]")
        return SlashResult(renderable=table)
    lines = []
    for name, desc in providers:
        marker = "●" if name == active else "○"
        lines.append(f"  {marker} {name:<12} {desc}")
    return SlashResult(message="\n".join(lines))


@slash("templates", "List available templates", category="explore")
def _cmd_templates(ctx: SlashContext) -> SlashResult:
    try:
        from ..forge.core.registry import template_registry
        names = template_registry.list_available()
    except Exception as exc:  # noqa: BLE001
        return SlashResult(message=f"  [yellow]template registry unavailable: {exc}[/yellow]")
    if not names:
        return SlashResult(message="  [dim](no templates registered)[/dim]")
    if RICH_AVAILABLE and ctx.console is not None:
        table = Table(show_header=False, box=None, padding=(0, 2))
        table.add_column("", style="bold magenta", min_width=26)
        table.add_column("", style="dim")
        for name in names:
            desc = ""
            try:
                tpl = template_registry.get(name)
                if tpl:
                    desc = tpl.get_metadata().description or ""
            except Exception:  # noqa: BLE001
                pass
            table.add_row(f"  {name}", desc)
        return SlashResult(renderable=table)
    return SlashResult(message="  " + "\n  ".join(names))


@slash("new", "Start a new copilot project", category="navigation")
def _cmd_new(ctx: SlashContext) -> SlashResult:
    return SlashResult(
        message="  [dim]type your project goal as plain text to start the copilot[/dim]"
    )


# ── Interview-scoped commands ────────────────────────────────────────────


@slash("show", "Show interview answers so far", scope="interview", category="interview")
def _cmd_show(ctx: SlashContext) -> SlashResult:
    state = getattr(ctx.session, "interview_state", None)
    if not state:
        return SlashResult(message="  [dim](no active interview)[/dim]")
    lines = ["  [bold]Answers so far[/bold]"]
    for i, (q, a) in enumerate(state.get("qa", []), 1):
        lines.append(f"    [cyan]{i}.[/cyan] {q}")
        lines.append(f"       → [magenta]{a}[/magenta]")
    return SlashResult(message="\n".join(lines))


@slash("edit", "Revise answer N", scope="interview", category="interview", usage="/edit 2")
def _cmd_edit(ctx: SlashContext) -> SlashResult:
    if not ctx.args:
        return SlashResult(message="  [bold]usage:[/bold] /edit <question-number>")
    try:
        n = int(ctx.args[0])
    except ValueError:
        return SlashResult(message="  [yellow]question number must be an integer[/yellow]")
    state = getattr(ctx.session, "interview_state", None)
    if not state:
        return SlashResult(message="  [dim](no active interview)[/dim]")
    state["edit_target"] = n
    return SlashResult(message=f"  [cyan]→ will jump back to question {n}[/cyan]")


@slash("back", "Go back one question", scope="interview", category="interview")
def _cmd_back(ctx: SlashContext) -> SlashResult:
    state = getattr(ctx.session, "interview_state", None)
    if not state:
        return SlashResult(message="  [dim](no active interview)[/dim]")
    state["back"] = True
    return SlashResult(message="  [cyan]→ stepping back[/cyan]")


@slash("skip", "Skip the current question", scope="interview", category="interview")
def _cmd_skip(ctx: SlashContext) -> SlashResult:
    state = getattr(ctx.session, "interview_state", None)
    if not state:
        return SlashResult(message="  [dim](no active interview)[/dim]")
    state["skip"] = True
    return SlashResult(message="  [dim]→ skipped[/dim]")


@slash("retry", "Re-ask the current question", scope="interview", category="interview")
def _cmd_retry(ctx: SlashContext) -> SlashResult:
    state = getattr(ctx.session, "interview_state", None)
    if not state:
        return SlashResult(message="  [dim](no active interview)[/dim]")
    state["retry"] = True
    return SlashResult(message="  [cyan]→ re-asking[/cyan]")


@slash("abort", "Abandon the interview", scope="interview", category="interview")
def _cmd_abort(ctx: SlashContext) -> SlashResult:
    state = getattr(ctx.session, "interview_state", None)
    if state:
        state["abort"] = True
    return SlashResult(message="  [yellow]→ interview aborted[/yellow]", continue_session=False)


__all__ = ["SlashResult", "SlashContext", "slash", "dispatch", "list_commands"]
