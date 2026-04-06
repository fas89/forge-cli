# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""First-run onboarding wizard for forge.

Triggered automatically when ``~/.fluid`` is missing. Chains existing
``doctor`` and ``auth`` flows into a single conversational experience.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import List, Optional, Tuple

from .forge_session import ForgeSession

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.prompt import Prompt
    from rich.rule import Rule
    from rich.text import Text
    from rich.table import Table

    RICH_AVAILABLE = True
except ImportError:  # pragma: no cover
    Console = None  # type: ignore[assignment]
    Panel = None  # type: ignore[assignment]
    Prompt = None  # type: ignore[assignment]
    Text = None  # type: ignore[assignment]
    Table = None  # type: ignore[assignment]
    RICH_AVAILABLE = False

LOG = logging.getLogger("fluid.cli.onboarding")

# ── Colour palette (matches forge_repl) ──────────────────────────────────
C_BRAND = "magenta"
C_ACCENT = "cyan"
C_OK = "green"
C_WARN = "yellow"
C_DIM = "dim"

PROVIDERS = [
    ("local", "Local filesystem — great for trying things out"),
    ("gcp", "Google Cloud — BigQuery, Dataplex, Dataproc"),
    ("snowflake", "Snowflake"),
    ("aws", "Amazon Web Services — Redshift, Glue, S3"),
    ("azure", "Microsoft Azure"),
    ("odps", "Alibaba MaxCompute / ODPS"),
]

LLM_PROVIDERS = [
    ("anthropic", "Claude (Anthropic)"),
    ("openai", "OpenAI GPT"),
    ("gemini", "Google Gemini"),
    ("ollama", "Local models via Ollama"),
]


def is_first_run(config_dir: Optional[Path] = None) -> bool:
    cfg = config_dir or (Path.home() / ".fluid")
    return not cfg.exists()


def run(session: Optional[ForgeSession] = None) -> ForgeSession:
    session = session or ForgeSession.load()
    console = Console(highlight=False) if RICH_AVAILABLE else None

    _welcome(console)
    _env_check(console)
    _pick_provider(console, session)
    _pick_llm(console, session)
    _save(console, session)
    _done(console)

    return session


# ── Steps ─────────────────────────────────────────────────────────────────


def _welcome(console: Optional["Console"]) -> None:
    if console and RICH_AVAILABLE:
        try:
            from fluid_build import __version__ as ver
        except Exception:  # noqa: BLE001
            ver = "?"

        title = Text()
        title.append(" forge", style=f"bold {C_BRAND}")
        title.append(f"  v{ver}", style=C_DIM)

        body = (
            "[bold]FLUID Forge[/bold] turns a single YAML contract into deployable "
            "data products — with governance, lineage, and AI assistance.\n\n"
            "Let's get you set up. This takes about 30 seconds."
        )
        console.print()
        console.print(Panel(body, title=title, title_align="left", border_style=C_ACCENT, padding=(1, 2)))
    else:
        print("\n  Welcome to FLUID Forge")
        print("  Let's get you set up.\n")


def _env_check(console: Optional["Console"]) -> None:
    checks: List[Tuple[str, str, bool]] = []

    # Python
    v = sys.version_info
    checks.append(("Python", f"{v.major}.{v.minor}.{v.micro}", v >= (3, 9)))

    # Rich
    checks.append(("Rich output", "yes" if RICH_AVAILABLE else "no", RICH_AVAILABLE))

    # LLM key detection
    llm_ready = False
    llm_detail = "no API key found"
    for var_name, label in [
        ("ANTHROPIC_API_KEY", "anthropic"),
        ("OPENAI_API_KEY", "openai"),
        ("GEMINI_API_KEY", "gemini"),
    ]:
        if os.environ.get(var_name):
            llm_ready = True
            llm_detail = f"{label} key detected"
            break
    try:
        from .forge_copilot_llm_providers import check_llm_readiness
        readiness = check_llm_readiness()
        if getattr(readiness, "ready", False):
            llm_ready = True
            llm_detail = getattr(readiness, "detail", "") or getattr(readiness, "message", "") or "configured"
    except Exception:  # noqa: BLE001
        pass
    checks.append(("Copilot LLM", llm_detail, llm_ready))

    if console and RICH_AVAILABLE:
        console.print()
        console.print(f"  [bold]Environment[/bold]")
        for label, detail, ok in checks:
            icon = f"[{C_OK}]✓[/{C_OK}]" if ok else f"[{C_WARN}]![/{C_WARN}]"
            console.print(f"    {icon} {label}: [{C_DIM}]{detail}[/{C_DIM}]")
    else:
        print("  Environment:")
        for label, detail, ok in checks:
            mark = "✓" if ok else "!"
            print(f"    {mark} {label}: {detail}")


def _pick_provider(console: Optional["Console"], session: ForgeSession) -> None:
    if console and RICH_AVAILABLE:
        console.print()
        console.print(f"  [bold]Default provider[/bold]")
        for i, (name, desc) in enumerate(PROVIDERS, 1):
            console.print(f"    [{C_BRAND}]{i}[/{C_BRAND}]  [bold]{name:<12}[/bold] [{C_DIM}]{desc}[/{C_DIM}]")
        raw = _ask(console, "Pick one", "1")
    else:
        print("\n  Default provider:")
        for i, (name, desc) in enumerate(PROVIDERS, 1):
            print(f"    {i}  {name:<12} {desc}")
        raw = input("\n  Pick one [1]: ").strip() or "1"

    try:
        idx = int(raw) - 1
        provider = PROVIDERS[idx][0]
    except (ValueError, IndexError):
        provider = raw if any(raw == p[0] for p in PROVIDERS) else "local"
    session.switch_provider(provider)
    if console and RICH_AVAILABLE:
        console.print(f"    → [bold {C_BRAND}]{provider}[/bold {C_BRAND}]")


def _pick_llm(console: Optional["Console"], session: ForgeSession) -> None:
    # Auto-detect if a key is already set
    auto_provider = None
    for var_name, label in [
        ("ANTHROPIC_API_KEY", "anthropic"),
        ("OPENAI_API_KEY", "openai"),
        ("GEMINI_API_KEY", "gemini"),
    ]:
        if os.environ.get(var_name):
            auto_provider = label
            break

    if auto_provider:
        if console and RICH_AVAILABLE:
            console.print()
            console.print(
                f"  [bold]LLM provider[/bold]  "
                f"[{C_DIM}]auto-detected from env[/{C_DIM}]"
            )
            console.print(
                f"    → [bold {C_ACCENT}]{auto_provider}[/bold {C_ACCENT}]"
            )
        else:
            print(f"\n  LLM: auto-detected {auto_provider}")
        return

    if console and RICH_AVAILABLE:
        console.print()
        console.print(
            f"  [bold]LLM for the copilot[/bold]  "
            f"[{C_DIM}](optional — you can skip and set an API key later)[/{C_DIM}]"
        )
        for i, (name, desc) in enumerate(LLM_PROVIDERS, 1):
            console.print(f"    [{C_BRAND}]{i}[/{C_BRAND}]  [bold]{name:<12}[/bold] [{C_DIM}]{desc}[/{C_DIM}]")
        console.print(f"    [{C_BRAND}]s[/{C_BRAND}]  [bold]{'skip':<12}[/bold] [{C_DIM}]set up later[/{C_DIM}]")
        raw = _ask(console, "Pick one (or s to skip)", "1")
    else:
        print("\n  LLM for copilot (optional):")
        for i, (name, desc) in enumerate(LLM_PROVIDERS, 1):
            print(f"    {i}  {name:<12} {desc}")
        print(f"    s  skip")
        raw = input("\n  Pick one [1]: ").strip() or "1"

    if raw.lower() in {"s", "skip"}:
        if console and RICH_AVAILABLE:
            console.print(f"    [{C_DIM}]skipped — set ANTHROPIC_API_KEY or OPENAI_API_KEY later[/{C_DIM}]")
        return

    try:
        idx = int(raw) - 1
        llm = LLM_PROVIDERS[idx][0]
    except (ValueError, IndexError):
        llm = raw if any(raw == p[0] for p in LLM_PROVIDERS) else "anthropic"

    env_var = {
        "anthropic": "ANTHROPIC_API_KEY",
        "openai": "OPENAI_API_KEY",
        "gemini": "GEMINI_API_KEY",
        "ollama": "(no key needed — Ollama runs locally)",
    }.get(llm, f"{llm.upper()}_API_KEY")

    if console and RICH_AVAILABLE:
        console.print(f"    → [bold {C_ACCENT}]{llm}[/bold {C_ACCENT}]")
        if llm != "ollama":
            console.print(f"    [{C_DIM}]make sure {env_var} is set in your shell[/{C_DIM}]")
    else:
        print(f"    → {llm}")


def _save(console: Optional["Console"], session: ForgeSession) -> None:
    session.config_dir.mkdir(parents=True, exist_ok=True)
    try:
        import yaml  # type: ignore
        cfg_path = session.config_dir / "config.yaml"
        payload = {"provider": session.provider, "onboarded": True}
        existing = {}
        if cfg_path.exists():
            try:
                existing = yaml.safe_load(cfg_path.read_text()) or {}
            except Exception:  # noqa: BLE001
                pass
        existing.update(payload)
        cfg_path.write_text(yaml.safe_dump(existing, sort_keys=True))
    except Exception as exc:  # noqa: BLE001
        LOG.debug("config save failed: %s", exc)
    session.save()


def _done(console: Optional["Console"]) -> None:
    if console and RICH_AVAILABLE:
        body = Text()
        body.append("You're ready.\n\n", style=f"bold {C_OK}")
        body.append("  Just describe what you want to build, and\n")
        body.append("  the copilot will guide you from there.\n\n")
        body.append("  /help   ", style=f"bold {C_ACCENT}")
        body.append("list commands\n")
        body.append("  /doctor ", style=f"bold {C_ACCENT}")
        body.append("check readiness\n")
        body.append("  /exit   ", style=f"bold {C_ACCENT}")
        body.append("leave forge")
        console.print()
        console.print(Panel(body, border_style=C_OK, padding=(0, 1)))
    else:
        print("\n  You're ready. Type /help inside the REPL for commands.\n")


def _ask(console: Optional["Console"], label: str, default: str) -> str:
    """Small input helper with consistent formatting."""
    if console and Prompt:
        return Prompt.ask(f"  {label}", default=default, show_default=True)
    try:
        return input(f"  {label} [{default}]: ").strip() or default
    except (EOFError, KeyboardInterrupt):
        return default


__all__ = ["is_first_run", "run"]
