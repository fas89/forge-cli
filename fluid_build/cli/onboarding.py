# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""First-run onboarding wizard for forge.

Triggered automatically when ``~/.fluid`` is missing, or on demand via
``fluid onboard`` / the REPL's ``/onboard`` hook. Keeps existing ``init``,
``doctor``, and ``auth`` commands untouched — this wizard simply *chains*
them into a single friendly experience.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from .forge_session import ForgeSession

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.prompt import Prompt, Confirm
    from rich.text import Text

    RICH_AVAILABLE = True
except ImportError:  # pragma: no cover
    Console = None  # type: ignore[assignment]
    Panel = None  # type: ignore[assignment]
    Prompt = None  # type: ignore[assignment]
    Confirm = None  # type: ignore[assignment]
    Text = None  # type: ignore[assignment]
    RICH_AVAILABLE = False

LOG = logging.getLogger("fluid.cli.onboarding")

PROVIDERS = [
    ("local", "Local filesystem (great for quick trials)"),
    ("gcp", "Google Cloud (BigQuery, Dataplex, Dataproc)"),
    ("snowflake", "Snowflake"),
    ("aws", "Amazon Web Services (Redshift, Glue, S3)"),
    ("azure", "Microsoft Azure"),
    ("odps", "Alibaba MaxCompute / ODPS"),
]

LLM_PROVIDERS = [
    ("anthropic", "Claude (Anthropic) — recommended"),
    ("openai", "OpenAI GPT"),
    ("gemini", "Google Gemini"),
    ("ollama", "Local models via Ollama"),
    ("skip", "Skip for now — I'll set this up later"),
]


def is_first_run(config_dir: Optional[Path] = None) -> bool:
    cfg = config_dir or (Path.home() / ".fluid")
    return not cfg.exists()


def run(session: Optional[ForgeSession] = None) -> ForgeSession:
    """Run the welcome wizard and return a populated ``ForgeSession``.

    Safe to call repeatedly; skips steps the user has already completed.
    """
    session = session or ForgeSession.load()
    console = Console() if RICH_AVAILABLE else None

    _step_welcome(console)
    _step_env_check(console)
    provider = _step_pick_provider(console, session)
    llm = _step_pick_llm(console, session)
    _step_save(console, session, provider, llm)
    _step_next_steps(console)

    return session


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


def _step_welcome(console) -> None:
    if console and RICH_AVAILABLE:
        body = Text()
        body.append("Welcome to ", style="bold")
        body.append("FLUID Forge", style="bold magenta")
        body.append(" 🔨\n\n", style="bold")
        body.append("Forge turns a single YAML contract into deployable\n")
        body.append("data products — with governance, lineage, and AI help baked in.\n\n")
        body.append("This 4-step wizard will:\n", style="bold")
        body.append("  1. Check your environment (doctor)\n", style="cyan")
        body.append("  2. Pick an infrastructure provider\n", style="cyan")
        body.append("  3. Pick an LLM for the copilot (optional)\n", style="cyan")
        body.append("  4. Save it all to ~/.fluid\n", style="cyan")
        console.print(Panel(body, title="🚀 First-run setup", border_style="cyan"))
    else:
        print("\n=== Welcome to FLUID Forge 🔨 ===\n")
        print("This wizard will set up your environment in 4 quick steps.\n")


def _step_env_check(console) -> None:
    header = "[1/4] Environment check"
    if console and RICH_AVAILABLE:
        console.print(f"\n[bold cyan]{header}[/bold cyan]")
    else:
        print(f"\n{header}")

    rows = []
    # Python version
    import sys as _sys

    rows.append(
        (
            "Python",
            f"{_sys.version_info.major}.{_sys.version_info.minor}",
            _sys.version_info >= (3, 9),
        )
    )
    # Rich
    rows.append(("Rich output", "available" if RICH_AVAILABLE else "missing", RICH_AVAILABLE))
    # LLM readiness (non-fatal)
    try:
        from .forge_copilot_llm_providers import check_llm_readiness

        readiness = check_llm_readiness()
        ok = getattr(readiness, "ready", False)
        detail = (
            getattr(readiness, "detail", "")
            or getattr(readiness, "message", "")
            or ("configured" if ok else "no API key found")
        )
        rows.append(("Copilot LLM", detail, ok))
    except Exception as exc:  # noqa: BLE001
        rows.append(("Copilot LLM", f"check failed: {exc}", False))

    # Working directory writable
    try:
        test = Path.cwd() / ".fluid_write_test"
        test.touch()
        test.unlink()
        rows.append(("Project dir writable", str(Path.cwd()), True))
    except Exception:  # noqa: BLE001
        rows.append(("Project dir writable", "no", False))

    for label, detail, ok in rows:
        mark = "✓" if ok else "⚠"
        style = "green" if ok else "yellow"
        if console and RICH_AVAILABLE:
            console.print(f"  [{style}]{mark}[/{style}] {label}: {detail}")
        else:
            print(f"  {mark} {label}: {detail}")


def _step_pick_provider(console, session: ForgeSession) -> str:
    header = "\n[2/4] Pick a default infrastructure provider"
    if console and RICH_AVAILABLE:
        console.print(f"[bold cyan]{header}[/bold cyan]")
        for i, (name, desc) in enumerate(PROVIDERS, 1):
            console.print(f"  [magenta]{i}[/magenta]. [bold]{name}[/bold] — {desc}")
        default = "1"
        raw = Prompt.ask(
            "\nYour choice", default=default, show_default=True
        ) if Prompt else input(f"\nYour choice [{default}]: ") or default
    else:
        print(header)
        for i, (name, desc) in enumerate(PROVIDERS, 1):
            print(f"  {i}. {name} — {desc}")
        raw = input("\nYour choice [1]: ").strip() or "1"

    try:
        idx = int(raw) - 1
        provider = PROVIDERS[idx][0]
    except (ValueError, IndexError):
        provider = raw if any(raw == p[0] for p in PROVIDERS) else "local"

    session.switch_provider(provider)
    return provider


def _step_pick_llm(console, session: ForgeSession) -> Optional[str]:
    header = "\n[3/4] Pick an LLM for the AI copilot (optional)"
    if console and RICH_AVAILABLE:
        console.print(f"[bold cyan]{header}[/bold cyan]")
        for i, (name, desc) in enumerate(LLM_PROVIDERS, 1):
            console.print(f"  [magenta]{i}[/magenta]. [bold]{name}[/bold] — {desc}")
        default = "1"
        raw = (
            Prompt.ask("\nYour choice", default=default, show_default=True)
            if Prompt
            else input(f"\nYour choice [{default}]: ") or default
        )
    else:
        print(header)
        for i, (name, desc) in enumerate(LLM_PROVIDERS, 1):
            print(f"  {i}. {name} — {desc}")
        raw = input("\nYour choice [1]: ").strip() or "1"

    try:
        idx = int(raw) - 1
        llm = LLM_PROVIDERS[idx][0]
    except (ValueError, IndexError):
        llm = "anthropic"
    if llm == "skip":
        if console and RICH_AVAILABLE:
            console.print(
                "  [dim]→ skipped. You can add an API key later via env vars "
                "(ANTHROPIC_API_KEY, OPENAI_API_KEY, ...)[/dim]"
            )
        else:
            print("  → skipped.")
        return None
    if console and RICH_AVAILABLE:
        console.print(
            f"  [dim]→ Forge will use {llm}. Make sure the appropriate env var "
            f"is set (e.g. ANTHROPIC_API_KEY for Claude).[/dim]"
        )
    else:
        print(f"  → Forge will use {llm}.")
    return llm


def _step_save(console, session: ForgeSession, provider: str, llm: Optional[str]) -> None:
    header = "\n[4/4] Saving configuration"
    if console and RICH_AVAILABLE:
        console.print(f"[bold cyan]{header}[/bold cyan]")
    else:
        print(header)

    session.config_dir.mkdir(parents=True, exist_ok=True)
    try:
        import yaml  # type: ignore

        cfg_path = session.config_dir / "config.yaml"
        payload = {"provider": provider, "llm_provider": llm, "onboarded": True}
        # Non-destructive merge.
        existing = {}
        if cfg_path.exists():
            try:
                existing = yaml.safe_load(cfg_path.read_text()) or {}
            except Exception:  # noqa: BLE001
                existing = {}
        existing.update(payload)
        cfg_path.write_text(yaml.safe_dump(existing, sort_keys=True))
        if console and RICH_AVAILABLE:
            console.print(f"  [green]✓[/green] wrote {cfg_path}")
        else:
            print(f"  ✓ wrote {cfg_path}")
    except Exception as exc:  # noqa: BLE001
        if console and RICH_AVAILABLE:
            console.print(f"  [yellow]⚠[/yellow] could not write config: {exc}")
        else:
            print(f"  ⚠ could not write config: {exc}")

    session.save()


def _step_next_steps(console) -> None:
    if console and RICH_AVAILABLE:
        body = Text()
        body.append("You're all set! 🎉\n\n", style="bold green")
        body.append("Try these next:\n", style="bold")
        body.append("  fluid forge                ", style="magenta")
        body.append("→ launch the REPL / copilot\n")
        body.append("  fluid forge --help         ", style="magenta")
        body.append("→ see command options\n")
        body.append("  fluid doctor               ", style="magenta")
        body.append("→ full diagnostics\n")
        body.append("  /help                      ", style="magenta")
        body.append("→ (inside the REPL) list slash commands\n")
        console.print(Panel(body, title="✅ Next steps", border_style="green"))
    else:
        print("\n=== Next steps ===")
        print("  fluid forge      — launch the REPL / copilot")
        print("  fluid doctor     — full diagnostics")
        print("  /help            — list slash commands in the REPL")


__all__ = ["is_first_run", "run"]
