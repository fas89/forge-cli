# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Progressive-disclosure help for forge.

Strategy:
    • ``fluid help`` shows the 6 core commands plus a pointer.
    • ``fluid help all`` shows the full command reference (delegates to
      the existing Rich help formatter).
    • ``fluid help <topic>`` shows a topic deep-dive: copilot, templates,
      providers, memory, repl.

Existing entry points (``fluid --help``, ``fluid <cmd> -h``) are
unchanged; this lives as a second, additive path.
"""

from __future__ import annotations

from typing import Optional

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text

    RICH_AVAILABLE = True
except ImportError:  # pragma: no cover
    Console = None  # type: ignore[assignment]
    Panel = None  # type: ignore[assignment]
    Table = None  # type: ignore[assignment]
    Text = None  # type: ignore[assignment]
    RICH_AVAILABLE = False


CORE_COMMANDS = [
    ("forge", "Start the AI copilot to build a new data product"),
    ("validate", "Validate a contract YAML against the schema"),
    ("plan", "Show the execution plan for a contract"),
    ("apply", "Deploy the contract to the target provider"),
    ("doctor", "Run built-in health and readiness checks"),
    ("help <topic>", "Deep-dive help (copilot, templates, providers, memory, repl)"),
]


TOPICS = {
    "copilot": (
        "AI Copilot",
        [
            "The copilot runs an adaptive interview that turns a natural-language",
            "goal into a FLUID contract YAML.",
            "",
            "  fluid forge                           # interactive copilot",
            "  fluid forge --quickstart              # defaults + minimal prompts",
            "  fluid forge --save-memory             # persist choices for this project",
            "  fluid forge --show-memory             # inspect saved project memory",
            "",
            "Required: one LLM API key (ANTHROPIC_API_KEY, OPENAI_API_KEY, …).",
            "Run `fluid doctor` to check readiness.",
        ],
    ),
    "templates": (
        "Templates",
        [
            "Templates are opinionated starting points.",
            "",
            "  fluid forge --mode template --template customer-360",
            "  fluid init --template hello-world",
            "",
            "Run /templates inside the REPL to see what's registered.",
        ],
    ),
    "providers": (
        "Providers",
        [
            "A provider is the target execution/storage backend.",
            "",
            "Supported: local · gcp · snowflake · aws · azure · odps",
            "",
            "  fluid --provider gcp forge",
            "  fluid doctor                    # check provider readiness",
        ],
    ),
    "memory": (
        "Copilot Memory",
        [
            "Copilot memory is project-scoped and lives in",
            "  ./runtime/.state/copilot-memory.json",
            "",
            "It remembers your preferred template, provider, domain, and recent",
            "outcomes so the next run picks up where you left off.",
            "",
            "  fluid forge --save-memory     # persist after a successful run",
            "  fluid forge --show-memory     # summarize current memory",
            "  fluid forge --no-memory       # ignore memory for one run",
            "  fluid forge --reset-memory    # wipe the file",
        ],
    ),
    "repl": (
        "Interactive REPL (worktree A)",
        [
            "Run `fluid` with no arguments on a TTY to enter the persistent",
            "REPL. Inside the REPL:",
            "",
            "  /help         list all slash commands",
            "  /doctor       quick readiness check",
            "  /memory       project-scoped copilot memory",
            "  /templates    registered templates",
            "  /providers    supported providers",
            "  /status       current session status line",
            "  /config <k> <v>   set provider / mode / memory",
            "  /exit         leave the REPL",
            "",
            "Any bare text is interpreted as a project-goal description and",
            "launches the copilot interview.",
        ],
    ),
}


def print_help_v2(parser, topic: Optional[str]) -> int:
    """Render progressive-disclosure help and return an exit code."""
    console = Console() if RICH_AVAILABLE else None

    if topic == "all":
        # Delegate to the existing verbose help for the full reference.
        try:
            from .help_formatter import print_main_help

            print_main_help(parser)
        except Exception:  # noqa: BLE001
            parser.print_help()
        return 0

    if topic:
        return _print_topic(topic, console)

    # Default: 6 core commands + pointer
    _print_core(console)
    return 0


def _print_core(console) -> None:
    if RICH_AVAILABLE and console is not None:
        table = Table(title="FLUID Forge — Core Commands", header_style="bold cyan")
        table.add_column("Command", style="magenta", no_wrap=True)
        table.add_column("What it does", style="white")
        for name, desc in CORE_COMMANDS:
            table.add_row(f"fluid {name}", desc)
        console.print(table)

        pointer = Text.from_markup(
            "\n[dim]For the full list of ~30 commands, run[/dim] "
            "[bold]fluid help all[/bold]\n"
            "[dim]For a topic deep-dive, run[/dim] "
            "[bold]fluid help <topic>[/bold] "
            "[dim](copilot, templates, providers, memory, repl)[/dim]"
        )
        console.print(pointer)
    else:
        print("FLUID Forge — Core Commands")
        print("-" * 36)
        for name, desc in CORE_COMMANDS:
            print(f"  fluid {name:<18} {desc}")
        print("\nFor the full list, run: fluid help all")
        print("For a topic deep-dive: fluid help <topic>")
        print("  (topics: copilot, templates, providers, memory, repl)")


def _print_topic(topic: str, console) -> int:
    if topic not in TOPICS:
        msg = (
            f"unknown help topic: {topic!r}. "
            f"Known topics: {', '.join(sorted(TOPICS))} (or 'all')."
        )
        if console:
            console.print(f"[red]{msg}[/red]")
        else:
            print(msg)
        return 1
    title, lines = TOPICS[topic]
    body = "\n".join(lines)
    if RICH_AVAILABLE and console is not None:
        console.print(Panel(body, title=f"📖 {title}", border_style="cyan"))
    else:
        print(f"\n=== {title} ===")
        print(body)
    return 0


__all__ = ["print_help_v2"]
