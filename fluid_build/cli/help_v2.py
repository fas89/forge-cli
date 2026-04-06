# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Progressive-disclosure help for forge.

    fluid help                → 6 core commands
    fluid help all            → full reference (delegates to existing formatter)
    fluid help <topic>        → deep dive (copilot, templates, providers, memory, repl)
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
    ("forge", "Start the AI copilot — describe what you want to build"),
    ("validate <file>", "Validate a contract YAML against the schema"),
    ("plan <file>", "Preview the execution plan for a contract"),
    ("apply <file>", "Deploy the contract to the target provider"),
    ("doctor", "Check environment, LLM, and provider readiness"),
    ("help <topic>", "Deep-dive into: copilot, templates, providers, memory, repl"),
]


TOPICS = {
    "copilot": (
        "AI Copilot",
        (
            "The copilot runs an adaptive interview that turns a natural-language\n"
            "goal into a FLUID contract YAML.\n"
            "\n"
            "  fluid forge                    interactive copilot\n"
            "  fluid forge --quickstart       minimal prompts, smart defaults\n"
            "  fluid forge --save-memory      remember choices for this project\n"
            "  fluid forge --show-memory      inspect saved project memory\n"
            "\n"
            "Requires an LLM API key (ANTHROPIC_API_KEY, OPENAI_API_KEY, etc.).\n"
            "Run fluid doctor to check readiness."
        ),
    ),
    "templates": (
        "Templates",
        (
            "Templates are opinionated starting points for data products.\n"
            "\n"
            "  fluid forge --mode template --template customer-360\n"
            "  fluid init --template hello-world\n"
            "\n"
            "Inside the REPL, type /templates to see what's registered."
        ),
    ),
    "providers": (
        "Providers",
        (
            "A provider is the target execution and storage backend.\n"
            "\n"
            "Supported: local · gcp · snowflake · aws · azure · odps\n"
            "\n"
            "  fluid --provider gcp forge\n"
            "  fluid doctor\n"
            "\n"
            "Inside the REPL, type /providers to see the list,\n"
            "or /config provider gcp to switch."
        ),
    ),
    "memory": (
        "Copilot Memory",
        (
            "Copilot memory is project-scoped. It remembers your preferred\n"
            "template, provider, domain, and recent outcomes so the next run\n"
            "picks up where you left off.\n"
            "\n"
            "  fluid forge --save-memory     persist after a successful run\n"
            "  fluid forge --show-memory     summarize current memory\n"
            "  fluid forge --no-memory       ignore memory for one run\n"
            "  fluid forge --reset-memory    wipe the file\n"
            "\n"
            "Stored in: ./runtime/.state/copilot-memory.json"
        ),
    ),
    "repl": (
        "Interactive REPL",
        (
            "Run fluid with no arguments to enter the REPL.\n"
            "Inside the REPL, just describe what you want to build —\n"
            "the copilot handles the rest.\n"
            "\n"
            "  /help           list commands\n"
            "  /doctor         readiness check\n"
            "  /memory         project copilot memory\n"
            "  /templates      browse templates\n"
            "  /providers      browse providers\n"
            "  /config k v     set provider, mode, or memory\n"
            "  /status         current session state\n"
            "  /history        recent commands\n"
            "  /exit           leave the REPL\n"
            "\n"
            "During a copilot interview:\n"
            "  /edit 2         revise answer 2\n"
            "  /back           step back one question\n"
            "  /skip           skip the current question\n"
            "  /retry          re-ask the current question\n"
            "  /abort          cancel the interview"
        ),
    ),
}


def print_help_v2(parser, topic: Optional[str]) -> int:
    console = Console(highlight=False) if RICH_AVAILABLE else None

    if topic == "all":
        try:
            from .help_formatter import print_main_help
            print_main_help(parser)
        except Exception:  # noqa: BLE001
            if parser:
                parser.print_help()
        return 0

    if topic:
        return _print_topic(topic, console)

    _print_core(console)
    return 0


def _print_core(console: Optional["Console"]) -> None:
    if RICH_AVAILABLE and console is not None:
        table = Table(show_header=False, box=None, padding=(0, 2))
        table.add_column("", style="bold magenta", no_wrap=True, min_width=20)
        table.add_column("", style="white")
        for name, desc in CORE_COMMANDS:
            table.add_row(f"fluid {name}", desc)
        console.print()
        console.print(table)
        console.print(
            f"\n  [dim]Full reference:[/dim] [bold]fluid help all[/bold]"
            f"   [dim]Topic:[/dim] [bold]fluid help <topic>[/bold]"
            f"   [dim]({', '.join(sorted(TOPICS))})[/dim]\n"
        )
    else:
        print()
        for name, desc in CORE_COMMANDS:
            print(f"  fluid {name:<20} {desc}")
        print(f"\n  Full reference: fluid help all")
        print(f"  Topics: {', '.join(sorted(TOPICS))}\n")


def _print_topic(topic: str, console: Optional["Console"]) -> int:
    if topic not in TOPICS:
        known = ", ".join(sorted(TOPICS))
        if console and RICH_AVAILABLE:
            console.print(f"  [yellow]unknown topic: {topic!r}[/yellow]  (try: {known})")
        else:
            print(f"  unknown topic: {topic!r}  (try: {known})")
        return 1
    title, body = TOPICS[topic]
    if RICH_AVAILABLE and console is not None:
        console.print()
        console.print(Panel(body, title=f"  {title}", title_align="left", border_style="cyan", padding=(1, 2)))
        console.print()
    else:
        print(f"\n  === {title} ===\n{body}\n")
    return 0


__all__ = ["print_help_v2"]
