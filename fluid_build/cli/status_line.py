# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Single status-line renderer shared by the full REPL and the hybrid launcher.

Falls back gracefully to plain text when Rich is unavailable. Never raises.
"""

from __future__ import annotations

from typing import Optional

from .forge_session import ForgeSession

try:
    from rich.console import Console
    from rich.text import Text

    RICH_AVAILABLE = True
except ImportError:  # pragma: no cover
    Console = None  # type: ignore[assignment]
    Text = None  # type: ignore[assignment]
    RICH_AVAILABLE = False


def render_status_line(session: ForgeSession, console: Optional["Console"] = None) -> None:
    """Render a compact one-line status banner.

    Format: ``forge › provider:gcp · mode:copilot · memory:on · project:my-proj``
    """
    status = session.to_status_dict()
    if RICH_AVAILABLE and console is not None:
        line = Text()
        line.append("forge ", style="bold cyan")
        line.append("› ", style="dim")
        line.append(f"provider:{status['provider']}", style="magenta")
        line.append(" · ", style="dim")
        line.append(f"mode:{status['mode']}", style="yellow")
        line.append(" · ", style="dim")
        mem_style = "green" if status["memory"] == "on" else "dim"
        line.append(f"memory:{status['memory']}", style=mem_style)
        line.append(" · ", style="dim")
        line.append(f"project:{status['project']}", style="blue")
        console.print(line)
    else:
        print(
            "forge › provider:{provider} · mode:{mode} · memory:{memory} · project:{project}".format(
                **status
            )
        )


def format_status_text(session: ForgeSession) -> str:
    """Plain-text status line — used by prompt_toolkit's bottom toolbar."""
    s = session.to_status_dict()
    return (
        f"forge › provider:{s['provider']} · mode:{s['mode']} · "
        f"memory:{s['memory']} · project:{s['project']}   "
        f"(type /help for commands, /exit to quit)"
    )


__all__ = ["render_status_line", "format_status_text"]
