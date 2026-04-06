# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Status-line renderer for the forge REPL and slash commands."""

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
    s = session.to_status_dict()
    if RICH_AVAILABLE and console is not None:
        line = Text()
        line.append(f"  {s['provider']}", style="bold magenta")
        line.append(f"  ·  {s['mode']}", style="cyan")
        mem_style = "green" if s["memory"] == "on" else "dim"
        line.append(f"  ·  memory:{s['memory']}", style=mem_style)
        line.append(f"  ·  {s['project']}", style="dim")
        console.print(line)
    else:
        print(
            f"  {s['provider']}  ·  {s['mode']}  ·  "
            f"memory:{s['memory']}  ·  {s['project']}"
        )


def format_status_text(session: ForgeSession) -> str:
    s = session.to_status_dict()
    return (
        f"  {s['provider']} · {s['mode']} · memory:{s['memory']} · "
        f"{s['project']}   /help commands  /exit quit"
    )


__all__ = ["render_status_line", "format_status_text"]
