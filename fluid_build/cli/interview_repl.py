# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Scoped copilot-interview REPL (worktree B prototype).

The hybrid UX model says: the rest of the CLI stays one-shot, but the
adaptive copilot interview is where the biggest UX win lives. During the
interview users can:

    /show         see all answers so far
    /edit <n>     revise answer n (next prompt re-asks it)
    /back         step back one question
    /skip         skip the current question
    /retry        re-ask the current question
    /doctor       quick LLM readiness check without leaving the flow
    /memory       show project-scoped copilot memory
    /help         list these commands
    /abort        abandon the interview

This module is a **self-contained, runnable prototype** of that flow.
It asks a fixed bootstrap set of questions (project goal, use case,
data sources, provider, refresh cadence), then hands the full answer
dict to the existing copilot generation pipeline if available.

The point of the prototype is to demonstrate *how the interactions
feel*. It deliberately does not re-wire the full adaptive LLM clarifier
— that's a follow-up patch inside forge_copilot_interview.py.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional

from .forge_session import ForgeSession
from .slash_commands import dispatch
from .status_line import render_status_line

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.text import Text

    RICH_AVAILABLE = True
except ImportError:  # pragma: no cover
    Console = None  # type: ignore[assignment]
    Panel = None  # type: ignore[assignment]
    Text = None  # type: ignore[assignment]
    RICH_AVAILABLE = False

LOG = logging.getLogger("fluid.cli.interview_repl")


@dataclass
class Question:
    key: str
    prompt: str
    hint: str
    default: Optional[str] = None
    options: Optional[List[str]] = None


BOOTSTRAP_QUESTIONS: List[Question] = [
    Question(
        key="project_goal",
        prompt="In one sentence, what are you trying to build?",
        hint=(
            "This becomes the description of the data product. Be concrete — "
            "e.g. 'daily retention metrics for the mobile app' is better than "
            "'user analytics'."
        ),
    ),
    Question(
        key="use_case",
        prompt="What is the primary use case?",
        hint="Pick the closest match; you can refine later.",
        options=[
            "analytics",
            "reporting",
            "ml-training",
            "ml-serving",
            "operational",
            "governance",
            "other",
        ],
        default="analytics",
    ),
    Question(
        key="data_sources",
        prompt="Where will the data come from? (comma-separated)",
        hint="Examples: postgres.orders, salesforce, s3://bucket/raw, kafka.events",
    ),
    Question(
        key="provider",
        prompt="Target infrastructure provider?",
        hint="This determines which runtime connectors and governance rules apply.",
        options=["local", "gcp", "snowflake", "aws", "azure", "odps"],
        default="gcp",
    ),
    Question(
        key="refresh_cadence",
        prompt="How often should the product refresh?",
        hint="Typical: hourly, daily, weekly. You can override later in the contract.",
        options=["real-time", "hourly", "daily", "weekly", "monthly", "on-demand"],
        default="daily",
    ),
]


@dataclass
class InterviewState:
    questions: List[Question]
    answers: dict = field(default_factory=dict)
    index: int = 0
    qa: List[tuple] = field(default_factory=list)  # [(prompt, answer), ...]

    # Mutation flags set by slash-command handlers
    edit_target: Optional[int] = None
    back: bool = False
    skip: bool = False
    retry: bool = False
    abort: bool = False

    def reset_flags(self) -> None:
        self.edit_target = None
        self.back = False
        self.skip = False
        self.retry = False

    def as_dict(self) -> dict:
        """Shape expected by the slash_commands /show handler."""
        return {"qa": list(self.qa), "answers": dict(self.answers)}


def run_scoped_interview(session: ForgeSession, console) -> int:
    """Run the scoped interview loop and return an exit code."""
    state = InterviewState(questions=list(BOOTSTRAP_QUESTIONS))
    # Expose state to slash handlers through the session object (duck-typed).
    setattr(session, "interview_state", state.as_dict())

    _banner(console, session, state)

    while state.index < len(state.questions) and not state.abort:
        q = state.questions[state.index]

        # Sync the live qa list back onto the session mirror for /show.
        session.interview_state["qa"] = list(state.qa)
        session.interview_state["answers"] = dict(state.answers)

        progress = f"[{state.index + 1}/{len(state.questions)}]"
        raw = _ask(console, q, progress, session)
        if raw is None:
            continue  # slash command handled, re-prompt

        # Check flags that handlers may have set
        mirror = session.interview_state
        if mirror.get("abort"):
            state.abort = True
            break
        if mirror.get("back"):
            mirror["back"] = False
            if state.index > 0:
                state.index -= 1
                if state.qa:
                    removed = state.qa.pop()
                    state.answers.pop(state.questions[state.index].key, None)
                    if console:
                        console.print(
                            f"[dim]↶ removed answer to {removed[0]!r}[/dim]"
                        )
            continue
        if mirror.get("skip"):
            mirror["skip"] = False
            state.qa.append((q.prompt, "(skipped)"))
            state.answers[q.key] = None
            state.index += 1
            continue
        if mirror.get("retry"):
            mirror["retry"] = False
            continue
        edit_n = mirror.get("edit_target")
        if edit_n is not None:
            mirror["edit_target"] = None
            # Jump back to that question
            target_idx = max(1, min(int(edit_n), len(state.qa))) - 1
            state.index = target_idx
            # Truncate qa/answers beyond the target
            state.qa = state.qa[:target_idx]
            for later_q in state.questions[target_idx:]:
                state.answers.pop(later_q.key, None)
            if console:
                console.print(f"[dim]↻ jumping back to question {target_idx + 1}[/dim]")
            continue

        # Normal answer path
        if raw == "":
            raw = q.default or ""
        state.qa.append((q.prompt, raw))
        state.answers[q.key] = raw
        state.index += 1

    if state.abort:
        if console:
            console.print("[yellow]⚠ interview aborted[/yellow]")
        else:
            print("⚠ interview aborted")
        # Detach state mirror
        try:
            delattr(session, "interview_state")
        except AttributeError:
            pass
        return 1

    _summary(console, state)
    session.last_result = state.answers.get("project_goal")
    session.save()
    try:
        delattr(session, "interview_state")
    except AttributeError:
        pass
    return 0


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------


def _banner(console, session: ForgeSession, state: InterviewState) -> None:
    if RICH_AVAILABLE and console is not None:
        body = Text.from_markup(
            "[bold]Copilot interview[/bold] — "
            f"[dim]{len(state.questions)} bootstrap questions[/dim]\n\n"
            "Type [bold magenta]?[/bold magenta] at any prompt for help on that question.\n"
            "Type [bold magenta]/help[/bold magenta] to list slash commands.\n"
            "Type [bold magenta]/edit 2[/bold magenta] to revise an earlier answer, "
            "[bold magenta]/back[/bold magenta] to step back, "
            "[bold magenta]/abort[/bold magenta] to cancel."
        )
        console.print(Panel(body, title="🤖 copilot", border_style="magenta"))
        render_status_line(session, console)
    else:
        print("\n=== 🤖 Copilot interview ===")
        print(
            "Type ? for help on a question, /help for slash commands, "
            "/edit <n> to revise, /abort to cancel."
        )


def _ask(
    console, q: Question, progress: str, session: ForgeSession
) -> Optional[str]:
    """Ask a single question. Returns the raw answer, or None if a slash
    command was handled and we should re-prompt."""
    # Render the question
    if RICH_AVAILABLE and console is not None:
        console.print()
        console.print(f"[dim]{progress}[/dim] [bold]{q.prompt}[/bold]")
        if q.options:
            for i, opt in enumerate(q.options, 1):
                marker = "●" if opt == q.default else " "
                console.print(f"   [magenta]{i}[/magenta]. {marker} {opt}")
        if q.default and not q.options:
            console.print(f"   [dim]default: {q.default}[/dim]")
    else:
        print(f"\n{progress} {q.prompt}")
        if q.options:
            for i, opt in enumerate(q.options, 1):
                marker = "*" if opt == q.default else " "
                print(f"   {i}. {marker} {opt}")
        if q.default and not q.options:
            print(f"   default: {q.default}")

    try:
        raw = input("› ").strip()
    except (EOFError, KeyboardInterrupt):
        return ""

    # ? → inline help
    if raw == "?":
        if RICH_AVAILABLE and console is not None:
            console.print(f"[dim]💡 {q.hint}[/dim]")
        else:
            print(f"? {q.hint}")
        return None

    # /cmd → slash dispatcher (interview scope)
    if raw.startswith("/"):
        result = dispatch(raw, session, console=console, scope="interview")
        if result.renderable is not None and console is not None:
            console.print(result.renderable)
        elif result.message:
            if console is not None:
                console.print(result.message)
            else:
                print(result.message)
        if not result.continue_session:
            session.interview_state["abort"] = True
        return None

    # Option selection by number
    if q.options and raw.isdigit():
        idx = int(raw) - 1
        if 0 <= idx < len(q.options):
            return q.options[idx]

    # Option selection by prefix
    if q.options and raw:
        for opt in q.options:
            if opt.lower().startswith(raw.lower()):
                return opt

    return raw


def _summary(console, state: InterviewState) -> None:
    if RICH_AVAILABLE and console is not None:
        body = Text()
        body.append("Interview complete ✓\n\n", style="bold green")
        for i, (q, a) in enumerate(state.qa, 1):
            body.append(f"  {i}. ", style="dim")
            body.append(f"{q}\n", style="bold")
            body.append(f"     → {a}\n", style="magenta")
        body.append(
            "\nNext: feed these answers to the copilot generator "
            "via `fluid forge --quickstart` or call the generator directly.",
            style="dim",
        )
        console.print(Panel(body, title="✅ answers", border_style="green"))
    else:
        print("\n=== ✓ Interview complete ===")
        for i, (q, a) in enumerate(state.qa, 1):
            print(f"  {i}. {q}")
            print(f"     → {a}")


__all__ = ["run_scoped_interview", "InterviewState", "Question", "BOOTSTRAP_QUESTIONS"]
