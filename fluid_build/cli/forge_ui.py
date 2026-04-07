# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Shared Forge user-facing copy and Rich panel helpers."""

from __future__ import annotations

__all__ = [
    "ask_numbered_choice",
    "build_copilot_analysis_text",
    "build_standard_next_steps",
    "print_assumptions_panel",
    "print_welcome_panel",
    "show_copilot_analysis",
    "show_domain_analysis",
    "show_lines_panel",
    "show_next_steps_panel",
]


from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

try:
    from rich.panel import Panel

    RICH_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised through non-Rich fallbacks elsewhere
    Panel = None  # type: ignore[assignment]
    RICH_AVAILABLE = False

FORGE_VALIDATE_COMMAND = "fluid validate contract.fluid.yaml"
FORGE_PLAN_COMMAND = "fluid plan contract.fluid.yaml --out runtime/plan.json"
FORGE_APPLY_COMMAND = "fluid apply runtime/plan.json"
FORGE_MARKET_SEARCH_COMMAND = 'fluid market --search "<keyword>"'
FORGE_DIALOG_HINT = (
    "Answer a few questions about your project using a number, short phrase, or your own wording"
)
FORGE_FLEXIBLE_INPUT_SUMMARY = "numbers, short phrases, and natural-language answers"
FORGE_WORKFLOW_STEPS = [
    "Run fluid forge",
    FORGE_DIALOG_HINT,
    "Copilot discovers local metadata and generates a full contract",
    "Forge validates and repairs the contract if needed",
    "Forge scaffolds only after validation passes",
    "Forge shows how memory influenced the run",
    "Save project-scoped memory only if you explicitly opt in",
]


def _build_panel(renderable: str, *, title: str, border_style: str) -> Any:
    if not RICH_AVAILABLE or Panel is None:
        return renderable
    return Panel(renderable, title=title, border_style=border_style)


def ask_numbered_choice(
    console: Any,
    prompt: str,
    options: Sequence[tuple],
    *,
    default: int = 1,
) -> str:
    """Show a numbered menu and return the selected value.

    *options* is a sequence of ``(value, label)`` tuples.  The user types
    a number (1-based) instead of the exact string.  Accepts Enter for
    the default.

    Falls back to plain ``input()`` when Rich is not available.

    Example::

        choice = ask_numbered_choice(
            console,
            "Where will this data product run?",
            [("local", "Local (DuckDB) -- great for getting started"),
             ("gcp", "Google Cloud (BigQuery)"),
             ("snowflake", "Snowflake"),
             ("aws", "AWS (S3 + Glue)")],
        )
    """
    if not options:
        return ""

    lines = []
    for i, (_, label) in enumerate(options, 1):
        marker = " [bold cyan](default)[/bold cyan]" if i == default and RICH_AVAILABLE else ""
        prefix = f"  [bold cyan]{i}[/bold cyan]" if RICH_AVAILABLE else f"  {i}"
        lines.append(f"{prefix}. {label}{marker}")
    menu_text = "\n".join(lines)

    if console and RICH_AVAILABLE:
        console.print(f"\n[bold]{prompt}[/bold]")
        console.print(menu_text)

        try:
            from rich.prompt import Prompt

            raw = Prompt.ask(
                "Enter number",
                default=str(default),
                show_default=True,
            )
        except (ImportError, EOFError):
            raw = str(default)
    else:
        print(f"\n{prompt}")
        for i, (_, label) in enumerate(options, 1):
            marker = " (default)" if i == default else ""
            print(f"  {i}. {label}{marker}")
        raw = input(f"Enter number [{default}]: ").strip() or str(default)

    try:
        idx = int(raw) - 1
        if 0 <= idx < len(options):
            return options[idx][0]
    except (ValueError, IndexError):
        pass

    # If input doesn't parse as a number, try matching by value or label
    raw_lower = raw.strip().lower()
    for value, label in options:
        if raw_lower == value.lower() or raw_lower == label.lower():
            return value

    # Fall back to default
    return options[default - 1][0]


def show_lines_panel(
    console: Any,
    lines: Sequence[str],
    *,
    title: str,
    border_style: str,
) -> None:
    """Print a simple multi-line panel when Rich is available."""
    if not console or not RICH_AVAILABLE or not lines:
        return
    console.print(_build_panel("\n".join(lines), title=title, border_style=border_style))


def print_welcome_panel(console: Any) -> None:
    """Render the shared Forge welcome panel."""
    if not console or not RICH_AVAILABLE:
        return
    welcome_text = (
        "🔨 **Forge** — Create a new data product\n\n"
        "AI Copilot will interview you and generate a complete\n"
        "FLUID contract, README, and scaffolding.\n\n"
        "[dim]Tip: use [bold]--blank[/bold] for an empty contract without AI.[/dim]"
    )
    console.print(_build_panel(welcome_text, title="FLUID Forge", border_style="blue"))


def print_assumptions_panel(console: Any, assumptions: Sequence[str]) -> None:
    """Render the bounded assumptions summary shown after interviews."""
    if not console or not RICH_AVAILABLE or not assumptions:
        return
    assumption_lines = "\n".join(f"• {item}" for item in assumptions[:4])
    console.print(_build_panel(assumption_lines, title="📝 Assumptions Used", border_style="cyan"))


def build_copilot_analysis_text(
    *,
    context: Mapping[str, Any],
    suggestions: Mapping[str, Any],
    use_case_label: str,
    memory_lines: Sequence[str],
) -> str:
    """Build the copilot analysis panel body."""
    lines = [
        f"🎯 **Project Goal:** {context.get('project_goal', 'Not specified')}",
        f"📊 **Data Sources:** {context.get('data_sources', 'Not specified')}",
        f"🏗️ **Use Case:** {use_case_label}",
        f"⚙️ **Complexity:** {context.get('complexity', 'intermediate')}",
        "",
        "🤖 **AI Recommendations:**",
        f"• **Template:** {suggestions['recommended_template']}",
        f"• **Provider:** {suggestions['recommended_provider']}",
        "• **Patterns:** "
        + (", ".join(suggestions.get("recommended_patterns", [])) or "Standard patterns"),
        "",
        "💡 **Architecture Suggestions:**",
    ]

    for suggestion in suggestions.get("architecture_suggestions", []):
        lines.append(f"• {suggestion}")

    best_practices = list(suggestions.get("best_practices", []) or [])
    if best_practices:
        lines.extend(["", "✨ **Best Practices:**"])
        lines.extend(f"• {practice}" for practice in best_practices)

    assumptions = list(context.get("assumptions_used") or [])
    if assumptions:
        lines.extend(["", "📝 **Assumptions Used:**"])
        lines.extend(f"• {item}" for item in assumptions[:4])

    if memory_lines:
        lines.extend(["", "🧠 **Project Memory Guidance:**"])
        lines.extend(f"• {line}" for line in memory_lines)

    return "\n".join(lines)


def show_copilot_analysis(
    console: Any,
    *,
    context: Mapping[str, Any],
    suggestions: Mapping[str, Any],
    use_case_label: str,
    memory_lines: Sequence[str],
) -> None:
    """Render the copilot AI analysis panel."""
    if not console or not RICH_AVAILABLE:
        return
    text = build_copilot_analysis_text(
        context=context,
        suggestions=suggestions,
        use_case_label=use_case_label,
        memory_lines=memory_lines,
    )
    console.print(_build_panel(text.strip(), title="🧠 AI Analysis", border_style="blue"))


def build_domain_analysis_text(
    *,
    goal: str,
    data_sources: str,
    product_type: str,
    suggestions: Mapping[str, Any],
    domain: str,
) -> str:
    """Build the shared domain-agent analysis body."""
    patterns = ", ".join(suggestions.get("recommended_patterns", []) or ["Standard scaffolding"])
    return (
        f"🎯 **Project Goal:** {goal}\n"
        f"📊 **Data Sources:** {data_sources}\n"
        f"🏷️ **Domain Focus:** {product_type}\n\n"
        "🤖 **Recommendations:**\n"
        f"• Template: {suggestions.get('recommended_template')}\n"
        f"• Provider: {suggestions.get('recommended_provider')}\n"
        f"• Patterns: {patterns}\n\n"
        f"[dim]Optimized for {domain} workflows and guardrails.[/dim]"
    )


def show_domain_analysis(
    console: Any,
    *,
    goal: str,
    data_sources: str,
    product_type: str,
    suggestions: Mapping[str, Any],
    domain: str,
) -> None:
    """Render the domain-agent analysis panel."""
    if not console or not RICH_AVAILABLE:
        return
    console.print(
        _build_panel(
            build_domain_analysis_text(
                goal=goal,
                data_sources=data_sources,
                product_type=product_type,
                suggestions=suggestions,
                domain=domain,
            ).strip(),
            title="🧠 AI Analysis",
            border_style="blue",
        )
    )


def build_standard_next_steps(
    *,
    target_dir: Optional[Path] = None,
    provider: str = "local",
) -> str:
    """Build the shared official-command next-steps text."""
    lines: list[str] = []
    if target_dir:
        contract_path = target_dir / "contract.fluid.yaml"
        lines.extend(
            [
                f"[bold]Project folder:[/bold]  {target_dir}",
                f"[bold]Contract file:[/bold]   {contract_path}",
                "",
                "[bold]Next steps:[/bold]",
                f"  cd {target_dir}",
                f"  {FORGE_VALIDATE_COMMAND}",
            ]
        )
    else:
        lines.extend(
            [
                "[bold]Next steps:[/bold]",
                f"  {FORGE_VALIDATE_COMMAND}",
            ]
        )
    return "\n".join(lines)


def show_next_steps_panel(
    console: Any,
    *,
    target_dir: Optional[Path] = None,
    provider: str = "local",
) -> None:
    """Render the shared next-steps panel."""
    if not console or not RICH_AVAILABLE:
        return
    text = build_standard_next_steps(
        target_dir=target_dir,
        provider=provider,
    )
    console.print(_build_panel(text.strip(), title="Forge Complete", border_style="green"))


def build_blueprint_next_steps(*, target_dir: Optional[Path] = None) -> str:
    """Build the shared blueprint next-steps panel body."""
    lines: list[str] = []
    if target_dir:
        contract_path = target_dir / "contract.fluid.yaml"
        lines.extend(
            [
                f"[bold]Project folder:[/bold]  {target_dir}",
                f"[bold]Contract file:[/bold]   {contract_path}",
                "",
                "[bold]Next steps:[/bold]",
                f"  cd {target_dir}",
                f"  {FORGE_VALIDATE_COMMAND}",
            ]
        )
    else:
        lines.extend(
            [
                "[bold]Next steps:[/bold]",
                f"  {FORGE_VALIDATE_COMMAND}",
            ]
        )
    return "\n".join(lines)


def show_blueprint_next_steps(console: Any, target_dir: Optional[Path] = None) -> None:
    """Render the blueprint next-steps panel."""
    if not console or not RICH_AVAILABLE:
        return
    console.print(
        _build_panel(
            build_blueprint_next_steps(target_dir=target_dir).strip(),
            title="Forge Complete",
            border_style="green",
        )
    )
