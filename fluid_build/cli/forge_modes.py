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

"""Mode handlers for `fluid forge`."""

from __future__ import annotations

__all__ = [
    "run_ai_copilot_mode",
    "run_blueprint_mode",
    "run_domain_agent_mode",
    "run_forge_blueprint_impl",
    "run_template_mode",
]


import logging
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

from fluid_build.cli.console import cprint, success, warning
from fluid_build.cli.console import error as console_error
from fluid_build.cli.forge_copilot_interview import (
    InterviewQuestion,
    run_adaptive_copilot_interview,
)
from fluid_build.cli.forge_copilot_llm_providers import (
    PROVIDER_DISPLAY_NAMES,
    CopilotGenerationError,
    LlmConfig,
    check_llm_readiness,
    detect_provider_from_api_key,
    get_catalog_default,
    get_llm_provider,
    resolve_ollama_model,
    save_api_key_to_keyring,
)
from fluid_build.cli.forge_copilot_taxonomy import normalize_copilot_context
from fluid_build.cli.forge_dialogs import (
    ask_confirmation,
    ask_dialog_question,
    ask_secret_text,
    print_dialog_status,
)
from fluid_build.cli.forge_ui import (
    print_assumptions_panel,
    print_copilot_intro_panel,
    print_copilot_recovery_panel,
    print_free_tier_guide,
    print_welcome_panel,
    show_blueprint_next_steps,
)

try:
    from rich.console import Console
    from rich.table import Table

    RICH_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised via non-Rich fallbacks
    Console = None  # type: ignore[assignment]
    Table = None  # type: ignore[assignment]
    RICH_AVAILABLE = False


def _create_session_llm_config(
    console: Any,
    *,
    default_provider: str = "openai",
    ask_dialog_question_fn: Callable[[Any, Any], Any] = ask_dialog_question,
    ask_secret_text_fn: Callable[..., Optional[str]] = ask_secret_text,
) -> Optional[LlmConfig]:
    """Collect a session-only LLM configuration via an API-key-first flow.

    The user pastes an API key and the provider is auto-detected from its
    format.  If detection fails, a short follow-up asks which provider the
    key belongs to.  Model and endpoint always use sensible provider defaults
    so the user never has to type them.
    """
    if console:
        console.print(
            "[dim]Paste an API key for OpenAI, Anthropic (Claude), or Google Gemini "
            "and I'll detect the provider automatically.\n"
            "Type [bold]ollama[/bold] if you want to use a local model instead.[/dim]"
        )

    api_key = ask_secret_text_fn(
        console,
        "API key for OpenAI / Anthropic / Gemini (or 'ollama')",
        required=True,
    )

    raw = (api_key or "").strip().lower()

    # --- Ollama shortcut ---
    if raw in ("ollama", "local", "ollama/local", "ollama (local)"):
        provider = get_llm_provider("ollama")
        model = resolve_ollama_model(os.environ)
        endpoint = provider.default_endpoint(model, os.environ)
        print_dialog_status(
            console,
            status="success",
            message=f"Using Ollama -- {model}",
        )
        return LlmConfig(provider="ollama", model=model, endpoint=endpoint, api_key=None)

    if not api_key:
        if console:
            print_dialog_status(
                console,
                status="error",
                message="A hosted provider needs an API key for this run.",
                detail="You can choose a different mode or try Ollama for a local setup.",
            )
        return None

    # --- Auto-detect provider from key format ---
    detected = detect_provider_from_api_key(api_key)
    if detected:
        provider_name = detected
    else:
        # Key format not recognised — ask which provider
        provider_question = InterviewQuestion(
            id="llm_provider",
            field="llm_provider",
            prompt="Which provider is this key for?",
            type="choice",
            choices=[
                {"label": "OpenAI", "value": "openai"},
                {"label": "Anthropic (Claude)", "value": "anthropic"},
                {"label": "Google Gemini", "value": "gemini"},
            ],
            required=True,
            allow_skip=False,
            default=default_provider,
        )
        selection = ask_dialog_question_fn(console, provider_question)
        provider_name = str(selection.value or default_provider or "openai").strip().lower()

    provider = get_llm_provider(provider_name)
    model = get_catalog_default(provider_name) or provider.default_model
    endpoint = provider.default_endpoint(model, os.environ)
    display = PROVIDER_DISPLAY_NAMES.get(provider_name, provider_name)

    if console:
        print_dialog_status(
            console,
            status="success",
            message=f"Detected {display} -- using {model}",
        )

    # Persist the key in the OS keychain so future runs resolve silently.
    if api_key and provider_name != "ollama":
        saved = save_api_key_to_keyring(provider_name, api_key)
        if console:
            if saved:
                print_dialog_status(
                    console,
                    status="info",
                    message="Key saved to your system keychain for future runs.",
                    detail="Use --llm-reauth to change it later.",
                )
            else:
                print_dialog_status(
                    console,
                    status="warning",
                    message="Could not save key to keychain (keyring unavailable).",
                    detail="Set an env var like OPENAI_API_KEY for persistence, or re-run the wizard next time.",
                )

    return LlmConfig(provider=provider.name, model=model, endpoint=endpoint, api_key=api_key)


def _choose_recovery_mode(
    console: Any,
    *,
    fallback_mode_choices: Sequence[Mapping[str, str]],
    ask_dialog_question_fn: Callable[[Any, Any], Any] = ask_dialog_question,
) -> Optional[str]:
    """Ask the user which non-copilot mode to use instead."""
    if not fallback_mode_choices:
        return None
    print_welcome_panel(console)
    question = InterviewQuestion(
        id="fallback_mode",
        field="fallback_mode",
        prompt="Which creation mode would you like to use instead?",
        type="choice",
        choices=list(fallback_mode_choices),
        required=False,
        allow_skip=True,
        default=str(fallback_mode_choices[0].get("value") or ""),
    )
    selection = ask_dialog_question_fn(console, question)
    return str(selection.value or fallback_mode_choices[0].get("value") or "").strip() or None


def _handle_copilot_recovery(
    *,
    args: Any,
    console: Any,
    error: CopilotGenerationError,
    llm_readiness_fn: Callable[[Any], Any],
    route_mode_fn: Optional[Callable[[str], int]],
    fallback_mode_choices: Sequence[Mapping[str, str]],
    ask_dialog_question_fn: Callable[[Any, Any], Any],
    ask_secret_text_fn: Callable[..., Optional[str]],
) -> Dict[str, Any] | int:
    """Offer session-only setup first, then alternate modes if the user declines."""
    if console:
        print_copilot_recovery_panel(
            console,
            message=error.message,
            suggestions=error.suggestions,
        )
        print_free_tier_guide(console)

    wants_setup = ask_confirmation(
        console,
        "Set up AI for this run now?",
        default=True,
        title="Copilot Setup Needed",
        preview=(
            "Forge needs a working LLM to power copilot.\n"
            "You can fix this permanently by setting an env var:\n"
            "  export OPENAI_API_KEY=sk-...       (OpenAI)\n"
            "  export ANTHROPIC_API_KEY=sk-ant-... (Claude)\n"
            "  export GEMINI_API_KEY=AIza...       (Gemini)\n\n"
            "Or choose Yes and I'll ask for a key just for this run."
        ),
        border_style="yellow",
    )
    if wants_setup:
        default_provider = getattr(llm_readiness_fn(args), "provider", "openai") or "openai"
        llm_config = _create_session_llm_config(
            console,
            default_provider=default_provider,
            ask_dialog_question_fn=ask_dialog_question_fn,
            ask_secret_text_fn=ask_secret_text_fn,
        )
        if llm_config:
            if console:
                print_dialog_status(
                    console,
                    status="success",
                    message=f"{llm_config.provider.title()} is configured for this run.",
                    detail="Continuing into AI Copilot.",
                )
            return {"llm_config": llm_config}

    selected_mode = _choose_recovery_mode(
        console,
        fallback_mode_choices=fallback_mode_choices,
        ask_dialog_question_fn=ask_dialog_question_fn,
    )
    if selected_mode and route_mode_fn:
        return route_mode_fn(selected_mode)

    if console:
        print_dialog_status(
            console,
            status="error",
            message=error.message,
            detail="Copilot setup was skipped and no alternate mode was selected.",
        )
    return 1


def _print_discovery_summary(console: Any, discovery: Any) -> None:
    """Show a one-liner about what the discovery scanner found."""
    if not console:
        return
    samples = len(getattr(discovery, "sample_files", None) or [])
    sqls = len(getattr(discovery, "sql_files", None) or [])
    contracts = len(getattr(discovery, "existing_contracts", None) or [])
    parts: List[str] = []
    if samples:
        parts.append(f"{samples} data file{'s' if samples != 1 else ''}")
    if sqls:
        parts.append(f"{sqls} SQL file{'s' if sqls != 1 else ''}")
    if contracts:
        parts.append(f"{contracts} existing contract{'s' if contracts != 1 else ''}")
    if parts:
        console.print(
            f"[dim]Data: found {', '.join(parts)} -- schemas will guide contract generation[/dim]"
        )
    else:
        _print_discovery_hint(console)


def _print_mode_awareness(console: Any) -> None:
    """Show what mode forge is in and list alternatives.

    Displayed only when the user ran ``fluid forge`` without ``--mode``
    inside an existing workspace, so they know other modes exist.
    """
    if not console:
        return
    try:
        from rich.panel import Panel  # noqa: F811 — local import for optional dep

        console.print(
            Panel(
                "[bold]Forge — Add a data product[/bold]\n\n"
                "Mode: [cyan]AI Copilot[/cyan] [dim](default)[/dim]\n\n"
                "[dim]Other modes available:[/dim]\n"
                "  [cyan]fluid forge --mode template[/cyan]   "
                "[dim]← from a pre-built template[/dim]\n"
                "  [cyan]fluid forge --mode agent[/cyan]      "
                "[dim]← domain expert (finance, healthcare)[/dim]\n"
                "  [cyan]fluid forge --mode blueprint[/cyan]  "
                "[dim]← enterprise patterns[/dim]\n"
                "  [cyan]fluid forge --blank[/cyan]           "
                "[dim]← empty contract[/dim]",
                border_style="bright_magenta",
            )
        )
    except ImportError:
        pass


def _apply_workspace_defaults(context: Dict[str, Any], console: Any) -> None:
    """Read ``fluid.workspace.yaml`` and inject shared defaults into *context*."""
    try:
        from fluid_build.cli.workspace_config import (
            discover_workspace_products,
            find_workspace_root,
            load_workspace_config,
        )

        ws_root = find_workspace_root()
        if ws_root is None:
            return
        ws = load_workspace_config(ws_root)
        if ws.is_empty:
            return

        # Show existing products.
        products = discover_workspace_products(ws_root)
        if products and console:
            console.print(
                f"[dim]📂 Workspace: [bold]{ws.name or ws_root.name}[/bold] "
                f"({len(products)} product{'s' if len(products) != 1 else ''})[/dim]"
            )
            for p in products[:8]:
                parts = [p.name]
                if p.expose_count:
                    parts.append(f"{p.expose_count} expose{'s' if p.expose_count != 1 else ''}")
                if p.provider:
                    parts.append(f"provider: {p.provider}")
                console.print(f"[dim]  • {', '.join(parts)}[/dim]")
            console.print()

        # Inject defaults (don't overwrite explicit values).
        if ws.domain and "domain" not in context:
            context["domain"] = ws.domain
        if ws.provider and "provider" not in context:
            context["provider"] = ws.provider
        if ws.owner_team and "owner_team" not in context:
            context["owner_team"] = ws.owner_team

        if console and (ws.domain or ws.provider or ws.owner_team):
            parts = []
            if ws.domain:
                parts.append(f"domain={ws.domain}")
            if ws.owner_team:
                parts.append(f"team={ws.owner_team}")
            if ws.provider:
                parts.append(f"provider={ws.provider}")
            console.print(f"[dim]Using workspace defaults: {', '.join(parts)}[/dim]\n")
    except Exception:  # noqa: BLE001
        pass  # Workspace config is optional — never block on it.


def _print_discovery_hint(console: Any) -> None:
    """Nudge the user to add sample data for better contracts."""
    if not console:
        return
    console.print(
        "[dim]Tip: drop sample CSV, Parquet, or JSON files in this directory "
        "(or use [bold]--discovery-path[/bold]) and copilot will use their schemas[/dim]"
    )


def run_ai_copilot_mode(
    args: Any,
    logger: logging.Logger,
    *,
    copilot_class: type,
    get_cli_arg_fn: Callable[[Any, str, Any], Any],
    load_context_fn: Callable[..., Dict[str, Any]],
    get_target_directory_fn: Callable[[Any, str], Path],
    context_error_cls: type[Exception],
    build_interview_summary_fn: Callable[[Mapping[str, Any]], Dict[str, Any]],
    console_factory: Optional[Callable[[], Any]] = Console if RICH_AVAILABLE else None,
    llm_readiness_fn: Callable[[Any], Any] = check_llm_readiness,
    ask_dialog_question_fn: Callable[[Any, Any], Any] = ask_dialog_question,
    ask_secret_text_fn: Callable[..., Optional[str]] = ask_secret_text,
    route_mode_fn: Optional[Callable[[str], int]] = None,
    fallback_mode_choices: Sequence[Mapping[str, str]] = (),
) -> int:
    """Run Forge with AI copilot assistance."""
    console = console_factory() if console_factory else None

    try:
        copilot = copilot_class()
        is_non_interactive = bool(get_cli_arg_fn(args, "non_interactive", False))
        enable_recovery = bool(get_cli_arg_fn(args, "_enable_copilot_recovery", False))

        context: Dict[str, Any] = {}

        # Inherit workspace defaults (domain, provider, owner) if available.
        _apply_workspace_defaults(context, console)

        # Show mode awareness when inside a workspace with implicit mode.
        implicit_mode = bool(get_cli_arg_fn(args, "_implicit_mode", False))
        if implicit_mode and not is_non_interactive and console:
            _print_mode_awareness(console)

        copilot_options = {
            "llm_provider": get_cli_arg_fn(args, "llm_provider"),
            "llm_model": get_cli_arg_fn(args, "llm_model"),
            "llm_endpoint": get_cli_arg_fn(args, "llm_endpoint"),
            "discover": get_cli_arg_fn(args, "discover", True),
            "discovery_path": get_cli_arg_fn(args, "discovery_path"),
            "memory": get_cli_arg_fn(args, "memory", True),
            "save_memory": get_cli_arg_fn(args, "save_memory", False),
            "non_interactive": is_non_interactive,
        }

        context_arg = get_cli_arg_fn(args, "context")
        if context_arg:
            try:
                loaded_context = load_context_fn(
                    context_arg,
                    console,
                    context_error_cls=context_error_cls,
                )
                context.update(loaded_context)
                if console:
                    print_dialog_status(console, status="success", message="Loaded extra context.")
            except context_error_cls as exc:
                if console:
                    print_dialog_status(
                        console,
                        status="error",
                        message=f"Couldn't use the context file: {exc}",
                        detail="Continuing without it for now.",
                    )
                else:
                    logger.warning("Context validation failed: %s", exc)

        if get_cli_arg_fn(args, "provider"):
            context["provider"] = get_cli_arg_fn(args, "provider")
        if get_cli_arg_fn(args, "template"):
            context["template"] = get_cli_arg_fn(args, "template")
        if get_cli_arg_fn(args, "domain") and "domain" not in context:
            context["domain"] = get_cli_arg_fn(args, "domain")
        explicit_target_dir = get_cli_arg_fn(args, "target_dir")
        if explicit_target_dir:
            copilot_options["target_dir"] = str(Path(explicit_target_dir).expanduser())

        force_llm_setup = bool(get_cli_arg_fn(args, "_force_llm_setup", False))
        if not is_non_interactive and (enable_recovery or force_llm_setup):
            needs_setup = force_llm_setup
            readiness_error = None
            if not force_llm_setup:
                readiness = llm_readiness_fn(args)
                needs_setup = not readiness.ready and readiness.error is not None
                readiness_error = readiness.error if needs_setup else None
            if needs_setup:
                # For --llm-reauth, synthesise a minimal error to enter the wizard.
                if readiness_error is None:
                    readiness_error = CopilotGenerationError(
                        "copilot_llm_reauth",
                        "Re-authenticating LLM credentials.",
                        suggestions=["Choose a provider and paste a new API key."],
                    )
                recovery_result = _handle_copilot_recovery(
                    args=args,
                    console=console,
                    error=readiness_error,
                    llm_readiness_fn=llm_readiness_fn,
                    route_mode_fn=route_mode_fn,
                    fallback_mode_choices=fallback_mode_choices,
                    ask_dialog_question_fn=ask_dialog_question_fn,
                    ask_secret_text_fn=ask_secret_text_fn,
                )
                if isinstance(recovery_result, int):
                    return recovery_result
                copilot_options.update(recovery_result)

        if not is_non_interactive:
            runtime_inputs = copilot.prepare_runtime_inputs(copilot_options)
            copilot_options.update(runtime_inputs)
            if console:
                llm_cfg = runtime_inputs.get("llm_config")
                if llm_cfg:
                    display = PROVIDER_DISPLAY_NAMES.get(llm_cfg.provider, llm_cfg.provider)
                    console.print(
                        f"[dim]AI: [bold]{display}[/bold] / {llm_cfg.model}  "
                        f"(change with [bold]fluid forge --llm-reauth[/bold])[/dim]"
                    )
                discovery = runtime_inputs.get("discovery_report")
                if discovery:
                    _print_discovery_summary(console, discovery)
                else:
                    _print_discovery_hint(console)
                console.print()
                print_copilot_intro_panel(console)
                console.print(
                    "[dim]I'll help you create the perfect data product by understanding your needs...[/dim]\n"
                )
            capability_warnings = list(runtime_inputs.get("capability_warnings") or [])
            if console and capability_warnings:
                print_dialog_status(
                    console,
                    status="warning",
                    message="Copilot couldn't fully verify some local providers.",
                    detail=(
                        f"{capability_warnings[0]} "
                        "Continuing with best-effort defaults. You can review or override the provider later."
                    ),
                )
            interview_state = run_adaptive_copilot_interview(
                initial_context=context,
                console=console,
                llm_config=runtime_inputs["llm_config"],
                discovery_report=runtime_inputs["discovery_report"],
                capability_matrix=runtime_inputs["capability_matrix"],
                project_memory=runtime_inputs["project_memory"],
            )
            copilot_options["interview_state"] = interview_state
            context = interview_state.finalize()
            assumptions = list(context.get("assumptions_used") or [])
            if console and assumptions:
                print_assumptions_panel(console, assumptions)
        else:
            for key, value in {
                "project_goal": "Data Analytics Platform",
                "data_sources": "Database tables",
                "use_case": "analytics",
                "complexity": "intermediate",
            }.items():
                context.setdefault(key, value)
            context["interview_summary"] = build_interview_summary_fn(context)

        context = normalize_copilot_context(context)
        project_name = context.get("project_goal", "my-data-product").lower().replace(" ", "-")
        target_dir = get_target_directory_fn(args, project_name)
        copilot_options["target_dir"] = str(target_dir)

        success_result = copilot.create_project(
            target_dir,
            context,
            copilot_options,
            dry_run=bool(get_cli_arg_fn(args, "dry_run", False)),
        )
        return 0 if success_result else 1
    except CopilotGenerationError as exc:
        logger.exception("AI Copilot mode failed")
        if console:
            console.print(f"[red]❌ AI Copilot failed: {exc.message}[/red]")
            for suggestion in exc.suggestions:
                console.print(f"[dim]• {suggestion}[/dim]")
        else:
            console_error(f"AI Copilot failed: {exc.message}")
            for suggestion in exc.suggestions:
                cprint(f"  • {suggestion}")
        return 1
    except Exception as exc:  # noqa: BLE001
        logger.exception("AI Copilot mode failed")
        if console:
            console.print(f"[red]❌ AI Copilot failed: {exc}[/red]")
        return 1


def run_domain_agent_mode(
    args: Any,
    logger: logging.Logger,
    *,
    ai_agents: Mapping[str, type],
    gather_context_fn: Callable[[Any, Any], Dict[str, Any]],
    load_context_fn: Callable[..., Dict[str, Any]],
    get_target_directory_fn: Callable[[Any, str], Path],
    context_error_cls: type[Exception],
    console_factory: Optional[Callable[[], Any]] = Console if RICH_AVAILABLE else None,
) -> int:
    """Run Forge with a specialized domain agent."""
    console = console_factory() if console_factory else None

    try:
        agent_name = args.agent

        if not agent_name:
            if console and not args.non_interactive and Table is not None:
                console.print("\n[bold blue]🎯 Available Domain Agents[/bold blue]")
                table = Table()
                table.add_column("Agent", style="cyan")
                table.add_column("Domain", style="green")
                table.add_column("Description", style="white")
                for name, agent_class in ai_agents.items():
                    agent_instance = agent_class()
                    table.add_row(name, agent_instance.domain, agent_instance.description)
                console.print(table)

                from fluid_build.cli.forge_copilot_interview import InterviewQuestion
                from fluid_build.cli.forge_dialogs import ask_dialog_question

                selection = ask_dialog_question(
                    console,
                    InterviewQuestion(
                        id="agent",
                        field="agent",
                        prompt="Which agent would you like to use?",
                        type="choice",
                        choices=[{"label": name, "value": name} for name in ai_agents.keys()],
                        required=False,
                        allow_skip=True,
                        default="copilot",
                    ),
                )
                agent_name = selection.value or "copilot"
            else:
                agent_name = "copilot"

        if agent_name not in ai_agents:
            if console:
                console.print(f"[red]❌ Unknown agent: {agent_name}[/red]")
                console.print(f"[dim]Available agents: {', '.join(ai_agents.keys())}[/dim]")
            return 1

        agent = ai_agents[agent_name]()

        if console and not args.non_interactive:
            console.print(f"\n[bold blue]🎯 Starting {agent.name.title()} Domain Agent[/bold blue]")
            console.print(f"[dim]{agent.description}[/dim]\n")

        context: Dict[str, Any] = {}
        if args.context:
            try:
                from fluid_build.cli.forge_validation import validate_context_dict

                loaded_context = load_context_fn(
                    args.context,
                    console,
                    context_error_cls=context_error_cls,
                )
                is_valid, error = validate_context_dict(loaded_context)
                if is_valid:
                    context.update(loaded_context)
                    if console:
                        print_dialog_status(
                            console, status="success", message="Loaded extra context."
                        )
                elif console:
                    print_dialog_status(
                        console,
                        status="warning",
                        message=f"Context loaded with a warning: {error}",
                    )
            except context_error_cls as exc:
                if console:
                    print_dialog_status(
                        console,
                        status="error",
                        message=f"Couldn't use the context file: {exc}",
                    )

        if not args.non_interactive:
            context.update(gather_context_fn(agent, console))
        else:
            context = {
                "project_goal": f"{agent.domain.title()} Data Product",
                "data_sources": "Various sources",
                "use_case": "analytics",
                "complexity": "intermediate",
            }

        suggestions = agent.analyze_requirements(context)
        if console and not args.non_interactive:
            console.print("\n[bold green]🤖 Agent Analysis Complete[/bold green]\n")
            console.print(
                f"[cyan]Recommended Template:[/cyan] {suggestions.get('recommended_template')}"
            )
            console.print(
                f"[cyan]Recommended Provider:[/cyan] {suggestions.get('recommended_provider')}"
            )
            if suggestions.get("security_requirements"):
                console.print("\n[yellow]🔒 Security Requirements:[/yellow]")
                for requirement in suggestions["security_requirements"][:3]:
                    console.print(f"  • {requirement}")
            console.print()

        project_name = (
            context.get("project_goal", f"{agent.domain}-data-product").lower().replace(" ", "-")
        )
        from fluid_build.cli.forge_validation import sanitize_project_name

        target_dir = get_target_directory_fn(args, sanitize_project_name(project_name))
        success_result = agent.create_project(target_dir, context)
        return 0 if success_result else 1
    except Exception as exc:  # noqa: BLE001
        logger.exception("Domain agent mode failed")
        if console:
            console.print(f"[red]❌ Domain agent failed: {exc}[/red]")
        return 1


def run_template_mode(
    args: Any,
    logger: logging.Logger,
    *,
    get_target_directory_fn: Callable[[Any, str], Path],
    console_factory: Optional[Callable[[], Any]] = Console if RICH_AVAILABLE else None,
) -> int:
    """Run Forge with traditional template mode."""
    console = console_factory() if console_factory else None

    try:
        if console and not args.non_interactive:
            console.print("\n[bold blue]📋 Template Mode[/bold blue]")
            console.print("[dim]Creating project from template...[/dim]\n")

        from datetime import datetime

        from fluid_build.forge.core.engine import ForgeEngine, GenerationContext
        from fluid_build.forge.core.registry import template_registry

        template_name = args.template or "starter"
        target_dir = get_target_directory_fn(args, f"{template_name}-project")
        provider = args.provider or "local"
        template = template_registry.get(template_name)
        if not template:
            available = template_registry.list_available()
            logger.error(
                "Template '%s' not found. Available templates: %s",
                template_name,
                ", ".join(available),
            )
            return 1

        metadata = template.get_metadata()
        context = GenerationContext(
            project_config={
                "name": target_dir.name,
                "description": f"A {template_name} data product",
                "domain": "analytics",
                "owner": "data-team",
                "provider": provider,
            },
            target_dir=target_dir,
            template_metadata=metadata,
            provider_config={"provider": provider},
            user_selections={},
            forge_version="2.0.0",
            creation_time=datetime.now().isoformat(),
        )

        ForgeEngine()
        logger.info("📝 Generating %s project...", template_name)

        if args.dry_run if hasattr(args, "dry_run") else False:
            logger.info("DRY RUN: Would create project in %s", target_dir)
            logger.info("Template: %s", metadata.name)
            logger.info("Description: %s", metadata.description)
            return 0

        target_dir.mkdir(parents=True, exist_ok=True)
        contract = template.generate_contract(context)
        import yaml

        with open(target_dir / "contract.fluid.yaml", "w") as handle:
            yaml.dump(contract, handle, default_flow_style=False, sort_keys=False)

        for path_str, content in template.generate_structure(context).items():
            if path_str.endswith("/"):
                (target_dir / path_str.rstrip("/")).mkdir(parents=True, exist_ok=True)

        try:
            template._create_readme(target_dir, context)
        except (AttributeError, TypeError):
            pass

        if console:
            console.print(f"[green]✅ Template project created at {target_dir}[/green]")
        else:
            success(f"Template project created at {target_dir}")

        logger.info("\n📖 Next Steps:")
        logger.info("1. cd %s", target_dir)
        logger.info("2. Review contract.fluid.yaml")
        logger.info("3. fluid validate contract.fluid.yaml")
        return 0
    except Exception as exc:  # noqa: BLE001
        logger.exception("Template mode failed")
        if console:
            console.print(f"[red]❌ Template mode failed: {exc}[/red]")
        else:
            console_error(f"Template mode failed: {exc}")
        return 1


def run_blueprint_mode(
    args: Any,
    logger: logging.Logger,
    *,
    blueprint_registry: Any,
    get_target_directory_fn: Callable[[Any, str], Path],
    ask_confirmation_fn: Callable[..., bool] = ask_confirmation,
    console_factory: Optional[Callable[[], Any]] = Console if RICH_AVAILABLE else None,
) -> int:
    """Run Forge with enterprise blueprint mode."""
    console = console_factory() if console_factory else None

    try:
        if console and not args.non_interactive:
            console.print("\n[bold blue]🏗️  Blueprint Mode[/bold blue]")
            console.print("[dim]Creating enterprise data product from blueprint...[/dim]\n")

        blueprint_name = args.blueprint or "customer-360-gcp"
        blueprint = blueprint_registry.get_blueprint(blueprint_name)
        if not blueprint:
            available = blueprint_registry.list_blueprints()
            if console:
                console.print(f"[red]❌ Blueprint '{blueprint_name}' not found[/red]")
                console.print("\n[bold]Available blueprints:[/bold]")
                for bp in available:
                    console.print(f"  • {bp.metadata.name} - {bp.metadata.title}")
            else:
                console_error(f"Blueprint '{blueprint_name}' not found")
                cprint("\nAvailable blueprints:")
                for bp in available:
                    cprint(f"  • {bp.metadata.name} - {bp.metadata.title}")
            return 1

        target_dir = get_target_directory_fn(args, blueprint_name)
        if target_dir.exists() and any(target_dir.iterdir()):
            if console:
                console.print(
                    f"[yellow]⚠️  Directory {target_dir} already exists and is not empty[/yellow]"
                )
            else:
                warning(f"Directory {target_dir} already exists and is not empty")

            if not args.non_interactive:
                if console:
                    if not ask_confirmation_fn(
                        console,
                        "Continue and overwrite the existing directory?",
                        default=False,
                    ):
                        return 1
                else:
                    response = input("Continue and overwrite? [y/N]: ")
                    if response.lower() != "y":
                        return 1
            else:
                return 1

        blueprint.generate_project(target_dir)

        if console:
            console.print(f"[green]✅ Blueprint project created at {target_dir}[/green]")
            console.print(
                f"[dim]{blueprint.metadata.title} - {blueprint.metadata.description}[/dim]\n"
            )
            show_blueprint_next_steps(console)
        else:
            success(f"Blueprint project created at {target_dir}")
            cprint(f"{blueprint.metadata.title} - {blueprint.metadata.description}\n")
            cprint("Next steps:")
            cprint("  1. fluid validate contract.fluid.yaml")
            cprint("  2. fluid plan contract.fluid.yaml --out runtime/plan.json")
            cprint("  3. fluid apply runtime/plan.json\n")
        return 0
    except Exception as exc:  # noqa: BLE001
        logger.exception("Blueprint mode failed")
        if console:
            console.print(f"[red]❌ Blueprint mode failed: {exc}[/red]")
        else:
            console_error(f"Blueprint mode failed: {exc}")
        return 1


def run_forge_blueprint_impl(
    args: Any,
    blueprint_registry: Any,
    *,
    get_target_directory_fn: Callable[[Any, str], Path],
) -> int:
    """Legacy blueprint execution path retained for compatibility."""
    logger = logging.getLogger(__name__)

    try:
        blueprint = blueprint_registry.get_blueprint(args.blueprint)
        if not blueprint:
            logger.error("Blueprint '%s' not found", args.blueprint)
            logger.info("Available blueprints:")
            for bp in blueprint_registry.list_blueprints():
                logger.info("  - %s: %s", bp.metadata.name, bp.metadata.title)
            return 1

        target_dir = get_target_directory_fn(args, args.blueprint)
        if target_dir.exists() and any(target_dir.iterdir()):
            if not args.non_interactive:
                response = input(
                    f"Directory {target_dir} exists and is not empty. Continue? (y/N): "
                )
                if response.lower() != "y":
                    logger.info("Operation cancelled")
                    return 1
            else:
                logger.error("Target directory %s exists and is not empty", target_dir)
                return 1

        errors = blueprint.validate()
        if errors:
            logger.error("Blueprint validation failed:")
            for error in errors:
                logger.error("  - %s", error)
            return 1

        if not args.non_interactive:
            logger.info("📋 Blueprint: %s", blueprint.metadata.title)
            logger.info("   Description: %s", blueprint.metadata.description)
            logger.info("   Complexity: %s", blueprint.metadata.complexity.value)
            logger.info("   Setup Time: %s", blueprint.metadata.setup_time)
            logger.info("   Providers: %s", ", ".join(blueprint.metadata.providers))
            if not args.quickstart:
                response = input("\nContinue with blueprint deployment? (Y/n): ")
                if response.lower() == "n":
                    logger.info("Operation cancelled")
                    return 1

        logger.info("🚀 Generating project from blueprint '%s'...", blueprint.metadata.name)
        if args.dry_run:
            logger.info("DRY RUN: Would create project in %s", target_dir)
            logger.info("Files that would be created:")
            for file_path in blueprint.path.rglob("*"):
                if file_path.is_file() and file_path.name != "blueprint.yaml":
                    logger.info("  - %s", file_path.relative_to(blueprint.path))
            return 0

        blueprint.generate_project(target_dir)
        logger.info("✅ Blueprint '%s' deployed successfully!", blueprint.metadata.name)
        logger.info("📁 Project created in: %s", target_dir)
        logger.info("\n📖 Next Steps:")
        logger.info("1. cd %s", target_dir)
        logger.info("2. Review the generated files and documentation")
        logger.info("3. Configure your data sources")
        logger.info("4. Run: fluid validate")
        logger.info("5. Run: dbt run (if using dbt)")
        return 0
    except Exception as exc:  # noqa: BLE001
        logger.error("Blueprint deployment failed: %s", exc, exc_info=True)
        return 1
