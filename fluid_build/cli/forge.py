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

"""Public Forge CLI entrypoint — AI-powered data product creation.

After the UX redesign, ``fluid forge`` is the **repeatable** command for
creating new data products inside an existing project.  It defaults to the
AI Copilot interview and has a ``--blank`` escape hatch for users who want
a bare contract scaffold without AI.

First-time project setup lives in ``fluid init``.
"""

from __future__ import annotations

import argparse
import logging
import warnings
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from fluid_build.cli.console import cprint
from fluid_build.cli.console import error as console_error
from fluid_build.cli.forge_agents import DOMAIN_AGENTS
from fluid_build.cli.forge_context import (
    gather_copilot_context as _gather_context,
)
from fluid_build.cli.forge_context import (
    get_cli_arg as _get_cli_arg,
)
from fluid_build.cli.forge_context import (
    get_target_directory as _get_target_dir,
)
from fluid_build.cli.forge_context import (
    handle_memory_management as _handle_memory,
)
from fluid_build.cli.forge_context import (
    load_context as _load_ctx,
)
from fluid_build.cli.forge_context import (
    resolve_memory_store as _resolve_store,
)
from fluid_build.cli.forge_copilot_agent import (
    AIAgent,
    CopilotAgentBase,
    recommend_template_for_use_case,
)
from fluid_build.cli.forge_copilot_interview import build_interview_summary_from_context
from fluid_build.cli.forge_copilot_llm_providers import check_llm_readiness
from fluid_build.cli.forge_copilot_memory import (
    CopilotMemoryStore,
    resolve_copilot_memory_root,
    summarize_copilot_memory,
)
from fluid_build.cli.forge_copilot_runtime import (
    CopilotGenerationError,
    CopilotGenerationResult,
    build_capability_matrix,
    discover_local_context,
    generate_copilot_artifacts,
    normalize_provider_name,
    normalize_template_name,
    resolve_llm_config,
)
from fluid_build.cli.forge_copilot_taxonomy import normalize_copilot_context
from fluid_build.cli.forge_dialogs import ask_confirmation
from fluid_build.cli.forge_ui import print_welcome_panel
from fluid_build.cli.forge_modes import (
    run_ai_copilot_mode as _run_copilot,
)
from fluid_build.cli.forge_modes import (
    run_blueprint_mode as _run_blueprint,
)
from fluid_build.cli.forge_modes import (
    run_domain_agent_mode as _run_agent,
)
from fluid_build.cli.forge_modes import (
    run_forge_blueprint_impl as _run_blueprint_legacy,
)
from fluid_build.cli.forge_modes import (
    run_guided_mode as _run_guided,
)
from fluid_build.cli.forge_modes import (
    run_template_mode as _run_template,
)

try:
    from rich.console import Console
    from rich.panel import Panel

    RICH_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised through non-Rich fallbacks
    Console = None  # type: ignore[assignment]
    Panel = None  # type: ignore[assignment]
    RICH_AVAILABLE = False

from ..blueprints import registry as blueprint_registry
from ._common import CLIError

COMMAND = "forge"
LOG = logging.getLogger("fluid.cli.forge")


class ForgeError(CLIError):
    """Base exception for Forge command errors."""


class TemplateNotFoundError(ForgeError):
    """Template not found in registry."""

    def __init__(self, template_name: str, available: List[str]):
        self.template_name = template_name
        self.available = available
        super().__init__(f"Template '{template_name}' not found. Available: {', '.join(available)}")


class BlueprintNotFoundError(ForgeError):
    """Blueprint not found in registry."""

    def __init__(self, blueprint_name: str, available: List[str]):
        self.blueprint_name = blueprint_name
        self.available = available
        super().__init__(
            f"Blueprint '{blueprint_name}' not found. Available: {', '.join(available)}"
        )


class InvalidProjectNameError(ForgeError):
    """Invalid project name format."""

    def __init__(self, name: str, reason: str):
        self.name = name
        self.reason = reason
        super().__init__(f"Invalid project name '{name}': {reason}")


class ProjectGenerationError(ForgeError):
    """Project generation failed."""


class ContextValidationError(ForgeError):
    """Context file validation failed."""


class ForgeMode(Enum):
    """Forge creation modes.

    After the UX redesign only ``AI_COPILOT`` and ``BLANK`` are first-class.
    ``TEMPLATE``, ``DOMAIN_AGENT``, and ``BLUEPRINT`` are kept for backward
    compatibility and emit deprecation warnings.
    """

    AI_COPILOT = "copilot"
    BLANK = "blank"

    # Deprecated — kept so ``ForgeMode("template")`` doesn't crash callers.
    TEMPLATE = "template"
    DOMAIN_AGENT = "agent"
    BLUEPRINT = "blueprint"


class CopilotAgent(CopilotAgentBase):
    """Public copilot agent wired to the compatibility aliases in this module."""

    def _resolve_llm_config_dependency(self, options):
        return resolve_llm_config(options)

    def _discover_local_context_dependency(self, options):
        return discover_local_context(
            getattr(options, "discovery_path", None),
            discover=getattr(options, "discover", True),
            workspace_root=Path.cwd(),
            logger=LOG,
        )

    def _build_capability_matrix_dependency(self):
        return build_capability_matrix()

    def _generate_copilot_artifacts_dependency(
        self,
        context: Dict[str, Any],
        *,
        llm_config: Any,
        discovery_report: Any,
        project_memory: Any,
        capability_matrix: Any,
    ) -> CopilotGenerationResult:
        return generate_copilot_artifacts(
            context,
            llm_config=llm_config,
            discovery_report=discovery_report,
            project_memory=project_memory,
            capability_matrix=capability_matrix,
            logger=LOG,
        )

    def _make_memory_store_dependency(self, project_root: Path) -> CopilotMemoryStore:
        return CopilotMemoryStore(project_root, logger=LOG)

    def _ask_confirmation_dependency(self, prompt: str, preview: str) -> bool:
        if self.console and RICH_AVAILABLE:
            return ask_confirmation(
                self.console,
                prompt,
                default=False,
                title="🧠 Save Project Memory?",
                preview=preview,
                border_style="cyan",
            )
        return super()._ask_confirmation_dependency(prompt, preview)


DOMAIN_AGENTS_AVAILABLE = bool(DOMAIN_AGENTS)
AI_AGENTS = {"copilot": CopilotAgent}
if DOMAIN_AGENTS_AVAILABLE:
    AI_AGENTS.update(DOMAIN_AGENTS)


# ---------------------------------------------------------------------------
# CLI registration
# ---------------------------------------------------------------------------


def register(subparsers: argparse._SubParsersAction):
    """Register the Forge command — AI-powered data product creation."""
    parser = subparsers.add_parser(
        COMMAND,
        help="🔨 Create a new data product with AI Copilot",
        add_help=False,
    )
    parser.add_argument("--help", "-h", action="store_true", help="Show this help message")

    # --- Primary flags ---
    parser.add_argument(
        "--blank",
        action="store_true",
        help="Scaffold an empty contract without AI (no LLM needed)",
    )
    parser.add_argument("--target-dir", "-d", help="Target directory for project creation")
    parser.add_argument("--provider", "-p", help="Infrastructure provider to use")
    parser.add_argument(
        "--domain",
        help="Domain hint for AI (e.g., finance, healthcare, retail, telco)",
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Use defaults without prompting",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be created without generating files",
    )
    parser.add_argument(
        "--context", help="Additional context for AI (JSON string or file path)"
    )

    # --- LLM flags ---
    parser.add_argument(
        "--llm-provider",
        choices=["openai", "anthropic", "claude", "gemini", "ollama"],
        help="LLM provider for copilot",
    )
    parser.add_argument("--llm-model", help="Model identifier for copilot")
    parser.add_argument(
        "--llm-endpoint",
        help="Exact HTTP endpoint override for the selected LLM adapter",
    )

    # --- Discovery ---
    parser.add_argument(
        "--discover",
        dest="discover",
        action="store_true",
        default=True,
        help="Inspect local files and manifests before generation",
    )
    parser.add_argument(
        "--no-discover",
        dest="discover",
        action="store_false",
        help="Skip local discovery and rely only on explicit context",
    )
    parser.add_argument(
        "--discovery-path",
        help="Additional path to scan for metadata-only discovery",
    )

    # --- Memory ---
    parser.add_argument(
        "--memory",
        dest="memory",
        action="store_true",
        default=True,
        help="Load project-scoped copilot memory",
    )
    parser.add_argument(
        "--no-memory",
        dest="memory",
        action="store_false",
        help="Do not load project-scoped copilot memory for this run",
    )
    parser.add_argument(
        "--save-memory",
        action="store_true",
        help="Persist copilot memory after a successful non-interactive run",
    )
    parser.add_argument(
        "--show-memory",
        action="store_true",
        help="Show the current copilot memory summary and exit",
    )
    parser.add_argument(
        "--reset-memory",
        action="store_true",
        help="Delete the copilot memory file and exit",
    )

    # --- Deprecated flags (kept for backward compat) ---
    parser.add_argument(
        "--mode",
        "-m",
        choices=["copilot", "template", "agent", "blueprint", "blank"],
        default=None,
        help=argparse.SUPPRESS,  # Hidden — deprecated
    )
    parser.add_argument(
        "--agent",
        "-a",
        choices=list(AI_AGENTS.keys()),
        help=argparse.SUPPRESS,  # Hidden — deprecated
    )
    parser.add_argument("--template", "-t", help=argparse.SUPPRESS)
    parser.add_argument("--blueprint", "-b", help=argparse.SUPPRESS)
    parser.add_argument("--quickstart", "-q", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--interactive", "-i", action="store_true", help=argparse.SUPPRESS)

    parser.set_defaults(func=run)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_target_directory(args, default_name: str = "my-fluid-project") -> Path:
    return _get_target_dir(args, default_name)


def get_cli_arg(args: Any, name: str, default: Any = None) -> Any:
    return _get_cli_arg(args, name, default)


def resolve_memory_store(args, logger: logging.Logger) -> CopilotMemoryStore:
    return _resolve_store(args, logger, memory_store_class=CopilotMemoryStore)


def handle_memory_management(args, logger: logging.Logger) -> int:
    return _handle_memory(
        args,
        logger,
        memory_store_class=CopilotMemoryStore,
        console_factory=Console if RICH_AVAILABLE else None,
    )


# ---------------------------------------------------------------------------
# Blank mode — scaffold empty contract, no AI
# ---------------------------------------------------------------------------


def _run_blank_mode(args: Any, logger: logging.Logger) -> int:
    """Create a minimal empty contract scaffold without AI."""
    from fluid_build.cli.forge_contract_factory import (
        build_minimal_contract,
        validate_contract_file,
        write_contract,
    )

    console = Console() if RICH_AVAILABLE else None
    target_dir = get_target_directory(args, "my-data-product")

    if get_cli_arg(args, "dry_run", False):
        if console:
            console.print(f"[dim]DRY RUN: Would create empty contract in {target_dir}[/dim]")
        return 0

    target_dir.mkdir(parents=True, exist_ok=True)
    contract_path = target_dir / "contract.fluid.yaml"
    if contract_path.exists():
        if console:
            console.print(
                f"[yellow]contract.fluid.yaml already exists in {target_dir}[/yellow]\n"
                "[dim]Delete it first or use a different --target-dir.[/dim]"
            )
        return 1

    contract = build_minimal_contract()
    write_contract(contract, contract_path)

    # Validate our own output
    error = validate_contract_file(contract_path)
    if error:
        logger.error("Generated contract failed validation: %s", error)
        if console:
            console.print(f"[red]Generated contract is invalid: {error}[/red]")
        return 1

    _print_next_steps(console, target_dir, contract_path)
    return 0


# ---------------------------------------------------------------------------
# Shared UI helpers
# ---------------------------------------------------------------------------

_DOCS_URL = "https://fluid-build.dev/docs/contracts"


def _print_next_steps(console: Any, target_dir: Path, contract_path: Path) -> None:
    """Show post-creation next steps with doc link."""
    steps = (
        f"[green]Created contract at:[/green] {contract_path}\n\n"
        "Next steps:\n"
        f"  1. cd {target_dir}\n"
        "  2. Edit contract.fluid.yaml\n"
        "  3. fluid validate contract.fluid.yaml\n"
        "  4. fluid plan contract.fluid.yaml --out runtime/plan.json\n"
        "  5. fluid apply runtime/plan.json\n\n"
        f"[dim]Docs: {_DOCS_URL}[/dim]"
    )
    if console and RICH_AVAILABLE:
        console.print(Panel(steps, title="Forge Complete", border_style="green"))
    else:
        cprint(f"Created contract at {contract_path}")
        cprint(f"Docs: {_DOCS_URL}")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def run(args, logger: logging.Logger) -> int:
    """Main entry point for ``fluid forge``."""
    console = Console() if RICH_AVAILABLE else None
    try:
        # --- Help ---
        if getattr(args, "help", False):
            if console:
                from .help_formatter import print_forge_help

                print_forge_help()
                return 0
            cprint("Run 'fluid forge' to start the AI Copilot, or 'fluid forge --blank' for an empty contract.")
            return 0

        # --- Memory management shortcuts ---
        if get_cli_arg(args, "show_memory", False) or get_cli_arg(args, "reset_memory", False):
            return handle_memory_management(args, logger)

        # --- Determine effective mode ---
        explicit_mode = get_cli_arg(args, "mode")
        is_blank = get_cli_arg(args, "blank", False)

        if is_blank or explicit_mode == "blank":
            LOG.debug("Forge: blank mode selected")
            return _run_blank_mode(args, logger)

        # --- Deprecated mode dispatch table ---
        _DEPRECATED_MODES = {
            "template": (
                run_template_mode,
                "forge --mode template is deprecated. Use 'fluid init --template' instead.",
                "Template mode has moved to 'fluid init --template'.",
            ),
            "agent": (
                None,  # Handled specially below
                "forge --mode agent is deprecated. Domain expertise is now auto-detected by copilot.",
                "Agent mode is deprecated -- copilot now auto-detects domains.",
            ),
            "blueprint": (
                run_blueprint_mode,
                "forge --mode blueprint is deprecated. Use 'fluid init' with a quickstart instead.",
                "Blueprint mode has moved to 'fluid init'.",
            ),
        }

        if explicit_mode in _DEPRECATED_MODES:
            handler, warn_msg, user_msg = _DEPRECATED_MODES[explicit_mode]
            warnings.warn(warn_msg, DeprecationWarning, stacklevel=2)
            if console:
                console.print(f"[yellow]{user_msg}[/yellow]")

            if explicit_mode == "agent":
                # Transfer agent name to domain context
                agent_name = get_cli_arg(args, "agent")
                if agent_name and agent_name != "copilot":
                    args.domain = agent_name
                return run_ai_copilot_mode(args, logger)

            return handler(args, logger)

        # --- Default: AI Copilot with inline LLM setup ---
        LOG.debug("Forge: copilot mode (default)")
        if console and not get_cli_arg(args, "non_interactive", False):
            print_welcome_panel(console)

        # Check LLM readiness; load saved config or offer inline setup
        if not get_cli_arg(args, "non_interactive", False):
            from fluid_build.cli.ai_setup import run_ai_setup_inline

            # Always go through inline setup — it handles all cases:
            # 1. Config file exists with key → loads it, sets env vars, returns config
            # 2. Keyring has key → loads it, sets env vars, returns config
            # 3. Nothing found → prompts user interactively
            llm_config = run_ai_setup_inline(console)
            if llm_config:
                # Inject into args so copilot's resolve_llm_config() finds them
                args.llm_provider = llm_config.provider
                args.llm_model = llm_config.model
                args.llm_endpoint = llm_config.endpoint
                if llm_config.api_key:
                    from fluid_build.cli.ai_setup import _set_session_env
                    _set_session_env(llm_config.provider, llm_config.api_key)
                LOG.debug("LLM config loaded: provider=%s", llm_config.provider)
            else:
                # AI not available — fall back to guided mode
                proceed = ask_confirmation(
                    console,
                    "Continue with guided mode (no AI)?",
                    default=True,
                ) if console else False
                if proceed:
                    return run_guided_mode(args, logger)
                if console:
                    console.print(
                        "[yellow]Use 'fluid forge --blank' for a bare contract,[/yellow]\n"
                        "[yellow]or run 'fluid ai setup' to configure an LLM provider.[/yellow]"
                    )
                return 1

        return run_ai_copilot_mode(args, logger)

    except KeyboardInterrupt:
        logger.info("Forge cancelled by user")
        return 130
    except Exception as exc:  # noqa: BLE001
        logger.exception("Forge command failed")
        if console:
            console.print(
                f"[red]Forge failed: {exc}[/red]\n"
                f"[dim]Run 'fluid doctor' to diagnose, or see {_DOCS_URL}[/dim]"
            )
        else:
            console_error(f"Forge failed: {exc}")
            cprint(f"Run 'fluid doctor' to diagnose, or see {_DOCS_URL}")
        return 1


# ---------------------------------------------------------------------------
# Mode wrappers (thin delegation)
# ---------------------------------------------------------------------------


def run_ai_copilot_mode(args, logger: logging.Logger) -> int:
    return _run_copilot(
        args,
        logger,
        copilot_class=CopilotAgent,
        get_cli_arg_fn=get_cli_arg,
        load_context_fn=load_context,
        get_target_directory_fn=get_target_directory,
        context_error_cls=ContextValidationError,
        build_interview_summary_fn=build_interview_summary_from_context,
        console_factory=Console if RICH_AVAILABLE else None,
    )


def run_guided_mode(args, logger: logging.Logger) -> int:
    """Lightweight guided prompts — no AI needed."""
    return _run_guided(
        args,
        logger,
        get_target_directory_fn=get_target_directory,
        console_factory=Console if RICH_AVAILABLE else None,
    )


def run_domain_agent_mode(args, logger: logging.Logger) -> int:
    """Deprecated — routes through copilot with domain context."""
    return _run_agent(
        args,
        logger,
        ai_agents=AI_AGENTS,
        gather_context_fn=gather_copilot_context,
        load_context_fn=load_context,
        get_target_directory_fn=get_target_directory,
        context_error_cls=ContextValidationError,
        console_factory=Console if RICH_AVAILABLE else None,
    )


def run_template_mode(args, logger: logging.Logger) -> int:
    """Deprecated — use ``fluid init --template``."""
    return _run_template(
        args,
        logger,
        get_target_directory_fn=get_target_directory,
        console_factory=Console if RICH_AVAILABLE else None,
    )


def gather_copilot_context(copilot: CopilotAgent, console) -> Dict[str, Any]:
    return _gather_context(copilot, console)


def run_blueprint_mode(args, logger: logging.Logger) -> int:
    """Deprecated — use ``fluid init`` quickstarts."""
    return _run_blueprint(
        args,
        logger,
        blueprint_registry=blueprint_registry,
        get_target_directory_fn=get_target_directory,
        ask_confirmation_fn=ask_confirmation,
        console_factory=Console if RICH_AVAILABLE else None,
    )


def load_context(
    context_input: str,
    console: Optional[Any] = None,
    *,
    context_error_cls: type[Exception] = ContextValidationError,
) -> Dict[str, Any]:
    return _load_ctx(
        context_input,
        console,
        context_error_cls=context_error_cls,
    )


def _run_forge_blueprint(args, blueprint_registry):
    return _run_blueprint_legacy(
        args,
        blueprint_registry,
        get_target_directory_fn=get_target_directory,
    )


def create_legacy_bootstrapper(target_dir: Optional[str] = None, **kwargs):
    """Create a legacy bootstrapper for backward compatibility."""
    from .forge_legacy import ForgeBootstrapper

    return ForgeBootstrapper(target_dir, **kwargs)


def get_enhanced_templates():
    """Get enhanced templates for backward compatibility."""
    from ..forge.core.registry import template_registry

    legacy_templates = {}
    for template_name in template_registry.list_available():
        template = template_registry.get(template_name)
        if template:
            metadata = template.get_metadata()
            legacy_templates[template_name] = {
                "name": metadata.name,
                "description": metadata.description,
                "complexity": metadata.complexity.value,
                "provider_support": metadata.provider_support,
                "use_cases": metadata.use_cases,
                "technologies": metadata.technologies,
                "estimated_time": metadata.estimated_time,
                "tags": metadata.tags,
            }
    return legacy_templates


__all__ = [
    "AIAgent",
    "AI_AGENTS",
    "BlueprintNotFoundError",
    "COMMAND",
    "ContextValidationError",
    "CopilotAgent",
    "ForgeError",
    "ForgeMode",
    "InvalidProjectNameError",
    "ProjectGenerationError",
    "TemplateNotFoundError",
    "create_legacy_bootstrapper",
    "gather_copilot_context",
    "get_cli_arg",
    "get_enhanced_templates",
    "get_target_directory",
    "handle_memory_management",
    "load_context",
    "register",
    "resolve_memory_store",
    "run",
    "run_ai_copilot_mode",
    "run_blueprint_mode",
    "run_domain_agent_mode",
    "run_guided_mode",
    "run_template_mode",
]
