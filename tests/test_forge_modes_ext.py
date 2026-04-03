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

"""Tests for fluid_build.cli.forge_modes."""

import argparse
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

from fluid_build.cli.forge_modes import (
    run_blueprint_mode,
    run_domain_agent_mode,
    run_forge_blueprint_impl,
    run_template_mode,
)

# ── helpers ────────────────────────────────────────────────────────────


def _args(**kwargs):
    defaults = {
        "non_interactive": True,
        "dry_run": False,
        "agent": None,
        "context": None,
        "blueprint": "test-blueprint",
        "template": None,
        "provider": None,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _logger():
    return logging.getLogger("test.forge_modes")


def _mock_blueprint(name="test-blueprint", title="Test Blueprint", description="A test blueprint"):
    bp = MagicMock()
    bp.metadata.name = name
    bp.metadata.title = title
    bp.metadata.description = description
    bp.metadata.complexity.value = "intermediate"
    bp.metadata.setup_time = "30 min"
    bp.metadata.providers = ["gcp"]
    bp.validate.return_value = []
    bp.path = Path("/fake/blueprint")
    return bp


def _mock_registry(blueprint=None, available=None):
    registry = MagicMock()
    registry.get_blueprint.return_value = blueprint
    registry.list_blueprints.return_value = available or []
    return registry


# ── run_domain_agent_mode ─────────────────────────────────────────────


class TestRunDomainAgentMode:
    def _make_agent_class(self, name="finance", domain="Finance", description="Finance agent"):
        agent = MagicMock()
        agent.name = name
        agent.domain = domain
        agent.description = description
        agent.analyze_requirements.return_value = {
            "recommended_template": "etl",
            "recommended_provider": "gcp",
            "security_requirements": [],
        }
        agent.create_project.return_value = True
        agent_class = MagicMock(return_value=agent)
        return agent_class

    def test_unknown_agent_name_returns_one(self):
        args = _args(agent="nonexistent")
        ai_agents = {"finance": self._make_agent_class()}

        result = run_domain_agent_mode(
            args,
            _logger(),
            ai_agents=ai_agents,
            gather_context_fn=MagicMock(return_value={}),
            load_context_fn=MagicMock(),
            get_target_directory_fn=MagicMock(return_value=Path("/tmp/out")),
            context_error_cls=Exception,
            console_factory=None,
        )
        assert result == 1

    def test_no_agent_name_non_interactive_defaults_to_copilot(self):
        args = _args(agent=None, non_interactive=True)
        copilot_class = self._make_agent_class()
        copilot_class.return_value.create_project.return_value = True

        result = run_domain_agent_mode(
            args,
            _logger(),
            ai_agents={"copilot": copilot_class},
            gather_context_fn=MagicMock(return_value={}),
            load_context_fn=MagicMock(),
            get_target_directory_fn=MagicMock(return_value=Path("/tmp/out")),
            context_error_cls=Exception,
            console_factory=None,
        )
        assert result == 0

    def test_successful_agent_returns_zero(self):
        agent_class = self._make_agent_class()
        args = _args(agent="finance", non_interactive=True, context=None)

        result = run_domain_agent_mode(
            args,
            _logger(),
            ai_agents={"finance": agent_class},
            gather_context_fn=MagicMock(return_value={}),
            load_context_fn=MagicMock(),
            get_target_directory_fn=MagicMock(return_value=Path("/tmp/out")),
            context_error_cls=Exception,
            console_factory=None,
        )
        assert result == 0

    def test_create_project_returns_false_returns_one(self):
        agent_class = self._make_agent_class()
        agent_class.return_value.create_project.return_value = False
        args = _args(agent="finance", non_interactive=True, context=None)

        result = run_domain_agent_mode(
            args,
            _logger(),
            ai_agents={"finance": agent_class},
            gather_context_fn=MagicMock(return_value={}),
            load_context_fn=MagicMock(),
            get_target_directory_fn=MagicMock(return_value=Path("/tmp/out")),
            context_error_cls=Exception,
            console_factory=None,
        )
        assert result == 1

    def test_exception_in_agent_returns_one(self):
        agent_class = MagicMock(side_effect=RuntimeError("agent error"))
        args = _args(agent="finance", non_interactive=True, context=None)

        result = run_domain_agent_mode(
            args,
            _logger(),
            ai_agents={"finance": agent_class},
            gather_context_fn=MagicMock(return_value={}),
            load_context_fn=MagicMock(),
            get_target_directory_fn=MagicMock(return_value=Path("/tmp/out")),
            context_error_cls=Exception,
            console_factory=None,
        )
        assert result == 1

    def test_context_loading_failure_is_caught(self):
        """context_error_cls exception during load_context_fn should be caught."""
        agent_class = self._make_agent_class()
        args = _args(agent="finance", non_interactive=True, context="ctx.yaml")

        class MyContextError(Exception):
            pass

        def bad_load(*a, **kw):
            raise MyContextError("ctx error")

        result = run_domain_agent_mode(
            args,
            _logger(),
            ai_agents={"finance": agent_class},
            gather_context_fn=MagicMock(return_value={}),
            load_context_fn=bad_load,
            get_target_directory_fn=MagicMock(return_value=Path("/tmp/out")),
            context_error_cls=MyContextError,
            console_factory=None,
        )
        # Should still succeed since context error is handled gracefully
        assert result == 0


# ── run_template_mode ─────────────────────────────────────────────────


class TestRunTemplateMode:
    def test_missing_template_returns_one(self):
        args = _args(template="nonexistent", provider="local", non_interactive=True)

        mock_registry = MagicMock()
        mock_registry.get.return_value = None
        mock_registry.list_available.return_value = ["starter", "analytics"]

        with (
            patch("fluid_build.forge.core.registry.template_registry", mock_registry),
            patch("fluid_build.forge.core.engine.ForgeEngine", MagicMock()),
            patch("fluid_build.forge.core.engine.GenerationContext", MagicMock()),
        ):
            result = run_template_mode(
                args,
                _logger(),
                get_target_directory_fn=MagicMock(return_value=Path("/tmp/out")),
                console_factory=None,
            )
        assert result == 1

    def test_dry_run_returns_zero(self, tmp_path):
        args = _args(template="starter", provider="local", dry_run=True, non_interactive=True)
        target = tmp_path / "starter-project"

        mock_template = MagicMock()
        mock_template.get_metadata.return_value = MagicMock(
            name="starter", description="Starter template"
        )
        mock_registry = MagicMock()
        mock_registry.get.return_value = mock_template
        mock_registry.list_available.return_value = ["starter"]

        with (
            patch("fluid_build.forge.core.registry.template_registry", mock_registry),
            patch("fluid_build.forge.core.engine.ForgeEngine", MagicMock()),
            patch("fluid_build.forge.core.engine.GenerationContext", MagicMock()),
        ):
            result = run_template_mode(
                args,
                _logger(),
                get_target_directory_fn=MagicMock(return_value=target),
                console_factory=None,
            )
        assert result == 0

    def test_successful_template_creation(self, tmp_path):
        args = _args(template="starter", provider="local", dry_run=False, non_interactive=True)
        target = tmp_path / "starter-project"

        mock_template = MagicMock()
        mock_template.get_metadata.return_value = MagicMock(
            name="starter", description="Starter template"
        )
        mock_template.generate_contract.return_value = {}
        mock_template.generate_structure.return_value = {}
        mock_registry = MagicMock()
        mock_registry.get.return_value = mock_template
        mock_registry.list_available.return_value = ["starter"]

        with (
            patch("fluid_build.forge.core.registry.template_registry", mock_registry),
            patch("fluid_build.forge.core.engine.ForgeEngine", MagicMock()),
            patch("fluid_build.forge.core.engine.GenerationContext", MagicMock()),
            patch("builtins.open", MagicMock()),
            patch("yaml.dump"),
            patch("fluid_build.cli.forge_modes.success"),
        ):
            result = run_template_mode(
                args,
                _logger(),
                get_target_directory_fn=MagicMock(return_value=target),
                console_factory=None,
            )
        assert result == 0

    def test_exception_returns_one(self):
        args = _args(template="starter", provider="local", non_interactive=True)

        # get_target_directory_fn raising causes generic exception handler to catch it
        result = run_template_mode(
            args,
            _logger(),
            get_target_directory_fn=MagicMock(side_effect=RuntimeError("oops")),
            console_factory=None,
        )
        assert result == 1


# ── run_blueprint_mode ────────────────────────────────────────────────


class TestRunBlueprintMode:
    def test_blueprint_not_found_returns_one(self):
        args = _args(blueprint="missing", non_interactive=True)
        registry = _mock_registry(blueprint=None, available=[])

        result = run_blueprint_mode(
            args,
            _logger(),
            blueprint_registry=registry,
            get_target_directory_fn=MagicMock(return_value=Path("/tmp/out")),
            console_factory=None,
        )
        assert result == 1

    def test_successful_blueprint_generation(self, tmp_path):
        args = _args(blueprint="test-bp", non_interactive=True)
        bp = _mock_blueprint("test-bp")
        registry = _mock_registry(blueprint=bp)
        target = tmp_path / "test-bp"

        result = run_blueprint_mode(
            args,
            _logger(),
            blueprint_registry=registry,
            get_target_directory_fn=MagicMock(return_value=target),
            console_factory=None,
        )
        assert result == 0
        bp.generate_project.assert_called_once_with(target)

    def test_non_empty_dir_non_interactive_returns_one(self, tmp_path):
        args = _args(blueprint="test-bp", non_interactive=True)
        bp = _mock_blueprint("test-bp")
        registry = _mock_registry(blueprint=bp)
        target = tmp_path / "test-bp"
        target.mkdir()
        (target / "existing_file.txt").touch()

        result = run_blueprint_mode(
            args,
            _logger(),
            blueprint_registry=registry,
            get_target_directory_fn=MagicMock(return_value=target),
            console_factory=None,
        )
        assert result == 1

    def test_exception_returns_one(self):
        args = _args(blueprint="test-bp", non_interactive=True)
        registry = MagicMock()
        registry.get_blueprint.side_effect = RuntimeError("registry error")

        result = run_blueprint_mode(
            args,
            _logger(),
            blueprint_registry=registry,
            get_target_directory_fn=MagicMock(return_value=Path("/tmp/out")),
            console_factory=None,
        )
        assert result == 1

    def test_blueprint_not_found_console_path(self):
        args = _args(blueprint="missing", non_interactive=True)
        mock_bp_item = MagicMock()
        mock_bp_item.metadata.name = "available-bp"
        mock_bp_item.metadata.title = "Available BP"
        registry = _mock_registry(blueprint=None, available=[mock_bp_item])
        mock_console = MagicMock()

        result = run_blueprint_mode(
            args,
            _logger(),
            blueprint_registry=registry,
            get_target_directory_fn=MagicMock(return_value=Path("/tmp/out")),
            console_factory=lambda: mock_console,
        )
        assert result == 1
        mock_console.print.assert_called()


# ── run_forge_blueprint_impl ──────────────────────────────────────────


class TestRunForgeBlueprintImpl:
    def test_blueprint_not_found_returns_one(self):
        args = _args(blueprint="missing", non_interactive=True, quickstart=False, dry_run=False)
        registry = _mock_registry(blueprint=None)

        result = run_forge_blueprint_impl(
            args,
            registry,
            get_target_directory_fn=MagicMock(return_value=Path("/tmp/out")),
        )
        assert result == 1

    def test_validate_errors_returns_one(self, tmp_path):
        args = _args(blueprint="test-bp", non_interactive=True, quickstart=False, dry_run=False)
        bp = _mock_blueprint("test-bp")
        bp.validate.return_value = ["error 1", "error 2"]
        registry = _mock_registry(blueprint=bp)
        target = tmp_path / "test-bp"

        result = run_forge_blueprint_impl(
            args,
            registry,
            get_target_directory_fn=MagicMock(return_value=target),
        )
        assert result == 1

    def test_dry_run_returns_zero(self, tmp_path):
        args = _args(blueprint="test-bp", non_interactive=True, quickstart=True, dry_run=True)
        bp = _mock_blueprint("test-bp")
        bp.validate.return_value = []
        bp.path = tmp_path
        (tmp_path / "test_file.py").touch()
        registry = _mock_registry(blueprint=bp)
        target = tmp_path / "out"

        result = run_forge_blueprint_impl(
            args,
            registry,
            get_target_directory_fn=MagicMock(return_value=target),
        )
        assert result == 0

    def test_successful_generation_returns_zero(self, tmp_path):
        args = _args(blueprint="test-bp", non_interactive=True, quickstart=True, dry_run=False)
        bp = _mock_blueprint("test-bp")
        bp.validate.return_value = []
        registry = _mock_registry(blueprint=bp)
        target = tmp_path / "out"

        result = run_forge_blueprint_impl(
            args,
            registry,
            get_target_directory_fn=MagicMock(return_value=target),
        )
        assert result == 0
        bp.generate_project.assert_called_once_with(target)

    def test_non_empty_dir_non_interactive_returns_one(self, tmp_path):
        args = _args(blueprint="test-bp", non_interactive=True, quickstart=True, dry_run=False)
        bp = _mock_blueprint("test-bp")
        bp.validate.return_value = []
        registry = _mock_registry(blueprint=bp)
        target = tmp_path / "out"
        target.mkdir()
        (target / "existing.txt").touch()

        result = run_forge_blueprint_impl(
            args,
            registry,
            get_target_directory_fn=MagicMock(return_value=target),
        )
        assert result == 1

    def test_exception_returns_one(self):
        args = _args(blueprint="test-bp", non_interactive=True, quickstart=True, dry_run=False)
        registry = MagicMock()
        registry.get_blueprint.side_effect = RuntimeError("unexpected")

        result = run_forge_blueprint_impl(
            args,
            registry,
            get_target_directory_fn=MagicMock(return_value=Path("/tmp/out")),
        )
        assert result == 1


# ── run_ai_copilot_mode ───────────────────────────────────────────────


class TestRunAiCopilotMode:
    """Test the run_ai_copilot_mode function (lines 50-171)."""

    def _make_copilot_class(self, success=True):
        copilot = MagicMock()
        copilot.prepare_runtime_inputs.return_value = {
            "llm_config": {"provider": "openai"},
            "discovery_report": {},
            "capability_matrix": {},
            "project_memory": {},
            "capability_warnings": [],
        }
        copilot.create_project.return_value = success
        copilot_class = MagicMock(return_value=copilot)
        return copilot_class

    def test_non_interactive_success(self):
        from fluid_build.cli.forge_modes import run_ai_copilot_mode

        args = _args(non_interactive=True, dry_run=False)
        args.provider = None
        args.template = None
        args.domain = None
        args.context = None
        args.target_dir = None
        args.llm_provider = None
        args.llm_model = None
        args.llm_endpoint = None

        copilot_class = self._make_copilot_class(success=True)

        def _get_arg(a, name, default=None):
            return getattr(a, name, default)

        with patch(
            "fluid_build.cli.forge_modes.normalize_copilot_context", side_effect=lambda x: x
        ):
            with patch("fluid_build.cli.forge_modes.build_interview_summary_fn", create=True):
                result = run_ai_copilot_mode(
                    args,
                    _logger(),
                    copilot_class=copilot_class,
                    get_cli_arg_fn=_get_arg,
                    load_context_fn=MagicMock(),
                    get_target_directory_fn=MagicMock(return_value=Path("/tmp/proj")),
                    context_error_cls=Exception,
                    build_interview_summary_fn=MagicMock(return_value={}),
                    console_factory=None,
                )
        assert result == 0

    def test_non_interactive_create_project_fails(self):
        from fluid_build.cli.forge_modes import run_ai_copilot_mode

        args = _args(non_interactive=True, dry_run=False)
        args.provider = None
        args.template = None
        args.domain = None
        args.context = None
        args.target_dir = None
        args.llm_provider = None
        args.llm_model = None
        args.llm_endpoint = None

        copilot_class = self._make_copilot_class(success=False)

        def _get_arg(a, name, default=None):
            return getattr(a, name, default)

        with patch(
            "fluid_build.cli.forge_modes.normalize_copilot_context", side_effect=lambda x: x
        ):
            result = run_ai_copilot_mode(
                args,
                _logger(),
                copilot_class=copilot_class,
                get_cli_arg_fn=_get_arg,
                load_context_fn=MagicMock(),
                get_target_directory_fn=MagicMock(return_value=Path("/tmp/proj")),
                context_error_cls=Exception,
                build_interview_summary_fn=MagicMock(return_value={}),
                console_factory=None,
            )
        assert result == 1

    def test_exception_in_copilot_returns_one(self):
        from fluid_build.cli.forge_modes import run_ai_copilot_mode

        args = _args(non_interactive=True)
        args.provider = None
        args.template = None
        args.domain = None
        args.context = None
        args.target_dir = None

        def _get_arg(a, name, default=None):
            return getattr(a, name, default)

        result = run_ai_copilot_mode(
            args,
            _logger(),
            copilot_class=MagicMock(side_effect=RuntimeError("copilot init failed")),
            get_cli_arg_fn=_get_arg,
            load_context_fn=MagicMock(),
            get_target_directory_fn=MagicMock(return_value=Path("/tmp/proj")),
            context_error_cls=Exception,
            build_interview_summary_fn=MagicMock(return_value={}),
            console_factory=None,
        )
        assert result == 1

    def test_with_provider_template_domain_in_context(self):
        """Lines 108-117: provider/template/domain set in context from args."""
        from fluid_build.cli.forge_modes import run_ai_copilot_mode

        args = _args(non_interactive=True, dry_run=False)
        args.provider = "gcp"
        args.template = "analytics"
        args.domain = "finance"
        args.context = None
        args.target_dir = "/tmp/my-dir"
        args.llm_provider = None
        args.llm_model = None
        args.llm_endpoint = None

        copilot_class = self._make_copilot_class(success=True)

        def _get_arg(a, name, default=None):
            return getattr(a, name, default)

        with patch(
            "fluid_build.cli.forge_modes.normalize_copilot_context", side_effect=lambda x: x
        ):
            result = run_ai_copilot_mode(
                args,
                _logger(),
                copilot_class=copilot_class,
                get_cli_arg_fn=_get_arg,
                load_context_fn=MagicMock(),
                get_target_directory_fn=MagicMock(return_value=Path("/tmp/proj")),
                context_error_cls=Exception,
                build_interview_summary_fn=MagicMock(return_value={}),
                console_factory=None,
            )
        assert result == 0

    def test_context_loaded_successfully(self):
        """Lines 88-96: context_arg provided and loaded successfully."""
        from fluid_build.cli.forge_modes import run_ai_copilot_mode

        args = _args(non_interactive=True, dry_run=False)
        args.provider = None
        args.template = None
        args.domain = None
        args.context = "ctx.yaml"
        args.target_dir = None
        args.llm_provider = None
        args.llm_model = None
        args.llm_endpoint = None

        copilot_class = self._make_copilot_class(success=True)

        def _get_arg(a, name, default=None):
            return getattr(a, name, default)

        mock_load = MagicMock(return_value={"extra_key": "extra_val"})

        with patch(
            "fluid_build.cli.forge_modes.normalize_copilot_context", side_effect=lambda x: x
        ):
            result = run_ai_copilot_mode(
                args,
                _logger(),
                copilot_class=copilot_class,
                get_cli_arg_fn=_get_arg,
                load_context_fn=mock_load,
                get_target_directory_fn=MagicMock(return_value=Path("/tmp/proj")),
                context_error_cls=Exception,
                build_interview_summary_fn=MagicMock(return_value={}),
                console_factory=None,
            )
        assert result == 0

    def test_context_error_is_caught(self):
        """Lines 97-106: context_error_cls exception is caught without crashing."""
        from fluid_build.cli.forge_modes import run_ai_copilot_mode

        args = _args(non_interactive=True, dry_run=False)
        args.provider = None
        args.template = None
        args.domain = None
        args.context = "bad_ctx.yaml"
        args.target_dir = None
        args.llm_provider = None
        args.llm_model = None
        args.llm_endpoint = None

        copilot_class = self._make_copilot_class(success=True)

        class ContextErr(Exception):
            pass

        def _get_arg(a, name, default=None):
            return getattr(a, name, default)

        def bad_load(*a, **kw):
            raise ContextErr("bad context")

        with patch(
            "fluid_build.cli.forge_modes.normalize_copilot_context", side_effect=lambda x: x
        ):
            result = run_ai_copilot_mode(
                args,
                _logger(),
                copilot_class=copilot_class,
                get_cli_arg_fn=_get_arg,
                load_context_fn=bad_load,
                get_target_directory_fn=MagicMock(return_value=Path("/tmp/proj")),
                context_error_cls=ContextErr,
                build_interview_summary_fn=MagicMock(return_value={}),
                console_factory=None,
            )
        assert result == 0


# ── run_domain_agent_mode – additional branches ───────────────────────


class TestRunDomainAgentModeAdditional:
    """Cover lines 193-302 not yet hit by the base test class."""

    def _make_agent_class(self):
        agent = MagicMock()
        agent.name = "analytics"
        agent.domain = "analytics"
        agent.description = "Analytics agent"
        agent.analyze_requirements.return_value = {
            "recommended_template": "etl",
            "recommended_provider": "gcp",
            "security_requirements": [],
        }
        agent.create_project.return_value = True
        return MagicMock(return_value=agent)

    def test_analyze_requirements_with_security(self):
        """Lines 285-289: security_requirements shown (console path skipped)."""
        agent_class = self._make_agent_class()
        agent = agent_class.return_value
        agent.analyze_requirements.return_value = {
            "recommended_template": "secure",
            "recommended_provider": "aws",
            "security_requirements": ["mfa", "encryption", "audit-logs"],
        }
        args = _args(agent="analytics", non_interactive=True, context=None)

        result = run_domain_agent_mode(
            args,
            _logger(),
            ai_agents={"analytics": agent_class},
            gather_context_fn=MagicMock(return_value={}),
            load_context_fn=MagicMock(),
            get_target_directory_fn=MagicMock(return_value=Path("/tmp/out")),
            context_error_cls=Exception,
            console_factory=None,
        )
        assert result == 0

    def test_context_dict_loaded_with_valid_context(self):
        """Lines 238-257: valid context dict loaded."""
        agent_class = self._make_agent_class()
        args = _args(agent="analytics", non_interactive=True, context="ctx.yaml")

        valid_context = {"project_goal": "Test", "data_sources": "DB"}

        def _load(*a, **kw):
            return valid_context

        with (
            patch(
                "fluid_build.cli.forge_validation.validate_context_dict", return_value=(True, None)
            )
            if False
            else patch(
                "fluid_build.cli.forge_modes.run_domain_agent_mode",
                wraps=run_domain_agent_mode,
            )
        ):
            # We just exercise the real function with a load that succeeds
            with patch(
                "fluid_build.cli.forge_validation.validate_context_dict",
                return_value=(True, None),
            ):
                result = run_domain_agent_mode(
                    args,
                    _logger(),
                    ai_agents={"analytics": agent_class},
                    gather_context_fn=MagicMock(return_value={}),
                    load_context_fn=_load,
                    get_target_directory_fn=MagicMock(return_value=Path("/tmp/out")),
                    context_error_cls=Exception,
                    console_factory=None,
                )
        assert result == 0


# ── run_template_mode – additional branches ───────────────────────────


class TestRunTemplateModeAdditional:
    """Cover lines 318-379, 382-397."""

    def test_template_creates_directory_structure(self, tmp_path):
        """Lines 365-379: generate_structure returns dir entries."""
        from fluid_build.cli.forge_modes import run_template_mode

        args = _args(template="starter", provider="local", dry_run=False, non_interactive=True)
        target = tmp_path / "starter-project"

        mock_template = MagicMock()
        mock_template.get_metadata.return_value = MagicMock(name="starter", description="A starter")
        mock_template.generate_contract.return_value = {}
        # One directory entry and one file entry
        mock_template.generate_structure.return_value = {
            "src/": None,
            "src/main.py": "print('hello')",
        }
        mock_registry = MagicMock()
        mock_registry.get.return_value = mock_template

        with (
            patch("fluid_build.forge.core.registry.template_registry", mock_registry),
            patch("fluid_build.forge.core.engine.ForgeEngine", MagicMock()),
            patch("fluid_build.forge.core.engine.GenerationContext", MagicMock()),
            patch("builtins.open", MagicMock()),
            patch("yaml.dump"),
            patch("fluid_build.cli.forge_modes.success"),
        ):
            result = run_template_mode(
                args,
                _logger(),
                get_target_directory_fn=MagicMock(return_value=target),
                console_factory=None,
            )
        assert result == 0

    def test_template_readme_attribute_error_is_swallowed(self, tmp_path):
        """Lines 376-378: AttributeError on _create_readme is handled."""
        from fluid_build.cli.forge_modes import run_template_mode

        args = _args(template="starter", provider="local", dry_run=False, non_interactive=True)
        target = tmp_path / "starter-project"

        mock_template = MagicMock()
        mock_template.get_metadata.return_value = MagicMock(name="starter", description="Starter")
        mock_template.generate_contract.return_value = {}
        mock_template.generate_structure.return_value = {}
        mock_template._create_readme.side_effect = AttributeError("no readme")
        mock_registry = MagicMock()
        mock_registry.get.return_value = mock_template

        with (
            patch("fluid_build.forge.core.registry.template_registry", mock_registry),
            patch("fluid_build.forge.core.engine.ForgeEngine", MagicMock()),
            patch("fluid_build.forge.core.engine.GenerationContext", MagicMock()),
            patch("builtins.open", MagicMock()),
            patch("yaml.dump"),
            patch("fluid_build.cli.forge_modes.success"),
        ):
            result = run_template_mode(
                args,
                _logger(),
                get_target_directory_fn=MagicMock(return_value=target),
                console_factory=None,
            )
        assert result == 0


# ── run_blueprint_mode – non-interactive user cancels ─────────────────


class TestRunBlueprintModeAdditional:
    """Cover lines 382-479 (non-empty dir prompt branches)."""

    def test_non_empty_dir_interactive_user_confirms(self, tmp_path):
        """Lines 443-453: interactive mode, no console, user confirms overwrite."""
        args = _args(blueprint="test-bp", non_interactive=False)
        bp = _mock_blueprint("test-bp")
        registry = _mock_registry(blueprint=bp)
        target = tmp_path / "test-bp"
        target.mkdir()
        (target / "existing.txt").touch()

        with (
            patch("builtins.input", return_value="y"),
            patch("fluid_build.cli.forge_modes.warning"),
        ):
            result = run_blueprint_mode(
                args,
                _logger(),
                blueprint_registry=registry,
                get_target_directory_fn=MagicMock(return_value=target),
                ask_confirmation_fn=MagicMock(return_value=True),
                console_factory=None,
            )
        # Should NOT return 1 (user confirmed with 'y')
        assert result == 0
        bp.generate_project.assert_called_once_with(target)

    def test_non_empty_dir_interactive_user_cancels_no_console(self, tmp_path):
        """Lines 451-453: non-interactive=False, no console, user types 'n'."""
        args = _args(blueprint="test-bp", non_interactive=False)
        bp = _mock_blueprint("test-bp")
        registry = _mock_registry(blueprint=bp)
        target = tmp_path / "test-bp"
        target.mkdir()
        (target / "existing.txt").touch()

        with patch("builtins.input", return_value="n"):
            result = run_blueprint_mode(
                args,
                _logger(),
                blueprint_registry=registry,
                get_target_directory_fn=MagicMock(return_value=target),
                console_factory=None,
            )
        assert result == 1

    def test_non_empty_dir_interactive_user_types_y_no_console(self, tmp_path):
        """Lines 451-453: non-interactive=False, no console, user types 'y'."""
        args = _args(blueprint="test-bp", non_interactive=False)
        bp = _mock_blueprint("test-bp")
        registry = _mock_registry(blueprint=bp)
        target = tmp_path / "test-bp"
        target.mkdir()
        (target / "existing.txt").touch()

        with patch("builtins.input", return_value="y"):
            result = run_blueprint_mode(
                args,
                _logger(),
                blueprint_registry=registry,
                get_target_directory_fn=MagicMock(return_value=target),
                console_factory=None,
            )
        assert result == 0

    def test_blueprint_not_found_no_console(self):
        """Lines 427-431: no console path when blueprint not found."""
        args = _args(blueprint="missing", non_interactive=True)
        mock_bp_item = MagicMock()
        mock_bp_item.metadata.name = "available-bp"
        mock_bp_item.metadata.title = "Available Blueprint"
        registry = _mock_registry(blueprint=None, available=[mock_bp_item])

        with patch("fluid_build.cli.forge_modes.cprint"):
            with patch("fluid_build.cli.forge_modes.console_error"):
                result = run_blueprint_mode(
                    args,
                    _logger(),
                    blueprint_registry=registry,
                    get_target_directory_fn=MagicMock(return_value=Path("/tmp/out")),
                    console_factory=None,
                )
        assert result == 1

    def test_success_no_console_prints_next_steps(self, tmp_path):
        """Lines 465-471: successful blueprint with no console calls cprint."""
        args = _args(blueprint="test-bp", non_interactive=True)
        bp = _mock_blueprint("test-bp")
        registry = _mock_registry(blueprint=bp)
        target = tmp_path / "test-bp"

        with patch("fluid_build.cli.forge_modes.cprint") as mock_cprint:
            with patch("fluid_build.cli.forge_modes.success"):
                result = run_blueprint_mode(
                    args,
                    _logger(),
                    blueprint_registry=registry,
                    get_target_directory_fn=MagicMock(return_value=target),
                    console_factory=None,
                )
        assert result == 0
        assert mock_cprint.called


# ── run_forge_blueprint_impl – interactive branches ───────────────────


class TestRunForgeBlueprintImplAdditional:
    """Cover lines 497-550 (interactive prompt paths)."""

    def test_non_interactive_info_logged_quickstart_true(self, tmp_path):
        """Lines 521-526: non_interactive=False quickstart=True skips confirm prompt."""
        args = _args(blueprint="test-bp", non_interactive=False, quickstart=True, dry_run=False)
        bp = _mock_blueprint("test-bp")
        bp.validate.return_value = []
        registry = _mock_registry(blueprint=bp)
        target = tmp_path / "out"

        result = run_forge_blueprint_impl(
            args,
            registry,
            get_target_directory_fn=MagicMock(return_value=target),
        )
        assert result == 0

    def test_non_interactive_quickstart_false_user_continues(self, tmp_path):
        """Lines 527-530: quickstart=False prompts user; user says yes."""
        args = _args(blueprint="test-bp", non_interactive=False, quickstart=False, dry_run=False)
        bp = _mock_blueprint("test-bp")
        bp.validate.return_value = []
        registry = _mock_registry(blueprint=bp)
        target = tmp_path / "out"

        with patch("builtins.input", return_value="y"):
            result = run_forge_blueprint_impl(
                args,
                registry,
                get_target_directory_fn=MagicMock(return_value=target),
            )
        assert result == 0

    def test_non_interactive_quickstart_false_user_cancels(self, tmp_path):
        """Lines 528-530: quickstart=False, user says 'n'."""
        args = _args(blueprint="test-bp", non_interactive=False, quickstart=False, dry_run=False)
        bp = _mock_blueprint("test-bp")
        bp.validate.return_value = []
        registry = _mock_registry(blueprint=bp)
        target = tmp_path / "out"

        with patch("builtins.input", return_value="n"):
            result = run_forge_blueprint_impl(
                args,
                registry,
                get_target_directory_fn=MagicMock(return_value=target),
            )
        assert result == 1

    def test_non_empty_dir_non_interactive_false_user_says_y(self, tmp_path):
        """Lines 503-506: non-empty directory, user confirms continuation."""
        args = _args(blueprint="test-bp", non_interactive=False, quickstart=True, dry_run=False)
        bp = _mock_blueprint("test-bp")
        bp.validate.return_value = []
        registry = _mock_registry(blueprint=bp)
        target = tmp_path / "out"
        target.mkdir()
        (target / "existing.txt").touch()

        with patch("builtins.input", return_value="y"):
            result = run_forge_blueprint_impl(
                args,
                registry,
                get_target_directory_fn=MagicMock(return_value=target),
            )
        assert result == 0

    def test_non_empty_dir_non_interactive_false_user_says_n(self, tmp_path):
        """Lines 507-508: non-empty directory, user declines."""
        args = _args(blueprint="test-bp", non_interactive=False, quickstart=True, dry_run=False)
        bp = _mock_blueprint("test-bp")
        bp.validate.return_value = []
        registry = _mock_registry(blueprint=bp)
        target = tmp_path / "out"
        target.mkdir()
        (target / "existing.txt").touch()

        with patch("builtins.input", return_value="n"):
            result = run_forge_blueprint_impl(
                args,
                registry,
                get_target_directory_fn=MagicMock(return_value=target),
            )
        assert result == 1
