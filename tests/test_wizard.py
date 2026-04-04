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

"""Tests for fluid_build.cli.wizard."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from fluid_build.cli._common import CLIError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_args(provider=None, skip_preview=False):
    return argparse.Namespace(provider=provider, skip_preview=skip_preview)


def _sample_config(provider="local"):
    return {
        "id": "test-product",
        "domain": "analytics",
        "layer": "silver",
        "owner": "data-team",
        "description": "A test product",
        "provider": provider,
    }


# ---------------------------------------------------------------------------
# Tests for run()
# ---------------------------------------------------------------------------


class TestWizardRun:
    """Tests for the top-level wizard.run() function."""

    def _run(self, args, config_override=None):
        """Patch away all I/O and run the wizard, returning exit code."""
        from fluid_build.cli import wizard

        config = config_override or _sample_config()

        with patch.object(wizard, "_detect_provider", return_value="local"):
            with patch.object(wizard, "_gather_product_info", return_value=config):
                with patch.object(wizard, "_create_directory_structure"):
                    with patch.object(wizard, "_generate_contract", return_value={}):
                        with patch.object(wizard, "_write_yaml"):
                            with patch.object(wizard, "_generate_scaffolding"):
                                with patch.object(wizard, "_save_context"):
                                    with patch.object(wizard, "_run_preview"):
                                        return wizard.run(args, logger)

    def test_run_returns_0_on_success(self):
        args = _make_args()
        code = self._run(args)
        assert code == 0

    def test_run_skip_preview_skips_preview_call(self):
        from fluid_build.cli import wizard

        config = _sample_config()
        args = _make_args(skip_preview=True)
        preview_mock = MagicMock()

        with patch.object(wizard, "_detect_provider", return_value="local"):
            with patch.object(wizard, "_gather_product_info", return_value=config):
                with patch.object(wizard, "_create_directory_structure"):
                    with patch.object(wizard, "_generate_contract", return_value={}):
                        with patch.object(wizard, "_write_yaml"):
                            with patch.object(wizard, "_generate_scaffolding"):
                                with patch.object(wizard, "_save_context"):
                                    with patch.object(wizard, "_run_preview", preview_mock):
                                        wizard.run(args, logger)

        preview_mock.assert_not_called()

    def test_run_uses_provider_from_args(self):
        """If args.provider is set, _detect_provider should not be called."""
        from fluid_build.cli import wizard

        config = _sample_config(provider="gcp")
        args = _make_args(provider="gcp")
        detect_mock = MagicMock(return_value="gcp")

        with patch.object(wizard, "_detect_provider", detect_mock):
            with patch.object(wizard, "_gather_product_info", return_value=config):
                with patch.object(wizard, "_create_directory_structure"):
                    with patch.object(wizard, "_generate_contract", return_value={}):
                        with patch.object(wizard, "_write_yaml"):
                            with patch.object(wizard, "_generate_scaffolding"):
                                with patch.object(wizard, "_save_context"):
                                    with patch.object(wizard, "_run_preview"):
                                        wizard.run(args, logger)

        detect_mock.assert_not_called()

    def test_run_returns_130_on_keyboard_interrupt(self):
        from fluid_build.cli import wizard

        args = _make_args()
        with patch.object(wizard, "_detect_provider", side_effect=KeyboardInterrupt()):
            code = wizard.run(args, logger)
        assert code == 130

    def test_run_raises_cli_error_on_unexpected_exception(self):
        from fluid_build.cli import wizard

        args = _make_args()
        with patch.object(wizard, "_detect_provider", side_effect=ValueError("unexpected")):
            with pytest.raises(CLIError):
                wizard.run(args, logger)

    def test_run_raises_cli_error_passthrough(self):
        """CLIError raised inside should propagate unchanged."""
        from fluid_build.cli import wizard

        args = _make_args()
        cli_err = CLIError(2, "some_event", {"msg": "test"})
        with patch.object(wizard, "_detect_provider", side_effect=cli_err):
            with pytest.raises(CLIError) as exc_info:
                wizard.run(args, logger)
        assert exc_info.value is cli_err

    def test_run_with_rich_available(self):
        """When rich is available the run function should succeed."""
        from fluid_build.cli import wizard

        config = _sample_config()
        args = _make_args()

        with patch.object(wizard, "_detect_provider", return_value="local"):
            with patch.object(wizard, "_gather_product_info", return_value=config):
                with patch.object(wizard, "_create_directory_structure"):
                    with patch.object(wizard, "_generate_contract", return_value={}):
                        with patch.object(wizard, "_write_yaml"):
                            with patch.object(wizard, "_generate_scaffolding"):
                                with patch.object(wizard, "_save_context"):
                                    with patch.object(wizard, "_run_preview"):
                                        code = wizard.run(args, logger)
        assert code == 0


# ---------------------------------------------------------------------------
# Tests for _detect_provider()
# ---------------------------------------------------------------------------


class TestDetectProvider:
    def _call(self, env_vars=None, has_rich=False, prompt_return=""):
        from fluid_build.cli.wizard import _detect_provider

        env_vars = env_vars or {}
        # Use empty input so the function falls through to the detected default
        with patch.dict(os.environ, env_vars, clear=False):
            with patch("builtins.input", return_value=prompt_return):
                return _detect_provider(console=None, has_rich=False, logger=logger)

    def test_detects_gcp_from_env(self):
        result = self._call(env_vars={"GCLOUD_PROJECT": "my-proj"})
        assert result == "gcp"

    def test_detects_snowflake_from_env(self):
        result = self._call(env_vars={"SNOWFLAKE_ACCOUNT": "xy12345"})
        assert result == "snowflake"

    def test_detects_aws_from_env(self):
        result = self._call(env_vars={"AWS_PROFILE": "default"})
        assert result == "aws"

    def test_defaults_to_local(self):
        # Remove cloud env vars to force local detection
        clean_env = {
            k: v
            for k, v in os.environ.items()
            if k
            not in {
                "GOOGLE_APPLICATION_CREDENTIALS",
                "GCLOUD_PROJECT",
                "SNOWFLAKE_ACCOUNT",
                "AWS_PROFILE",
                "AWS_ACCESS_KEY_ID",
            }
        }
        from fluid_build.cli.wizard import _detect_provider

        with patch.dict(os.environ, clean_env, clear=True):
            with patch("builtins.input", return_value=""):
                result = _detect_provider(console=None, has_rich=False, logger=logger)
        assert result == "local"


# ---------------------------------------------------------------------------
# Tests for _gather_product_info()
# ---------------------------------------------------------------------------


class TestGatherProductInfo:
    def test_uses_defaults_when_input_blank(self):
        from fluid_build.cli.wizard import _gather_product_info

        with patch("builtins.input", return_value=""):
            config = _gather_product_info(console=None, has_rich=False, provider="local")

        assert config["id"] == "my-data-product"
        assert config["domain"] == "analytics"
        assert config["layer"] == "silver"
        assert config["provider"] == "local"

    def test_uses_user_provided_values(self):
        from fluid_build.cli.wizard import _gather_product_info

        responses = iter(["my-product", "finance", "gold", "team-alpha", "A great product"])
        with patch("builtins.input", side_effect=lambda _: next(responses)):
            config = _gather_product_info(console=None, has_rich=False, provider="gcp")

        assert config["id"] == "my-product"
        assert config["domain"] == "finance"
        assert config["layer"] == "gold"
        assert config["owner"] == "team-alpha"
        assert config["description"] == "A great product"


# ---------------------------------------------------------------------------
# Tests for _generate_contract()
# ---------------------------------------------------------------------------


class TestGenerateContract:
    def test_generates_valid_structure(self):
        from fluid_build.cli.wizard import _generate_contract

        config = _sample_config(provider="gcp")
        contract = _generate_contract(config, "gcp")

        assert contract["version"] == "0.5.7"
        assert contract["kind"] == "DataProduct"
        assert contract["metadata"]["id"] == "test-product"
        assert contract["spec"]["builds"][0]["runtime"] == "dbt"

    def test_generates_sql_runtime_for_snowflake(self):
        from fluid_build.cli.wizard import _generate_contract

        config = _sample_config(provider="snowflake")
        contract = _generate_contract(config, "snowflake")

        assert contract["spec"]["builds"][0]["runtime"] == "sql"

    def test_generates_dbt_runtime_for_local(self):
        from fluid_build.cli.wizard import _generate_contract

        config = _sample_config(provider="local")
        contract = _generate_contract(config, "local")

        assert contract["spec"]["builds"][0]["runtime"] == "dbt"


# ---------------------------------------------------------------------------
# Tests for _create_directory_structure()
# ---------------------------------------------------------------------------


class TestCreateDirectoryStructure:
    def test_creates_base_and_config_and_docs(self, tmp_path):
        from fluid_build.cli.wizard import _create_directory_structure

        base = tmp_path / "my-product"
        _create_directory_structure(base, "local", logger)

        assert (base / "config").is_dir()
        assert (base / "docs").is_dir()

    def test_creates_dbt_dir_for_gcp(self, tmp_path):
        from fluid_build.cli.wizard import _create_directory_structure

        base = tmp_path / "gcp-product"
        _create_directory_structure(base, "gcp", logger)

        assert (base / "dbt" / "models").is_dir()

    def test_creates_sql_dir_for_snowflake(self, tmp_path):
        from fluid_build.cli.wizard import _create_directory_structure

        base = tmp_path / "sf-product"
        _create_directory_structure(base, "snowflake", logger)

        assert (base / "sql").is_dir()


# ---------------------------------------------------------------------------
# Tests for _generate_scaffolding()
# ---------------------------------------------------------------------------


class TestGenerateScaffolding:
    def test_creates_readme(self, tmp_path):
        from fluid_build.cli.wizard import _generate_scaffolding

        config = _sample_config(provider="local")
        base = tmp_path / "prod"
        base.mkdir()
        (base / "dbt").mkdir()
        (base / "dbt" / "models").mkdir()

        _generate_scaffolding(base, "local", config, logger)

        readme = base / "README.md"
        assert readme.exists()
        assert "test-product" in readme.read_text()

    def test_creates_dbt_project_for_gcp(self, tmp_path):
        from fluid_build.cli.wizard import _generate_scaffolding

        config = _sample_config(provider="gcp")
        base = tmp_path / "gcp-prod"
        base.mkdir()
        (base / "dbt").mkdir()
        (base / "dbt" / "models").mkdir()

        _generate_scaffolding(base, "gcp", config, logger)

        assert (base / "dbt" / "dbt_project.json").exists()
        assert (base / "dbt" / "models" / "example.sql").exists()


# ---------------------------------------------------------------------------
# Tests for _save_context()
# ---------------------------------------------------------------------------


class TestSaveContext:
    def test_writes_context_json(self, tmp_path, monkeypatch):
        import json

        from fluid_build.cli.wizard import _save_context

        monkeypatch.chdir(tmp_path)
        config = _sample_config()
        _save_context(config, logger)

        ctx_file = tmp_path / ".fluid" / "context.json"
        assert ctx_file.exists()
        data = json.loads(ctx_file.read_text())
        assert data["last_product"] == "test-product"
        assert data["default_provider"] == "local"


# ---------------------------------------------------------------------------
# Tests for _run_preview()
# ---------------------------------------------------------------------------


class TestRunPreview:
    def test_runs_without_error(self):
        from fluid_build.cli.wizard import _run_preview

        _run_preview("contract.fluid.yaml", "local", logger)

    def test_handles_inner_exception_gracefully(self):
        """Any exception raised in the try block is caught and logged."""
        from fluid_build.cli.wizard import _run_preview

        # First three info calls succeed; the fourth would be in the except block.
        # We simulate an error on the planning step and ensure the except branch runs.
        call_log = []

        def fake_info(log, event, **kwargs):
            call_log.append(event)
            if event == "wizard_planning":
                raise ValueError("planning error")

        with patch("fluid_build.cli.wizard.info", side_effect=fake_info):
            _run_preview("contract.fluid.yaml", "local", logger)

        assert "wizard_preview_skipped" in call_log
