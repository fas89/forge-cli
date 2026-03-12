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

"""Branch-coverage tests for fluid_build.cli.blueprint"""

import argparse
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from fluid_build.blueprints.base import BlueprintCategory, BlueprintComplexity
from fluid_build.cli.blueprint import (
    COMMAND,
    create_project,
    describe_blueprint,
    list_blueprints,
    register,
    run,
    search_blueprints,
    validate_blueprints,
)


@pytest.fixture
def logger():
    return logging.getLogger("test_blueprint")


def _make_metadata(**overrides):
    m = MagicMock()
    m.name = overrides.get("name", "test-bp")
    m.title = overrides.get("title", "Test Blueprint")
    m.description = overrides.get("description", "A test blueprint for coverage" + "x" * 80)
    m.category = overrides.get("category", BlueprintCategory.ANALYTICS)
    m.complexity = overrides.get("complexity", BlueprintComplexity.BEGINNER)
    m.providers = overrides.get("providers", ["local", "gcp"])
    m.runtimes = overrides.get("runtimes", ["python"])
    m.setup_time = overrides.get("setup_time", "5 minutes")
    m.tags = overrides.get("tags", ["test", "demo"])
    m.has_sample_data = overrides.get("has_sample_data", True)
    m.has_tests = overrides.get("has_tests", True)
    m.has_docs = overrides.get("has_docs", True)
    m.has_cicd = overrides.get("has_cicd", False)
    m.use_cases = overrides.get("use_cases", ["testing"])
    m.best_practices = overrides.get("best_practices", ["write tests"])
    m.dependencies = overrides.get("dependencies", [])
    m.author = overrides.get("author", "test-author")
    m.created_at = overrides.get("created_at", "2024-01-01")
    m.updated_at = overrides.get("updated_at", None)
    return m


def _make_blueprint(**overrides):
    bp = MagicMock()
    bp.metadata = _make_metadata(**overrides)
    bp.validate.return_value = overrides.get("validate_errors", [])
    bp.path = overrides.get("path", Path("/fake/blueprint"))
    return bp


# ── Constants ────────────────────────────────────────────────────────


class TestConstants:
    def test_command(self):
        assert COMMAND == "blueprint"


# ── register ─────────────────────────────────────────────────────────


class TestRegister:
    def test_register_creates_subparser(self):
        parser = argparse.ArgumentParser()
        subs = parser.add_subparsers()
        register(subs)
        # Should be parseable
        args = parser.parse_args(["blueprint", "list"])
        assert args.blueprint_action == "list"

    def test_register_describe(self):
        parser = argparse.ArgumentParser()
        subs = parser.add_subparsers()
        register(subs)
        args = parser.parse_args(["blueprint", "describe", "my-bp"])
        assert args.name == "my-bp"


# ── run dispatch ─────────────────────────────────────────────────────


class TestRun:
    def test_no_action_returns_1(self, logger):
        args = SimpleNamespace(blueprint_action=None)
        with patch("fluid_build.cli.blueprint.blueprint_registry"):
            result = run(args, logger)
        assert result == 1

    @patch("fluid_build.cli.blueprint.list_blueprints", return_value=0)
    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_list_dispatch(self, mock_reg, mock_list, logger):
        args = SimpleNamespace(blueprint_action="list")
        result = run(args, logger)
        mock_list.assert_called_once()
        assert result == 0

    @patch("fluid_build.cli.blueprint.describe_blueprint", return_value=0)
    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_describe_dispatch(self, mock_reg, mock_desc, logger):
        args = SimpleNamespace(blueprint_action="describe")
        run(args, logger)
        mock_desc.assert_called_once()

    @patch("fluid_build.cli.blueprint.create_project", return_value=0)
    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_create_dispatch(self, mock_reg, mock_create, logger):
        args = SimpleNamespace(blueprint_action="create")
        run(args, logger)
        mock_create.assert_called_once()

    @patch("fluid_build.cli.blueprint.search_blueprints", return_value=0)
    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_search_dispatch(self, mock_reg, mock_search, logger):
        args = SimpleNamespace(blueprint_action="search")
        run(args, logger)
        mock_search.assert_called_once()

    @patch("fluid_build.cli.blueprint.validate_blueprints", return_value=0)
    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_validate_dispatch(self, mock_reg, mock_val, logger):
        args = SimpleNamespace(blueprint_action="validate")
        run(args, logger)
        mock_val.assert_called_once()

    def test_unknown_action_returns_1(self, logger):
        args = SimpleNamespace(blueprint_action="xyz")
        with patch("fluid_build.cli.blueprint.blueprint_registry"):
            result = run(args, logger)
        assert result == 1

    def test_keyboard_interrupt_returns_1(self, logger):
        args = SimpleNamespace(blueprint_action="list")
        with patch("fluid_build.cli.blueprint.blueprint_registry") as mock_reg:
            mock_reg.refresh.side_effect = KeyboardInterrupt()
            result = run(args, logger)
        assert result == 1

    def test_exception_returns_1(self, logger):
        args = SimpleNamespace(blueprint_action="list")
        with patch("fluid_build.cli.blueprint.blueprint_registry") as mock_reg:
            mock_reg.refresh.side_effect = RuntimeError("boom")
            result = run(args, logger)
        assert result == 1


# ── list_blueprints ──────────────────────────────────────────────────


class TestListBlueprints:
    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_no_results(self, mock_reg, logger):
        mock_reg.list_blueprints.return_value = []
        args = SimpleNamespace(category=None, complexity=None, provider=None, verbose=False)
        assert list_blueprints(args, logger) == 0

    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_with_results_brief(self, mock_reg, logger):
        bp = _make_blueprint()
        mock_reg.list_blueprints.return_value = [bp]
        args = SimpleNamespace(category=None, complexity=None, provider=None, verbose=False)
        assert list_blueprints(args, logger) == 0

    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_with_results_verbose(self, mock_reg, logger):
        bp = _make_blueprint()
        mock_reg.list_blueprints.return_value = [bp]
        args = SimpleNamespace(category=None, complexity=None, provider=None, verbose=True)
        assert list_blueprints(args, logger) == 0

    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_category_filter(self, mock_reg, logger):
        mock_reg.list_blueprints.return_value = []
        args = SimpleNamespace(category="analytics", complexity=None, provider=None, verbose=False)
        list_blueprints(args, logger)
        call_kwargs = mock_reg.list_blueprints.call_args[1]
        assert call_kwargs["category"] == BlueprintCategory.ANALYTICS

    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_complexity_filter(self, mock_reg, logger):
        mock_reg.list_blueprints.return_value = []
        args = SimpleNamespace(category=None, complexity="beginner", provider=None, verbose=False)
        list_blueprints(args, logger)
        call_kwargs = mock_reg.list_blueprints.call_args[1]
        assert call_kwargs["complexity"] == BlueprintComplexity.BEGINNER

    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_verbose_with_tags(self, mock_reg, logger):
        bp = _make_blueprint(tags=["tag1", "tag2"])
        mock_reg.list_blueprints.return_value = [bp]
        args = SimpleNamespace(category=None, complexity=None, provider=None, verbose=True)
        assert list_blueprints(args, logger) == 0

    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_verbose_no_tags(self, mock_reg, logger):
        bp = _make_blueprint(tags=[])
        mock_reg.list_blueprints.return_value = [bp]
        args = SimpleNamespace(category=None, complexity=None, provider=None, verbose=True)
        assert list_blueprints(args, logger) == 0


# ── describe_blueprint ───────────────────────────────────────────────


class TestDescribeBlueprint:
    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_not_found(self, mock_reg, logger):
        mock_reg.get_blueprint.return_value = None
        args = SimpleNamespace(name="nonexistent")
        assert describe_blueprint(args, logger) == 1

    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_found_minimal(self, mock_reg, logger):
        bp = _make_blueprint(
            use_cases=[],
            best_practices=[],
            tags=[],
            dependencies=[],
            has_sample_data=False,
            has_tests=False,
            has_docs=False,
            has_cicd=False,
            updated_at=None,
        )
        mock_reg.get_blueprint.return_value = bp
        args = SimpleNamespace(name="test-bp")
        assert describe_blueprint(args, logger) == 0

    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_found_full(self, mock_reg, logger):
        dep = MagicMock()
        dep.name = "pandas"
        dep.version = "2.0"
        dep.required = True
        bp = _make_blueprint(
            has_sample_data=True,
            has_tests=True,
            has_docs=True,
            has_cicd=True,
            use_cases=["analytics"],
            best_practices=["test first"],
            tags=["data"],
            dependencies=[dep],
            updated_at="2024-06-01",
        )
        mock_reg.get_blueprint.return_value = bp
        args = SimpleNamespace(name="test-bp")
        assert describe_blueprint(args, logger) == 0

    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_optional_dep_no_version(self, mock_reg, logger):
        dep = MagicMock()
        dep.name = "optional-lib"
        dep.version = None
        dep.required = False
        bp = _make_blueprint(dependencies=[dep])
        mock_reg.get_blueprint.return_value = bp
        args = SimpleNamespace(name="test-bp")
        assert describe_blueprint(args, logger) == 0


# ── create_project ───────────────────────────────────────────────────


class TestCreateProject:
    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_not_found(self, mock_reg, logger):
        mock_reg.get_blueprint.return_value = None
        args = SimpleNamespace(name="no-bp", target_dir=None, quickstart=False, dry_run=False)
        assert create_project(args, logger) == 1

    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_validation_errors(self, mock_reg, logger, tmp_path):
        bp = _make_blueprint(validate_errors=["missing file"])
        mock_reg.get_blueprint.return_value = bp
        target = str(tmp_path / "new-project")
        args = SimpleNamespace(name="test-bp", target_dir=target, quickstart=True, dry_run=False)
        assert create_project(args, logger) == 1

    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_dry_run(self, mock_reg, logger, tmp_path):
        bp_path = tmp_path / "bp_src"
        bp_path.mkdir()
        (bp_path / "contract.yaml").write_text("test")
        (bp_path / "blueprint.yaml").write_text("meta")
        bp = _make_blueprint(path=bp_path)
        mock_reg.get_blueprint.return_value = bp
        target = str(tmp_path / "output")
        args = SimpleNamespace(name="test-bp", target_dir=target, quickstart=True, dry_run=True)
        assert create_project(args, logger) == 0
        bp.generate_project.assert_not_called()

    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_generate_success(self, mock_reg, logger, tmp_path):
        bp = _make_blueprint()
        mock_reg.get_blueprint.return_value = bp
        target = str(tmp_path / "new-project")
        args = SimpleNamespace(name="test-bp", target_dir=target, quickstart=True, dry_run=False)
        assert create_project(args, logger) == 0
        bp.generate_project.assert_called_once()

    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_existing_dir_quickstart_fails(self, mock_reg, logger, tmp_path):
        bp = _make_blueprint()
        mock_reg.get_blueprint.return_value = bp
        target = tmp_path / "existing"
        target.mkdir()
        (target / "file.txt").write_text("exists")
        args = SimpleNamespace(
            name="test-bp", target_dir=str(target), quickstart=True, dry_run=False
        )
        assert create_project(args, logger) == 1

    @patch("builtins.input", return_value="n")
    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_existing_dir_user_cancels(self, mock_reg, mock_input, logger, tmp_path):
        bp = _make_blueprint()
        mock_reg.get_blueprint.return_value = bp
        target = tmp_path / "existing"
        target.mkdir()
        (target / "file.txt").write_text("exists")
        args = SimpleNamespace(
            name="test-bp", target_dir=str(target), quickstart=False, dry_run=False
        )
        assert create_project(args, logger) == 1

    @patch("builtins.input", return_value="y")
    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_existing_dir_user_confirms(self, mock_reg, mock_input, logger, tmp_path):
        bp = _make_blueprint()
        mock_reg.get_blueprint.return_value = bp
        target = tmp_path / "existing"
        target.mkdir()
        (target / "file.txt").write_text("exists")
        args = SimpleNamespace(
            name="test-bp", target_dir=str(target), quickstart=False, dry_run=False
        )
        # Will fail because validate() returns [] (ok) but then hits second input prompt
        # Need to handle the second prompt too
        mock_input.side_effect = ["y", "Y"]
        assert create_project(args, logger) == 0

    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_default_target_dir(self, mock_reg, logger, tmp_path, monkeypatch):
        bp = _make_blueprint()
        mock_reg.get_blueprint.return_value = bp
        monkeypatch.chdir(tmp_path)
        args = SimpleNamespace(name="test-bp", target_dir=None, quickstart=True, dry_run=False)
        assert create_project(args, logger) == 0

    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_dbt_runtime_extra_steps(self, mock_reg, logger, tmp_path):
        bp = _make_blueprint(runtimes=["python", "dbt"])
        mock_reg.get_blueprint.return_value = bp
        target = str(tmp_path / "dbt-project")
        args = SimpleNamespace(name="test-bp", target_dir=target, quickstart=True, dry_run=False)
        assert create_project(args, logger) == 0


# ── search_blueprints ────────────────────────────────────────────────


class TestSearchBlueprints:
    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_no_results(self, mock_reg, logger):
        mock_reg.search_blueprints.return_value = []
        args = SimpleNamespace(query="nonexistent")
        assert search_blueprints(args, logger) == 0

    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_results_found(self, mock_reg, logger):
        bp = _make_blueprint(tags=["a", "b", "c", "d"])
        mock_reg.search_blueprints.return_value = [bp]
        args = SimpleNamespace(query="test")
        assert search_blueprints(args, logger) == 0

    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_few_tags(self, mock_reg, logger):
        bp = _make_blueprint(tags=["x"])
        mock_reg.search_blueprints.return_value = [bp]
        args = SimpleNamespace(query="x")
        assert search_blueprints(args, logger) == 0


# ── validate_blueprints ─────────────────────────────────────────────


class TestValidateBlueprints:
    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_single_not_found(self, mock_reg, logger):
        mock_reg.get_blueprint.return_value = None
        args = SimpleNamespace(name="no-bp")
        assert validate_blueprints(args, logger) == 1

    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_single_valid(self, mock_reg, logger):
        bp = _make_blueprint(validate_errors=[])
        mock_reg.get_blueprint.return_value = bp
        args = SimpleNamespace(name="test-bp")
        assert validate_blueprints(args, logger) == 0

    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_single_invalid(self, mock_reg, logger):
        bp = _make_blueprint(validate_errors=["error 1"])
        mock_reg.get_blueprint.return_value = bp
        args = SimpleNamespace(name="test-bp")
        assert validate_blueprints(args, logger) == 1

    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_all_valid(self, mock_reg, logger):
        mock_reg.validate_all.return_value = {}
        args = SimpleNamespace(name=None)
        assert validate_blueprints(args, logger) == 0

    @patch("fluid_build.cli.blueprint.blueprint_registry")
    def test_all_with_errors(self, mock_reg, logger):
        mock_reg.validate_all.return_value = {"bp1": ["err1"], "bp2": ["err2"]}
        args = SimpleNamespace(name=None)
        assert validate_blueprints(args, logger) == 1
