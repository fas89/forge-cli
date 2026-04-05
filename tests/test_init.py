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

"""Comprehensive tests for fluid_build.cli.init — mode handlers, helpers, scanners."""

import argparse
import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from fluid_build.schema_manager import FluidSchemaManager

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_args(**overrides):
    defaults = dict(
        name=None,
        quickstart=False,
        scan=False,
        wizard=False,
        blank=False,
        template=None,
        provider="local",
        use_case=None,
        no_run=False,
        no_dag=False,
        dry_run=False,
        yes=True,
    )
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


@pytest.fixture
def logger():
    return logging.getLogger("test_init")


# ===========================================================================
# quickstart_mode
# ===========================================================================


class TestQuickstartMode:
    @patch("fluid_build.cli.init.show_success_message")
    @patch("fluid_build.cli.init.generate_cicd")
    @patch("fluid_build.cli.init.run_local_pipeline")
    @patch("fluid_build.cli.init.init_local_db")
    @patch("fluid_build.cli.init.copy_sample_data")
    @patch("fluid_build.cli.init.copy_template", return_value=True)
    def test_happy_path(
        self,
        mock_copy,
        _mock_data,
        _mock_db,
        _mock_run,
        _mock_cicd,
        _mock_success,
        tmp_path,
        logger,
    ):
        from fluid_build.cli.init import quickstart_mode

        args = _make_args(name=str(tmp_path / "qs-project"), no_run=True, no_dag=True)
        result = quickstart_mode(args, logger)
        assert result == 0
        mock_copy.assert_called_once()

    @patch("fluid_build.cli.init.copy_template", return_value=False)
    def test_copy_template_fails_returns_1(self, _mock_copy, tmp_path, logger):
        from fluid_build.cli.init import quickstart_mode

        args = _make_args(name=str(tmp_path / "qs-fail"), no_run=True, no_dag=True)
        result = quickstart_mode(args, logger)
        assert result == 1

    def test_dry_run_returns_0(self, tmp_path, logger):
        from fluid_build.cli.init import quickstart_mode

        args = _make_args(name=str(tmp_path / "qs-dry"), dry_run=True)
        result = quickstart_mode(args, logger)
        assert result == 0

    def test_existing_nonempty_dir_returns_1(self, tmp_path, logger):
        from fluid_build.cli.init import quickstart_mode

        existing = tmp_path / "existing-project"
        existing.mkdir()
        (existing / "some_file.txt").write_text("content")
        args = _make_args(name=str(existing))
        result = quickstart_mode(args, logger)
        assert result == 1

    @patch("fluid_build.cli.init.show_success_message")
    @patch("fluid_build.cli.init.generate_cicd")
    @patch("fluid_build.cli.init.run_local_pipeline")
    @patch("fluid_build.cli.init.init_local_db")
    @patch("fluid_build.cli.init.copy_sample_data")
    @patch("fluid_build.cli.init.copy_template", return_value=True)
    def test_auto_name_my_first_product(
        self,
        _mock_copy,
        _mock_data,
        _mock_db,
        _mock_run,
        _mock_cicd,
        _mock_success,
        tmp_path,
        logger,
        monkeypatch,
    ):
        from fluid_build.cli.init import quickstart_mode

        monkeypatch.chdir(tmp_path)
        args = _make_args(no_run=True, no_dag=True)
        result = quickstart_mode(args, logger)
        assert result == 0

    @patch("fluid_build.cli.init.show_success_message")
    @patch("fluid_build.cli.init.generate_cicd")
    @patch("fluid_build.cli.init.run_local_pipeline")
    @patch("fluid_build.cli.init.init_local_db")
    @patch("fluid_build.cli.init.copy_sample_data")
    @patch("fluid_build.cli.init.copy_template", return_value=True)
    def test_pipeline_runs_when_no_run_false(
        self,
        _mock_copy,
        _mock_data,
        _mock_db,
        mock_run_pipeline,
        _mock_cicd,
        _mock_success,
        tmp_path,
        logger,
    ):
        from fluid_build.cli.init import quickstart_mode

        args = _make_args(name=str(tmp_path / "run-test"), no_run=False, no_dag=True)
        quickstart_mode(args, logger)
        mock_run_pipeline.assert_called_once()

    @patch("fluid_build.cli.init.show_success_message")
    @patch("fluid_build.cli.init.generate_cicd")
    @patch("fluid_build.cli.init.run_local_pipeline")
    @patch("fluid_build.cli.init.init_local_db")
    @patch("fluid_build.cli.init.copy_sample_data")
    @patch("fluid_build.cli.init.copy_template", return_value=True)
    def test_no_run_skips_pipeline(
        self,
        _mock_copy,
        _mock_data,
        _mock_db,
        mock_run_pipeline,
        _mock_cicd,
        _mock_success,
        tmp_path,
        logger,
    ):
        from fluid_build.cli.init import quickstart_mode

        args = _make_args(name=str(tmp_path / "no-run-test"), no_run=True, no_dag=True)
        quickstart_mode(args, logger)
        mock_run_pipeline.assert_not_called()

    @patch("fluid_build.cli.init.copy_template", side_effect=RuntimeError("boom"))
    def test_exception_returns_1(self, _mock_copy, tmp_path, logger):
        from fluid_build.cli.init import quickstart_mode

        args = _make_args(name=str(tmp_path / "qs-exc"), no_run=True, no_dag=True)
        result = quickstart_mode(args, logger)
        assert result == 1

    @patch("fluid_build.cli.init.show_success_message")
    @patch("fluid_build.cli.init.generate_cicd")
    @patch("fluid_build.cli.init.run_local_pipeline")
    @patch("fluid_build.cli.init.init_local_db")
    @patch("fluid_build.cli.init.copy_sample_data")
    @patch("fluid_build.cli.init.generate_dag_for_project", return_value=True)
    @patch("fluid_build.cli.init.should_generate_dag", return_value=True)
    def test_dag_generated_when_contract_exists(
        self,
        _mock_should,
        _mock_dag,
        _mock_data,
        _mock_db,
        _mock_run,
        _mock_cicd,
        _mock_success,
        tmp_path,
        logger,
    ):
        from fluid_build.cli.init import quickstart_mode

        project_name = str(tmp_path / "dag-project")
        args = _make_args(name=project_name, no_run=True, no_dag=False)

        def _create_contract(project_dir, template, lgr):
            project_dir.mkdir(parents=True, exist_ok=True)
            (project_dir / "contract.fluid.yaml").write_text("name: test\n")
            return True

        with patch("fluid_build.cli.init.copy_template", side_effect=_create_contract):
            with patch("yaml.safe_load", return_value={"name": "test", "orchestration": {}}):
                result = quickstart_mode(args, logger)
        assert result == 0


# ===========================================================================
# scan_mode
# ===========================================================================


class TestScanMode:
    def test_no_project_detected_returns_1(self, logger):
        from fluid_build.cli.init import scan_mode

        args = _make_args(provider="local")
        with patch("fluid_build.cli.init.detect_project_type", return_value=None):
            result = scan_mode(args, logger)
        assert result == 1

    @patch("fluid_build.cli.init.show_migration_summary")
    @patch("fluid_build.cli.init.generate_cicd")
    @patch("fluid_build.cli.init.generate_contracts_from_scan")
    @patch("fluid_build.cli.init.show_scan_results")
    def test_scan_success_no_sensitive(
        self, _mock_results, mock_gen, _mock_cicd, _mock_summary, tmp_path, logger
    ):
        from fluid_build.cli.init import scan_mode

        args = _make_args(provider="local")
        mock_detector = MagicMock()
        mock_detector.scan.return_value = {
            "project_type": "dbt",
            "metadata": {},
            "models": [],
            "sensitive_columns": [],
        }
        mock_gen.return_value = [
            {"name": "c1", "version": FluidSchemaManager.latest_bundled_version()}
        ]

        with patch("fluid_build.cli.init.detect_project_type", return_value=mock_detector):
            with patch("fluid_build.cli.init.RICH_AVAILABLE", False):
                with patch("fluid_build.cli.init.Path") as mock_path_cls:
                    mock_path_cls.cwd.return_value = tmp_path
                    result = scan_mode(args, logger)
        assert result == 0

    @patch("fluid_build.cli.init.detect_project_type", side_effect=RuntimeError("scan boom"))
    def test_exception_returns_1(self, _mock_detect, logger):
        from fluid_build.cli.init import scan_mode

        args = _make_args()
        result = scan_mode(args, logger)
        assert result == 1

    def test_scan_zero_model_dbt_fails_without_writing_contract(
        self, tmp_path, logger, monkeypatch
    ):
        from fluid_build.cli.init import scan_mode

        monkeypatch.chdir(tmp_path)
        args = _make_args(provider="local")
        mock_detector = MagicMock()
        mock_detector.scan.return_value = {
            "project_type": "dbt",
            "metadata": {"project_name": "empty-dbt", "target_platform": "duckdb"},
            "models": [],
            "sensitive_columns": [],
        }

        with patch("fluid_build.cli.init.detect_project_type", return_value=mock_detector):
            with patch("fluid_build.cli.init.RICH_AVAILABLE", False):
                result = scan_mode(args, logger)

        assert result == 1
        assert list(tmp_path.glob("*.fluid.yaml")) == []

    @patch("fluid_build.cli.init.show_migration_summary")
    @patch("fluid_build.cli.init.generate_cicd")
    @patch("fluid_build.cli.init.apply_governance_policies")
    @patch("fluid_build.cli.init.generate_contracts_from_scan")
    @patch("fluid_build.cli.init.show_scan_results")
    def test_scan_with_sensitive_columns_calls_governance(
        self,
        _mock_show,
        mock_gen,
        mock_governance,
        _mock_cicd,
        _mock_summary,
        tmp_path,
        logger,
    ):
        from fluid_build.cli.init import scan_mode

        args = _make_args(provider="local")
        mock_detector = MagicMock()
        mock_detector.scan.return_value = {
            "project_type": "sql",
            "metadata": {},
            "files": [],
            "sensitive_columns": [{"col": "email", "type": "EMAIL"}],
        }
        mock_gen.return_value = [{"name": "sql-import"}]
        mock_governance.return_value = [{"name": "sql-import"}]

        with patch("fluid_build.cli.init.detect_project_type", return_value=mock_detector):
            with patch("fluid_build.cli.init.RICH_AVAILABLE", False):
                with patch("fluid_build.cli.init.Path") as mock_path_cls:
                    mock_path_cls.cwd.return_value = tmp_path
                    result = scan_mode(args, logger)
        assert result == 0
        mock_governance.assert_called_once()


# ===========================================================================
# wizard_mode
# ===========================================================================


class TestWizardMode:
    def test_wizard_import_error_no_rich_returns_1(self, logger, monkeypatch):
        from fluid_build.cli.init import wizard_mode

        args = _make_args(provider="local")
        # Remove wizard module if cached so import fails inside wizard_mode
        monkeypatch.delitem(sys.modules, "fluid_build.cli.wizard", raising=False)
        with patch("fluid_build.cli.init.RICH_AVAILABLE", False):
            with patch.dict("sys.modules", {"fluid_build.cli.wizard": None}):
                result = wizard_mode(args, logger)
        assert result == 1

    def test_wizard_run_delegates_when_importable(self, logger):
        from fluid_build.cli.init import wizard_mode

        args = _make_args(name="wiz-proj", provider="gcp")
        mock_run = MagicMock(return_value=7)
        mock_mod = MagicMock()
        mock_mod.run = mock_run
        with patch("fluid_build.cli.init.RICH_AVAILABLE", False):
            with patch.dict("sys.modules", {"fluid_build.cli.wizard": mock_mod}):
                result = wizard_mode(args, logger)
        assert result == 7
        mock_run.assert_called_once()


# ===========================================================================
# blank_mode
# ===========================================================================


class TestBlankMode:
    def test_existing_directory_returns_1(self, tmp_path, logger):
        from fluid_build.cli.init import blank_mode

        existing = tmp_path / "blank-existing"
        existing.mkdir()
        args = _make_args(name=str(existing))
        result = blank_mode(args, logger)
        assert result == 1

    def test_creates_contract_via_import_error_path(self, tmp_path, logger, monkeypatch):
        from fluid_build.cli.init import blank_mode

        monkeypatch.chdir(tmp_path)
        args = _make_args(name="blank-new-project", provider="local", dry_run=False)
        # Remove product_new from sys.modules to ensure ImportError inside blank_mode
        monkeypatch.delitem(sys.modules, "fluid_build.cli.product_new", raising=False)
        with patch("fluid_build.cli.init.RICH_AVAILABLE", False):
            with patch.dict("sys.modules", {"fluid_build.cli.product_new": None}):
                result = blank_mode(args, logger)
        assert result == 0
        contract = tmp_path / "blank-new-project" / "contract.fluid.yaml"
        assert contract.exists()
        content = contract.read_text()
        assert f'fluidVersion: "{FluidSchemaManager.latest_bundled_version()}"' in content
        assert "id: blank.blank-new-project" in content
        assert 'name: "blank-new-project"' in content

    def test_product_new_run_called_when_available(self, tmp_path, logger, monkeypatch):
        from fluid_build.cli.init import blank_mode

        monkeypatch.chdir(tmp_path)
        args = _make_args(name="blank-delegate", provider="local", dry_run=False)
        mock_run = MagicMock(return_value=0)
        mock_mod = MagicMock()
        mock_mod.run = mock_run
        monkeypatch.delitem(sys.modules, "fluid_build.cli.product_new", raising=False)
        with patch.dict("sys.modules", {"fluid_build.cli.product_new": mock_mod}):
            result = blank_mode(args, logger)
        assert result == 0
        mock_run.assert_called_once()

    def test_default_name_my_project(self, tmp_path, logger, monkeypatch):
        from fluid_build.cli.init import blank_mode

        monkeypatch.chdir(tmp_path)
        args = _make_args(provider="local", dry_run=False)
        monkeypatch.delitem(sys.modules, "fluid_build.cli.product_new", raising=False)
        with patch("fluid_build.cli.init.RICH_AVAILABLE", False):
            with patch.dict("sys.modules", {"fluid_build.cli.product_new": None}):
                result = blank_mode(args, logger)
        assert result == 0
        assert (tmp_path / "my-project" / "contract.fluid.yaml").exists()


# ===========================================================================
# template_mode
# ===========================================================================


class TestTemplateMode:
    def test_blueprint_create_from_template_success(self, tmp_path, logger, monkeypatch):
        from fluid_build.cli.init import template_mode

        monkeypatch.chdir(tmp_path)
        args = _make_args(template="customer-360", name="my-c360", provider="local")
        mock_blueprint = MagicMock()
        mock_blueprint.create_from_template.return_value = True
        monkeypatch.delitem(sys.modules, "fluid_build.cli.blueprint", raising=False)
        with patch.dict("sys.modules", {"fluid_build.cli.blueprint": mock_blueprint}):
            result = template_mode(args, logger)
        assert result == 0

    def test_blueprint_create_returns_false(self, tmp_path, logger, monkeypatch):
        from fluid_build.cli.init import template_mode

        monkeypatch.chdir(tmp_path)
        args = _make_args(template="bad-tmpl", name="x", provider="local")
        mock_blueprint = MagicMock()
        mock_blueprint.create_from_template.return_value = False
        monkeypatch.delitem(sys.modules, "fluid_build.cli.blueprint", raising=False)
        with patch.dict("sys.modules", {"fluid_build.cli.blueprint": mock_blueprint}):
            result = template_mode(args, logger)
        assert result == 1

    @patch("fluid_build.cli.init.copy_template", return_value=True)
    def test_fallback_copy_template_success(self, _mock_copy, tmp_path, logger, monkeypatch):
        from fluid_build.cli.init import template_mode

        monkeypatch.chdir(tmp_path)
        args = _make_args(template="my-tmpl", name="proj", provider="local")
        monkeypatch.delitem(sys.modules, "fluid_build.cli.blueprint", raising=False)
        with patch.dict("sys.modules", {"fluid_build.cli.blueprint": None}):
            result = template_mode(args, logger)
        assert result == 0

    @patch("fluid_build.cli.init.copy_template", return_value=False)
    def test_fallback_copy_template_failure(self, _mock_copy, tmp_path, logger, monkeypatch):
        from fluid_build.cli.init import template_mode

        monkeypatch.chdir(tmp_path)
        args = _make_args(template="bad-tmpl", name="proj", provider="local")
        monkeypatch.delitem(sys.modules, "fluid_build.cli.blueprint", raising=False)
        with patch.dict("sys.modules", {"fluid_build.cli.blueprint": None}):
            result = template_mode(args, logger)
        assert result == 1

    def test_uses_template_name_when_no_project_name(self, tmp_path, logger, monkeypatch):
        from fluid_build.cli.init import template_mode

        monkeypatch.chdir(tmp_path)
        args = _make_args(template="sales-analytics", name=None, provider="local")
        mock_blueprint = MagicMock()
        mock_blueprint.create_from_template.return_value = True
        monkeypatch.delitem(sys.modules, "fluid_build.cli.blueprint", raising=False)
        with patch.dict("sys.modules", {"fluid_build.cli.blueprint": mock_blueprint}):
            result = template_mode(args, logger)
        assert result == 0
        call_args = mock_blueprint.create_from_template.call_args
        assert call_args[0][0] == "sales-analytics"


# ===========================================================================
# copy_template
# ===========================================================================


class TestCopyTemplate:
    def test_missing_template_returns_false(self, tmp_path, logger):
        from fluid_build.cli.init import copy_template

        result = copy_template(tmp_path, "nonexistent-template-xyz-999", logger)
        assert result is False

    def test_copies_files_from_template(self, tmp_path, logger):
        import shutil

        from fluid_build.cli.init import copy_template

        cli_dir = Path(__file__).parent.parent / "fluid_build" / "cli"
        templates_dir = cli_dir.parent / "templates" / "test-tmpl-pytest"
        templates_dir.mkdir(parents=True, exist_ok=True)
        (templates_dir / "contract.fluid.yaml").write_text("name: test-template\n")

        project_dir = tmp_path / "output"
        project_dir.mkdir()

        try:
            result = copy_template(project_dir, "test-tmpl-pytest", logger)
            assert result is True
            assert (project_dir / "contract.fluid.yaml").exists()
        finally:
            shutil.rmtree(str(templates_dir))

    def test_copies_subdirectory(self, tmp_path, logger):
        import shutil

        from fluid_build.cli.init import copy_template

        cli_dir = Path(__file__).parent.parent / "fluid_build" / "cli"
        templates_dir = cli_dir.parent / "templates" / "test-tmpl-subdir"
        templates_dir.mkdir(parents=True, exist_ok=True)
        sub = templates_dir / "data"
        sub.mkdir()
        (sub / "sample.csv").write_text("a,b\n1,2\n")

        project_dir = tmp_path / "output2"
        project_dir.mkdir()

        try:
            result = copy_template(project_dir, "test-tmpl-subdir", logger)
            assert result is True
            assert (project_dir / "data" / "sample.csv").exists()
        finally:
            shutil.rmtree(str(templates_dir))

    def test_copy_exception_returns_false(self, tmp_path, logger):
        import shutil

        from fluid_build.cli.init import copy_template

        cli_dir = Path(__file__).parent.parent / "fluid_build" / "cli"
        templates_dir = cli_dir.parent / "templates" / "test-tmpl-perm"
        templates_dir.mkdir(parents=True, exist_ok=True)
        (templates_dir / "contract.fluid.yaml").write_text("name: x\n")

        project_dir = tmp_path / "output3"
        project_dir.mkdir()

        try:
            with patch("shutil.copy2", side_effect=PermissionError("no write")):
                result = copy_template(project_dir, "test-tmpl-perm", logger)
            assert result is False
        finally:
            shutil.rmtree(str(templates_dir))


# ===========================================================================
# run_local_pipeline
# ===========================================================================


class TestRunLocalPipeline:
    def test_no_rich_exits_early(self, tmp_path, logger):
        from fluid_build.cli.init import run_local_pipeline

        with patch("fluid_build.cli.init.RICH_AVAILABLE", False):
            result = run_local_pipeline(tmp_path, logger)
        assert result is None

    def test_apply_run_called_on_success(self, tmp_path, logger, monkeypatch):
        from fluid_build.cli.init import run_local_pipeline

        (tmp_path / "contract.fluid.yaml").write_text("name: test\n")
        mock_apply = MagicMock(return_value=0)
        mock_apply_mod = MagicMock()
        mock_apply_mod.run = mock_apply
        monkeypatch.delitem(sys.modules, "fluid_build.cli.apply", raising=False)
        with patch.dict("sys.modules", {"fluid_build.cli.apply": mock_apply_mod}):
            with patch("fluid_build.cli.init.RICH_AVAILABLE", True):
                with patch("fluid_build.cli.init.console") as mock_con:
                    mock_con.print = MagicMock()
                    run_local_pipeline(tmp_path, logger)
        mock_apply.assert_called_once()

    def test_exception_handled_gracefully(self, tmp_path, logger, monkeypatch):
        from fluid_build.cli.init import run_local_pipeline

        mock_apply_mod = MagicMock()
        mock_apply_mod.run.side_effect = RuntimeError("apply failed")
        monkeypatch.delitem(sys.modules, "fluid_build.cli.apply", raising=False)
        with patch.dict("sys.modules", {"fluid_build.cli.apply": mock_apply_mod}):
            with patch("fluid_build.cli.init.RICH_AVAILABLE", True):
                with patch("fluid_build.cli.init.console"):
                    # Should not raise
                    run_local_pipeline(tmp_path, logger)


# ===========================================================================
# show_success_message
# ===========================================================================


class TestShowSuccessMessage:
    def test_no_rich_prints_basic(self, tmp_path, logger):
        from fluid_build.cli.init import show_success_message

        with patch("fluid_build.cli.init.RICH_AVAILABLE", False):
            with patch("fluid_build.cli.init._mark_first_run_complete"):
                with patch("fluid_build.cli.init.cprint") as mock_cprint:
                    show_success_message(tmp_path, "local", logger)
        mock_cprint.assert_called()

    def test_no_rich_with_dag_prints_dag_message(self, tmp_path, logger):
        from fluid_build.cli.init import show_success_message

        with patch("fluid_build.cli.init.RICH_AVAILABLE", False):
            with patch("fluid_build.cli.init._mark_first_run_complete"):
                with patch("fluid_build.cli.init.cprint") as mock_cprint:
                    show_success_message(tmp_path, "local", logger, has_dag=True)
        calls = " ".join(str(c) for c in mock_cprint.call_args_list)
        assert "DAG" in calls

    def test_rich_local_provider_with_output_files(self, tmp_path, logger):
        from fluid_build.cli.init import show_success_message

        (tmp_path / "output").mkdir()
        (tmp_path / "output" / "results.csv").write_text("a,b\n1,2\n")
        (tmp_path / ".fluid").mkdir()
        (tmp_path / ".fluid" / "db.duckdb").write_text("")

        with patch("fluid_build.cli.init.RICH_AVAILABLE", True):
            with patch("fluid_build.cli.init._mark_first_run_complete"):
                with patch("fluid_build.cli.init.console") as mock_con:
                    mock_con.print = MagicMock()
                    show_success_message(tmp_path, "local", logger)
        mock_con.print.assert_called()

    def test_rich_cloud_provider_shows_plan_step(self, tmp_path, logger):
        from fluid_build.cli.init import show_success_message

        with patch("fluid_build.cli.init.RICH_AVAILABLE", True):
            with patch("fluid_build.cli.init._mark_first_run_complete"):
                with patch("fluid_build.cli.init.console") as mock_con:
                    mock_con.print = MagicMock()
                    show_success_message(tmp_path, "gcp", logger)
        calls = " ".join(str(c) for c in mock_con.print.call_args_list)
        assert "gcp" in calls

    def test_rich_with_dag_shows_dag_files(self, tmp_path, logger):
        from fluid_build.cli.init import show_success_message

        dag_dir = tmp_path / "dags"
        dag_dir.mkdir()
        (dag_dir / "my_product_dag.py").write_text("# dag")

        with patch("fluid_build.cli.init.RICH_AVAILABLE", True):
            with patch("fluid_build.cli.init._mark_first_run_complete"):
                with patch("fluid_build.cli.init.console") as mock_con:
                    mock_con.print = MagicMock()
                    show_success_message(tmp_path, "local", logger, has_dag=True)
        calls = " ".join(str(c) for c in mock_con.print.call_args_list)
        assert "my_product_dag.py" in calls


# ===========================================================================
# generate_cicd
# ===========================================================================


class TestGenerateCicd:
    def test_no_rich_generates_jenkinsfile(self, tmp_path, logger):
        from fluid_build.cli.init import generate_cicd

        with patch("fluid_build.cli.init.RICH_AVAILABLE", False):
            generate_cicd(tmp_path, logger)
        assert (tmp_path / "Jenkinsfile").exists()

    def test_rich_user_confirms_jenkins(self, tmp_path, logger):
        from fluid_build.cli.init import generate_cicd

        with patch("fluid_build.cli.init.RICH_AVAILABLE", True):
            with patch("fluid_build.cli.init.console"):
                with patch("fluid_build.cli.init.Confirm") as mock_confirm:
                    with patch("fluid_build.cli.init.Prompt") as mock_prompt:
                        mock_confirm.ask.return_value = True
                        mock_prompt.ask.return_value = "jenkins"
                        generate_cicd(tmp_path, logger)
        assert (tmp_path / "Jenkinsfile").exists()

    def test_rich_user_confirms_github(self, tmp_path, logger):
        from fluid_build.cli.init import generate_cicd

        with patch("fluid_build.cli.init.RICH_AVAILABLE", True):
            with patch("fluid_build.cli.init.console"):
                with patch("fluid_build.cli.init.Confirm") as mock_confirm:
                    with patch("fluid_build.cli.init.Prompt") as mock_prompt:
                        mock_confirm.ask.return_value = True
                        mock_prompt.ask.return_value = "github"
                        generate_cicd(tmp_path, logger)
        assert (tmp_path / ".github" / "workflows" / "fluid.yml").exists()

    def test_rich_user_confirms_gitlab(self, tmp_path, logger):
        from fluid_build.cli.init import generate_cicd

        with patch("fluid_build.cli.init.RICH_AVAILABLE", True):
            with patch("fluid_build.cli.init.console"):
                with patch("fluid_build.cli.init.Confirm") as mock_confirm:
                    with patch("fluid_build.cli.init.Prompt") as mock_prompt:
                        mock_confirm.ask.return_value = True
                        mock_prompt.ask.return_value = "gitlab"
                        generate_cicd(tmp_path, logger)
        assert (tmp_path / ".gitlab-ci.yml").exists()

    def test_rich_user_confirms_cloudbuild(self, tmp_path, logger):
        from fluid_build.cli.init import generate_cicd

        with patch("fluid_build.cli.init.RICH_AVAILABLE", True):
            with patch("fluid_build.cli.init.console"):
                with patch("fluid_build.cli.init.Confirm") as mock_confirm:
                    with patch("fluid_build.cli.init.Prompt") as mock_prompt:
                        mock_confirm.ask.return_value = True
                        mock_prompt.ask.return_value = "cloudbuild"
                        generate_cicd(tmp_path, logger)
        assert (tmp_path / "cloudbuild.yaml").exists()

    def test_rich_user_declines_cicd(self, tmp_path, logger):
        from fluid_build.cli.init import generate_cicd

        with patch("fluid_build.cli.init.RICH_AVAILABLE", True):
            with patch("fluid_build.cli.init.console"):
                with patch("fluid_build.cli.init.Confirm") as mock_confirm:
                    mock_confirm.ask.return_value = False
                    generate_cicd(tmp_path, logger)
        assert not (tmp_path / "Jenkinsfile").exists()

    def test_rich_user_picks_skip(self, tmp_path, logger):
        from fluid_build.cli.init import generate_cicd

        with patch("fluid_build.cli.init.RICH_AVAILABLE", True):
            with patch("fluid_build.cli.init.console"):
                with patch("fluid_build.cli.init.Confirm") as mock_confirm:
                    with patch("fluid_build.cli.init.Prompt") as mock_prompt:
                        mock_confirm.ask.return_value = True
                        mock_prompt.ask.return_value = "skip"
                        generate_cicd(tmp_path, logger)
        assert not (tmp_path / "Jenkinsfile").exists()


# ===========================================================================
# show_scan_results
# ===========================================================================


class TestShowScanResults:
    def test_no_rich_prints_project_type(self):
        from fluid_build.cli.init import show_scan_results

        results = {"project_type": "dbt", "metadata": {}, "sensitive_columns": []}
        with patch("fluid_build.cli.init.RICH_AVAILABLE", False):
            with patch("fluid_build.cli.init.cprint") as mock_cprint:
                show_scan_results(results)
        calls = " ".join(str(c) for c in mock_cprint.call_args_list)
        assert "dbt" in calls

    def test_rich_dbt_type_with_metadata(self):
        from fluid_build.cli.init import show_scan_results

        results = {
            "project_type": "dbt",
            "metadata": {
                "project_name": "myproj",
                "target_platform": "gcp",
                "target_database": "",
            },
            "models": [{"name": "m1"}, {"name": "m2"}],
            "sensitive_columns": [],
        }
        with patch("fluid_build.cli.init.RICH_AVAILABLE", True):
            with patch("fluid_build.cli.init.console") as mock_con:
                mock_con.print = MagicMock()
                show_scan_results(results)
        mock_con.print.assert_called()

    def test_rich_terraform_type(self):
        from fluid_build.cli.init import show_scan_results

        results = {
            "project_type": "terraform",
            "metadata": {"files_count": 3, "target_platform": "aws"},
            "sensitive_columns": [],
        }
        with patch("fluid_build.cli.init.RICH_AVAILABLE", True):
            with patch("fluid_build.cli.init.console") as mock_con:
                mock_con.print = MagicMock()
                show_scan_results(results)
        mock_con.print.assert_called()

    def test_rich_sql_type(self):
        from fluid_build.cli.init import show_scan_results

        results = {
            "project_type": "sql",
            "metadata": {"files_count": 5},
            "sensitive_columns": [],
        }
        with patch("fluid_build.cli.init.RICH_AVAILABLE", True):
            with patch("fluid_build.cli.init.console") as mock_con:
                mock_con.print = MagicMock()
                show_scan_results(results)
        mock_con.print.assert_called()

    def test_sensitive_columns_rendered_in_table(self):
        from fluid_build.cli.init import show_scan_results

        results = {
            "project_type": "dbt",
            "metadata": {},
            "models": [],
            "sensitive_columns": [
                {
                    "model": "users",
                    "column": "email",
                    "type": "EMAIL",
                    "confidence": 0.85,
                    "method": "heuristic",
                },
                {
                    "model": "orders",
                    "column": "ssn",
                    "type": "SSN",
                    "confidence": 0.95,
                    "method": "heuristic",
                },
            ],
        }
        with patch("fluid_build.cli.init.RICH_AVAILABLE", True):
            with patch("fluid_build.cli.init.console") as mock_con:
                with patch("fluid_build.cli.init.Table") as mock_table_cls:
                    mock_table = MagicMock()
                    mock_table_cls.return_value = mock_table
                    mock_con.print = MagicMock()
                    show_scan_results(results)
        mock_table.add_row.assert_called()

    def test_eu_database_shows_gdpr_hint(self):
        from fluid_build.cli.init import show_scan_results

        results = {
            "project_type": "dbt",
            "metadata": {
                "project_name": "eu_proj",
                "target_platform": "gcp",
                "target_database": "eu-west-db",
            },
            "models": [],
            "sensitive_columns": [],
        }
        with patch("fluid_build.cli.init.RICH_AVAILABLE", True):
            with patch("fluid_build.cli.init.console") as mock_con:
                mock_con.print = MagicMock()
                show_scan_results(results)
        calls = " ".join(str(c) for c in mock_con.print.call_args_list)
        assert "EU" in calls or "GDPR" in calls

    def test_many_sensitive_columns_truncated(self):
        from fluid_build.cli.init import show_scan_results

        sensitive = [
            {
                "model": "m",
                "column": f"col_{i}",
                "type": "EMAIL",
                "confidence": 0.8,
                "method": "h",
            }
            for i in range(15)
        ]
        results = {
            "project_type": "dbt",
            "metadata": {},
            "models": [],
            "sensitive_columns": sensitive,
        }
        with patch("fluid_build.cli.init.RICH_AVAILABLE", True):
            with patch("fluid_build.cli.init.console") as mock_con:
                with patch("fluid_build.cli.init.Table") as mock_table_cls:
                    mock_table = MagicMock()
                    mock_table_cls.return_value = mock_table
                    mock_con.print = MagicMock()
                    show_scan_results(results)
        # Only 10 rows shown
        assert mock_table.add_row.call_count == 10


# ===========================================================================
# copy_sample_data
# ===========================================================================


class TestCopySampleData:
    def test_no_data_dir_does_not_raise(self, tmp_path, logger):
        from fluid_build.cli.init import copy_sample_data

        copy_sample_data(tmp_path, "customer-360", logger)

    def test_with_csv_files_prints_count(self, tmp_path, logger):
        from fluid_build.cli.init import copy_sample_data

        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "customers.csv").write_text("id,name\n1,Alice\n")
        (data_dir / "orders.csv").write_text("id,amount\n1,100\n")

        with patch("fluid_build.cli.init.RICH_AVAILABLE", True):
            with patch("fluid_build.cli.init.console") as mock_con:
                mock_con.print = MagicMock()
                copy_sample_data(tmp_path, "customer-360", logger)
        mock_con.print.assert_called()


# ===========================================================================
# init_local_db
# ===========================================================================


class TestInitLocalDb:
    def test_skips_non_local_provider(self, tmp_path, logger):
        from fluid_build.cli.init import init_local_db

        init_local_db(tmp_path, "gcp", logger)
        assert not (tmp_path / ".fluid").exists()

    def test_duckdb_not_installed_no_raise(self, tmp_path, logger):
        from fluid_build.cli.init import init_local_db

        with patch("fluid_build.cli.init.RICH_AVAILABLE", False):
            with patch.dict("sys.modules", {"duckdb": None}):
                init_local_db(tmp_path, "local", logger)

    def test_duckdb_available_creates_db_dir(self, tmp_path, logger, monkeypatch):
        from fluid_build.cli.init import init_local_db

        mock_conn = MagicMock()
        mock_duckdb = MagicMock()
        mock_duckdb.connect.return_value = mock_conn
        monkeypatch.delitem(sys.modules, "duckdb", raising=False)
        with patch.dict("sys.modules", {"duckdb": mock_duckdb}):
            with patch("fluid_build.cli.init.RICH_AVAILABLE", False):
                init_local_db(tmp_path, "local", logger)
        mock_duckdb.connect.assert_called_once()
        mock_conn.close.assert_called_once()


# ===========================================================================
# detect_project_type
# ===========================================================================


class TestDetectProjectType:
    def test_detects_dbt(self, tmp_path):
        from fluid_build.cli.init import DbtDetector, detect_project_type

        (tmp_path / "dbt_project.yml").write_text("name: myproject\n")
        detector = detect_project_type(tmp_path)
        assert isinstance(detector, DbtDetector)

    def test_detects_terraform(self, tmp_path):
        from fluid_build.cli.init import TerraformDetector, detect_project_type

        (tmp_path / "main.tf").write_text("resource {}")
        detector = detect_project_type(tmp_path)
        assert isinstance(detector, TerraformDetector)

    def test_detects_sql(self, tmp_path):
        from fluid_build.cli.init import SqlFileDetector, detect_project_type

        (tmp_path / "query.sql").write_text("SELECT 1")
        detector = detect_project_type(tmp_path)
        assert isinstance(detector, SqlFileDetector)

    def test_returns_none_when_nothing_found(self, tmp_path):
        from fluid_build.cli.init import detect_project_type

        detector = detect_project_type(tmp_path)
        assert detector is None

    def test_dbt_takes_priority_over_sql(self, tmp_path):
        from fluid_build.cli.init import DbtDetector, detect_project_type

        (tmp_path / "dbt_project.yml").write_text("name: x\n")
        (tmp_path / "model.sql").write_text("SELECT 1")
        detector = detect_project_type(tmp_path)
        assert isinstance(detector, DbtDetector)


# ===========================================================================
# DbtDetector
# ===========================================================================


class TestDbtDetector:
    def test_can_detect_true(self, tmp_path):
        from fluid_build.cli.init import DbtDetector

        (tmp_path / "dbt_project.yml").write_text("name: x\n")
        assert DbtDetector().can_detect(tmp_path) is True

    def test_can_detect_false(self, tmp_path):
        from fluid_build.cli.init import DbtDetector

        assert DbtDetector().can_detect(tmp_path) is False

    def test_parse_model_extracts_columns(self, tmp_path, logger):
        from fluid_build.cli.init import DbtDetector

        sql_file = tmp_path / "orders.sql"
        sql_file.write_text("SELECT id, name, amount FROM raw.orders")
        model = DbtDetector()._parse_model(sql_file, logger)
        assert model is not None
        assert model["name"] == "orders"
        assert any(c["name"] == "amount" for c in model["columns"])

    def test_parse_model_with_table_materialization(self, tmp_path, logger):
        from fluid_build.cli.init import DbtDetector

        sql_file = tmp_path / "facts.sql"
        sql_file.write_text("{{ config(materialized='table') }}\nSELECT id FROM raw.facts")
        model = DbtDetector()._parse_model(sql_file, logger)
        assert model["materialization"] == "table"

    def test_parse_model_with_incremental_materialization(self, tmp_path, logger):
        from fluid_build.cli.init import DbtDetector

        sql_file = tmp_path / "inc.sql"
        sql_file.write_text("{{ config(materialized='incremental') }}\nSELECT id FROM t")
        model = DbtDetector()._parse_model(sql_file, logger)
        assert model["materialization"] == "incremental"

    def test_parse_model_returns_none_on_missing_file(self, tmp_path, logger):
        from fluid_build.cli.init import DbtDetector

        non_existent = tmp_path / "nope.sql"
        model = DbtDetector()._parse_model(non_existent, logger)
        assert model is None

    def test_detect_pii_finds_email(self):
        from fluid_build.cli.init import DbtDetector

        models = [{"name": "users", "columns": [{"name": "email_address"}, {"name": "user_id"}]}]
        findings = DbtDetector()._detect_pii(models)
        assert any(f["type"] == "EMAIL" for f in findings)

    def test_detect_pii_finds_phone(self):
        from fluid_build.cli.init import DbtDetector

        models = [{"name": "contacts", "columns": [{"name": "phone_number"}]}]
        findings = DbtDetector()._detect_pii(models)
        assert any(f["type"] == "PHONE" for f in findings)

    def test_detect_pii_finds_credit_card(self):
        from fluid_build.cli.init import DbtDetector

        models = [{"name": "payments", "columns": [{"name": "credit_card_num"}]}]
        findings = DbtDetector()._detect_pii(models)
        assert any(f["type"] == "CREDIT_CARD" for f in findings)

    def test_detect_pii_finds_ssn(self):
        from fluid_build.cli.init import DbtDetector

        models = [{"name": "hr", "columns": [{"name": "social_security_number"}]}]
        findings = DbtDetector()._detect_pii(models)
        assert any(f["type"] == "SSN" for f in findings)

    def test_detect_pii_finds_name(self):
        from fluid_build.cli.init import DbtDetector

        models = [{"name": "people", "columns": [{"name": "first_name"}]}]
        findings = DbtDetector()._detect_pii(models)
        assert any(f["type"] == "NAME" for f in findings)

    def test_detect_pii_no_pii(self):
        from fluid_build.cli.init import DbtDetector

        models = [{"name": "metrics", "columns": [{"name": "revenue"}, {"name": "count"}]}]
        findings = DbtDetector()._detect_pii(models)
        assert findings == []

    def test_scan_parses_project_name(self, tmp_path, logger, monkeypatch):
        from fluid_build.cli.init import DbtDetector

        monkeypatch.chdir(tmp_path)
        (tmp_path / "dbt_project.yml").write_text("name: analytics\nversion: 1.0.0\n")
        with patch("fluid_build.cli.init.RICH_AVAILABLE", False):
            results = DbtDetector().scan(logger)
        assert results["project_type"] == "dbt"
        assert results["metadata"]["project_name"] == "analytics"


# ===========================================================================
# TerraformDetector
# ===========================================================================


class TestTerraformDetector:
    def test_can_detect_true(self, tmp_path):
        from fluid_build.cli.init import TerraformDetector

        (tmp_path / "main.tf").write_text("resource {}")
        assert TerraformDetector().can_detect(tmp_path) is True

    def test_can_detect_false(self, tmp_path):
        from fluid_build.cli.init import TerraformDetector

        assert TerraformDetector().can_detect(tmp_path) is False

    def test_scan_detects_gcp(self, tmp_path, logger, monkeypatch):
        from fluid_build.cli.init import TerraformDetector

        monkeypatch.chdir(tmp_path)
        (tmp_path / "main.tf").write_text('resource "google_bigquery_dataset" "ds" {}')
        with patch("fluid_build.cli.init.RICH_AVAILABLE", False):
            results = TerraformDetector().scan(logger)
        assert results["metadata"].get("target_platform") == "gcp"

    def test_scan_detects_snowflake(self, tmp_path, logger, monkeypatch):
        from fluid_build.cli.init import TerraformDetector

        monkeypatch.chdir(tmp_path)
        (tmp_path / "main.tf").write_text('resource "snowflake_database" "db" {}')
        with patch("fluid_build.cli.init.RICH_AVAILABLE", False):
            results = TerraformDetector().scan(logger)
        assert results["metadata"].get("target_platform") == "snowflake"

    def test_scan_returns_files_count(self, tmp_path, logger, monkeypatch):
        from fluid_build.cli.init import TerraformDetector

        monkeypatch.chdir(tmp_path)
        (tmp_path / "main.tf").write_text("resource {}")
        (tmp_path / "variables.tf").write_text("variable x {}")
        with patch("fluid_build.cli.init.RICH_AVAILABLE", False):
            results = TerraformDetector().scan(logger)
        assert results["metadata"]["files_count"] == 2


# ===========================================================================
# SqlFileDetector
# ===========================================================================


class TestSqlFileDetector:
    def test_can_detect_true(self, tmp_path):
        from fluid_build.cli.init import SqlFileDetector

        (tmp_path / "query.sql").write_text("SELECT 1")
        assert SqlFileDetector().can_detect(tmp_path) is True

    def test_can_detect_false_no_sql(self, tmp_path):
        from fluid_build.cli.init import SqlFileDetector

        assert SqlFileDetector().can_detect(tmp_path) is False

    def test_can_detect_false_when_dbt_exists(self, tmp_path):
        from fluid_build.cli.init import SqlFileDetector

        (tmp_path / "query.sql").write_text("SELECT 1")
        (tmp_path / "dbt_project.yml").write_text("name: x\n")
        assert SqlFileDetector().can_detect(tmp_path) is False

    def test_scan_lists_files(self, tmp_path, logger, monkeypatch):
        from fluid_build.cli.init import SqlFileDetector

        monkeypatch.chdir(tmp_path)
        (tmp_path / "a.sql").write_text("SELECT 1")
        (tmp_path / "b.sql").write_text("SELECT 2")
        with patch("fluid_build.cli.init.RICH_AVAILABLE", False):
            results = SqlFileDetector().scan(logger)
        assert results["project_type"] == "sql"
        assert results["metadata"]["files_count"] == 2


# ===========================================================================
# apply_governance_policies
# ===========================================================================


class TestApplyGovernancePolicies:
    def test_no_rich_returns_contracts_unchanged(self, logger):
        from fluid_build.cli.init_scan import apply_governance_policies

        contracts = [{"name": "c1"}]
        results = {"sensitive_columns": [{"col": "email"}]}
        with patch("fluid_build.cli.init_scan.RICH_AVAILABLE", False):
            out = apply_governance_policies(contracts, results, logger)
        assert out == contracts

    def test_no_sensitive_returns_unchanged(self, logger):
        from fluid_build.cli.init_scan import apply_governance_policies

        contracts = [{"name": "c1"}]
        results = {"sensitive_columns": []}
        with patch("fluid_build.cli.init_scan.RICH_AVAILABLE", True):
            out = apply_governance_policies(contracts, results, logger)
        assert out == contracts

    def test_applies_masking_rules_when_user_confirms(self, logger):
        from fluid_build.cli.init_scan import apply_governance_policies

        # 0.7.2 shape: ``exposes[*]`` with ``exposeId``.
        contracts = [
            {
                "name": "c1",
                "exposes": [{"exposeId": "users", "contract": {"schema": []}}],
            }
        ]
        results = {
            "sensitive_columns": [
                {
                    "model": "users",
                    "column": "email",
                    "type": "EMAIL",
                    "confidence": 0.85,
                }
            ],
            "metadata": {"target_database": ""},
        }
        with patch("fluid_build.cli.init_scan.RICH_AVAILABLE", True):
            with patch("fluid_build.cli.init_scan.console") as mock_con:
                mock_con.print = MagicMock()
                with patch("fluid_build.cli.init_scan.Confirm") as mock_confirm:
                    mock_confirm.ask.return_value = True
                    out = apply_governance_policies(contracts, results, logger)
        assert "policy" in out[0]["exposes"][0]
        masking = out[0]["exposes"][0]["policy"]["masking"]
        assert masking[0]["column"] == "email"

    def test_user_declines_governance_unchanged(self, logger):
        from fluid_build.cli.init_scan import apply_governance_policies

        contracts = [{"name": "c1", "exposes": [{"exposeId": "users"}]}]
        results = {
            "sensitive_columns": [
                {"model": "users", "column": "email", "type": "EMAIL", "confidence": 0.85}
            ],
            "metadata": {},
        }
        with patch("fluid_build.cli.init_scan.RICH_AVAILABLE", True):
            with patch("fluid_build.cli.init_scan.console"):
                with patch("fluid_build.cli.init_scan.Confirm") as mock_confirm:
                    mock_confirm.ask.return_value = False
                    out = apply_governance_policies(contracts, results, logger)
        assert out == contracts

    def test_high_confidence_uses_sha256(self, logger):
        from fluid_build.cli.init_scan import apply_governance_policies

        contracts = [{"name": "c1", "exposes": [{"exposeId": "payments"}]}]
        results = {
            "sensitive_columns": [
                {
                    "model": "payments",
                    "column": "cc_number",
                    "type": "CREDIT_CARD",
                    "confidence": 0.95,
                }
            ],
            "metadata": {},
        }
        with patch("fluid_build.cli.init_scan.RICH_AVAILABLE", True):
            with patch("fluid_build.cli.init_scan.console") as mock_con:
                mock_con.print = MagicMock()
                with patch("fluid_build.cli.init_scan.Confirm") as mock_confirm:
                    mock_confirm.ask.return_value = True
                    out = apply_governance_policies(contracts, results, logger)
        masking = out[0]["exposes"][0]["policy"]["masking"]
        assert masking[0]["method"] == "SHA256"


class TestShowMigrationSummary:
    def test_no_rich_prints_count(self, logger):
        from fluid_build.cli.init_scan import show_migration_summary

        contracts = [{"name": "c1"}, {"name": "c2"}]
        results = {}
        with patch("fluid_build.cli.init_scan.RICH_AVAILABLE", False):
            with patch("fluid_build.cli.init_scan.cprint") as mock_cprint:
                show_migration_summary(contracts, results, logger)
        calls = " ".join(str(c) for c in mock_cprint.call_args_list)
        assert "2" in calls

    def test_rich_shows_contract_details(self, logger):
        from fluid_build.cli.init_scan import show_migration_summary

        contracts = [
            {
                "name": "analytics",
                "fluidVersion": FluidSchemaManager.latest_bundled_version(),
                "exposes": [
                    {"exposeId": "m1", "binding": {"platform": "gcp"}},
                    {"exposeId": "m2", "binding": {"platform": "gcp"}},
                ],
            }
        ]
        results = {}
        with patch("fluid_build.cli.init_scan.RICH_AVAILABLE", True):
            with patch("fluid_build.cli.init_scan.console") as mock_con:
                mock_con.print = MagicMock()
                show_migration_summary(contracts, results, logger)
        mock_con.print.assert_called()

    def test_rich_shows_gdpr_flag_when_sovereignty(self, logger):
        from fluid_build.cli.init_scan import show_migration_summary

        contracts = [
            {
                "name": "eu-data",
                "fluidVersion": FluidSchemaManager.latest_bundled_version(),
                "exposes": [],
                "sovereignty": {"jurisdiction": "EU"},
            }
        ]
        results = {}
        with patch("fluid_build.cli.init_scan.RICH_AVAILABLE", True):
            with patch("fluid_build.cli.init_scan.console") as mock_con:
                mock_con.print = MagicMock()
                show_migration_summary(contracts, results, logger)
        calls = " ".join(str(c) for c in mock_con.print.call_args_list)
        assert "GDPR" in calls


# ===========================================================================
# run() top-level routing
# ===========================================================================


class TestRunRouting:
    @patch("fluid_build.cli.init.quickstart_mode", return_value=0)
    @patch("fluid_build.cli.init.detect_mode", return_value="quickstart")
    def test_routes_quickstart(self, _mock_detect, _mock_qs, logger):
        from fluid_build.cli.init import run

        assert run(_make_args(quickstart=True), logger) == 0

    @patch("fluid_build.cli.init.scan_mode", return_value=0)
    @patch("fluid_build.cli.init.detect_mode", return_value="scan")
    def test_routes_scan(self, _mock_detect, _mock_scan, logger):
        from fluid_build.cli.init import run

        assert run(_make_args(scan=True), logger) == 0

    @patch("fluid_build.cli.init.wizard_mode", return_value=0)
    @patch("fluid_build.cli.init.detect_mode", return_value="wizard")
    def test_routes_wizard(self, _mock_detect, _mock_wiz, logger):
        from fluid_build.cli.init import run

        assert run(_make_args(wizard=True), logger) == 0

    @patch("fluid_build.cli.init.blank_mode", return_value=0)
    @patch("fluid_build.cli.init.detect_mode", return_value="blank")
    def test_routes_blank(self, _mock_detect, _mock_blank, logger):
        from fluid_build.cli.init import run

        assert run(_make_args(blank=True), logger) == 0

    @patch("fluid_build.cli.init.template_mode", return_value=0)
    @patch("fluid_build.cli.init.detect_mode", return_value="template")
    def test_routes_template(self, _mock_detect, _mock_tmpl, logger):
        from fluid_build.cli.init import run

        assert run(_make_args(template="customer-360"), logger) == 0

    @patch("fluid_build.cli.init.detect_mode", return_value=None)
    def test_none_mode_returns_1(self, _mock_detect, logger):
        from fluid_build.cli.init import run

        assert run(_make_args(), logger) == 1

    @patch("fluid_build.cli.init.detect_mode", return_value="unknown-mode")
    def test_unknown_mode_returns_1(self, _mock_detect, logger):
        from fluid_build.cli.init import run

        assert run(_make_args(), logger) == 1

    @patch("fluid_build.cli.init.detect_mode", side_effect=KeyboardInterrupt)
    def test_keyboard_interrupt_returns_130(self, _mock_detect, logger):
        from fluid_build.cli.init import run

        assert run(_make_args(), logger) == 130

    @patch("fluid_build.cli.init.detect_mode", side_effect=RuntimeError("boom"))
    def test_exception_returns_1(self, _mock_detect, logger):
        from fluid_build.cli.init import run

        assert run(_make_args(), logger) == 1
