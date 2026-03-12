# Copyright 2024-2026 Agentics Transformation Ltd
# Licensed under the Apache License, Version 2.0
"""Branch coverage tests for init.py."""

import pytest
import logging
import argparse
from pathlib import Path
from unittest.mock import patch, MagicMock


def _make_init_args(**overrides):
    defaults = dict(
        name=None, quickstart=False, scan=False, wizard=False,
        blank=False, template=None, provider="local",
        use_case=None, no_run=False, no_dag=False,
        dry_run=False, yes=True
    )
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


class TestRegister:
    def test_register(self):
        from fluid_build.cli.init import register
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        register(sub)

    def test_register_args(self):
        from fluid_build.cli.init import register
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        register(sub)
        args = parser.parse_args(["init", "--quickstart", "--provider", "gcp", "--yes"])
        assert args.quickstart is True
        assert args.provider == "gcp"


class TestDetectMode:
    def test_explicit_quickstart(self):
        from fluid_build.cli.init import detect_mode
        args = _make_init_args(quickstart=True)
        assert detect_mode(args, logging.getLogger("test")) == "quickstart"

    def test_explicit_scan(self):
        from fluid_build.cli.init import detect_mode
        args = _make_init_args(scan=True)
        assert detect_mode(args, logging.getLogger("test")) == "scan"

    def test_explicit_wizard(self):
        from fluid_build.cli.init import detect_mode
        args = _make_init_args(wizard=True)
        assert detect_mode(args, logging.getLogger("test")) == "wizard"

    def test_explicit_blank(self):
        from fluid_build.cli.init import detect_mode
        args = _make_init_args(blank=True)
        assert detect_mode(args, logging.getLogger("test")) == "blank"

    def test_explicit_template(self):
        from fluid_build.cli.init import detect_mode
        args = _make_init_args(template="customer-360")
        assert detect_mode(args, logging.getLogger("test")) == "template"

    def test_existing_contract(self, tmp_path):
        from fluid_build.cli.init import detect_mode
        (tmp_path / "contract.fluid.yaml").write_text("name: test")
        args = _make_init_args()
        with patch("fluid_build.cli.init.Path") as mock_path_cls:
            mock_path_cls.cwd.return_value = tmp_path
            mock_path_cls.home.return_value = tmp_path
            result = detect_mode(args, logging.getLogger("test"))
        assert result is None

    def test_detect_dbt(self, tmp_path):
        from fluid_build.cli.init import detect_mode
        (tmp_path / "dbt_project.yml").write_text("name: dbt")
        args = _make_init_args()
        with patch("fluid_build.cli.init.Path") as mock_path_cls:
            mock_path_cls.cwd.return_value = tmp_path
            mock_path_cls.home.return_value = tmp_path
            result = detect_mode(args, logging.getLogger("test"))
        assert result == "scan"

    def test_detect_terraform(self, tmp_path):
        from fluid_build.cli.init import detect_mode
        (tmp_path / "main.tf").write_text("resource {}")
        args = _make_init_args()
        with patch("fluid_build.cli.init.Path") as mock_path_cls:
            mock_path_cls.cwd.return_value = tmp_path
            mock_path_cls.home.return_value = tmp_path
            result = detect_mode(args, logging.getLogger("test"))
        assert result == "scan"

    def test_detect_sql_files(self, tmp_path):
        from fluid_build.cli.init import detect_mode
        (tmp_path / "query.sql").write_text("SELECT 1")
        args = _make_init_args(name=None)
        with patch("fluid_build.cli.init.Path") as mock_path_cls:
            mock_path_cls.cwd.return_value = tmp_path
            mock_path_cls.home.return_value = tmp_path
            result = detect_mode(args, logging.getLogger("test"))
        assert result == "scan"

    def test_first_time_user(self, tmp_path):
        from fluid_build.cli.init import detect_mode
        args = _make_init_args(name="myproject")
        with patch("fluid_build.cli.init.Path") as mock_path_cls:
            mock_path_cls.cwd.return_value = tmp_path
            # Ensure home/.fluid doesn't exist
            mock_home = tmp_path / "fakehome"
            mock_path_cls.home.return_value = mock_home
            result = detect_mode(args, logging.getLogger("test"))
        assert result == "quickstart"


class TestShouldGenerateDag:
    def test_with_orchestration(self):
        from fluid_build.cli.init import should_generate_dag
        assert should_generate_dag({"orchestration": {"schedule": "@daily"}}) is True

    def test_with_template(self):
        from fluid_build.cli.init import should_generate_dag
        assert should_generate_dag({}, template="customer-360") is True

    def test_with_provider_actions(self):
        from fluid_build.cli.init import should_generate_dag
        contract = {"binding": {"providerActions": [{"op": "a"}, {"op": "b"}]}}
        assert should_generate_dag(contract) is True

    def test_simple_contract(self):
        from fluid_build.cli.init import should_generate_dag
        assert should_generate_dag({}) is False

    def test_single_action(self):
        from fluid_build.cli.init import should_generate_dag
        contract = {"binding": {"providerActions": [{"op": "a"}]}}
        assert should_generate_dag(contract) is False

    @pytest.mark.parametrize("template", [
        "customer-360", "sales-analytics", "ml-features", "data-quality"
    ])
    def test_orchestrated_templates(self, template):
        from fluid_build.cli.init import should_generate_dag
        assert should_generate_dag({}, template=template) is True

    def test_non_orchestrated_template(self):
        from fluid_build.cli.init import should_generate_dag
        assert should_generate_dag({}, template="simple-api") is False


class TestCreateBasicDag:
    def test_creates_dag_file(self, tmp_path):
        from fluid_build.cli.init import create_basic_dag
        contract = {"name": "test-product", "orchestration": {"schedule": "@hourly", "retries": 2, "retry_delay": "10m"}}
        create_basic_dag(tmp_path, contract, logging.getLogger("test"))
        dag_file = tmp_path / "dags" / "test_product_dag.py"
        assert dag_file.exists()
        content = dag_file.read_text()
        assert "test_product" in content
        assert "@hourly" in content

    def test_creates_dag_defaults(self, tmp_path):
        from fluid_build.cli.init import create_basic_dag
        contract = {"name": "my-product"}
        create_basic_dag(tmp_path, contract, logging.getLogger("test"))
        dag_file = tmp_path / "dags" / "my_product_dag.py"
        assert dag_file.exists()


class TestCreateDagsReadme:
    def test_creates_readme(self, tmp_path):
        from fluid_build.cli.init import create_dags_readme
        dag_dir = tmp_path / "dags"
        dag_dir.mkdir()
        create_dags_readme(dag_dir, "test_dag", "@daily", "test_dag_dag.py")
        readme = dag_dir / "README.md"
        assert readme.exists()
        assert "test_dag" in readme.read_text()


class TestMarkFirstRunComplete:
    @patch("fluid_build.cli.init.Path")
    def test_creates_directory(self, mock_path_cls):
        from fluid_build.cli.init import _mark_first_run_complete
        mock_home = MagicMock()
        mock_path_cls.home.return_value = mock_home
        _mark_first_run_complete()

    @patch("fluid_build.cli.init.Path")
    def test_handles_oserror(self, mock_path_cls):
        from fluid_build.cli.init import _mark_first_run_complete
        mock_home = MagicMock()
        mock_path_cls.home.return_value = mock_home
        (mock_home / ".fluid").mkdir.side_effect = OSError("permission denied")
        _mark_first_run_complete()  # Should not raise


class TestRunFunction:
    @patch("fluid_build.cli.init.quickstart_mode", return_value=0)
    @patch("fluid_build.cli.init.detect_mode", return_value="quickstart")
    def test_quickstart_route(self, mock_detect, mock_qs):
        from fluid_build.cli.init import run
        args = _make_init_args(quickstart=True)
        assert run(args, logging.getLogger("test")) == 0

    @patch("fluid_build.cli.init.scan_mode", return_value=0)
    @patch("fluid_build.cli.init.detect_mode", return_value="scan")
    def test_scan_route(self, mock_detect, mock_scan):
        from fluid_build.cli.init import run
        args = _make_init_args(scan=True)
        assert run(args, logging.getLogger("test")) == 0

    @patch("fluid_build.cli.init.wizard_mode", return_value=0)
    @patch("fluid_build.cli.init.detect_mode", return_value="wizard")
    def test_wizard_route(self, mock_detect, mock_wiz):
        from fluid_build.cli.init import run
        args = _make_init_args(wizard=True)
        assert run(args, logging.getLogger("test")) == 0

    @patch("fluid_build.cli.init.blank_mode", return_value=0)
    @patch("fluid_build.cli.init.detect_mode", return_value="blank")
    def test_blank_route(self, mock_detect, mock_blank):
        from fluid_build.cli.init import run
        args = _make_init_args(blank=True)
        assert run(args, logging.getLogger("test")) == 0

    @patch("fluid_build.cli.init.template_mode", return_value=0)
    @patch("fluid_build.cli.init.detect_mode", return_value="template")
    def test_template_route(self, mock_detect, mock_tmpl):
        from fluid_build.cli.init import run
        args = _make_init_args(template="customer-360")
        assert run(args, logging.getLogger("test")) == 0

    @patch("fluid_build.cli.init.detect_mode", return_value=None)
    def test_none_mode(self, mock_detect):
        from fluid_build.cli.init import run
        args = _make_init_args()
        assert run(args, logging.getLogger("test")) == 1

    @patch("fluid_build.cli.init.detect_mode", return_value="bogus_mode")
    def test_unknown_mode(self, mock_detect):
        from fluid_build.cli.init import run
        args = _make_init_args()
        assert run(args, logging.getLogger("test")) == 1

    @patch("fluid_build.cli.init.detect_mode", side_effect=KeyboardInterrupt())
    def test_keyboard_interrupt(self, mock_detect):
        from fluid_build.cli.init import run
        args = _make_init_args()
        assert run(args, logging.getLogger("test")) == 130

    @patch("fluid_build.cli.init.detect_mode", side_effect=RuntimeError("boom"))
    def test_exception(self, mock_detect):
        from fluid_build.cli.init import run
        args = _make_init_args()
        assert run(args, logging.getLogger("test")) == 1


class TestGenerateDagForProject:
    @patch("subprocess.run")
    def test_dag_generation_success(self, mock_run, tmp_path):
        from fluid_build.cli.init import generate_dag_for_project
        mock_run.return_value = MagicMock(returncode=0)
        contract = {"name": "test-product", "orchestration": {"schedule": "@daily"}}
        mock_console = MagicMock()
        result = generate_dag_for_project(tmp_path, contract, logging.getLogger("test"), mock_console)
        assert result is True

    @patch("subprocess.run")
    def test_dag_generation_fallback(self, mock_run, tmp_path):
        from fluid_build.cli.init import generate_dag_for_project
        mock_run.return_value = MagicMock(returncode=1)
        contract = {"name": "test-product"}
        mock_console = MagicMock()
        result = generate_dag_for_project(tmp_path, contract, logging.getLogger("test"), mock_console)
        assert result is True

    def test_dag_generation_exception(self, tmp_path):
        from fluid_build.cli.init import generate_dag_for_project
        contract = {"name": "test-product"}
        with patch("subprocess.run", side_effect=FileNotFoundError("no fluid")):
            result = generate_dag_for_project(tmp_path, contract, logging.getLogger("test"), MagicMock())
            assert result is False
