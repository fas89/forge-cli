"""Tests for fluid_build.cli.init — should_generate_dag and detect_mode."""
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

from fluid_build.cli.init import should_generate_dag


class TestShouldGenerateDag:
    def test_has_orchestration_config(self):
        contract = {"orchestration": {"schedule": "@daily"}}
        assert should_generate_dag(contract) is True

    def test_orchestration_focused_template(self):
        contract = {}
        assert should_generate_dag(contract, template="customer-360") is True
        assert should_generate_dag(contract, template="sales-analytics") is True
        assert should_generate_dag(contract, template="ml-features") is True
        assert should_generate_dag(contract, template="data-quality") is True

    def test_multiple_provider_actions(self):
        contract = {"binding": {"providerActions": [{"name": "a"}, {"name": "b"}]}}
        assert should_generate_dag(contract) is True

    def test_single_provider_action(self):
        contract = {"binding": {"providerActions": [{"name": "a"}]}}
        assert should_generate_dag(contract) is False

    def test_empty_contract(self):
        assert should_generate_dag({}) is False

    def test_non_orchestrated_template(self):
        assert should_generate_dag({}, template="starter") is False


class TestDetectMode:
    """Test detect_mode with mocked args and filesystem."""

    def _args(self, **kwargs):
        defaults = dict(
            quickstart=False, scan=False, wizard=False, blank=False,
            template=None, name=None,
        )
        defaults.update(kwargs)
        a = MagicMock()
        for k, v in defaults.items():
            setattr(a, k, v)
        return a

    def test_explicit_quickstart(self):
        from fluid_build.cli.init import detect_mode
        result = detect_mode(self._args(quickstart=True), MagicMock())
        assert result == "quickstart"

    def test_explicit_scan(self):
        from fluid_build.cli.init import detect_mode
        result = detect_mode(self._args(scan=True), MagicMock())
        assert result == "scan"

    def test_explicit_wizard(self):
        from fluid_build.cli.init import detect_mode
        result = detect_mode(self._args(wizard=True), MagicMock())
        assert result == "wizard"

    def test_explicit_blank(self):
        from fluid_build.cli.init import detect_mode
        result = detect_mode(self._args(blank=True), MagicMock())
        assert result == "blank"

    def test_explicit_template(self):
        from fluid_build.cli.init import detect_mode
        result = detect_mode(self._args(template="starter"), MagicMock())
        assert result == "template"

    def test_existing_contract_returns_none(self, tmp_path):
        """If contract.fluid.yaml already exists, detect_mode returns None."""
        from fluid_build.cli.init import detect_mode
        (tmp_path / "contract.fluid.yaml").write_text("name: test")
        with patch("fluid_build.cli.init.Path") as mock_path_cls:
            mock_cwd = MagicMock()
            mock_path_cls.cwd.return_value = mock_cwd
            (tmp_path / "contract.fluid.yaml").exists()  # pre-check
            # Mock the (cwd / "contract.fluid.yaml").exists() to return True
            mock_cwd.__truediv__ = lambda self, x: tmp_path / x
            mock_path_cls.home.return_value = tmp_path
            result = detect_mode(self._args(), MagicMock())
            assert result is None

    def test_first_time_user_returns_quickstart(self, tmp_path):
        """Non-existent ~/.fluid dir means first-time user → quickstart."""
        from fluid_build.cli.init import detect_mode
        with patch("fluid_build.cli.init.Path") as mock_path_cls:
            mock_cwd = MagicMock()
            # Nothing exists in cwd
            mock_cwd.__truediv__ = lambda self, x: tmp_path / x  # nothing exists
            mock_path_cls.cwd.return_value = mock_cwd
            # home dir has no .fluid
            mock_home = tmp_path / "fakehome"
            mock_path_cls.home.return_value = mock_home
            mock_cwd.glob = MagicMock(return_value=[])
            result = detect_mode(self._args(), MagicMock())
            assert result == "quickstart"
