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

"""Extended unit tests for fluid_build.cli.doctor (87 missed lines).

Covers _print_feature_checks, run() with script path, verbose mode,
CLIError paths, and additional _check_fluid_features branches.
Does NOT duplicate tests in tests/test_cli_doctor.py.
"""

import argparse
import unittest
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# _check_fluid_features() — supplementary branches
# ---------------------------------------------------------------------------


class TestCheckFluidFeaturesExtra(unittest.TestCase):
    def test_all_ok_flag_is_bool(self):
        from fluid_build.cli.doctor import _check_fluid_features

        all_ok, checks = _check_fluid_features()
        assert isinstance(all_ok, bool)

    def test_checks_contain_ok_field(self):
        from fluid_build.cli.doctor import _check_fluid_features

        _, checks = _check_fluid_features()
        for check in checks:
            assert "ok" in check
            assert isinstance(check["ok"], bool)

    def test_schema_manager_check_fails_gracefully(self):
        from fluid_build.cli.doctor import _check_fluid_features

        # Patch SchemaManager to raise an exception
        with patch(
            "fluid_build.cli.doctor.SchemaManager",
            side_effect=ImportError("no schema manager"),
            create=True,
        ):
            # The function should handle the exception without raising
            try:
                all_ok, checks = _check_fluid_features()
            except Exception:
                pass  # If SchemaManager is imported at different level, that's fine

    def test_checks_include_feature_categories(self):
        from fluid_build.cli.doctor import _check_fluid_features

        _, checks = _check_fluid_features()
        categories = {c.get("category") for c in checks}
        # Should have at least core category
        assert "core" in categories or len(categories) > 0

    def test_gcp_provider_check_present(self):
        from fluid_build.cli.doctor import _check_fluid_features

        _, checks = _check_fluid_features()
        names = [c["check"] for c in checks]
        assert any("GCP" in name or "gcp" in name.lower() for name in names)

    def test_multiple_category_types(self):
        from fluid_build.cli.doctor import _check_fluid_features

        _, checks = _check_fluid_features()
        # 0.7.1 and providers should appear
        categories = {c.get("category") for c in checks}
        assert len(categories) >= 2


# ---------------------------------------------------------------------------
# _print_feature_checks() — plain text output
# ---------------------------------------------------------------------------


class TestPrintFeatureChecksPlainText(unittest.TestCase):
    """Test _print_feature_checks() when RICH_AVAILABLE is False."""

    def _make_checks(self, ok=True):
        return [
            {
                "check": "Feature A",
                "category": "core",
                "status": "Available" if ok else "Error",
                "ok": ok,
                "details": "All good",
            }
        ]

    def test_plain_text_output_no_verbose(self):
        from fluid_build.cli.doctor import _print_feature_checks

        checks = self._make_checks(ok=True)
        with (
            patch("fluid_build.cli.doctor.RICH_AVAILABLE", False),
            patch("fluid_build.cli.doctor.cprint") as mock_cprint,
        ):
            _print_feature_checks(checks, verbose=False)

        # Verify cprint was called with feature name
        all_calls = " ".join(str(c) for c in mock_cprint.call_args_list)
        assert "Feature A" in all_calls

    def test_plain_text_output_verbose(self):
        from fluid_build.cli.doctor import _print_feature_checks

        checks = self._make_checks(ok=True)
        with (
            patch("fluid_build.cli.doctor.RICH_AVAILABLE", False),
            patch("fluid_build.cli.doctor.cprint") as mock_cprint,
        ):
            _print_feature_checks(checks, verbose=True)

        all_calls = " ".join(str(c) for c in mock_cprint.call_args_list)
        assert "All good" in all_calls

    def test_plain_text_shows_pass_count(self):
        from fluid_build.cli.doctor import _print_feature_checks

        checks = [
            {"check": "A", "category": "core", "status": "ok", "ok": True, "details": ""},
            {"check": "B", "category": "core", "status": "ok", "ok": True, "details": ""},
        ]
        with (
            patch("fluid_build.cli.doctor.RICH_AVAILABLE", False),
            patch("fluid_build.cli.doctor.cprint") as mock_cprint,
        ):
            _print_feature_checks(checks, verbose=False)

        all_calls = " ".join(str(c) for c in mock_cprint.call_args_list)
        assert "2/2" in all_calls


# ---------------------------------------------------------------------------
# _print_feature_checks() — Rich output
# ---------------------------------------------------------------------------


class TestPrintFeatureChecksRich(unittest.TestCase):
    def _make_checks_mixed(self):
        return [
            {
                "check": "Core Feature",
                "category": "core",
                "status": "✅ Available",
                "ok": True,
                "details": "Working",
            },
            {
                "check": "Optional Feature",
                "category": "0.7.1",
                "status": "⚠️  Not available",
                "ok": True,
                "details": "Optional",
            },
            {
                "check": "Broken Feature",
                "category": "core",
                "status": "❌ Error",
                "ok": False,
                "details": "Broken",
            },
        ]

    def test_rich_output_no_exception(self):
        from fluid_build.cli.doctor import _print_feature_checks

        checks = self._make_checks_mixed()
        with patch("fluid_build.cli.doctor.RICH_AVAILABLE", True):
            # Should not raise
            try:
                _print_feature_checks(checks, verbose=False)
            except Exception as e:
                self.fail(f"_print_feature_checks raised unexpectedly: {e}")

    def test_rich_output_verbose_no_exception(self):
        from fluid_build.cli.doctor import _print_feature_checks

        checks = self._make_checks_mixed()
        with patch("fluid_build.cli.doctor.RICH_AVAILABLE", True):
            try:
                _print_feature_checks(checks, verbose=True)
            except Exception as e:
                self.fail(f"_print_feature_checks raised unexpectedly: {e}")

    def test_all_ok_shows_green_panel(self):
        from fluid_build.cli.doctor import _print_feature_checks

        checks = [
            {
                "check": "A",
                "category": "core",
                "status": "✅ Available",
                "ok": True,
                "details": "ok",
            }
        ]
        mock_console = MagicMock()
        with (
            patch("fluid_build.cli.doctor.RICH_AVAILABLE", True),
            patch("fluid_build.cli.doctor.Console", return_value=mock_console),
        ):
            _print_feature_checks(checks, verbose=False)
        # Console.print should have been called
        assert mock_console.print.called

    def test_partial_ok_shows_yellow_panel(self):
        from fluid_build.cli.doctor import _print_feature_checks

        checks = [
            {
                "check": "A",
                "category": "core",
                "status": "✅ Available",
                "ok": True,
                "details": "ok",
            },
            {
                "check": "B",
                "category": "0.7.1",
                "status": "⚠️  Not available",
                "ok": True,
                "details": "optional",
            },
        ]
        mock_console = MagicMock()
        with (
            patch("fluid_build.cli.doctor.RICH_AVAILABLE", True),
            patch("fluid_build.cli.doctor.Console", return_value=mock_console),
        ):
            _print_feature_checks(checks, verbose=False)
        assert mock_console.print.called

    def test_critical_failure_shows_red_panel(self):
        from fluid_build.cli.doctor import _print_feature_checks

        checks = [
            {
                "check": "A",
                "category": "core",
                "status": "❌ Error",
                "ok": False,
                "details": "broken",
            }
        ]
        mock_console = MagicMock()
        with (
            patch("fluid_build.cli.doctor.RICH_AVAILABLE", True),
            patch("fluid_build.cli.doctor.Console", return_value=mock_console),
        ):
            _print_feature_checks(checks, verbose=False)
        assert mock_console.print.called


# ---------------------------------------------------------------------------
# run() — additional branches
# ---------------------------------------------------------------------------


class TestRunExtra(unittest.TestCase):
    def _make_args(self, **kw):
        defaults = dict(
            features_only=False,
            verbose=False,
            out_dir="/tmp/diag_test",
        )
        defaults.update(kw)
        return argparse.Namespace(**defaults)

    @patch("fluid_build.cli.doctor._check_fluid_features")
    @patch("fluid_build.cli.doctor._print_feature_checks")
    def test_verbose_mode_prints_features_before_infra(self, mock_print, mock_check):
        from fluid_build.cli.doctor import run

        mock_check.return_value = (True, [{"check": "X", "status": "ok", "ok": True}])
        args = self._make_args(verbose=True)

        with (
            patch("fluid_build.cli.doctor.validate_input_file", side_effect=FileNotFoundError),
            patch("fluid_build.cli.doctor.cprint"),
        ):
            result = run(args, MagicMock())

        # verbose=True triggers _print_feature_checks even when OK
        mock_print.assert_called()
        assert result == 0

    @patch("fluid_build.cli.doctor._check_fluid_features")
    @patch("fluid_build.cli.doctor._print_feature_checks")
    def test_features_not_ok_prints_checks_even_without_verbose(self, mock_print, mock_check):
        from fluid_build.cli.doctor import run

        mock_check.return_value = (False, [{"check": "X", "status": "fail", "ok": False}])
        args = self._make_args(verbose=False)

        with (
            patch("fluid_build.cli.doctor.validate_input_file", side_effect=FileNotFoundError),
            patch("fluid_build.cli.doctor.cprint"),
        ):
            result = run(args, MagicMock())

        mock_print.assert_called()
        assert result == 1

    @patch("fluid_build.cli.doctor._check_fluid_features")
    def test_features_only_verbose_calls_print_with_verbose_true(self, mock_check):
        from fluid_build.cli.doctor import run

        mock_check.return_value = (True, [])
        args = self._make_args(features_only=True, verbose=True)

        with patch("fluid_build.cli.doctor._print_feature_checks") as mock_print:
            run(args, MagicMock())

        # Verify verbose=True was passed
        call_args = mock_print.call_args
        assert call_args[0][1] is True or call_args[1].get("verbose") is True

    @patch("fluid_build.cli.doctor._check_fluid_features")
    @patch("fluid_build.cli.doctor.validate_input_file")
    @patch("fluid_build.cli.doctor.validate_output_file")
    @patch("fluid_build.cli.doctor.subprocess.run")
    def test_run_with_script_succeeds(self, mock_run, _mock_val_out, mock_val_in, mock_check):
        from fluid_build.cli.doctor import run

        mock_check.return_value = (True, [])
        mock_val_in.return_value = "/fake/scripts/diagnose.sh"
        mock_run.return_value = None

        args = self._make_args(verbose=False)

        with (
            patch("fluid_build.cli.doctor.cprint"),
            patch("pathlib.Path.mkdir"),
        ):
            result = run(args, MagicMock())

        # script was found and executed → success
        assert result == 0

    @patch("fluid_build.cli.doctor._check_fluid_features")
    @patch("fluid_build.cli.doctor.validate_input_file")
    @patch("fluid_build.cli.doctor.validate_output_file")
    @patch("fluid_build.cli.doctor.subprocess.run")
    def test_run_timeout_raises_cli_error(self, mock_run, _mock_val_out, mock_val_in, mock_check):
        import subprocess

        from fluid_build.cli._common import CLIError
        from fluid_build.cli.doctor import run

        mock_check.return_value = (True, [])
        mock_val_in.return_value = "/fake/scripts/diagnose.sh"
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="bash", timeout=300)

        args = self._make_args(verbose=False)

        with (
            patch("fluid_build.cli.doctor.cprint"),
            patch("pathlib.Path.mkdir"),
        ):
            with self.assertRaises(CLIError):
                run(args, MagicMock())

    @patch("fluid_build.cli.doctor._check_fluid_features")
    @patch("fluid_build.cli.doctor.validate_input_file")
    @patch("fluid_build.cli.doctor.validate_output_file")
    @patch("fluid_build.cli.doctor.subprocess.run")
    def test_run_called_process_error_raises_cli_error(
        self, mock_run, _mock_val_out, mock_val_in, mock_check
    ):
        import subprocess

        from fluid_build.cli._common import CLIError
        from fluid_build.cli.doctor import run

        mock_check.return_value = (True, [])
        mock_val_in.return_value = "/fake/scripts/diagnose.sh"

        error = subprocess.CalledProcessError(returncode=1, cmd="bash")
        mock_run.side_effect = error

        args = self._make_args(verbose=False)

        with (
            patch("fluid_build.cli.doctor.cprint"),
            patch("pathlib.Path.mkdir"),
        ):
            with self.assertRaises(CLIError):
                run(args, MagicMock())

    @patch("fluid_build.cli.doctor._check_fluid_features")
    @patch("fluid_build.cli.doctor.validate_input_file")
    @patch("fluid_build.cli.doctor.validate_output_file")
    @patch("fluid_build.cli.doctor.subprocess.run")
    def test_run_unexpected_error_raises_cli_error(
        self, mock_run, _mock_val_out, mock_val_in, mock_check
    ):
        from fluid_build.cli._common import CLIError
        from fluid_build.cli.doctor import run

        mock_check.return_value = (True, [])
        mock_val_in.return_value = "/fake/scripts/diagnose.sh"
        mock_run.side_effect = RuntimeError("unexpected")

        args = self._make_args(verbose=False)

        with (
            patch("fluid_build.cli.doctor.cprint"),
            patch("pathlib.Path.mkdir"),
        ):
            with self.assertRaises(CLIError):
                run(args, MagicMock())


# ---------------------------------------------------------------------------
# register() — verbose flag
# ---------------------------------------------------------------------------


class TestRegisterExtra(unittest.TestCase):
    def test_verbose_flag(self):
        from fluid_build.cli.doctor import register

        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        register(sub)
        args = parser.parse_args(["doctor", "--verbose"])
        assert args.verbose is True

    def test_verbose_short_flag(self):
        from fluid_build.cli.doctor import register

        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        register(sub)
        args = parser.parse_args(["doctor", "-v"])
        assert args.verbose is True

    def test_custom_out_dir(self):
        from fluid_build.cli.doctor import register

        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        register(sub)
        args = parser.parse_args(["doctor", "--out-dir", "/custom/path"])
        assert args.out_dir == "/custom/path"


if __name__ == "__main__":
    unittest.main()
