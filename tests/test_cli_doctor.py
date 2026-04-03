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

"""Tests for fluid_build.cli.doctor."""

import argparse
from unittest.mock import MagicMock, patch

import pytest


class TestRegister:
    def test_registers_doctor_command(self):
        from fluid_build.cli.doctor import register

        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        register(sub)

        # Verify the subparser was created by parsing a known argument
        args = parser.parse_args(["doctor"])
        assert args.cmd == "doctor"

    def test_registers_with_default_out_dir(self):
        from fluid_build.cli.doctor import register

        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        register(sub)

        args = parser.parse_args(["doctor"])
        assert args.out_dir == "runtime/diag"

    def test_registers_features_only_flag(self):
        from fluid_build.cli.doctor import register

        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        register(sub)

        args = parser.parse_args(["doctor", "--features-only"])
        assert args.features_only is True


class TestCheckFluidFeatures:
    def test_returns_checks_list(self):
        from fluid_build.cli.doctor import _check_fluid_features

        all_ok, checks = _check_fluid_features()
        assert isinstance(checks, list)
        assert len(checks) > 0
        assert all(isinstance(c, dict) for c in checks)
        assert all("check" in c and "status" in c and "ok" in c for c in checks)

    def test_checks_have_categories(self):
        from fluid_build.cli.doctor import _check_fluid_features

        all_ok, checks = _check_fluid_features()
        categories = {c.get("category") for c in checks}
        assert "core" in categories

    def test_schema_manager_check_present(self):
        from fluid_build.cli.doctor import _check_fluid_features

        all_ok, checks = _check_fluid_features()
        names = [c["check"] for c in checks]
        assert "FLUID Schema Manager" in names


class TestRun:
    @patch("fluid_build.cli.doctor._check_fluid_features")
    @patch("fluid_build.cli.doctor._print_feature_checks")
    def test_features_only_mode(self, mock_print, mock_check):
        from fluid_build.cli.doctor import run

        mock_check.return_value = (True, [{"check": "test", "status": "ok", "ok": True}])
        args = MagicMock()
        args.features_only = True
        args.verbose = False
        logger = MagicMock()

        result = run(args, logger)
        assert result == 0
        mock_print.assert_called_once()

    @patch("fluid_build.cli.doctor._check_fluid_features")
    @patch("fluid_build.cli.doctor._print_feature_checks")
    def test_features_only_returns_1_on_failure(self, mock_print, mock_check):
        from fluid_build.cli.doctor import run

        mock_check.return_value = (False, [{"check": "test", "status": "fail", "ok": False}])
        args = MagicMock()
        args.features_only = True
        args.verbose = False
        logger = MagicMock()

        result = run(args, logger)
        assert result == 1

    @patch("fluid_build.cli.doctor._check_fluid_features")
    @patch("fluid_build.cli.doctor.validate_input_file", side_effect=FileNotFoundError)
    def test_missing_diagnostic_script(self, mock_validate, mock_check):
        from fluid_build.cli.doctor import run

        mock_check.return_value = (True, [])
        args = MagicMock()
        args.features_only = False
        args.verbose = False
        args.out_dir = "/tmp/diag"
        logger = MagicMock()

        result = run(args, logger)
        assert result == 0  # Still returns based on feature checks
