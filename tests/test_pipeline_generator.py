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

"""Unit tests for fluid_build.cli.pipeline_generator — run() and helper functions."""

import argparse
import logging
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from fluid_build.cli.pipeline_generator import _show_next_steps, register, run
from fluid_build.forge.core.pipeline_templates import (
    PipelineComplexity,
    PipelineConfig,
    PipelineProvider,
    PipelineTemplateGenerator,
)

LOG = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_args(**kwargs):
    """Build a minimal argparse Namespace for pipeline-generator tests."""
    defaults = dict(
        provider="github_actions",
        complexity="standard",
        environments=["dev", "staging", "prod"],
        enable_approvals=False,
        enable_security_scan=True,
        enable_marketplace=False,
        output_dir=".",
        preview=False,
        interactive=False,
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ---------------------------------------------------------------------------
# register
# ---------------------------------------------------------------------------


class TestRegisterPipelineGenerator(unittest.TestCase):
    def test_register_adds_subparser(self):
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register(subparsers)
        args = parser.parse_args(["generate-pipeline", "--provider", "github_actions"])
        self.assertEqual(args.provider, "github_actions")

    def test_register_default_complexity(self):
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register(subparsers)
        args = parser.parse_args(["generate-pipeline"])
        self.assertEqual(args.complexity, "standard")

    def test_register_default_environments(self):
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register(subparsers)
        args = parser.parse_args(["generate-pipeline"])
        self.assertEqual(args.environments, ["dev", "staging", "prod"])

    def test_register_preview_flag(self):
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register(subparsers)
        args = parser.parse_args(["generate-pipeline", "--preview"])
        self.assertTrue(args.preview)


# ---------------------------------------------------------------------------
# PipelineConfig / PipelineTemplateGenerator
# ---------------------------------------------------------------------------


class TestPipelineConfig(unittest.TestCase):
    def test_basic_complexity_defaults_to_dev_only(self):
        config = PipelineConfig(
            provider=PipelineProvider.GITHUB_ACTIONS,
            complexity=PipelineComplexity.BASIC,
        )
        self.assertEqual(config.environments, ["dev"])

    def test_standard_complexity_has_two_envs(self):
        config = PipelineConfig(
            provider=PipelineProvider.GITLAB_CI,
            complexity=PipelineComplexity.STANDARD,
        )
        self.assertEqual(config.environments, ["dev", "staging"])

    def test_enterprise_complexity_has_three_envs(self):
        config = PipelineConfig(
            provider=PipelineProvider.AZURE_DEVOPS,
            complexity=PipelineComplexity.ENTERPRISE,
        )
        self.assertEqual(config.environments, ["dev", "staging", "prod"])

    def test_custom_environments_preserved(self):
        config = PipelineConfig(
            provider=PipelineProvider.JENKINS,
            complexity=PipelineComplexity.ADVANCED,
            environments=["qa", "prod"],
        )
        self.assertEqual(config.environments, ["qa", "prod"])

    def test_notification_channels_defaults_to_empty(self):
        config = PipelineConfig(
            provider=PipelineProvider.GITHUB_ACTIONS,
            complexity=PipelineComplexity.BASIC,
        )
        self.assertEqual(config.notification_channels, [])


class TestPipelineTemplateGenerator(unittest.TestCase):
    def test_list_available_providers_returns_all(self):
        gen = PipelineTemplateGenerator()
        providers = gen.list_available_providers()
        self.assertIn("github_actions", providers)
        self.assertIn("gitlab_ci", providers)
        self.assertEqual(len(providers), 7)

    def test_generate_github_actions_basic(self):
        config = PipelineConfig(
            provider=PipelineProvider.GITHUB_ACTIONS,
            complexity=PipelineComplexity.BASIC,
        )
        gen = PipelineTemplateGenerator()
        files = gen.generate_pipeline(config)
        self.assertGreater(len(files), 0)
        filename = next(iter(files))
        self.assertIn("github", filename.lower())

    def test_generate_gitlab_ci(self):
        config = PipelineConfig(
            provider=PipelineProvider.GITLAB_CI,
            complexity=PipelineComplexity.STANDARD,
        )
        gen = PipelineTemplateGenerator()
        files = gen.generate_pipeline(config)
        self.assertGreater(len(files), 0)

    def test_generate_unsupported_provider_raises(self):
        gen = PipelineTemplateGenerator()
        bad_provider = MagicMock()
        bad_provider.value = "unsupported"
        config = MagicMock()
        config.provider = bad_provider
        # Override templates to exclude this provider
        gen.templates = {}
        with self.assertRaises(ValueError):
            gen.generate_pipeline(config)

    def test_get_provider_features_github(self):
        gen = PipelineTemplateGenerator()
        features = gen.get_provider_features(PipelineProvider.GITHUB_ACTIONS)
        self.assertIn("multi_environment", features)
        self.assertTrue(features["multi_environment"])

    def test_get_provider_features_unknown(self):
        gen = PipelineTemplateGenerator()
        features = gen.get_provider_features(MagicMock())
        self.assertEqual(features, {})


# ---------------------------------------------------------------------------
# run()
# ---------------------------------------------------------------------------


class TestRunPipelineGenerator(unittest.TestCase):
    def test_run_with_provider_returns_zero(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            args = _make_args(
                provider="github_actions",
                complexity="basic",
                output_dir=tmpdir,
            )
            result = run(args, LOG)
            self.assertEqual(result, 0)

    def test_run_creates_files_in_output_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            args = _make_args(
                provider="github_actions",
                complexity="basic",
                output_dir=tmpdir,
            )
            run(args, LOG)
            created = list(Path(tmpdir).rglob("*.yml")) + list(Path(tmpdir).rglob("*.yaml"))
            self.assertGreater(len(created), 0)

    def test_run_preview_mode_returns_zero(self):
        args = _make_args(
            provider="github_actions",
            complexity="basic",
            preview=True,
        )
        with patch("fluid_build.cli.pipeline_generator.cprint"):
            result = run(args, LOG)
        self.assertEqual(result, 0)

    def test_run_returns_one_on_exception(self):
        args = _make_args(provider="github_actions")
        with patch(
            "fluid_build.cli.pipeline_generator.PipelineTemplateGenerator",
            side_effect=RuntimeError("boom"),
        ):
            result = run(args, LOG)
        self.assertEqual(result, 1)

    def test_run_gitlab_ci_provider(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            args = _make_args(
                provider="gitlab_ci",
                complexity="standard",
                output_dir=tmpdir,
            )
            result = run(args, LOG)
            self.assertEqual(result, 0)


# ---------------------------------------------------------------------------
# _show_next_steps
# ---------------------------------------------------------------------------


class TestShowNextSteps(unittest.TestCase):
    @patch("fluid_build.cli.pipeline_generator.cprint")
    def test_show_next_steps_github(self, mock_cprint):
        _show_next_steps("github_actions", Path("/tmp/out"))
        calls = " ".join(str(c) for c in mock_cprint.call_args_list)
        self.assertIn("secrets", calls.lower())

    @patch("fluid_build.cli.pipeline_generator.cprint")
    def test_show_next_steps_gitlab(self, mock_cprint):
        _show_next_steps("gitlab_ci", Path("/tmp/out"))
        calls = " ".join(str(c) for c in mock_cprint.call_args_list)
        self.assertIn("gitlab", calls.lower())

    @patch("fluid_build.cli.pipeline_generator.cprint")
    def test_show_next_steps_azure(self, mock_cprint):
        _show_next_steps("azure_devops", Path("/tmp/out"))
        calls = " ".join(str(c) for c in mock_cprint.call_args_list)
        self.assertIn("azure", calls.lower())

    @patch("fluid_build.cli.pipeline_generator.cprint")
    def test_show_next_steps_jenkins(self, mock_cprint):
        _show_next_steps("jenkins", Path("/tmp/out"))
        calls = " ".join(str(c) for c in mock_cprint.call_args_list)
        self.assertIn("jenkins", calls.lower())

    @patch("fluid_build.cli.pipeline_generator.cprint")
    def test_show_next_steps_generic(self, mock_cprint):
        _show_next_steps("circleci", Path("/tmp/out"))
        # Generic branch — should still print something
        self.assertTrue(mock_cprint.called)


if __name__ == "__main__":
    unittest.main()
