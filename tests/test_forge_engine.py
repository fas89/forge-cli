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

"""Tests for fluid_build.forge.core.engine — ForgeEngine helpers."""

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from fluid_build.forge.core.interfaces import ComplexityLevel


class TestForgeEngineHelpers:
    """Test pure helper methods on ForgeEngine (avoid full __init__ registry setup)."""

    def _make_engine(self):
        """Create a ForgeEngine with registries stubbed out."""
        from fluid_build.forge.core.engine import ForgeEngine

        with patch("fluid_build.forge.core.engine.initialize_all_registries"):
            with patch(
                "fluid_build.forge.core.engine.get_registry_status",
                return_value={
                    "templates": {"count": 1},
                    "providers": {"count": 1},
                },
            ):
                return ForgeEngine(auto_init_registries=True)

    def test_get_complexity_icon_beginner(self):
        engine = self._make_engine()
        assert engine._get_complexity_icon(ComplexityLevel.BEGINNER) == "🟢"

    def test_get_complexity_icon_intermediate(self):
        engine = self._make_engine()
        assert engine._get_complexity_icon(ComplexityLevel.INTERMEDIATE) == "🟡"

    def test_get_complexity_icon_advanced(self):
        engine = self._make_engine()
        assert engine._get_complexity_icon(ComplexityLevel.ADVANCED) == "🔴"

    def test_get_complexity_icon_unknown(self):
        engine = self._make_engine()
        assert engine._get_complexity_icon("UNKNOWN") == "🟡"

    def test_validate_project_name_valid(self):
        engine = self._make_engine()
        assert engine._validate_project_name("my-project") is True

    def test_validate_project_name_empty(self):
        engine = self._make_engine()
        assert engine._validate_project_name("") is False

    def test_validate_project_name_short(self):
        engine = self._make_engine()
        assert engine._validate_project_name("a") is False

    def test_apply_intelligent_defaults(self):
        engine = self._make_engine()
        engine.project_config = {}
        engine._apply_intelligent_defaults()
        assert engine.project_config["name"] == "my-data-product"
        assert engine.project_config["template"] == "starter"
        assert engine.project_config["provider"] == "local"

    def test_apply_intelligent_defaults_preserves_existing(self):
        engine = self._make_engine()
        engine.project_config = {"name": "custom-name"}
        engine._apply_intelligent_defaults()
        assert engine.project_config["name"] == "custom-name"

    def test_create_folder_structure(self):
        engine = self._make_engine()
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            structure = {
                "src/": {
                    "models/": {},
                },
                "tests/": {},
            }
            engine._create_folder_structure(base, structure)
            assert (base / "src").is_dir()
            assert (base / "src" / "models").is_dir()
            assert (base / "tests").is_dir()

    def test_write_contract_file(self):
        engine = self._make_engine()
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            contract = {"name": "test", "version": "1.0.0"}
            engine._write_contract_file(target, contract)
            f = target / "contract.fluid.yaml"
            assert f.exists()
            content = f.read_text()
            assert "test" in content
            assert "1.0.0" in content

    def test_write_provider_config(self):
        engine = self._make_engine()
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            config = {"provider": "gcp", "project": "my-project"}
            engine._write_provider_config(target, config)
            f = target / "config" / "provider.json"
            assert f.exists()
            loaded = json.loads(f.read_text())
            assert loaded["provider"] == "gcp"

    def test_session_stats_initialized(self):
        engine = self._make_engine()
        assert "start_time" in engine.session_stats
        assert engine.session_stats["steps_completed"] == []
        assert engine.session_stats["errors_encountered"] == []

    def test_validate_registry_setup_warns_on_empty(self):
        """Registry setup should log warnings when registries are empty."""
        with patch("fluid_build.forge.core.engine.initialize_all_registries"):
            with patch(
                "fluid_build.forge.core.engine.get_registry_status",
                return_value={
                    "templates": {"count": 0},
                    "providers": {"count": 0},
                },
            ):
                with patch("fluid_build.forge.core.engine.logger") as mock_logger:
                    from fluid_build.forge.core.engine import ForgeEngine

                    ForgeEngine(auto_init_registries=True)
                    assert mock_logger.warning.call_count >= 1
