# Copyright 2024-2026 Agentics Transformation Ltd
# Licensed under the Apache License, Version 2.0

"""Tests for fluid_build.cli.forge_copilot_personal_memory."""

from __future__ import annotations

import json
import stat
from unittest.mock import patch


class TestLoadPersonalMemory:
    def test_returns_none_when_no_file(self, tmp_path):
        from fluid_build.cli.forge_copilot_personal_memory import load_personal_memory

        with patch("fluid_build.cli.forge_copilot_personal_memory._MEMORY_FILE", tmp_path / "nope.json"):
            assert load_personal_memory() is None

    def test_loads_valid_json(self, tmp_path):
        from fluid_build.cli.forge_copilot_personal_memory import load_personal_memory

        mem_file = tmp_path / "engineer_memory.json"
        mem_file.write_text('{"preferred_provider": "gcp"}')
        with patch("fluid_build.cli.forge_copilot_personal_memory._MEMORY_FILE", mem_file):
            result = load_personal_memory()
            assert result is not None
            assert result["preferred_provider"] == "gcp"

    def test_returns_none_for_invalid_json(self, tmp_path):
        from fluid_build.cli.forge_copilot_personal_memory import load_personal_memory

        mem_file = tmp_path / "engineer_memory.json"
        mem_file.write_text("not json at all")
        with patch("fluid_build.cli.forge_copilot_personal_memory._MEMORY_FILE", mem_file):
            assert load_personal_memory() is None

    def test_returns_none_for_non_dict(self, tmp_path):
        from fluid_build.cli.forge_copilot_personal_memory import load_personal_memory

        mem_file = tmp_path / "engineer_memory.json"
        mem_file.write_text('["a", "list"]')
        with patch("fluid_build.cli.forge_copilot_personal_memory._MEMORY_FILE", mem_file):
            assert load_personal_memory() is None


class TestSavePersonalMemory:
    def test_save_creates_file(self, tmp_path):
        from fluid_build.cli.forge_copilot_personal_memory import save_personal_memory

        mem_file = tmp_path / "engineer_memory.json"
        with patch("fluid_build.cli.forge_copilot_personal_memory._MEMORY_FILE", mem_file):
            result = save_personal_memory({"provider": "gcp", "domain": "finance"})
            assert result is True
            assert mem_file.exists()
            data = json.loads(mem_file.read_text())
            assert data["preferred_provider"] == "gcp"
            assert data["preferred_domain"] == "finance"

    def test_save_sets_permissions_600(self, tmp_path):
        from fluid_build.cli.forge_copilot_personal_memory import save_personal_memory

        mem_file = tmp_path / "engineer_memory.json"
        with patch("fluid_build.cli.forge_copilot_personal_memory._MEMORY_FILE", mem_file):
            save_personal_memory({"provider": "local"})
            mode = mem_file.stat().st_mode
            assert mode & stat.S_IRUSR
            assert mode & stat.S_IWUSR
            assert not (mode & stat.S_IRGRP)

    def test_save_merges_with_existing(self, tmp_path):
        from fluid_build.cli.forge_copilot_personal_memory import save_personal_memory

        mem_file = tmp_path / "engineer_memory.json"
        mem_file.write_text('{"preferred_engine": "dbt", "owner_team": "data-eng"}')
        with patch("fluid_build.cli.forge_copilot_personal_memory._MEMORY_FILE", mem_file):
            save_personal_memory({"provider": "gcp"})
            data = json.loads(mem_file.read_text())
            # New value applied
            assert data["preferred_provider"] == "gcp"
            # Existing value preserved
            assert data["preferred_engine"] == "dbt"
            assert data["owner_team"] == "data-eng"

    def test_save_tracks_recent_domains(self, tmp_path):
        from fluid_build.cli.forge_copilot_personal_memory import save_personal_memory

        mem_file = tmp_path / "engineer_memory.json"
        with patch("fluid_build.cli.forge_copilot_personal_memory._MEMORY_FILE", mem_file):
            save_personal_memory({"domain": "finance"})
            save_personal_memory({"domain": "healthcare"})
            data = json.loads(mem_file.read_text())
            assert data["recent_domains"] == ["healthcare", "finance"]

    def test_save_deduplicates_recent_domains(self, tmp_path):
        from fluid_build.cli.forge_copilot_personal_memory import save_personal_memory

        mem_file = tmp_path / "engineer_memory.json"
        with patch("fluid_build.cli.forge_copilot_personal_memory._MEMORY_FILE", mem_file):
            save_personal_memory({"domain": "finance"})
            save_personal_memory({"domain": "finance"})
            data = json.loads(mem_file.read_text())
            assert data["recent_domains"] == ["finance"]

    def test_save_limits_recent_items(self, tmp_path):
        from fluid_build.cli.forge_copilot_personal_memory import save_personal_memory

        mem_file = tmp_path / "engineer_memory.json"
        with patch("fluid_build.cli.forge_copilot_personal_memory._MEMORY_FILE", mem_file):
            for i in range(10):
                save_personal_memory({"domain": f"domain-{i}"})
            data = json.loads(mem_file.read_text())
            assert len(data["recent_domains"]) <= 5

    def test_first_save_shows_hint(self, tmp_path):
        from fluid_build.cli.forge_copilot_personal_memory import save_personal_memory
        from unittest.mock import MagicMock

        mem_file = tmp_path / "engineer_memory.json"
        console = MagicMock()
        with patch("fluid_build.cli.forge_copilot_personal_memory._MEMORY_FILE", mem_file):
            save_personal_memory({"provider": "local"}, console=console)
            console.print.assert_called()  # Should show first-save hint

    def test_second_save_no_hint(self, tmp_path):
        from fluid_build.cli.forge_copilot_personal_memory import save_personal_memory
        from unittest.mock import MagicMock

        mem_file = tmp_path / "engineer_memory.json"
        with patch("fluid_build.cli.forge_copilot_personal_memory._MEMORY_FILE", mem_file):
            save_personal_memory({"provider": "local"})  # First save

            console = MagicMock()
            save_personal_memory({"provider": "gcp"}, console=console)
            # On second save, no hint should be shown
            console.print.assert_not_called()
