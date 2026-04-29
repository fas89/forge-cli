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

"""Tests for the workspace AGENTS.md loader."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from fluid_build.cli.forge_copilot_agent_loop import _build_agent_system_prompt
from fluid_build.cli.forge_copilot_agents_md import (
    AGENTS_MD_FILENAME,
    MAX_AGENTS_MD_BYTES,
    load_agents_md,
)

# ---------------------------------------------------------------------------
# load_agents_md
# ---------------------------------------------------------------------------


def test_returns_none_when_file_absent(tmp_path: Path) -> None:
    assert load_agents_md(tmp_path) is None


def test_returns_content_when_file_present(tmp_path: Path) -> None:
    body = "# Project conventions\n\n- Always use Snowflake Tasks.\n"
    (tmp_path / AGENTS_MD_FILENAME).write_text(body, encoding="utf-8")

    loaded = load_agents_md(tmp_path)

    assert loaded == body.strip()


def test_returns_none_when_file_empty(tmp_path: Path) -> None:
    (tmp_path / AGENTS_MD_FILENAME).write_text("", encoding="utf-8")
    assert load_agents_md(tmp_path) is None


def test_returns_none_when_file_only_whitespace(tmp_path: Path) -> None:
    (tmp_path / AGENTS_MD_FILENAME).write_text("   \n  \n", encoding="utf-8")
    assert load_agents_md(tmp_path) is None


def test_strips_leading_and_trailing_whitespace(tmp_path: Path) -> None:
    (tmp_path / AGENTS_MD_FILENAME).write_text("\n\n  hello\nworld\n  \n", encoding="utf-8")

    loaded = load_agents_md(tmp_path)

    assert loaded == "hello\nworld"


def test_truncates_oversized_content(tmp_path: Path) -> None:
    body = "x" * (MAX_AGENTS_MD_BYTES + 4096)
    (tmp_path / AGENTS_MD_FILENAME).write_text(body, encoding="utf-8")

    loaded = load_agents_md(tmp_path)

    assert loaded is not None
    # The truncation marker is appended after the (possibly stripped)
    # content; the prefix should still be intact and the marker should
    # explicitly call out the cut.
    assert loaded.startswith("x" * 64)
    assert "[truncated — AGENTS.md exceeds" in loaded
    # We never want the loaded payload to balloon back over the
    # ceiling (some slack is allowed for the marker line).
    assert len(loaded) <= MAX_AGENTS_MD_BYTES + 256


def test_does_not_truncate_at_the_exact_ceiling(tmp_path: Path) -> None:
    body = "y" * MAX_AGENTS_MD_BYTES
    (tmp_path / AGENTS_MD_FILENAME).write_text(body, encoding="utf-8")

    loaded = load_agents_md(tmp_path)

    assert loaded == body
    assert "truncated" not in loaded


def test_respects_caller_supplied_max_bytes(tmp_path: Path) -> None:
    body = "z" * 256
    (tmp_path / AGENTS_MD_FILENAME).write_text(body, encoding="utf-8")

    loaded = load_agents_md(tmp_path, max_bytes=64)

    assert loaded is not None
    assert loaded.startswith("z" * 32)
    assert "[truncated — AGENTS.md exceeds 64 bytes]" in loaded


@pytest.mark.skipif(os.name == "nt", reason="POSIX symlink semantics")
def test_rejects_symlink_pointing_outside_workspace(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    target = outside / "secret.md"
    target.write_text("LEAKED", encoding="utf-8")

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    link = workspace / AGENTS_MD_FILENAME
    link.symlink_to(target)

    assert load_agents_md(workspace) is None


@pytest.mark.skipif(os.name == "nt", reason="POSIX symlink semantics")
def test_accepts_symlink_pointing_inside_workspace(tmp_path: Path) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    target = docs / "conventions.md"
    target.write_text("SHARED CONVENTIONS", encoding="utf-8")

    link = tmp_path / AGENTS_MD_FILENAME
    link.symlink_to(target)

    loaded = load_agents_md(tmp_path)

    assert loaded == "SHARED CONVENTIONS"


def test_returns_none_when_path_is_a_directory(tmp_path: Path) -> None:
    (tmp_path / AGENTS_MD_FILENAME).mkdir()
    assert load_agents_md(tmp_path) is None


def test_handles_non_utf8_bytes_gracefully(tmp_path: Path) -> None:
    # Latin-1 0xff is invalid UTF-8; the loader uses errors="replace"
    # so the file should still load with replacement characters
    # rather than raising or returning None.
    (tmp_path / AGENTS_MD_FILENAME).write_bytes(b"hello \xff world")

    loaded = load_agents_md(tmp_path)

    assert loaded is not None
    assert "hello" in loaded
    assert "world" in loaded


# ---------------------------------------------------------------------------
# _build_agent_system_prompt integration
# ---------------------------------------------------------------------------


def test_system_prompt_unchanged_when_agents_md_absent() -> None:
    prompt = _build_agent_system_prompt(agents_md=None)
    assert "PROJECT CONVENTIONS" not in prompt
    assert "FLUID Forge Copilot" in prompt


def test_system_prompt_unchanged_for_empty_string() -> None:
    # Empty / falsy input should be treated identically to ``None``
    # so that ``load_agents_md`` returning an empty string (which it
    # actually returns ``None`` for, but defense in depth) doesn't
    # produce a dangling "PROJECT CONVENTIONS" header.
    prompt = _build_agent_system_prompt(agents_md="")
    assert "PROJECT CONVENTIONS" not in prompt


def test_system_prompt_includes_agents_md_when_provided() -> None:
    body = "Always prefer DuckDB for local previews."
    prompt = _build_agent_system_prompt(agents_md=body)

    assert "PROJECT CONVENTIONS" in prompt
    assert "honor these" in prompt
    assert body in prompt
    # The base prompt should still be intact.
    assert "FLUID Forge Copilot" in prompt
    assert "discover_workspace" in prompt
