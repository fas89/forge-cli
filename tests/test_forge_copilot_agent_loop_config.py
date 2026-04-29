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

"""Tests for the new agent-loop env knobs and visibility helpers."""

from __future__ import annotations

import importlib
from typing import List
from unittest.mock import patch

import pytest

from fluid_build.cli import forge_copilot_agent_loop as agent_loop_mod
from fluid_build.cli.forge_copilot_agent_loop import (
    _bool_env,
    _extract_planning_text,
)

# ---------------------------------------------------------------------------
# _bool_env
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value", ["1", "true", "yes", "on", "TRUE", " 1 ", "On"])
def test_bool_env_truthy(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("FLUID_TEST_FLAG", value)
    assert _bool_env("FLUID_TEST_FLAG") is True


@pytest.mark.parametrize("value", ["", "0", "false", "no", "off", "anything-else"])
def test_bool_env_falsy(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("FLUID_TEST_FLAG", value)
    assert _bool_env("FLUID_TEST_FLAG") is False


def test_bool_env_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLUID_TEST_FLAG", raising=False)
    assert _bool_env("FLUID_TEST_FLAG") is False


# ---------------------------------------------------------------------------
# _extract_planning_text — provider response shapes
# ---------------------------------------------------------------------------


def test_planning_text_anthropic_text_alongside_tool_use() -> None:
    response = {
        "content": [
            {"type": "text", "text": "I need to discover the workspace first."},
            {"type": "tool_use", "id": "x", "name": "discover_workspace", "input": {}},
        ]
    }
    assert _extract_planning_text(response) == "I need to discover the workspace first."


def test_planning_text_anthropic_only_tool_use_returns_none() -> None:
    response = {
        "content": [
            {"type": "tool_use", "id": "x", "name": "discover_workspace", "input": {}},
        ]
    }
    assert _extract_planning_text(response) is None


def test_planning_text_anthropic_multiple_text_blocks_joined() -> None:
    response = {
        "content": [
            {"type": "text", "text": "First, I'll discover."},
            {"type": "tool_use", "id": "1", "name": "x", "input": {}},
            {"type": "text", "text": "Then validate."},
        ]
    }
    result = _extract_planning_text(response)
    assert result is not None
    assert "First, I'll discover." in result
    assert "Then validate." in result


def test_planning_text_openai_message_content() -> None:
    response = {
        "choices": [
            {
                "message": {
                    "content": "Planning to call discover then validate.",
                    "tool_calls": [{"id": "1", "function": {"name": "x", "arguments": "{}"}}],
                }
            }
        ]
    }
    assert _extract_planning_text(response) == "Planning to call discover then validate."


def test_planning_text_openai_null_content_returns_none() -> None:
    response = {
        "choices": [
            {
                "message": {
                    "content": None,
                    "tool_calls": [{"id": "1", "function": {"name": "x", "arguments": "{}"}}],
                }
            }
        ]
    }
    assert _extract_planning_text(response) is None


def test_planning_text_gemini_text_part() -> None:
    response = {
        "candidates": [
            {
                "content": {
                    "parts": [
                        {"text": "Calling discover_workspace next."},
                        {"functionCall": {"name": "discover_workspace", "args": {}}},
                    ]
                }
            }
        ]
    }
    assert _extract_planning_text(response) == "Calling discover_workspace next."


def test_planning_text_gemini_no_text_part_returns_none() -> None:
    response = {
        "candidates": [
            {
                "content": {
                    "parts": [{"functionCall": {"name": "discover_workspace", "args": {}}}],
                }
            }
        ]
    }
    assert _extract_planning_text(response) is None


def test_planning_text_empty_response_returns_none() -> None:
    assert _extract_planning_text({}) is None


def test_planning_text_whitespace_only_returns_none() -> None:
    response = {"content": [{"type": "text", "text": "   \n  "}]}
    assert _extract_planning_text(response) is None


# ---------------------------------------------------------------------------
# FLUID_AGENT_MAX_ROUNDS — env override of the iteration ceiling
# ---------------------------------------------------------------------------


def test_max_rounds_default_is_twelve(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLUID_AGENT_MAX_ROUNDS", raising=False)
    importlib.reload(agent_loop_mod)
    try:
        assert agent_loop_mod.MAX_AGENT_ITERATIONS == 12
    finally:
        # Restore baseline for any subsequent tests that import the
        # module (no env set → default 12).
        importlib.reload(agent_loop_mod)


def test_max_rounds_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLUID_AGENT_MAX_ROUNDS", "3")
    importlib.reload(agent_loop_mod)
    try:
        assert agent_loop_mod.MAX_AGENT_ITERATIONS == 3
    finally:
        monkeypatch.delenv("FLUID_AGENT_MAX_ROUNDS", raising=False)
        importlib.reload(agent_loop_mod)


def test_max_rounds_invalid_env_falls_back_to_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FLUID_AGENT_MAX_ROUNDS", "not-a-number")
    importlib.reload(agent_loop_mod)
    try:
        assert agent_loop_mod.MAX_AGENT_ITERATIONS == 12
    finally:
        monkeypatch.delenv("FLUID_AGENT_MAX_ROUNDS", raising=False)
        importlib.reload(agent_loop_mod)


# ---------------------------------------------------------------------------
# FLUID_AGENT_EXPLAIN — narration printing in the loop
# ---------------------------------------------------------------------------


class _RecordingConsole:
    """Minimal rich-style console stub that records printed lines."""

    def __init__(self) -> None:
        self.lines: List[str] = []

    def print(self, message: str) -> None:
        self.lines.append(message)


def _stub_provider(response_json: dict) -> object:
    """Return a minimal LlmProvider stand-in for the loop integration test."""

    class _StubProvider:
        def build_tool_request(self, *args, **kwargs):  # pragma: no cover — unused
            return ("", {}, {})

        def extract_tool_calls(self, _response):
            return [
                {"id": "1", "name": "discover_workspace", "arguments": {}},
            ]

        def extract_text_from_tool_response(self, _response):
            return None

        def build_tool_result_messages(self, tool_calls, results):
            return []

    return _StubProvider()


def test_explain_mode_prints_planning_text(monkeypatch: pytest.MonkeyPatch) -> None:
    """When FLUID_AGENT_EXPLAIN=1, the model's narration text reaches the console."""
    monkeypatch.setenv("FLUID_AGENT_EXPLAIN", "1")

    response = {
        "content": [
            {"type": "text", "text": "I will discover the workspace."},
            {"type": "tool_use", "id": "1", "name": "discover_workspace", "input": {}},
        ]
    }

    console = _RecordingConsole()

    # Patch out the network + tool dispatch so we can drive the loop
    # with a controlled response and bail before the second iteration.
    with (
        patch.object(agent_loop_mod, "get_llm_provider", return_value=_stub_provider(response)),
        patch.object(agent_loop_mod, "_call_llm_with_tools", return_value=response),
        patch.object(agent_loop_mod, "_dispatch_tools", return_value=[{"ok": True}]),
    ):
        from fluid_build.cli.forge_copilot_llm_providers import (
            CopilotGenerationError,
            LlmConfig,
        )

        cfg = LlmConfig(provider="anthropic", model="m", api_key="k", endpoint="http://x")
        with pytest.raises(CopilotGenerationError):
            agent_loop_mod.run_copilot_agent_loop(
                context={"project_goal": "test"},
                llm_config=cfg,
                max_iterations=1,
                console=console,
            )

    explain_lines = [line for line in console.lines if "planning:" in line]
    assert explain_lines, f"expected a 'planning:' line, got: {console.lines}"
    assert "I will discover the workspace." in explain_lines[0]


def test_explain_mode_off_suppresses_planning_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without the env knob, the planning line stays hidden."""
    monkeypatch.delenv("FLUID_AGENT_EXPLAIN", raising=False)

    response = {
        "content": [
            {"type": "text", "text": "Should not be shown."},
            {"type": "tool_use", "id": "1", "name": "discover_workspace", "input": {}},
        ]
    }

    console = _RecordingConsole()

    with (
        patch.object(agent_loop_mod, "get_llm_provider", return_value=_stub_provider(response)),
        patch.object(agent_loop_mod, "_call_llm_with_tools", return_value=response),
        patch.object(agent_loop_mod, "_dispatch_tools", return_value=[{"ok": True}]),
    ):
        from fluid_build.cli.forge_copilot_llm_providers import (
            CopilotGenerationError,
            LlmConfig,
        )

        cfg = LlmConfig(provider="anthropic", model="m", api_key="k", endpoint="http://x")
        with pytest.raises(CopilotGenerationError):
            agent_loop_mod.run_copilot_agent_loop(
                context={"project_goal": "test"},
                llm_config=cfg,
                max_iterations=1,
                console=console,
            )

    assert not any("planning:" in line for line in console.lines)
    assert not any("Should not be shown." in line for line in console.lines)
