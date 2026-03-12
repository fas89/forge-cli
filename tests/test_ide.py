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

"""Tests for fluid_build.cli.ide — data structures AND language server logic."""

from pathlib import Path

from fluid_build.cli.ide import (
    CompletionItem,
    CompletionType,
    DiagnosticItem,
    FluidLanguageServer,
    IDEType,
)

# ── Enum tests ──


class TestEnums:
    def test_ide_types(self):
        assert IDEType.VSCODE.value == "vscode"
        assert IDEType.VIM.value == "vim"
        assert len(IDEType) == 6

    def test_completion_types(self):
        assert CompletionType.COMMAND.value == "command"
        assert CompletionType.FIELD_NAME.value == "field_name"


# ── CompletionItem ──


class TestCompletionItem:
    def test_basic_creation(self):
        ci = CompletionItem(
            label="validate",
            kind=CompletionType.COMMAND,
            detail="Validate contract",
            documentation="docs",
            insert_text="validate ${1:file}",
        )
        assert ci.label == "validate"
        assert ci.score == 1.0

    def test_to_dict(self):
        ci = CompletionItem(
            label="gcp",
            kind=CompletionType.PROVIDER,
            detail="GCP",
            documentation="Google Cloud",
            insert_text="gcp",
            score=0.9,
        )
        d = ci.to_dict()
        assert d["label"] == "gcp"
        assert d["kind"] == "provider"
        assert d["insertText"] == "gcp"
        assert d["score"] == 0.9


# ── DiagnosticItem ──


class TestDiagnosticItem:
    def test_basic(self):
        di = DiagnosticItem(
            file_path="c.yaml",
            line=10,
            column=5,
            severity="error",
            message="Missing field",
        )
        assert di.source == "fluid"
        assert di.code is None

    def test_to_dict(self):
        di = DiagnosticItem(
            file_path="c.yaml",
            line=1,
            column=1,
            severity="warning",
            message="Warn",
            code="W001",
        )
        d = di.to_dict()
        assert d["file"] == "c.yaml"
        assert d["severity"] == "warning"
        assert d["code"] == "W001"


# ── FluidLanguageServer ──


class TestFluidLanguageServer:
    def test_init_loads_completion_data(self):
        fls = FluidLanguageServer()
        assert "commands" in fls.completion_data
        assert "providers" in fls.completion_data
        assert "contract_fields" in fls.completion_data

    def test_initialize_returns_capabilities(self):
        fls = FluidLanguageServer()
        result = fls.initialize("/tmp/workspace")
        caps = result["capabilities"]
        assert caps["completionProvider"]["resolveProvider"] is True
        assert caps["diagnosticProvider"] is True
        assert fls.workspace_root == Path("/tmp/workspace")

    def test_get_completions_empty_prefix(self):
        fls = FluidLanguageServer()
        completions = fls.get_completions("test.sh", 0, 0, "")
        labels = [c.label for c in completions]
        assert "validate" in labels
        assert "plan" in labels

    def test_get_completions_provider_context(self):
        fls = FluidLanguageServer()
        # When context contains --provider, provider items should be included
        completions = fls.get_completions("test.sh", 0, 20, "--provider gcp")
        labels = [c.label for c in completions]
        assert "gcp" in labels

    def test_get_completions_limited_to_20(self):
        fls = FluidLanguageServer()
        # Even with many potential completions, should cap at 20
        completions = fls.get_completions("test.sh", 0, 0, "")
        assert len(completions) <= 20

    def test_completion_data_has_standard_commands(self):
        fls = FluidLanguageServer()
        cmd_labels = [c.label for c in fls.completion_data["commands"]]
        assert "validate" in cmd_labels
        assert "plan" in cmd_labels
        assert "apply" in cmd_labels

    def test_completion_data_has_providers(self):
        fls = FluidLanguageServer()
        prov_labels = [c.label for c in fls.completion_data["providers"]]
        assert "gcp" in prov_labels
        assert "aws" in prov_labels
        assert "snowflake" in prov_labels
