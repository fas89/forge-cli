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

"""Round-2 regression tests: ``fluid forge --seed-from``, ``fluid opds`` CLI
edge cases, and the Bitol interop docs.

Each test class names the defect it pins down.
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import sys
from argparse import Namespace
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest
import yaml

from fluid_build.cli.forge_copilot_prompts import build_system_prompt
from fluid_build.cli.forge_copilot_runtime import generate_copilot_artifacts
from fluid_build.cli.forge_copilot_seed import (
    SeedOptions,
    diff_against_seed,
    format_ground_truth_prompt_block,
    load_seed,
)
from fluid_build.cli.opds import cmd_opds_export, cmd_opds_import
from fluid_build.providers.base import ProviderError
from fluid_build.schema_manager import FluidSchemaManager

LOG = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent
FIXTURES = Path(__file__).parent / "fixtures"
BUNDLE_DIR = FIXTURES / "odps" / "product-bitol"
HOURLY_ODCS = FIXTURES / "odps" / "contracts-only" / "hourly_orders.odcs.yaml"
MULTI_EXPOSE_FLUID = FIXTURES / "fluid" / "contract-multi-expose.fluid.yaml"
BITOL_DOC = REPO / "docs" / "BITOL_INTEROP.md"
README = REPO / "README.md"

IDENTIFIER_RE = re.compile(r"^[a-z0-9_][a-z0-9_.-]*[a-z0-9_]$|^[a-z0-9_]$")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _capability_matrix() -> Dict[str, Any]:
    return {
        "providers": ["local", "gcp", "aws", "snowflake"],
        "templates": {"starter": {}, "analytics": {}},
        "build_engines": ["sql", "python", "dbt"],
    }


def _contract_from_ground_truth(gt: Dict[str, Any]) -> Dict[str, Any]:
    """A minimal FLUID 0.7.2 contract that copies the ground truth verbatim."""
    exposes = []
    for expose in gt["exposes"]:
        schema = expose["contract"]["schema"]
        first_col = schema[0]["name"] if schema else "x"
        built = copy.deepcopy(expose)
        built.update(
            {
                "kind": "table",
                "binding": {
                    "platform": "local",
                    "format": "csv",
                    "location": {"path": "runtime/out/data.csv"},
                },
                "semantics": {
                    "name": "model",
                    "description": "Semantic model.",
                    "entities": [{"name": first_col, "type": "primary"}],
                    "measures": [{"name": "row_count", "agg": "count", "expr": first_col}],
                    "dimensions": [{"name": first_col, "type": "categorical"}],
                    "metrics": [{"name": "rows", "type": "simple", "measure": "row_count"}],
                },
            }
        )
        exposes.append(built)
    contract: Dict[str, Any] = {
        "fluidVersion": "0.7.2",
        "kind": "DataProduct",
        "id": gt.get("id") or "seeded.product",
        "name": gt.get("name") or "Seeded Product",
        "metadata": copy.deepcopy(gt.get("metadata"))
        or {"owner": {"team": "data-team", "email": "data-team@example.com"}},
        "builds": [
            {
                "id": "main_build",
                "pattern": "embedded-logic",
                "engine": "sql",
                "properties": {"sql": "SELECT 1 AS id"},
                "execution": {
                    "trigger": {"type": "manual", "iterations": 1},
                    "runtime": {"platform": "local", "resources": {"cpu": "1", "memory": "2Gi"}},
                },
            }
        ],
        "exposes": exposes,
    }
    for key in ("description", "domain", "tags"):
        if key in gt:
            contract[key] = copy.deepcopy(gt[key])
    contract["metadata"].setdefault("layer", "Bronze")
    return contract


class _CopierLlm:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, provider_adapter, llm_config, system_prompt, user_prompt):
        self.calls += 1
        gt = json.loads(user_prompt)["seed_ground_truth"]["ground_truth"]
        return json.dumps(
            {
                "recommended_template": "starter",
                "recommended_provider": "local",
                "readme_markdown": "# Seeded\n",
                "contract": _contract_from_ground_truth(gt),
            }
        )


def _run_copilot(seed_path: Path, llm: _CopierLlm):
    from fluid_build.cli.forge_copilot_discovery import DiscoveryReport
    from fluid_build.cli.forge_copilot_llm_providers import LlmConfig

    call_patch = patch("fluid_build.cli.forge_copilot_runtime.call_llm", side_effect=llm)
    provider_patch = patch(
        "fluid_build.cli.forge_copilot_runtime.get_llm_provider", return_value=MagicMock()
    )
    with call_patch, provider_patch:
        return generate_copilot_artifacts(
            context={"project_goal": "seeded product"},
            llm_config=LlmConfig(
                provider="anthropic",
                model="mock-model",
                endpoint="https://mock.example.com/v1/messages",
                api_key="mock-key",
            ),
            discovery_report=DiscoveryReport(workspace_roots=["/tmp"]),
            capability_matrix=_capability_matrix(),
            seed_options=SeedOptions(seed_from=seed_path, allow_remote=False),
        )


def _write_yaml(path: Path, data: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    return path


def _odcs(contract_id: str, objects: List[str]) -> Dict[str, Any]:
    return {
        "version": "1.0.0",
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "id": contract_id,
        "status": "active",
        "name": "orders",
        "schema": [
            {
                "name": name,
                "logicalType": "object",
                "physicalType": "table",
                "properties": [{"name": "id", "logicalType": "string"}],
            }
            for name in objects
        ],
    }


def _camel_odcs(tmp_path: Path) -> Path:
    return _write_yaml(tmp_path / "orders.odcs.yaml", _odcs("minimal.contract", ["CustomerOrders"]))


def _spaced_port_bundle(tmp_path: Path) -> Path:
    bundle = tmp_path / "bundle"
    shutil.copytree(BUNDLE_DIR, bundle)
    odps_path = next(bundle.glob("*.odps.yaml"))
    doc = yaml.safe_load(odps_path.read_text(encoding="utf-8"))
    doc["outputPorts"][0]["name"] = "Daily Orders"
    _write_yaml(odps_path, doc)
    return bundle


def _strict_errors(contract: Dict[str, Any]) -> List[Any]:
    return FluidSchemaManager().validate_contract(contract, strict=True, offline_only=True).errors


# ---------------------------------------------------------------------------
# Defect 9 — exposeId in the ground-truth view must be a FLUID 0.7.2 identifier
# ---------------------------------------------------------------------------


class TestSeedExposeIdIsFluidIdentifier:
    def test_uppercase_schema_name_is_sanitised_in_prompt(self, tmp_path: Path) -> None:
        seed = load_seed(_camel_odcs(tmp_path), allow_remote=False)
        gt = format_ground_truth_prompt_block(seed)["ground_truth"]
        assert [e["exposeId"] for e in gt["exposes"]] == ["customerorders"]

    def test_spaced_port_name_is_sanitised_in_prompt(self, tmp_path: Path) -> None:
        seed = load_seed(_spaced_port_bundle(tmp_path), allow_remote=False)
        gt = format_ground_truth_prompt_block(seed)["ground_truth"]
        ids = [e["exposeId"] for e in gt["exposes"]]
        assert ids == ["daily_orders", "hourly_orders"]
        assert all(IDENTIFIER_RE.match(i) for i in ids)

    @pytest.mark.parametrize("builder", [_camel_odcs, _spaced_port_bundle], ids=["camel", "spaced"])
    def test_verbatim_copy_passes_strict_validation_and_guard(self, tmp_path, builder) -> None:
        seed = load_seed(builder(tmp_path), allow_remote=False)
        block = json.loads(json.dumps(format_ground_truth_prompt_block(seed)))
        contract = _contract_from_ground_truth(block["ground_truth"])
        assert _strict_errors(contract) == []
        assert diff_against_seed(seed, contract) == []

    @pytest.mark.parametrize("builder", [_camel_odcs, _spaced_port_bundle], ids=["camel", "spaced"])
    def test_copilot_succeeds_on_first_attempt(self, tmp_path, builder) -> None:
        llm = _CopierLlm()
        result = _run_copilot(builder(tmp_path), llm)
        assert llm.calls == 1
        assert result.attempt_reports[0].validation_errors == []

    def test_guard_enforces_the_sanitised_expose_id(self, tmp_path: Path) -> None:
        seed = load_seed(_spaced_port_bundle(tmp_path), allow_remote=False)
        gt = format_ground_truth_prompt_block(seed)["ground_truth"]
        contract = _contract_from_ground_truth(gt)
        contract["exposes"][0]["exposeId"] = "renamed_orders"
        mismatches = diff_against_seed(seed, contract)
        assert [m["path"] for m in mismatches] == ["exposes[0].exposeId"]
        assert mismatches[0]["seed"] == "daily_orders"
        assert "exposes[0].exposeId" in format_ground_truth_prompt_block(seed)["ground_truth_paths"]

    def test_raw_seed_copy_is_still_clean(self, tmp_path: Path) -> None:
        seed = load_seed(_spaced_port_bundle(tmp_path), allow_remote=False)
        assert diff_against_seed(seed, seed.fluid) == []

    def test_colliding_names_get_distinct_deterministic_ids(self, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path / "dup.odcs.yaml", _odcs("dup.contract", ["Orders", "orders"]))
        seed = load_seed(path, allow_remote=False)
        first = format_ground_truth_prompt_block(seed)["ground_truth"]
        second = format_ground_truth_prompt_block(load_seed(path, allow_remote=False))
        ids = [e["exposeId"] for e in first["exposes"]]
        assert ids == ["orders", "orders_2"]
        assert ids == [e["exposeId"] for e in second["ground_truth"]["exposes"]]
        contract = _contract_from_ground_truth(first)
        assert _strict_errors(contract) == []
        assert diff_against_seed(seed, contract) == []

    def test_valid_expose_ids_are_unchanged(self) -> None:
        seed = load_seed(BUNDLE_DIR, allow_remote=False)
        gt = format_ground_truth_prompt_block(seed)["ground_truth"]
        assert [e["exposeId"] for e in gt["exposes"]] == ["daily_orders", "hourly_orders"]


# ---------------------------------------------------------------------------
# Defect 10 — the --seed-from docs must match what the seed actually enforces
# ---------------------------------------------------------------------------


def _forge_seed_help() -> str:
    from fluid_build.cli import forge

    parser = argparse.ArgumentParser(prog="fluid")
    forge.register(parser.add_subparsers(dest="cmd"))
    forge_parser = parser._subparsers._group_actions[0].choices["forge"]
    action = next(a for a in forge_parser._actions if a.dest == "seed_from")
    return " ".join(action.help.split())


def _doc_seed_section() -> str:
    text = BITOL_DOC.read_text(encoding="utf-8")
    section = text.split("## `fluid forge --seed-from`", 1)[1].split("\n## ", 1)[0]
    return " ".join(section.split())


class TestSeedDocsMatchCode:
    def test_help_says_schema_and_qos_only(self) -> None:
        help_text = _forge_seed_help()
        assert "schema/quality/qos" not in help_text
        assert "schema and qos" in help_text
        assert "not carried" in help_text
        assert "governance" not in help_text

    def test_bitol_doc_seed_section(self) -> None:
        section = _doc_seed_section()
        assert "schema/quality/qos" not in section
        assert "schema and qos" in section
        assert "not carried" in section
        assert "governance" not in section

    def test_readme_seed_comment(self) -> None:
        lines = README.read_text(encoding="utf-8").splitlines()
        idx = next(i for i, ln in enumerate(lines) if "--seed-from" in ln)
        comment_lines = []
        for ln in reversed(lines[:idx]):
            if not ln.startswith("#"):
                break
            comment_lines.append(ln)
        comment = " ".join(reversed(comment_lines))
        assert "governance" not in comment
        assert "schema" in comment and "qos" in comment

    def test_system_prompt_does_not_promise_quality(self) -> None:
        prompt = build_system_prompt(_capability_matrix(), ["sql"], has_seed_ground_truth=True)
        clause = prompt.split("GROUND TRUTH FROM SEED", 1)[1]
        assert "governance" not in clause


# ---------------------------------------------------------------------------
# Defect 18 — the seed directory scanner must not follow symlinks out
# ---------------------------------------------------------------------------

SECRET = "SECRET_MARKER_987"


def _symlink_or_skip(link: Path, target: Path) -> None:
    try:
        link.symlink_to(target)
    except (OSError, NotImplementedError):  # pragma: no cover - platform dependent
        pytest.skip("symlinks are not supported here")


def _secret_tree(tmp_path: Path) -> Path:
    secret = _odcs("shop.orders", ["orders"])
    secret["schema"][0]["properties"] = [
        {"name": "ceo_salary", "logicalType": "integer", "description": SECRET}
    ]
    return _write_yaml(tmp_path / "private" / "secret.yaml", secret)


class TestSeedDirectoryContainment:
    def test_bundle_symlink_out_of_root_is_not_read(self, tmp_path: Path) -> None:
        secret = _secret_tree(tmp_path)
        bundle = tmp_path / "bundle"
        _write_yaml(
            bundle / "p.odps.yaml",
            {
                "apiVersion": "v1.0.0",
                "kind": "DataProduct",
                "id": "shop",
                "name": "Shop",
                "status": "active",
                "version": "1.0.0",
                "outputPorts": [
                    {"name": "orders", "version": "1.0.0", "contractId": "shop.orders"}
                ],
            },
        )
        _symlink_or_skip(bundle / "innocent.odcs.yaml", Path("..") / "private" / secret.name)
        try:
            seed = load_seed(bundle, allow_remote=False)
        except ProviderError as exc:
            assert SECRET not in str(exc)
        else:  # pragma: no cover - the port must not resolve to the outside file
            assert SECRET not in json.dumps(seed.fluid, default=str)
            pytest.fail("output port resolved through a symlink outside the bundle")

    def test_odcs_only_symlink_out_of_root_is_not_read(self, tmp_path: Path) -> None:
        secret = _secret_tree(tmp_path)
        bundle = tmp_path / "bundle2"
        shutil.copy(HOURLY_ODCS, bundle.parent / "hourly.tmp")
        bundle.mkdir()
        shutil.move(str(bundle.parent / "hourly.tmp"), str(bundle / "hourly_orders.odcs.yaml"))
        _symlink_or_skip(bundle / "innocent.odcs.yaml", secret.resolve())
        seed = load_seed(bundle, allow_remote=False)
        assert SECRET not in json.dumps(seed.fluid, default=str)
        assert SECRET not in json.dumps(format_ground_truth_prompt_block(seed))
        assert all("innocent" not in str(p.get("origin")) for p in seed.provenance)

    def test_symlinked_subdirectory_out_of_root_is_not_read(self, tmp_path: Path) -> None:
        _secret_tree(tmp_path)
        bundle = tmp_path / "bundle3"
        bundle.mkdir()
        shutil.copy(HOURLY_ODCS, bundle / "hourly_orders.odcs.yaml")
        _symlink_or_skip(bundle / "linked", (tmp_path / "private").resolve())
        seed = load_seed(bundle, allow_remote=False)
        assert SECRET not in json.dumps(seed.fluid, default=str)

    def test_only_outside_symlink_means_no_documents(self, tmp_path: Path) -> None:
        secret = _secret_tree(tmp_path)
        bundle = tmp_path / "bundle4"
        bundle.mkdir()
        _symlink_or_skip(bundle / "innocent.odcs.yaml", secret.resolve())
        with pytest.raises(ProviderError, match="contains no Bitol ODPS or ODCS documents"):
            load_seed(bundle, allow_remote=False)

    def test_symlink_inside_root_is_still_followed(self, tmp_path: Path) -> None:
        bundle = tmp_path / "bundle5"
        (bundle / "real").mkdir(parents=True)
        shutil.copy(HOURLY_ODCS, bundle / "real" / "hourly_orders.odcs.yaml")
        _symlink_or_skip(bundle / "alias.odcs.yaml", Path("real") / "hourly_orders.odcs.yaml")
        seed = load_seed(bundle, allow_remote=False)
        assert seed.fluid.get("exposes")


# ---------------------------------------------------------------------------
# Defect 24 — no hint may recommend the deprecated `fluid odps-bitol`
# ---------------------------------------------------------------------------


class TestNoDeprecatedCommandHints:
    def test_fluid_input_hint_points_at_opds_export(self, tmp_path: Path) -> None:
        path = tmp_path / "c.yaml"
        path.write_text(MULTI_EXPOSE_FLUID.read_text(encoding="utf-8"), encoding="utf-8")
        with pytest.raises(ProviderError) as exc_info:
            load_seed(path, allow_remote=False)
        message = str(exc_info.value)
        assert "odps-bitol" not in message
        assert "fluid opds export --spec bitol-1.0.0" in message

    @pytest.mark.parametrize(
        "path",
        [
            REPO / "fluid_build" / "cli" / "forge_copilot_seed.py",
            REPO / "fluid_build" / "cli" / "forge.py",
            REPO / "fluid_build" / "cli" / "opds.py",
            BITOL_DOC,
            README,
        ],
        ids=lambda p: p.name,
    )
    def test_owned_sources_do_not_recommend_odps_bitol(self, path: Path) -> None:
        assert "fluid odps-bitol" not in path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Defects 33 / 35 / 36 — fluid opds import/export CLI edge cases
# ---------------------------------------------------------------------------


def _parse(argv: List[str]) -> Namespace:
    from fluid_build.cli.opds import register

    parser = argparse.ArgumentParser(prog="fluid")
    register(parser.add_subparsers(dest="cmd"))
    return parser.parse_args(argv)


class TestImportFormatFollowsOutSuffix:
    def test_json_suffix_writes_json(self, tmp_path: Path) -> None:
        out = tmp_path / "c.fluid.json"
        args = _parse(["opds", "import", str(BUNDLE_DIR), "--no-remote", "-o", str(out)])
        assert cmd_opds_import(args, LOG) == 0
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data["exposes"]

    def test_yaml_suffix_writes_yaml(self, tmp_path: Path) -> None:
        out = tmp_path / "c.fluid.yaml"
        args = _parse(["opds", "import", str(BUNDLE_DIR), "--no-remote", "-o", str(out)])
        assert cmd_opds_import(args, LOG) == 0
        text = out.read_text(encoding="utf-8")
        assert not text.lstrip().startswith("{")
        assert yaml.safe_load(text)["exposes"]

    def test_explicit_format_wins_over_suffix(self, tmp_path: Path) -> None:
        out = tmp_path / "c.fluid.json"
        args = _parse(
            ["opds", "import", str(BUNDLE_DIR), "--no-remote", "-o", str(out), "-f", "yaml"]
        )
        assert cmd_opds_import(args, LOG) == 0
        assert not out.read_text(encoding="utf-8").lstrip().startswith("{")

    def test_unknown_suffix_defaults_to_yaml(self, tmp_path: Path) -> None:
        out = tmp_path / "c.fluid"
        args = _parse(["opds", "import", str(BUNDLE_DIR), "--no-remote", "-o", str(out)])
        assert cmd_opds_import(args, LOG) == 0
        assert not out.read_text(encoding="utf-8").lstrip().startswith("{")

    def test_stdout_defaults_to_yaml(self, capsys) -> None:
        args = _parse(["opds", "import", str(BUNDLE_DIR), "--no-remote"])
        assert cmd_opds_import(args, LOG) == 0
        out = capsys.readouterr().out
        assert not out.lstrip().startswith("{")
        assert yaml.safe_load(out)["exposes"]

    def test_json_import_reexports(self, tmp_path: Path) -> None:
        out = tmp_path / "c.fluid.json"
        args = _parse(["opds", "import", str(BUNDLE_DIR), "--no-remote", "-o", str(out)])
        assert cmd_opds_import(args, LOG) == 0
        export_args = _parse(["opds", "export", str(out), "--out-dir", str(tmp_path / "rt")])
        assert cmd_opds_export(export_args, LOG) == 0


class TestOdpiRejectsBitolOnlyOptions:
    def test_out_dir_is_rejected(self, tmp_path: Path, capsys) -> None:
        target = tmp_path / "d2"
        args = _parse(
            [
                "opds",
                "export",
                str(MULTI_EXPOSE_FLUID),
                "--spec",
                "odpi-4.1",
                "--out-dir",
                str(target),
            ]
        )
        assert cmd_opds_export(args, LOG) == 2
        captured = capsys.readouterr()
        assert "--out-dir" in captured.out + captured.err
        assert captured.out.strip() == "" or not captured.out.lstrip().startswith("{")
        assert not target.exists()

    def test_yaml_format_is_rejected(self, tmp_path: Path, capsys) -> None:
        out = tmp_path / "o.json"
        args = _parse(
            [
                "opds",
                "export",
                str(MULTI_EXPOSE_FLUID),
                "--spec",
                "odpi-4.1",
                "--out",
                str(out),
                "-f",
                "yaml",
            ]
        )
        assert cmd_opds_export(args, LOG) == 2
        captured = capsys.readouterr()
        assert "--format" in captured.out + captured.err
        assert not out.exists()

    def test_json_format_is_accepted(self, tmp_path: Path) -> None:
        out = tmp_path / "o.json"
        args = _parse(
            [
                "opds",
                "export",
                str(MULTI_EXPOSE_FLUID),
                "--spec",
                "odpi-4.1",
                "--out",
                str(out),
                "-f",
                "json",
            ]
        )
        assert cmd_opds_export(args, LOG) == 0
        assert out.exists()

    def test_legacy_version_flag_still_exports(self, tmp_path: Path) -> None:
        out = tmp_path / "legacy.json"
        args = _parse(
            ["opds", "export", str(MULTI_EXPOSE_FLUID), "--version", "4.1", "--out", str(out)]
        )
        assert cmd_opds_export(args, LOG) == 0
        assert out.exists()

    def test_format_help_says_bitol_only(self) -> None:
        from fluid_build.cli.opds import register

        parser = argparse.ArgumentParser(prog="fluid")
        register(parser.add_subparsers(dest="cmd"))
        odps = parser._subparsers._group_actions[0].choices["odps"]
        export = odps._subparsers._group_actions[0].choices["export"]
        helps = {a.dest: a.help for a in export._actions}
        assert "Bitol specs only" in helps["format"]
        assert "odpi-4.1" in helps["out_dir"]


class TestSuccessLinesAreNotMangled:
    def test_import_success_line_keeps_brackets_and_is_unwrapped(self, tmp_path: Path) -> None:
        out = tmp_path / ("[red]" + "x" * 90) / "c.fluid.yaml"
        args = _parse(["opds", "import", str(HOURLY_ODCS), "-o", str(out)])
        with patch("fluid_build.cli.opds.cprint") as cprint:
            assert cmd_opds_import(args, LOG) == 0
        assert out.exists()
        lines = [c for c in cprint.call_args_list if str(out) in " ".join(map(str, c.args))]
        assert lines, cprint.call_args_list
        for call in lines:
            assert call.kwargs.get("markup") is False
            assert call.kwargs.get("soft_wrap") is True

    def test_import_success_line_rendered_by_rich(self, tmp_path: Path) -> None:
        from rich.console import Console

        out = tmp_path / ("[red]" + "x" * 90) / "c.fluid.yaml"
        args = _parse(["opds", "import", str(HOURLY_ODCS), "-o", str(out)])
        rich_console = Console(width=60, record=True, force_terminal=False)
        with patch("fluid_build.cli.console.console", rich_console):
            assert cmd_opds_import(args, LOG) == 0
        text = rich_console.export_text()
        assert str(out) in text

    def test_odpi_success_line_keeps_brackets(self, tmp_path: Path) -> None:
        out = tmp_path / ("[red]" + "y" * 90) / "o.json"
        out.parent.mkdir(parents=True)
        args = _parse(
            ["opds", "export", str(MULTI_EXPOSE_FLUID), "--spec", "odpi-4.1", "--out", str(out)]
        )
        with patch("fluid_build.cli.opds.cprint") as cprint:
            assert cmd_opds_export(args, LOG) == 0
        lines = [c for c in cprint.call_args_list if str(out) in " ".join(map(str, c.args))]
        assert lines, cprint.call_args_list
        for call in lines:
            assert call.kwargs.get("markup") is False
            assert call.kwargs.get("soft_wrap") is True


# ---------------------------------------------------------------------------
# Defect 34 — the documented command sequences must work when run literally
# ---------------------------------------------------------------------------

_PLACEHOLDERS = {
    "<productId>": "commerce.orders-product",
    "<port1>": "daily_orders",
    "<port2>": "hourly_orders",
    "<port>": "daily_orders",
}


def _documented_commands(text: str) -> List[str]:
    commands: List[str] = []
    for block in re.findall(r"```bash\n(.*?)```", text, flags=re.S):
        for raw in block.splitlines():
            line = raw.split(" #", 1)[0].strip() if not raw.lstrip().startswith("#") else ""
            if not line.startswith("fluid ") or line.startswith("fluid forge"):
                continue
            for placeholder, value in _PLACEHOLDERS.items():
                line = line.replace(placeholder, value)
            commands.append(line)
    return commands


def _run_documented(commands: List[str], cwd: Path) -> None:
    contract = yaml.safe_load(MULTI_EXPOSE_FLUID.read_text(encoding="utf-8"))
    # The ODPI v4.1 exporter copies metadata.status verbatim and the ODPI
    # schema only accepts its own lifecycle values (draft, production, ...),
    # so use one both specs accept for the documented ODPI export → validate.
    contract["metadata"]["status"] = "draft"
    _write_yaml(cwd / "contract.fluid.yaml", contract)
    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO) + os.pathsep + env.get("PYTHONPATH", "")
    env.pop("ODPS_API_VERSION", None)
    env.pop("ODCS_API_VERSION", None)
    python = shlex.quote(sys.executable)
    for command in commands:
        shell_cmd = f"{python} -m fluid_build {command[len('fluid '):]}"
        proc = subprocess.run(
            shell_cmd,
            shell=True,
            cwd=str(cwd),
            env=env,
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert proc.returncode == 0, (command, proc.stdout[-2000:], proc.stderr[-2000:])


class TestDocumentedSequencesRun:
    def test_bitol_interop_doc_commands(self, tmp_path: Path) -> None:
        text = BITOL_DOC.read_text(encoding="utf-8")
        commands = _documented_commands(text.split("## `fluid forge --seed-from`", 1)[0])
        assert any(" import out/ " in f" {c} " for c in commands)
        assert any("--spec odpi-4.1" in c for c in commands)
        _run_documented(commands, tmp_path)

    def test_readme_bitol_commands(self, tmp_path: Path) -> None:
        text = README.read_text(encoding="utf-8")
        section = text.split("### Which ODPS? — disambiguation", 1)[1].split("\n---", 1)[0]
        commands = _documented_commands(section)
        assert commands
        _run_documented(commands, tmp_path)


# ---------------------------------------------------------------------------
# Defect 22 — features.yaml provider keys must match the provider packages
# ---------------------------------------------------------------------------


class TestFeaturesYamlProviderKeys:
    @staticmethod
    def _features() -> Dict[str, Any]:
        return yaml.safe_load((REPO / "fluid_build" / "features.yaml").read_text(encoding="utf-8"))

    def test_every_provider_key_is_a_package(self) -> None:
        for key in self._features()["providers"]:
            assert (REPO / "fluid_build" / "providers" / key).is_dir(), key

    def test_bitol_provider_is_listed_under_its_package_name(self) -> None:
        providers = self._features()["providers"]
        assert "odps_standard" in providers
        assert "odps_bitol" in providers["odps_standard"]["note"]

    def test_every_manifest_provider_has_a_features_entry(self) -> None:
        manifest = yaml.safe_load(
            (REPO / "fluid_build" / "build-manifest.yaml").read_text(encoding="utf-8")
        )
        providers = self._features()["providers"]
        for profile, build in (manifest.get("builds") or {}).items():
            for name in build.get("providers") or []:
                if (REPO / "fluid_build" / "providers" / name).is_dir():
                    assert name in providers, (profile, name)

    def test_assess_provider_summary_lists_bitol_provider(self) -> None:
        proc = subprocess.run(
            [sys.executable, str(REPO / "scripts" / "assess_provider.py"), "--summary"],
            cwd=str(REPO),
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert proc.returncode == 0, proc.stderr
        assert "odps_standard" in proc.stdout
