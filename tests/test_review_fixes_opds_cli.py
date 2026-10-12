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

"""Regression tests for the ``fluid opds`` / ``fluid odps-bitol`` CLI review fixes.

Each test class names the review finding it pins down.
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import warnings
from argparse import Namespace
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest
import yaml

from fluid_build.cli.opds import (
    ODPS_VERSIONS,
    SPEC_BITOL_1_0_0,
    SPEC_BITOL_1_1_0,
    SPEC_ODPI_4_1,
    cmd_opds_export,
    cmd_opds_import,
    cmd_opds_info,
    cmd_opds_validate,
    register,
)
from fluid_build.providers.base import ProviderError

LOG = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent
FIXTURES = Path(__file__).parent / "fixtures"
MULTI_EXPOSE_FLUID = FIXTURES / "fluid" / "contract-multi-expose.fluid.yaml"
BUNDLE_DIR = FIXTURES / "odps" / "product-bitol"
HOURLY_ODCS = FIXTURES / "odps" / "contracts-only" / "hourly_orders.odcs.yaml"
UPSTREAM_V110 = FIXTURES / "upstream" / "odps" / "simple-data-product.v1.1.0.odps.yaml"

LONG_TEXT = (
    "Orders [beta] data product sourced from the OLTP replica and refreshed every hour "
    "by the commerce team; the pattern ^[a-z0-9_]+$ must survive verbatim"
)
PATTERN = "^[a-z0-9_]+$"


def _parse(argv):
    parser = argparse.ArgumentParser(prog="fluid")
    sub = parser.add_subparsers(dest="cmd")
    register(sub)
    return parser.parse_args(argv)


def _export_args(**overrides: Any) -> Namespace:
    base: Dict[str, Any] = dict(
        contract=str(MULTI_EXPOSE_FLUID),
        spec=None,
        version=None,
        out="-",
        out_dir=None,
        format=None,
        env=None,
        validate_strict=True,
        pretty=True,
    )
    base.update(overrides)
    return Namespace(**base)


def _import_args(path: Path, **overrides: Any) -> Namespace:
    base: Dict[str, Any] = dict(
        path=str(path),
        spec=None,
        version=None,
        out=None,
        format="yaml",
        no_remote=True,
        lenient=False,
    )
    base.update(overrides)
    return Namespace(**base)


def _validate_args(path: Path, **overrides: Any) -> Namespace:
    base: Dict[str, Any] = dict(file=str(path), spec=None, version=None, full_schema=True)
    base.update(overrides)
    return Namespace(**base)


def _product_file(directory: Path) -> Path:
    return next(p for p in directory.iterdir() if ".odps." in p.name)


# ---------------------------------------------------------------------------
# Finding 5 — no explicit --spec ⇒ provider's own version chain applies
# ---------------------------------------------------------------------------


class TestExportVersionChain:
    def test_env_var_selects_version_when_spec_omitted(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setenv("ODPS_API_VERSION", "v1.1.0")
        rc = cmd_opds_export(_export_args(out_dir=str(tmp_path)), LOG)
        assert rc == 0
        product = yaml.safe_load(_product_file(tmp_path).read_text())
        assert product["apiVersion"] == "v1.1.0"
        assert "v1.1.0" in capsys.readouterr().out  # message reports what was written

    def test_explicit_spec_pins_version_over_env(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setenv("ODPS_API_VERSION", "v1.1.0")
        rc = cmd_opds_export(_export_args(out_dir=str(tmp_path), spec=SPEC_BITOL_1_0_0), LOG)
        assert rc == 0
        product = yaml.safe_load(_product_file(tmp_path).read_text())
        assert product["apiVersion"] == "v1.0.0"
        assert "v1.0.0" in capsys.readouterr().out

    def test_default_without_env_stays_v1_0_0(self, tmp_path, monkeypatch):
        monkeypatch.delenv("ODPS_API_VERSION", raising=False)
        assert cmd_opds_export(_export_args(out_dir=str(tmp_path)), LOG) == 0
        product = yaml.safe_load(_product_file(tmp_path).read_text())
        assert product["apiVersion"] == "v1.0.0"

    def test_provider_built_without_explicit_version_when_spec_omitted(self, tmp_path):
        seen = {}

        def _fake_init(self, api_version=None):
            seen["api_version"] = api_version
            raise RuntimeError("stop")

        with patch("fluid_build.providers.odps_standard.BitolOdpsProvider.__init__", _fake_init):
            with patch("fluid_build.cli.opds.console_error"):
                cmd_opds_export(_export_args(out_dir=str(tmp_path)), LOG)
        assert seen["api_version"] is None

        with patch("fluid_build.providers.odps_standard.BitolOdpsProvider.__init__", _fake_init):
            with patch("fluid_build.cli.opds.console_error"):
                cmd_opds_export(_export_args(out_dir=str(tmp_path), spec=SPEC_BITOL_1_1_0), LOG)
        assert seen["api_version"] == "v1.1.0"

    def test_reexport_of_imported_v110_bundle_preserves_version(self, tmp_path, monkeypatch):
        monkeypatch.delenv("ODPS_API_VERSION", raising=False)
        first = tmp_path / "v11"
        assert (
            cmd_opds_export(
                _export_args(out_dir=str(first), spec=SPEC_BITOL_1_1_0, validate_strict=False),
                LOG,
            )
            == 0
        )
        fluid_path = tmp_path / "imported.fluid.yaml"
        assert cmd_opds_import(_import_args(first, out=str(fluid_path)), LOG) == 0
        second = tmp_path / "re"
        assert (
            cmd_opds_export(
                _export_args(contract=str(fluid_path), out_dir=str(second), validate_strict=False),
                LOG,
            )
            == 0
        )
        product = yaml.safe_load(_product_file(second).read_text())
        assert product["apiVersion"] == "v1.1.0"


# ---------------------------------------------------------------------------
# Finding 14 — machine-readable stdout must bypass Rich
# ---------------------------------------------------------------------------


def _odcs_with_pattern(tmp_path: Path) -> Path:
    doc = yaml.safe_load(HOURLY_ODCS.read_text())
    doc["description"]["purpose"] = LONG_TEXT
    prop = doc["schema"][0]["properties"][0]
    prop["logicalTypeOptions"] = {"pattern": PATTERN}
    path = tmp_path / "sku.odcs.yaml"
    path.write_text(yaml.safe_dump(doc, sort_keys=False))
    return path


class TestStdoutPayloadsAreRaw:
    def test_import_yaml_to_stdout_round_trips(self, tmp_path, capsys):
        src = _odcs_with_pattern(tmp_path)
        out_file = tmp_path / "file.fluid.yaml"
        assert cmd_opds_import(_import_args(src, out=str(out_file)), LOG) == 0
        capsys.readouterr()
        assert cmd_opds_import(_import_args(src), LOG) == 0
        stdout = capsys.readouterr().out
        assert PATTERN in stdout
        assert yaml.safe_load(stdout) == yaml.safe_load(out_file.read_text())

    def test_import_json_to_stdout_round_trips(self, tmp_path, capsys):
        src = _odcs_with_pattern(tmp_path)
        out_file = tmp_path / "file.fluid.json"
        assert cmd_opds_import(_import_args(src, out=str(out_file), format="json"), LOG) == 0
        capsys.readouterr()
        assert cmd_opds_import(_import_args(src, format="json"), LOG) == 0
        stdout = capsys.readouterr().out
        assert json.loads(stdout) == json.loads(out_file.read_text())

    def test_bitol_export_to_stdout_is_valid_json(self, tmp_path, capsys):
        contract = yaml.safe_load(MULTI_EXPOSE_FLUID.read_text())
        contract["metadata"]["description"] = LONG_TEXT
        src = tmp_path / "c.fluid.yaml"
        src.write_text(yaml.safe_dump(contract, sort_keys=False))
        assert cmd_opds_export(_export_args(contract=str(src)), LOG) == 0
        product = json.loads(capsys.readouterr().out)
        assert LONG_TEXT in json.dumps(product, ensure_ascii=False)

    @pytest.mark.parametrize("pretty", [True, False])
    def test_odpi_export_to_stdout_is_valid_json(self, pretty, capsys):
        payload = {"dataProductId": "x", "dataProductDescription": LONG_TEXT, "n": 1}
        provider = MagicMock()
        provider.render.return_value = payload
        with patch("fluid_build.cli.bootstrap.build_provider", return_value=provider):
            with patch("fluid_build.cli.bootstrap.load_contract_with_overlay", return_value={}):
                rc = cmd_opds_export(_export_args(spec=SPEC_ODPI_4_1, pretty=pretty), LOG)
        assert rc == 0
        assert json.loads(capsys.readouterr().out) == payload

    def test_info_json_to_stdout_is_valid_json(self, capsys):
        assert cmd_opds_info(Namespace(version=None, spec=None, json=True), LOG) == 0
        assert json.loads(capsys.readouterr().out) == ODPS_VERSIONS


# ---------------------------------------------------------------------------
# Findings 15 / 55 — --out FILE writes the full bundle, format from suffix
# ---------------------------------------------------------------------------


class TestExportOutFile:
    def test_parser_format_default_is_unset(self):
        ns = _parse(["opds", "export", "c.yaml", "--out", "p.json"])
        assert ns.format is None

    def test_out_json_infers_json_and_writes_sibling_contracts(self, tmp_path, capsys):
        out = tmp_path / "product.json"
        assert cmd_opds_export(_export_args(out=str(out)), LOG) == 0
        product = json.loads(out.read_text())
        siblings = sorted(p.name for p in tmp_path.glob("*.odcs.json"))
        expected = sorted(f"{p['contractId']}.odcs.json" for p in product["outputPorts"])
        assert siblings == expected and len(siblings) == 2
        for name in siblings:
            json.loads((tmp_path / name).read_text())
        message = capsys.readouterr().out
        assert str(out) in message
        for name in siblings:
            assert name in message

        # The written bundle is importable (schema information intact).
        fluid_out = tmp_path / "back.fluid.yaml"
        assert cmd_opds_import(_import_args(out, out=str(fluid_out)), LOG) == 0
        fluid = yaml.safe_load(fluid_out.read_text())
        names = {
            f["name"]
            for e in fluid["exposes"]
            for f in (e.get("contract") or {}).get("schema") or []
        }
        assert "order_id" in names

    def test_out_yaml_infers_yaml(self, tmp_path):
        out = tmp_path / "product.yaml"
        assert cmd_opds_export(_export_args(out=str(out)), LOG) == 0
        assert yaml.safe_load(out.read_text())["kind"] == "DataProduct"
        assert len(list(tmp_path.glob("*.odcs.yaml"))) == 2

    def test_explicit_format_wins_over_suffix(self, tmp_path):
        out = tmp_path / "product.yaml"
        assert cmd_opds_export(_export_args(out=str(out), format="json"), LOG) == 0
        assert json.loads(out.read_text())["kind"] == "DataProduct"
        assert len(list(tmp_path.glob("*.odcs.json"))) == 2

    @pytest.mark.parametrize("bad_id", ["../evil", "a/b", "/abs/x", "a\\b", ".."])
    def test_unsafe_contract_ids_rejected(self, tmp_path, bad_id):
        bundle = {
            "product": {"apiVersion": "v1.0.0", "kind": "DataProduct", "id": "p"},
            "contracts": {bad_id: {"id": bad_id}},
        }
        out_dir = tmp_path / "nested" / "out"
        out_dir.mkdir(parents=True)
        out = out_dir / "product.json"
        err = MagicMock()
        with patch(
            "fluid_build.providers.odps_standard.BitolOdpsProvider.render", return_value=bundle
        ):
            with patch("fluid_build.cli.opds.console_error", err):
                rc = cmd_opds_export(_export_args(out=str(out)), LOG)
        assert rc != 0
        assert "contractId" in " ".join(str(c) for c in err.call_args_list)
        written = [p for p in tmp_path.rglob("*") if p.is_file()]
        assert written == []

    def test_unsafe_contract_ids_rejected_for_out_dir(self, tmp_path):
        bundle = {
            "product": {"apiVersion": "v1.0.0", "kind": "DataProduct", "id": "p"},
            "contracts": {"../evil": {"id": "../evil"}},
        }
        out_dir = tmp_path / "out"
        with patch(
            "fluid_build.providers.odps_standard.BitolOdpsProvider.render", return_value=bundle
        ):
            with patch("fluid_build.cli.opds.console_error"):
                rc = cmd_opds_export(_export_args(out_dir=str(out_dir)), LOG)
        assert rc != 0
        assert [p for p in tmp_path.rglob("*") if p.is_file()] == []

    def test_out_and_out_dir_together_rejected(self, tmp_path):
        with patch("fluid_build.cli.opds.console_error"):
            rc = cmd_opds_export(
                _export_args(out=str(tmp_path / "p.json"), out_dir=str(tmp_path / "d")), LOG
            )
        assert rc == 2
        assert list(tmp_path.iterdir()) == []


# ---------------------------------------------------------------------------
# Findings 39 / 46 / 68 — validate/info understand Bitol
# ---------------------------------------------------------------------------


@pytest.fixture
def bitol_bundle(tmp_path) -> Dict[str, Path]:
    ydir = tmp_path / "yaml"
    jdir = tmp_path / "json"
    assert cmd_opds_export(_export_args(out_dir=str(ydir), spec=SPEC_BITOL_1_0_0), LOG) == 0
    assert (
        cmd_opds_export(_export_args(out_dir=str(jdir), spec=SPEC_BITOL_1_0_0, format="json"), LOG)
        == 0
    )
    return {"yaml": _product_file(ydir), "json": _product_file(jdir)}


class TestValidateBitol:
    def test_parser_accepts_spec_on_validate_and_info(self):
        ns = _parse(["opds", "validate", "p.yaml", "--spec", SPEC_BITOL_1_0_0])
        assert ns.spec == SPEC_BITOL_1_0_0 and ns.version is None
        ns = _parse(["odps", "info", "--spec", SPEC_BITOL_1_1_0])
        assert ns.spec == SPEC_BITOL_1_1_0
        ns = _parse(["odps", "validate", "p.json", "--version", "4.1"])
        assert ns.version == "4.1"

    @pytest.mark.parametrize("fmt", ["yaml", "json"])
    def test_autodetects_bitol_document(self, bitol_bundle, fmt, capsys):
        rc = cmd_opds_validate(_validate_args(bitol_bundle[fmt]), LOG)
        assert rc == 0
        assert "commerce.orders-product" in capsys.readouterr().out

    def test_explicit_bitol_spec(self, bitol_bundle):
        args = _validate_args(bitol_bundle["yaml"], spec=SPEC_BITOL_1_0_0)
        assert cmd_opds_validate(args, LOG) == 0

    def test_invalid_bitol_document_fails(self, bitol_bundle, tmp_path):
        doc = yaml.safe_load(bitol_bundle["yaml"].read_text())
        del doc["id"]
        bad = tmp_path / "bad.odps.yaml"
        bad.write_text(yaml.safe_dump(doc))
        with patch("fluid_build.cli.opds.console_error") as err:
            assert cmd_opds_validate(_validate_args(bad), LOG) == 1
        assert "id" in " ".join(str(c) for c in err.call_args_list)

    def test_schema_follows_document_api_version(self, capsys):
        assert cmd_opds_validate(_validate_args(UPSTREAM_V110), LOG) == 0
        assert "v1.1.0" in capsys.readouterr().out

    def test_explicit_spec_pins_schema_version(self):
        ok = _validate_args(UPSTREAM_V110, spec=SPEC_BITOL_1_1_0)
        assert cmd_opds_validate(ok, LOG) == 0
        pinned = _validate_args(UPSTREAM_V110, spec=SPEC_BITOL_1_0_0)
        with patch("fluid_build.cli.opds.console_error"):
            assert cmd_opds_validate(pinned, LOG) == 1

    def test_odpi_document_still_uses_odpi_validator(self, tmp_path):
        doc = {"dataProductId": "x", "dataProductName": "X", "dataProductDescription": "d"}
        path = tmp_path / "odpi.json"
        path.write_text(json.dumps(doc))
        result = {"valid": True, "validation_type": "basic", "warnings": []}
        with patch(
            "fluid_build.providers.odps.validator.validate_opds_structure", return_value=result
        ) as v:
            assert cmd_opds_validate(_validate_args(path), LOG) == 0
        assert v.call_args.kwargs["version"] == "4.1"
        assert v.call_args.args[0] == doc

    def test_odpi_document_in_yaml_is_accepted(self, tmp_path):
        doc = {"dataProductId": "x", "dataProductName": "X", "dataProductDescription": "d"}
        path = tmp_path / "odpi.yaml"
        path.write_text(yaml.safe_dump(doc))
        result = {"valid": True, "validation_type": "basic", "warnings": []}
        with patch(
            "fluid_build.providers.odps.validator.validate_opds_structure", return_value=result
        ) as v:
            assert cmd_opds_validate(_validate_args(path), LOG) == 0
        assert v.call_args.args[0] == doc

    @pytest.mark.parametrize(
        "flags", [{"version": "4.1"}, {"spec": SPEC_ODPI_4_1}], ids=["version", "spec"]
    )
    def test_explicit_odpi_forces_odpi_path(self, bitol_bundle, flags):
        result = {"valid": False, "validation_type": "basic", "errors": ["nope"]}
        with patch(
            "fluid_build.providers.odps.validator.validate_opds_structure", return_value=result
        ) as v:
            with patch("fluid_build.cli.opds.console_error"):
                assert cmd_opds_validate(_validate_args(bitol_bundle["json"], **flags), LOG) == 1
        assert v.called


class TestInfoSpec:
    @pytest.mark.parametrize("spec", [SPEC_BITOL_1_0_0, SPEC_BITOL_1_1_0])
    def test_info_bitol_spec(self, spec, capsys):
        assert cmd_opds_info(Namespace(version=None, spec=spec, json=False), LOG) == 0
        out = capsys.readouterr().out
        assert "bitol-io/open-data-product-standard" in out
        assert spec.split("-")[1] in out

    def test_info_bitol_spec_json(self, capsys):
        args = Namespace(version=None, spec=SPEC_BITOL_1_1_0, json=True)
        assert cmd_opds_info(args, LOG) == 0
        data = json.loads(capsys.readouterr().out)
        assert data[SPEC_BITOL_1_1_0]["apiVersion"] == "v1.1.0"

    def test_info_odpi_spec_matches_version_flag(self, capsys):
        assert cmd_opds_info(Namespace(version=None, spec=SPEC_ODPI_4_1, json=True), LOG) == 0
        assert json.loads(capsys.readouterr().out) == {"4.1": ODPS_VERSIONS["4.1"]}

    def test_info_usage_uses_spec_not_deprecated_version(self, capsys):
        assert cmd_opds_info(Namespace(version=None, spec=None, json=False), LOG) == 0
        out = capsys.readouterr().out
        assert "export contract.yaml --version 4.1" not in out
        assert "--spec odpi-4.1" in out
        assert SPEC_BITOL_1_0_0 in out


# ---------------------------------------------------------------------------
# Finding 69 — --no-validate-strict reports problems as warnings
# ---------------------------------------------------------------------------


class TestNonStrictValidationWarns:
    def test_problems_reported_as_warnings(self, tmp_path, capsys):
        with patch(
            "fluid_build.providers.odps_standard.validation.validate",
            side_effect=ProviderError("ODPS validation failed: boom-marker"),
        ):
            rc = cmd_opds_export(_export_args(out_dir=str(tmp_path), validate_strict=False), LOG)
        assert rc == 0
        captured = capsys.readouterr()
        assert "boom-marker" in captured.err
        assert "boom-marker" not in captured.out
        assert _product_file(tmp_path).exists()

    def test_clean_bundle_emits_no_warnings(self, tmp_path, capsys):
        rc = cmd_opds_export(_export_args(out_dir=str(tmp_path), validate_strict=False), LOG)
        assert rc == 0
        assert "⚠" not in capsys.readouterr().err

    def test_strict_mode_still_fails(self, tmp_path):
        with patch(
            "fluid_build.providers.odps_standard.provider.validate",
            side_effect=ProviderError("ODPS validation failed: boom-marker"),
        ):
            with patch("fluid_build.cli.opds.console_error"):
                rc = cmd_opds_export(_export_args(out_dir=str(tmp_path)), LOG)
        assert rc == 1

    def test_help_text_matches_behaviour(self):
        parser = argparse.ArgumentParser(prog="fluid")
        register(parser.add_subparsers(dest="cmd"))
        sub = parser._subparsers._group_actions[0].choices["odps"]
        export = sub._subparsers._group_actions[0].choices["export"]
        action = next(a for a in export._actions if "--no-validate-strict" in a.option_strings)
        assert "warning" in action.help.lower()


# Finding 63 (--no-remote really skips the network) is pinned in
# tests/test_opds_cli_spec.py::TestNoRemoteFlag.


# ---------------------------------------------------------------------------
# Finding 67 — `fluid odps-bitol` prints its deprecation notice
# ---------------------------------------------------------------------------


class TestOdpsBitolDeprecation:
    def _ns(self, argv):
        from fluid_build.cli import odps_standard

        parser = argparse.ArgumentParser(prog="fluid")
        odps_standard.register(parser.add_subparsers(dest="cmd"))
        return parser.parse_args(argv)

    def test_export_warns(self, tmp_path, capsys):
        ns = self._ns(
            ["odps-bitol", "export", str(MULTI_EXPOSE_FLUID), "-o", str(tmp_path / "p.yaml")]
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            assert ns.func(ns, LOG) == 0
        assert any(issubclass(w.category, DeprecationWarning) for w in caught)
        err = capsys.readouterr().err
        assert "deprecated" in err and "fluid opds export --spec bitol-1.0.0" in err

    def test_validate_and_info_warn(self, tmp_path, capsys):
        product = next(BUNDLE_DIR.glob("*.odps.yaml"))
        for argv in (["odps-bitol", "validate", str(product)], ["odps-bitol", "info"]):
            ns = self._ns(argv)
            with warnings.catch_warnings(record=True):
                warnings.simplefilter("always")
                ns.func(ns, LOG)
            assert "deprecated" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Finding 64 — top-level help describes the new default
# ---------------------------------------------------------------------------


class TestMainHelpLabels:
    def test_odps_row_mentions_bitol_default_and_bitol_cli_deprecated(self, capsys):
        from fluid_build.cli.help_formatter import print_main_help

        print_main_help(argparse.ArgumentParser(prog="fluid"))
        out = capsys.readouterr().out
        odps_line = next(line for line in out.splitlines() if line.strip().startswith("odps "))
        assert "Bitol" in odps_line and "ODPI" in odps_line
        bitol_line = next(line for line in out.splitlines() if "odps-bitol" in line)
        assert "deprecated" in bitol_line.lower()


# ---------------------------------------------------------------------------
# Findings 38 / 40 — docs and help match the code
# ---------------------------------------------------------------------------


class TestDocsMatchCode:
    DOC = REPO / "docs" / "BITOL_INTEROP.md"

    def test_resolver_matrix_documented_as_implemented(self):
        from fluid_build.providers.odps_standard.resolver import (
            DEFAULT_EXTENSIONS,
            DEFAULT_SUBDIRS,
        )

        text = self.DOC.read_text(encoding="utf-8")
        line = next(ln for ln in text.splitlines() if "Local file probes" in ln)
        for ext in DEFAULT_EXTENSIONS:
            assert f"`{ext}`" in line or f"{ext}," in line or f"{ext}}}" in line
        for sub in DEFAULT_SUBDIRS:
            assert (f"{sub}/" if sub else "<base>") in line
        assert "odcs/contracts/" not in line
        assert ".yml, .json" not in line

    def test_import_not_promised_as_validated_fluid(self):
        text = self.DOC.read_text(encoding="utf-8")
        assert "one validated FLUID" not in text
        parser = argparse.ArgumentParser(prog="fluid")
        register(parser.add_subparsers(dest="cmd"))
        sub = parser._subparsers._group_actions[0].choices["odps"]
        importp = sub._subparsers._group_actions[0].choices["import"]
        assert "validated FLUID contract" not in importp.description
        assert "skeleton" in importp.description


# ---------------------------------------------------------------------------
# Integration seams (found when merging the review-fix workstreams)
# ---------------------------------------------------------------------------

_MINIMAL_ODCS = Path(__file__).parent / "fixtures" / "odcs" / "contract-minimal.yaml"


class TestImportSeams:
    def _invalid_odcs(self, tmp_path: Path) -> Path:
        doc = yaml.safe_load(_MINIMAL_ODCS.read_text())
        doc["schema"][0]["properties"][0]["logicalType"] = "not-a-type"
        target = tmp_path / "broken.odcs.yaml"
        target.write_text(yaml.safe_dump(doc, sort_keys=False))
        return target

    def test_lone_invalid_odcs_fails_without_lenient(self, tmp_path: Path) -> None:
        # ODCS import is strict again (origin/main behaviour).
        assert cmd_opds_import(_import_args(self._invalid_odcs(tmp_path)), LOG) == 1

    def test_lone_invalid_odcs_imports_with_lenient(self, tmp_path: Path) -> None:
        out_file = tmp_path / "out.fluid.yaml"
        args = _import_args(self._invalid_odcs(tmp_path), lenient=True, out=str(out_file))
        assert cmd_opds_import(args, LOG) == 0
        assert yaml.safe_load(out_file.read_text())["exposes"]

    def test_kind_sniffed_invalid_odcs_honours_lenient(self, tmp_path: Path) -> None:
        # No .odcs suffix: routed by ``kind: DataContract``.
        src = self._invalid_odcs(tmp_path)
        plain = tmp_path / "broken.yaml"
        plain.write_text(src.read_text())
        assert cmd_opds_import(_import_args(plain), LOG) == 1
        assert cmd_opds_import(_import_args(plain, lenient=True), LOG) == 0

    def test_fluid_contract_is_rejected_not_imported_as_odps(self, tmp_path: Path) -> None:
        fluid = tmp_path / "contract.yaml"
        fluid.write_text(
            yaml.safe_dump({"fluidVersion": "0.7.2", "kind": "DataProduct", "id": "x", "name": "x"})
        )
        with patch("fluid_build.cli.opds.console_error") as err:
            assert cmd_opds_import(_import_args(fluid), LOG) == 1
        assert "is a FLUID contract" in err.call_args[0][0]

    def test_odcs_with_unquoted_yaml_date_validates_and_imports(self, tmp_path: Path) -> None:
        doc = yaml.safe_load(_MINIMAL_ODCS.read_text())
        target = tmp_path / "dated.odcs.yaml"
        text = yaml.safe_dump(doc, sort_keys=False)
        # An unquoted date loads as datetime.date; JSON Schema sees its ISO string.
        text += "slaProperties:\n  - property: effectiveFrom\n    value: 2024-01-01\n"
        target.write_text(text)
        assert isinstance(yaml.safe_load(text)["slaProperties"][0]["value"], datetime.date)
        assert cmd_opds_import(_import_args(target), LOG) == 0
