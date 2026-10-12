# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Phase 4 — ``fluid opds`` CLI integration tests.

Covers the ``--spec`` dispatcher (bitol-1.0.0 vs odpi-4.1), ``--out-dir``
bundle writes, the import dispatch by input type, the legacy ``--version 4.1``
deprecation alias, and the ``--no-remote`` flag.
"""

from __future__ import annotations

import logging
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch
from urllib.error import URLError

import pytest
import yaml

from fluid_build.cli.opds import (
    SPEC_BITOL_1_0_0,
    SPEC_ODPI_4_1,
    cmd_opds_export,
    cmd_opds_import,
    resolve_spec,
)

LOG = logging.getLogger(__name__)

FIXTURES = Path(__file__).parent / "fixtures"
MULTI_EXPOSE_FLUID = FIXTURES / "fluid" / "contract-multi-expose.fluid.yaml"
BUNDLE_DIR = FIXTURES / "odps" / "product-bitol"


# ---------------------------------------------------------------------------
# resolve_spec — flag precedence
# ---------------------------------------------------------------------------


class TestResolveSpec:
    def test_default_is_bitol(self) -> None:
        args = Namespace(spec=None, version=None)
        assert resolve_spec(args) == SPEC_BITOL_1_0_0

    def test_explicit_spec_wins(self) -> None:
        args = Namespace(spec=SPEC_ODPI_4_1, version="4.1")
        assert resolve_spec(args) == SPEC_ODPI_4_1

    def test_legacy_version_4_1_maps_to_odpi(self, caplog) -> None:
        args = Namespace(spec=None, version="4.1")
        with caplog.at_level(logging.WARNING):
            assert resolve_spec(args) == SPEC_ODPI_4_1
        assert any("deprecated" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Export — Bitol path
# ---------------------------------------------------------------------------


class TestExportBitol:
    def test_export_bundle_to_out_dir(self, tmp_path: Path) -> None:
        args = Namespace(
            contract=str(MULTI_EXPOSE_FLUID),
            spec=SPEC_BITOL_1_0_0,
            version=None,
            out="-",
            out_dir=str(tmp_path),
            format="yaml",
            env=None,
            validate_strict=False,
            pretty=True,
        )
        rc = cmd_opds_export(args, LOG)
        assert rc == 0
        odps_files = list(tmp_path.glob("*.odps.yaml"))
        odcs_files = list(tmp_path.glob("*.odcs.yaml"))
        assert len(odps_files) == 1
        assert len(odcs_files) >= 1

    def test_export_bundle_emits_valid_product(self, tmp_path: Path) -> None:
        from fluid_build.providers.odps_standard.validation import (
            load_schema,
            validate,
        )

        args = Namespace(
            contract=str(MULTI_EXPOSE_FLUID),
            spec=SPEC_BITOL_1_0_0,
            version=None,
            out="-",
            out_dir=str(tmp_path),
            format="yaml",
            env=None,
            validate_strict=False,
            pretty=True,
        )
        cmd_opds_export(args, LOG)

        product_path = next(tmp_path.glob("*.odps.yaml"))
        with open(product_path) as f:
            product = yaml.safe_load(f)
        validate(product, load_schema())  # raises on failure


# ---------------------------------------------------------------------------
# Import dispatch — file / directory / lone ODCS
# ---------------------------------------------------------------------------


def _import_args(path: Path, *, out: Path | None = None, **overrides) -> Namespace:
    base = dict(
        path=str(path),
        spec=SPEC_BITOL_1_0_0,
        version=None,
        out=str(out) if out else None,
        format="yaml",
        no_remote=False,
        lenient=False,
    )
    base.update(overrides)
    return Namespace(**base)


class TestImportDispatch:
    def test_import_odps_file(self, tmp_path: Path) -> None:
        out = tmp_path / "out.fluid.yaml"
        product = next(BUNDLE_DIR.glob("*.odps.yaml"))
        args = _import_args(product, out=out)
        assert cmd_opds_import(args, LOG) == 0
        with open(out) as f:
            fluid = yaml.safe_load(f)
        assert len(fluid["exposes"]) == 2

    def test_import_directory(self, tmp_path: Path) -> None:
        out = tmp_path / "from-dir.fluid.yaml"
        args = _import_args(BUNDLE_DIR, out=out)
        assert cmd_opds_import(args, LOG) == 0
        with open(out) as f:
            fluid = yaml.safe_load(f)
        assert len(fluid["exposes"]) == 2

    def test_import_lone_odcs_file(self, tmp_path: Path) -> None:
        out = tmp_path / "from-odcs.fluid.yaml"
        odcs = next(BUNDLE_DIR.glob("*.odcs.yaml"))
        args = _import_args(odcs, out=out)
        assert cmd_opds_import(args, LOG) == 0
        with open(out) as f:
            fluid = yaml.safe_load(f)
        assert len(fluid["exposes"]) == 1

    def test_import_odpi_v4_1_rejected_as_export_only(self, tmp_path: Path) -> None:
        product = next(BUNDLE_DIR.glob("*.odps.yaml"))
        args = _import_args(product, spec=SPEC_ODPI_4_1)
        rc = cmd_opds_import(args, LOG)
        assert rc == 2

    def test_import_nonexistent_path_returns_1(self, tmp_path: Path) -> None:
        args = _import_args(tmp_path / "does-not-exist.odps.yaml")
        rc = cmd_opds_import(args, LOG)
        assert rc == 1

    def test_directory_with_two_odps_docs_returns_1(self) -> None:
        broken = FIXTURES / "odps" / "product-bitol-broken" / "two-odps-docs"
        args = _import_args(broken)
        rc = cmd_opds_import(args, LOG)
        assert rc == 1


class TestImportDispatchYieldsEquivalentFluid:
    def test_file_and_directory_imports_are_equivalent(self, tmp_path: Path) -> None:
        out_file = tmp_path / "from-file.fluid.yaml"
        out_dir = tmp_path / "from-dir.fluid.yaml"
        product = next(BUNDLE_DIR.glob("*.odps.yaml"))
        cmd_opds_import(_import_args(product, out=out_file), LOG)
        cmd_opds_import(_import_args(BUNDLE_DIR, out=out_dir), LOG)
        with open(out_file) as f:
            file_fluid = yaml.safe_load(f)
        with open(out_dir) as f:
            dir_fluid = yaml.safe_load(f)
        assert file_fluid == dir_fluid


# ---------------------------------------------------------------------------
# --no-remote propagation
# ---------------------------------------------------------------------------


class TestNoRemoteFlag:
    @staticmethod
    def _synth_doc(tmp_path: Path) -> Path:
        # Synthetic ODPS doc whose port contractId is a URL — the resolver can
        # only satisfy it with an http(s) fetch.
        odps_doc = tmp_path / "synth.odps.yaml"
        odps_doc.write_text(
            yaml.dump(
                {
                    "apiVersion": "v1.0.0",
                    "kind": "DataProduct",
                    "id": "synth.product",
                    "name": "synth-product",
                    "version": "1.0.0",
                    "status": "draft",
                    "outputPorts": [
                        {
                            "name": "remote_only",
                            "version": "1.0.0",
                            "contractId": "https://example.invalid/c.odcs.yaml",
                        }
                    ],
                }
            )
        )
        return odps_doc

    def test_no_remote_flag_blocks_url_contract_ids(self, tmp_path: Path) -> None:
        # With --no-remote the import must fail loud *without* touching the
        # network. ``example.invalid`` never resolves, so the return code alone
        # cannot tell the two behaviours apart — assert on the fetch itself.
        args = _import_args(self._synth_doc(tmp_path), out=tmp_path / "f.yaml", no_remote=True)
        with patch(
            "fluid_build.providers.odps_standard.resolver.urlopen",
            side_effect=URLError("network must not be used"),
        ) as fake_urlopen:
            rc = cmd_opds_import(args, LOG)
        assert rc == 1  # output-port resolution required, no remote allowed
        fake_urlopen.assert_not_called()

    def test_without_no_remote_the_fetch_is_attempted(self, tmp_path: Path) -> None:
        # Control: the same doc without --no-remote does try the URL, so the
        # test above would catch a regression that stops propagating the flag.
        args = _import_args(self._synth_doc(tmp_path), out=tmp_path / "f.yaml", no_remote=False)
        with patch(
            "fluid_build.providers.odps_standard.resolver.urlopen",
            side_effect=URLError("blocked in test"),
        ) as fake_urlopen:
            rc = cmd_opds_import(args, LOG)
        assert rc == 1
        fake_urlopen.assert_called_once()
