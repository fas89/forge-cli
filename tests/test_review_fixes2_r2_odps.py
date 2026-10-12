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

"""Regression tests for the round-2 ODPS / ODCS review findings.

Covers schema-less per-port ODCS documents in a bundle, ODCS-only
directory imports, YAML anchors in imported FLUID, FLUID owner / expects
edits reaching the ODPS team and the per-port ODCS teams / servers, the
nested < v1.1.0 downgrade, the legacy contractId pin, the remote-fetch total
deadline, malformed contractIds and YAML alias bombs.
"""

from __future__ import annotations

import contextlib
import copy
import datetime
import http.server
import logging
import shutil
import threading
import time
import tracemalloc
from pathlib import Path
from typing import Any, Dict, Iterator, List

import pytest
import yaml

from fluid_build.providers.base import ProviderError
from fluid_build.providers.odcs import OdcsProvider
from fluid_build.providers.odcs.io import load_yaml, read_input
from fluid_build.providers.odcs.validation import json_instance
from fluid_build.providers.odps_standard import BitolOdpsProvider, OdpsStandardProvider
from fluid_build.providers.odps_standard.resolver import ContractNotFound, ContractResolver
from fluid_build.providers.odps_standard.validation import load_schema
from fluid_build.providers.odps_standard.validation import validate as validate_odps

FIXTURES = Path(__file__).parent / "fixtures"
BUNDLE_DIR = FIXTURES / "odps" / "product-bitol"
CONTRACTS_ONLY = FIXTURES / "odps" / "contracts-only"
UPSTREAM_ODPS = sorted((FIXTURES / "upstream" / "odps").glob("*.odps.yaml"))
PRODUCT_FILE = BUNDLE_DIR / "commerce.orders-product.odps.yaml"
DAILY = "commerce.orders-product.daily_orders"
HOURLY = "commerce.orders-product.hourly_orders"


def _load(path: Path) -> Any:
    with open(path) as f:
        return yaml.safe_load(f)


def _write(path: Path, doc: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(doc, sort_keys=False))
    return path


def _via_yaml(fluid: Dict[str, Any]) -> Dict[str, Any]:
    """Mimic `fluid opds import -o f.yaml` + `fluid opds export f.yaml`."""
    return yaml.safe_load(yaml.safe_dump(fluid, sort_keys=False))


def _strict_provider(api_version: Any = None) -> BitolOdpsProvider:
    prov = BitolOdpsProvider(api_version=api_version)
    prov.strict_validation = True
    return prov


@contextlib.contextmanager
def _captured(logger: logging.Logger) -> Iterator[List[str]]:
    """Warnings logged on ``logger`` itself (independent of propagation,
    which other tests' logging setup may turn off)."""
    messages: List[str] = []

    class _Handler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            messages.append(record.getMessage())

    handler = _Handler(level=logging.WARNING)
    old_level, old_disabled = logger.level, logger.disabled
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING)
    logger.disabled = False
    try:
        yield messages
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old_level)
        logger.disabled = old_disabled


def _server(odcs: Dict[str, Any], name: str) -> Dict[str, Any]:
    return next(s for s in odcs["servers"] if s.get("server") == name)


# ---------------------------------------------------------------------------
# Defect 3: a schema-less per-port ODCS keeps its document on bundle import
# ---------------------------------------------------------------------------


def _schema_less_odcs(api_version: str) -> Dict[str, Any]:
    return {
        "apiVersion": api_version,
        "kind": "DataContract",
        "id": "c-orders",
        "version": "2.0.0",
        "status": "active",
        "name": "Orders contract",
        "description": {"purpose": "Orders"},
        "domain": "sales",
        "dataProduct": "p1",
        "servers": [
            {
                "server": "prod",
                "type": "postgres",
                "host": "db.example.com",
                "port": 5432,
                "database": "shop",
                "schema": "public",
            }
        ],
        "slaProperties": [{"property": "latency", "value": 4, "unit": "d"}],
    }


def _schema_less_bundle(root: Path, api_version: str) -> Path:
    _write(
        root / "p.odps.yaml",
        {
            "apiVersion": "v1.1.0",
            "kind": "DataProduct",
            "id": "p1",
            "name": "P1",
            "status": "active",
            "outputPorts": [{"name": "orders", "version": "1.0.0", "contractId": "c-orders"}],
        },
    )
    _write(root / "c-orders.odcs.yaml", _schema_less_odcs(api_version))
    return root


class TestSchemaLessPortContract:
    @pytest.mark.parametrize("api_version", ["v3.1.0", "v3.2.0"])
    def test_document_level_fields_round_trip(self, tmp_path: Path, api_version: str) -> None:
        prov = _strict_provider()
        bundle = prov.render(
            _via_yaml(prov.import_directory(_schema_less_bundle(tmp_path, api_version)))
        )
        odcs = bundle["contracts"]["c-orders"]
        source = _schema_less_odcs(api_version)
        # The ODCS exporter always writes ``schema`` (an empty list here).
        assert odcs.pop("schema") == []
        assert odcs == source

    def test_snapshot_is_kept_on_the_expose(self, tmp_path: Path) -> None:
        fluid = BitolOdpsProvider().import_directory(_schema_less_bundle(tmp_path, "v3.1.0"))
        doc = fluid["exposes"][0]["odcs_passthrough"]["_document"]
        assert doc["metadata"]["odcs_passthrough"]["_verbatim"]["version"] == "2.0.0"


# ---------------------------------------------------------------------------
# Defect 12: ODCS-only directory import keeps each contract's document
# ---------------------------------------------------------------------------


class TestOdcsOnlyDirectory:
    def test_single_file_directory_round_trips(self, tmp_path: Path) -> None:
        src = FIXTURES / "odcs" / "contract-full.yaml"
        root = tmp_path / "contracts"
        root.mkdir()
        shutil.copy(src, root / "orders.odcs.yaml")
        source = _load(src)

        prov = BitolOdpsProvider()
        prov.strict_validation = False
        fluid = _via_yaml(prov.import_directory(root))
        bundle = prov.render(fluid)
        assert list(bundle["contracts"]) == [source["id"]]
        odcs = bundle["contracts"][source["id"]]
        for key in ("id", "name", "version", "description", "domain", "team", "servers"):
            assert odcs.get(key) == source.get(key), key
        assert bundle["product"]["outputPorts"][0]["contractId"] == source["id"]

    def test_fixture_directory_round_trips_every_contract(self) -> None:
        prov = _strict_provider()
        bundle = prov.render(_via_yaml(prov.import_directory(CONTRACTS_ONLY)))
        for path in sorted(CONTRACTS_ONLY.glob("*.odcs.yaml")):
            source = _load(path)
            assert bundle["contracts"][source["id"]] == source, path.name

    def test_same_file_and_directory_import_agree(self, tmp_path: Path) -> None:
        src = CONTRACTS_ONLY / "hourly_orders.odcs.yaml"
        root = tmp_path / "one"
        root.mkdir()
        shutil.copy(src, root / src.name)
        prov = _strict_provider()
        odcs = prov.render(prov.import_directory(root))["contracts"][_load(src)["id"]]
        assert odcs == _load(src)


# ---------------------------------------------------------------------------
# Defect 16: imported FLUID has no YAML anchors
# ---------------------------------------------------------------------------


class TestNoYamlAnchors:
    @pytest.mark.parametrize("entry", ["directory", "file"])
    def test_imported_bundle_dumps_without_aliases(self, entry: str) -> None:
        prov = BitolOdpsProvider()
        if entry == "directory":
            fluid = prov.import_directory(BUNDLE_DIR)
        else:
            fluid = prov.import_contract(PRODUCT_FILE)
        text = yaml.safe_dump(fluid, sort_keys=False)
        assert "&id" not in text and "*id" not in text

    def test_upstream_products_dump_without_aliases(self) -> None:
        prov = BitolOdpsProvider()
        for path in UPSTREAM_ODPS:
            fluid = prov.import_contract(path, allow_remote=False, lenient=True)
            text = yaml.safe_dump(fluid, sort_keys=False)
            assert "&id" not in text and "*id" not in text, path.name

    def test_render_shares_nothing_with_the_fluid(self) -> None:
        prov = BitolOdpsProvider()
        fluid = prov.import_contract(PRODUCT_FILE)
        product = prov.render(fluid)["product"]
        product["team"]["members"].append({"username": "x"})
        assert len(fluid["metadata"]["odps_passthrough"]["team"]["members"]) == 1


# ---------------------------------------------------------------------------
# Defects 17 / 20: owner and expects edits reach the ODPS team and the
# per-port ODCS teams / servers
# ---------------------------------------------------------------------------


def _imported_bundle() -> Dict[str, Any]:
    return _via_yaml(_strict_provider().import_directory(BUNDLE_DIR))


class TestProductEditsReachEveryDocument:
    def test_unedited_bundle_still_round_trips_byte_identically(self, tmp_path: Path) -> None:
        prov = _strict_provider()
        out = tmp_path / "out"
        prov.render(_imported_bundle(), out_dir=out)
        for src in BUNDLE_DIR.iterdir():
            assert (out / src.name).read_bytes() == src.read_bytes(), src.name

    def test_owner_edit_reaches_product_and_port_teams(self) -> None:
        fluid = _imported_bundle()
        fluid["owner"]["team"] = "NEW-TEAM"
        fluid["owner"]["email"] = "carol@acme.com"
        bundle = _strict_provider().render(fluid)

        team = bundle["product"]["team"]
        assert team["name"] == "NEW-TEAM"
        assert team["members"][0]["username"] == "carol@acme.com"
        assert team["members"][0]["name"] == "Alice"  # unedited keys survive
        for cid in (DAILY, HOURLY):
            odcs_team = bundle["contracts"][cid]["team"]
            assert odcs_team["name"] == "NEW-TEAM", cid
            assert odcs_team["members"][0]["username"] == "carol@acme.com", cid
            assert odcs_team["members"][0]["role"] == "data-owner", cid

    def test_owner_edit_leaves_everything_else_alone(self) -> None:
        fluid = _imported_bundle()
        fluid["owner"]["team"] = "NEW-TEAM"
        bundle = _strict_provider().render(fluid)
        product = dict(bundle["product"])
        product.pop("team")
        expected = _load(PRODUCT_FILE)
        expected.pop("team")
        assert product == expected
        for cid in (DAILY, HOURLY):
            odcs = dict(bundle["contracts"][cid])
            source = _load(BUNDLE_DIR / f"{cid}.odcs.yaml")
            odcs.pop("team"), source.pop("team")
            assert odcs == source, cid

    def test_expects_edit_reaches_every_port_server(self) -> None:
        fluid = _imported_bundle()
        customers = next(e for e in fluid["expects"] if e["id"] == "customers_raw")
        customers["location"]["database"] = "RAW_DB2"
        bundle = _strict_provider().render(fluid)
        assert bundle["product"] == _load(PRODUCT_FILE)
        for cid in (DAILY, HOURLY):
            odcs = bundle["contracts"][cid]
            source = _load(BUNDLE_DIR / f"{cid}.odcs.yaml")
            assert _server(odcs, "customers_raw")["database"] == "RAW_DB2", cid
            expected = copy.deepcopy(source)
            _server(expected, "customers_raw")["database"] = "RAW_DB2"
            assert odcs == expected, cid

    def test_removed_expect_drops_its_port_servers(self) -> None:
        fluid = _imported_bundle()
        fluid["expects"] = [e for e in fluid["expects"] if e["id"] != "customers_raw"]
        bundle = _strict_provider().render(fluid)
        assert "inputPorts" not in bundle["product"]
        for cid in (DAILY, HOURLY):
            names = [s["server"] for s in bundle["contracts"][cid]["servers"]]
            assert "customers_raw" not in names, cid
            assert len(names) == 1, cid

    def test_owner_edit_on_a_single_file_import(self) -> None:
        prov = _strict_provider()
        fluid = _via_yaml(prov.import_contract(PRODUCT_FILE))
        fluid["owner"]["team"] = "NEW-TEAM"
        bundle = prov.render(fluid)
        assert bundle["product"]["team"]["name"] == "NEW-TEAM"
        assert bundle["contracts"][DAILY]["team"]["name"] == "NEW-TEAM"


# ---------------------------------------------------------------------------
# Defect 19: every upstream v1.1.0 product exports valid at v1.0.0
# ---------------------------------------------------------------------------


class TestNestedDowngradeToV100:
    @pytest.mark.parametrize("path", UPSTREAM_ODPS, ids=lambda p: p.name)
    def test_upstream_v110_product_exports_valid_at_v100(self, path: Path) -> None:
        fluid = _via_yaml(
            BitolOdpsProvider().import_contract(path, allow_remote=False, lenient=True)
        )
        prov = BitolOdpsProvider(api_version="v1.0.0")  # what --spec bitol-1.0.0 selects
        prov.strict_validation = False  # their per-port contracts are not vendored
        product = prov.render(fluid)["product"]
        assert product["apiVersion"] == "v1.0.0"
        validate_odps(product)  # judged by the v1.0.0 schema it declares
        validate_odps(product, load_schema("v1.0.0"))

    def test_nested_keys_move_to_custom_properties_or_are_dropped(self) -> None:
        path = FIXTURES / "upstream" / "odps" / "deprecated-data-product.v1.1.0.odps.yaml"
        fluid = BitolOdpsProvider().import_contract(path, allow_remote=False, lenient=True)
        prov = BitolOdpsProvider(api_version="v1.0.0")
        prov.strict_validation = False
        with _captured(prov.logger) as logs:
            product = prov.render(fluid)["product"]
        source = _load(path)
        mgmt_src = source["managementPorts"][0]
        mgmt = product["managementPorts"][0]
        assert "deprecated" not in mgmt
        custom = {c["property"]: c["value"] for c in mgmt.get("customProperties") or []}
        assert custom["deprecated"] == mgmt_src["deprecated"]
        assert "managementPorts[0]" in "\n".join(logs)

    def test_sbom_extras_are_dropped_with_a_warning(self) -> None:
        path = FIXTURES / "upstream" / "odps" / "customer-data-product.v1.1.0.odps.yaml"
        fluid = BitolOdpsProvider().import_contract(path, allow_remote=False, lenient=True)
        prov = BitolOdpsProvider(api_version="v1.0.0")
        prov.strict_validation = False
        with _captured(prov.logger) as logs:
            product = prov.render(fluid)["product"]
        for port in product["outputPorts"]:
            for sbom in port.get("sbom") or []:
                assert set(sbom) <= {"type", "url"}
        assert "sbom[0]" in "\n".join(logs)

    def test_v110_export_of_the_same_product_is_untouched(self) -> None:
        path = FIXTURES / "upstream" / "odps" / "customer-data-product.v1.1.0.odps.yaml"
        fluid = BitolOdpsProvider().import_contract(path, allow_remote=False, lenient=True)
        prov = BitolOdpsProvider()
        prov.strict_validation = False
        assert prov.render(fluid)["product"] == _load(path)

    def test_cli_strict_export_at_bitol_1_0_0_succeeds(self, tmp_path: Path) -> None:
        from fluid_build.cli import main

        src = FIXTURES / "upstream" / "odps" / "customer-data-product.v1.1.0.odps.yaml"
        imported = tmp_path / "up.fluid.yaml"
        rc = main(["opds", "import", str(src), "--no-remote", "--lenient", "-o", str(imported)])
        assert rc in (0, None)
        out = tmp_path / "o.odps.yaml"
        rc = main(["opds", "export", str(imported), "--spec", "bitol-1.0.0", "--out", str(out)])
        assert rc in (0, None)
        validate_odps(_load(out), load_schema("v1.0.0"))


# ---------------------------------------------------------------------------
# Defect 5 (documented): expose.contract_id pins the 0.7.8 legacy id
# ---------------------------------------------------------------------------


def test_legacy_contract_id_can_be_pinned() -> None:
    fluid = {
        "id": "data-product",
        "metadata": {"name": "Data product", "version": "1.0.0"},
        "exposes": [{"id": "orders", "contract_id": "orders_contract"}],
    }
    product = OdpsStandardProvider().render(fluid)
    assert product["outputPorts"][0]["contractId"] == "orders_contract"


# ---------------------------------------------------------------------------
# Defect 30: the total deadline holds while headers / chunk sizes drip
# ---------------------------------------------------------------------------


class _DripHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args: Any) -> None:  # silence test output
        pass

    def do_GET(self) -> None:  # noqa: N802
        try:
            if self.path == "/slowhdr":
                self.wfile.write(b"HTTP/1.1 200 OK\r\n")
                for i in range(40):
                    self.wfile.write(f"X-Drip-{i}: 1\r\n".encode())
                    self.wfile.flush()
                    time.sleep(0.1)
                self.wfile.write(b"Content-Length: 0\r\n\r\n")
            elif self.path == "/slowchunk":
                self.wfile.write(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                    b"Transfer-Encoding: chunked\r\n\r\n"
                )
                self.wfile.flush()
                for _ in range(40):
                    self.wfile.write(b"0")  # a never-ending chunk-size line
                    self.wfile.flush()
                    time.sleep(0.1)
            elif self.path == "/ok":
                body = b'{"apiVersion": "v3.1.0", "kind": "DataContract", "id": "r", '
                body += b'"version": "1.0.0", "status": "active", "schema": []}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif self.path == "/chunked":
                body = b'{"apiVersion": "v3.1.0", "kind": "DataContract", "id": "r", '
                body += b'"version": "1.0.0", "status": "active", "schema": []}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                for part in (body[:20], body[20:]):
                    self.wfile.write(f"{len(part):x}\r\n".encode() + part + b"\r\n")
                self.wfile.write(b"0\r\n\r\n")
            elif self.path == "/redirect":
                self.send_response(302)
                self.send_header("Location", "/ok")
                self.send_header("Content-Length", "0")
                self.end_headers()
        except OSError:
            pass


@pytest.fixture
def drip_base(monkeypatch) -> Iterator[str]:
    for var in ("http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY"):
        monkeypatch.delenv(var, raising=False)
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _DripHandler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


class TestRemoteDeadline:
    @pytest.mark.parametrize("path", ["/slowhdr", "/slowchunk"])
    def test_deadline_holds_while_headers_or_chunk_sizes_drip(
        self, drip_base: str, path: str
    ) -> None:
        resolver = ContractResolver(allow_remote=True, timeout=2, total_timeout=0.5)
        started = time.monotonic()
        with pytest.raises(ContractNotFound):
            resolver.resolve(f"{drip_base}{path}")
        assert time.monotonic() - started < 1.5

    @pytest.mark.parametrize("path", ["/ok", "/chunked", "/redirect"])
    def test_normal_responses_still_resolve(self, drip_base: str, path: str) -> None:
        resolved = ContractResolver(allow_remote=True).resolve(f"{drip_base}{path}")
        assert resolved.odcs["id"] == "r"


# ---------------------------------------------------------------------------
# Defect 31: malformed contractIds are warnings when lenient
# ---------------------------------------------------------------------------


def _nul_bundle(root: Path, *, on: str) -> Path:
    product = _load(PRODUCT_FILE)
    bad = "commerce.orders-product.input.cust\x00omers"
    if on == "input":
        product["inputPorts"][0]["contractId"] = bad
    else:
        product["outputPorts"][1]["contractId"] = bad
    _write(root / "commerce.orders-product.odps.yaml", product)
    for src in BUNDLE_DIR.glob("*.odcs.yaml"):
        shutil.copy(src, root / src.name)
    return root


class TestMalformedContractIds:
    def test_input_port_nul_is_a_warning(self, tmp_path: Path) -> None:
        prov = BitolOdpsProvider()
        with _captured(prov.logger) as logs:
            fluid = prov.import_directory(_nul_bundle(tmp_path, on="input"))
        assert [e["id"] for e in fluid["expects"]] == ["customers_raw"]
        assert "Skipping unresolved contract" in "\n".join(logs)

    def test_output_port_nul_is_a_warning_when_lenient(self, tmp_path: Path) -> None:
        fluid = BitolOdpsProvider().import_directory(
            _nul_bundle(tmp_path, on="output"), lenient=True
        )
        assert len(fluid["exposes"]) == 2

    def test_output_port_nul_is_a_provider_error_when_strict(self, tmp_path: Path) -> None:
        with pytest.raises(ProviderError):
            BitolOdpsProvider().import_directory(_nul_bundle(tmp_path, on="output"))

    @pytest.mark.parametrize("bad", ["a\x00b", 42, None, ["x"], "http://["])
    def test_resolver_never_crashes(self, tmp_path: Path, bad: Any) -> None:
        with pytest.raises(ContractNotFound):
            ContractResolver(base_path=tmp_path, allow_remote=False).resolve(bad)


# ---------------------------------------------------------------------------
# Defect 32: YAML alias bombs are refused, fast and small
# ---------------------------------------------------------------------------


def _alias_bomb(depth: int = 8, width: int = 9) -> str:
    lines = [
        "apiVersion: v3.1.0",
        "kind: DataContract",
        "id: shop.orders",
        "version: 1.0.0",
        "status: active",
        "customProperties:",
        "- property: bomb",
        "  value:",
        "    l0: &l0 [" + ",".join(["lol"] * width) + "]",
    ]
    for i in range(1, depth + 1):
        lines.append(f"    l{i}: &l{i} [" + ",".join([f"*l{i - 1}"] * width) + "]")
    lines.append("schema: []")
    return "\n".join(lines) + "\n"


def _fails_fast_and_small(fn: Any) -> None:
    tracemalloc.start()
    started = time.monotonic()
    try:
        with pytest.raises(ProviderError):
            fn()
        elapsed = time.monotonic() - started
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert elapsed < 5
    assert peak < 64 * 1024 * 1024


class TestAliasBombs:
    def test_bomb_is_small(self) -> None:
        assert len(_alias_bomb().encode()) < 700

    def test_read_input_refuses_it(self, tmp_path: Path) -> None:
        path = tmp_path / "shop.orders.odcs.yaml"
        path.write_text(_alias_bomb())
        _fails_fast_and_small(lambda: read_input(path))

    def test_odcs_import_refuses_it(self, tmp_path: Path) -> None:
        path = tmp_path / "shop.orders.odcs.yaml"
        path.write_text(_alias_bomb())
        _fails_fast_and_small(lambda: OdcsProvider().import_contract(path, lenient=True))

    def test_bundle_import_refuses_it(self, tmp_path: Path) -> None:
        root = tmp_path / "bombb"
        _write(
            root / "shop.odps.yaml",
            {
                "apiVersion": "v1.1.0",
                "kind": "DataProduct",
                "id": "shop",
                "status": "active",
                "outputPorts": [{"name": "orders", "contractId": "shop.orders"}],
            },
        )
        (root / "shop.orders.odcs.yaml").write_text(_alias_bomb())
        _fails_fast_and_small(lambda: BitolOdpsProvider().import_directory(root))
        # Lenient: the port is skipped with a warning, still fast.
        started = time.monotonic()
        fluid = BitolOdpsProvider().import_directory(root, lenient=True)
        assert time.monotonic() - started < 5
        assert "contract" not in fluid["exposes"][0]

    def test_remote_bomb_is_refused(self, monkeypatch) -> None:
        from fluid_build.providers.odps_standard import resolver as resolver_mod

        body = _alias_bomb().encode()

        class _Resp:
            headers = {"Content-Type": "application/yaml"}

            def __init__(self) -> None:
                self._body = body

            def read(self, amt: int = -1) -> bytes:
                out, self._body = self._body, b""
                return out

            def __enter__(self):
                return self

            def __exit__(self, *_: Any) -> bool:
                return False

        monkeypatch.setattr(resolver_mod, "urlopen", lambda url, **kw: _Resp())
        _fails_fast_and_small(
            lambda: ContractResolver(allow_remote=True).resolve("https://example.invalid/c.yaml")
        )

    def test_recursive_alias_is_refused(self) -> None:
        with pytest.raises(ProviderError):
            load_yaml("a: &a [*a]\n")

    def test_modest_aliases_still_load(self) -> None:
        doc = load_yaml("base: &b {x: 1, y: [1, 2]}\nother: *b\nmerged: {<<: *b, z: 3}\n")
        assert doc["other"] == {"x": 1, "y": [1, 2]}
        assert doc["merged"] == {"x": 1, "y": [1, 2], "z": 3}

    def test_large_alias_free_document_still_loads(self) -> None:
        text = "items:\n" + "".join(f"- {{a: {i}, b: [x, y]}}\n" for i in range(50_000))
        assert len(load_yaml(text)["items"]) == 50_000

    def test_json_instance_preserves_shared_references(self) -> None:
        shared = [{"when": datetime.date(2024, 1, 1)}]
        doc = {"a": shared, "b": shared, "c": (shared,)}
        out = json_instance(doc)
        assert out["a"] is out["b"]
        assert out["c"][0] is out["a"]
        assert out["a"][0]["when"] == "2024-01-01"
        assert doc["a"][0]["when"] != "2024-01-01"  # input untouched

    def test_json_instance_stays_small_on_a_shared_tree(self) -> None:
        node: Any = ["lol"] * 9
        for _ in range(8):
            node = [node] * 9
        tracemalloc.start()
        started = time.monotonic()
        try:
            json_instance({"bomb": node})
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        assert time.monotonic() - started < 2
        assert peak < 8 * 1024 * 1024
