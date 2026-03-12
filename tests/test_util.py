"""Tests for fluid_build/util/ modules — contract, io, cron helpers."""
import json
import os
import pytest
from pathlib import Path
from unittest.mock import patch

from fluid_build.util.contract import (
    get_expose_id, get_expose_kind, get_expose_binding,
    get_expose_location, get_expose_schema, get_expose_contract,
    get_expose_format, get_builds, get_primary_build,
    get_build_engine, get_contract_version,
    normalize_expose, normalize_contract,
)
from fluid_build.util.io import load_contract, dump_json, read_json
from fluid_build.util.cron import get_cron


# ═══════════════════════════════════════════════════════════════════════
# util/contract.py
# ═══════════════════════════════════════════════════════════════════════

class TestGetExposeId:
    def test_057_format(self):
        assert get_expose_id({"exposeId": "abc"}) == "abc"

    def test_040_format(self):
        assert get_expose_id({"id": "xyz"}) == "xyz"

    def test_057_preferred(self):
        assert get_expose_id({"exposeId": "new", "id": "old"}) == "new"

    def test_missing(self):
        assert get_expose_id({}) is None


class TestGetExposeKind:
    def test_057(self):
        assert get_expose_kind({"kind": "table"}) == "table"

    def test_040(self):
        assert get_expose_kind({"type": "view"}) == "view"

    def test_missing(self):
        assert get_expose_kind({}) is None


class TestGetExposeBinding:
    def test_binding_object(self):
        b = {"platform": "gcp", "location": "gs://bucket/path"}
        assert get_expose_binding({"binding": b}) == b

    def test_location_string_fallback(self):
        result = get_expose_binding({"location": "s3://bucket/key"})
        assert result == {"location": "s3://bucket/key"}

    def test_location_dict_not_fallback(self):
        # location as dict (0.4.0 structured) — not converted
        assert get_expose_binding({"location": {"format": "csv"}}) is None

    def test_missing(self):
        assert get_expose_binding({}) is None


class TestGetExposeLocation:
    def test_from_binding(self):
        assert get_expose_location({"binding": {"location": "/data/out"}}) == "/data/out"

    def test_direct_fallback(self):
        # Location as a string at expose level — get_expose_binding wraps it,
        # but the resulting binding dict lacks a "location" key, so fall through
        # to expose.get("location") which returns the string.
        assert get_expose_location({"location": "gs://bucket"}) == "gs://bucket"

    def test_missing(self):
        assert get_expose_location({}) is None


class TestGetExposeSchema:
    def test_present(self):
        schema = [{"name": "id", "type": "INT"}]
        assert get_expose_schema({"schema": schema}) == schema

    def test_missing(self):
        assert get_expose_schema({}) is None


class TestGetExposeContract:
    def test_present(self):
        assert get_expose_contract({"contract": {"dq": []}}) == {"dq": []}

    def test_missing(self):
        assert get_expose_contract({}) is None


class TestGetExposeFormat:
    def test_present(self):
        assert get_expose_format({"format": "parquet"}) == "parquet"

    def test_missing(self):
        assert get_expose_format({}) is None


class TestGetBuilds:
    def test_builds_array(self):
        result = get_builds({"builds": [{"engine": "dbt"}, {"engine": "spark"}]})
        assert len(result) == 2

    def test_single_build_wrapped(self):
        result = get_builds({"build": {"engine": "dbt"}})
        assert len(result) == 1
        assert result[0]["engine"] == "dbt"

    def test_no_builds(self):
        assert get_builds({}) == []

    def test_builds_not_list(self):
        assert get_builds({"builds": "bad"}) == []


class TestGetPrimaryBuild:
    def test_from_builds(self):
        result = get_primary_build({"builds": [{"engine": "dbt"}]})
        assert result["engine"] == "dbt"

    def test_from_build(self):
        result = get_primary_build({"build": {"engine": "spark"}})
        assert result["engine"] == "spark"

    def test_empty(self):
        assert get_primary_build({}) is None


class TestGetBuildEngine:
    def test_engine(self):
        assert get_build_engine({"engine": "dbt"}) == "dbt"

    def test_type_fallback(self):
        assert get_build_engine({"type": "spark"}) == "spark"

    def test_missing(self):
        assert get_build_engine({}) is None


class TestGetContractVersion:
    def test_present(self):
        assert get_contract_version({"fluidVersion": "0.5.7"}) == "0.5.7"

    def test_missing(self):
        assert get_contract_version({}) is None


class TestNormalizeExpose:
    def test_id_to_exposeId(self):
        result = normalize_expose({"id": "old_id", "type": "table"})
        assert result["exposeId"] == "old_id"
        assert "id" not in result

    def test_type_to_kind(self):
        result = normalize_expose({"type": "view"})
        assert result["kind"] == "view"
        assert "type" not in result

    def test_location_string_to_binding(self):
        result = normalize_expose({"location": "/data/path"})
        assert result["binding"] == {"location": "/data/path"}
        assert "location" not in result

    def test_already_normalized(self):
        expose = {"exposeId": "x", "kind": "table", "binding": {"location": "/x"}}
        result = normalize_expose(expose)
        assert result["exposeId"] == "x"
        assert result["kind"] == "table"


class TestNormalizeContract:
    def test_build_to_builds(self):
        result = normalize_contract({"build": {"engine": "dbt"}})
        assert result["builds"] == [{"engine": "dbt"}]
        assert "build" not in result

    def test_exposes_normalized(self):
        result = normalize_contract({
            "exposes": [{"id": "x", "type": "table"}],
        })
        assert result["exposes"][0]["exposeId"] == "x"
        assert result["exposes"][0]["kind"] == "table"

    def test_already_normalized(self):
        c = {"builds": [{"engine": "dbt"}], "exposes": [{"exposeId": "x"}]}
        result = normalize_contract(c)
        assert result["builds"] == [{"engine": "dbt"}]


# ═══════════════════════════════════════════════════════════════════════
# util/io.py
# ═══════════════════════════════════════════════════════════════════════

class TestLoadContract:
    def test_yaml_file(self, tmp_path):
        f = tmp_path / "contract.yaml"
        f.write_text("id: test\nname: Test Product\n")
        result = load_contract(str(f))
        assert result["id"] == "test"
        assert result["name"] == "Test Product"

    def test_json_file(self, tmp_path):
        f = tmp_path / "contract.json"
        f.write_text(json.dumps({"id": "test", "version": "1.0"}))
        result = load_contract(str(f))
        assert result["id"] == "test"

    def test_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            load_contract("/nonexistent/contract.yaml")


class TestDumpAndReadJson:
    def test_round_trip(self, tmp_path):
        path = str(tmp_path / "output.json")
        data = {"key": "value", "num": 42}
        dump_json(path, data)
        result = read_json(path)
        assert result == data

    def test_auto_mkdir(self, tmp_path):
        path = str(tmp_path / "sub" / "dir" / "output.json")
        dump_json(path, {"x": 1})
        assert read_json(path) == {"x": 1}


# ═══════════════════════════════════════════════════════════════════════
# util/cron.py
# ═══════════════════════════════════════════════════════════════════════

class TestGetCron:
    def test_with_cron(self):
        contract = {
            "build": {
                "execution": {
                    "trigger": {"cron": "0 * * * *"}
                }
            }
        }
        assert get_cron(contract) == "0 * * * *"

    def test_builds_array(self):
        contract = {
            "builds": [{
                "execution": {
                    "trigger": {"cron": "30 6 * * *"}
                }
            }]
        }
        assert get_cron(contract) == "30 6 * * *"

    def test_no_cron(self):
        assert get_cron({"build": {}}) is None

    def test_empty_contract(self):
        assert get_cron({}) is None

    def test_no_execution(self):
        assert get_cron({"build": {"engine": "dbt"}}) is None
