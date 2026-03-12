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

"""Tests for util/contract.py — version-agnostic field adapters and normalization."""

from fluid_build.util.contract import (
    get_build_engine,
    get_builds,
    get_contract_version,
    get_expose_binding,
    get_expose_contract,
    get_expose_format,
    get_expose_id,
    get_expose_kind,
    get_expose_location,
    get_expose_schema,
    get_primary_build,
    normalize_contract,
    normalize_expose,
)


# ── get_expose_id ───────────────────────────────────────────────────
class TestGetExposeId:
    def test_expose_id_057(self):
        assert get_expose_id({"exposeId": "e1"}) == "e1"

    def test_expose_id_040(self):
        assert get_expose_id({"id": "e2"}) == "e2"

    def test_prefers_expose_id(self):
        assert get_expose_id({"exposeId": "new", "id": "old"}) == "new"

    def test_empty(self):
        assert get_expose_id({}) is None


# ── get_expose_kind ─────────────────────────────────────────────────
class TestGetExposeKind:
    def test_kind_057(self):
        assert get_expose_kind({"kind": "table"}) == "table"

    def test_type_040(self):
        assert get_expose_kind({"type": "view"}) == "view"

    def test_prefers_kind(self):
        assert get_expose_kind({"kind": "table", "type": "view"}) == "table"

    def test_empty(self):
        assert get_expose_kind({}) is None


# ── get_expose_binding ──────────────────────────────────────────────
class TestGetExposeBinding:
    def test_binding_dict(self):
        b = get_expose_binding({"binding": {"provider": "aws"}})
        assert b == {"provider": "aws"}

    def test_location_string_fallback(self):
        b = get_expose_binding({"location": "s3://bucket/path"})
        assert b == {"location": "s3://bucket/path"}

    def test_empty(self):
        assert get_expose_binding({}) is None

    def test_location_non_string_ignored(self):
        assert get_expose_binding({"location": 42}) is None


# ── get_expose_location ─────────────────────────────────────────────
class TestGetExposeLocation:
    def test_from_binding(self):
        loc = get_expose_location({"binding": {"location": "s3://b"}})
        assert loc == "s3://b"

    def test_from_direct_location(self):
        loc = get_expose_location({"location": "gs://b"})
        assert loc == "gs://b"

    def test_empty(self):
        assert get_expose_location({}) is None


# ── get_builds ──────────────────────────────────────────────────────
class TestGetBuilds:
    def test_builds_array(self):
        assert get_builds({"builds": [{"engine": "dbt"}]}) == [{"engine": "dbt"}]

    def test_single_build_wrapped(self):
        assert get_builds({"build": {"engine": "spark"}}) == [{"engine": "spark"}]

    def test_empty(self):
        assert get_builds({}) == []

    def test_builds_non_list_ignored(self):
        assert get_builds({"builds": "invalid"}) == []


# ── get_primary_build ───────────────────────────────────────────────
class TestGetPrimaryBuild:
    def test_returns_first(self):
        assert get_primary_build({"builds": [{"engine": "a"}, {"engine": "b"}]}) == {"engine": "a"}

    def test_none_when_empty(self):
        assert get_primary_build({}) is None


# ── get_build_engine ────────────────────────────────────────────────
class TestGetBuildEngine:
    def test_engine(self):
        assert get_build_engine({"engine": "dbt"}) == "dbt"

    def test_type_fallback(self):
        assert get_build_engine({"type": "spark"}) == "spark"

    def test_empty(self):
        assert get_build_engine({}) is None


# ── simple getters ──────────────────────────────────────────────────
class TestSimpleGetters:
    def test_contract_version(self):
        assert get_contract_version({"fluidVersion": "0.5.7"}) == "0.5.7"

    def test_contract_version_empty(self):
        assert get_contract_version({}) is None

    def test_expose_schema(self):
        assert get_expose_schema({"schema": {"fields": []}}) == {"fields": []}

    def test_expose_contract(self):
        assert get_expose_contract({"contract": {"dq": []}}) == {"dq": []}

    def test_expose_format(self):
        assert get_expose_format({"format": "parquet"}) == "parquet"


# ── normalize_expose ────────────────────────────────────────────────
class TestNormalizeExpose:
    def test_id_to_expose_id(self):
        n = normalize_expose({"id": "e1"})
        assert n["exposeId"] == "e1"
        assert "id" not in n

    def test_type_to_kind(self):
        n = normalize_expose({"type": "view"})
        assert n["kind"] == "view"
        assert "type" not in n

    def test_location_to_binding(self):
        n = normalize_expose({"location": "s3://b"})
        assert n["binding"] == {"location": "s3://b"}
        assert "location" not in n

    def test_already_normalized(self):
        n = normalize_expose({"exposeId": "e1", "kind": "table", "binding": {"x": 1}})
        assert n["exposeId"] == "e1"
        assert n["kind"] == "table"
        assert n["binding"] == {"x": 1}


# ── normalize_contract ──────────────────────────────────────────────
class TestNormalizeContract:
    def test_build_to_builds(self):
        n = normalize_contract({"build": {"engine": "dbt"}})
        assert n["builds"] == [{"engine": "dbt"}]
        assert "build" not in n

    def test_exposes_normalized(self):
        n = normalize_contract({"exposes": [{"id": "e1", "type": "table"}]})
        assert n["exposes"][0]["exposeId"] == "e1"
        assert n["exposes"][0]["kind"] == "table"

    def test_already_normalized(self):
        n = normalize_contract({"builds": [{"engine": "dbt"}]})
        assert n["builds"] == [{"engine": "dbt"}]
