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

"""Pin the 0.7.3 schema acceptance criteria.

Schema is additive vs 0.7.2: every existing 0.7.2 contract still
loads under 0.7.2, and the new 0.7.3 fields (``expose.openapi``,
``expose.mcp``, ``$source`` pointers) validate cleanly.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft7Validator

REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMAS_DIR = REPO_ROOT / "fluid_build" / "schemas"
FIXTURES_DIR = REPO_ROOT / "tests" / "fixtures" / "contracts" / "0.7.3"


@pytest.fixture(scope="module")
def schema_0_7_3() -> dict:
    return json.loads((SCHEMAS_DIR / "fluid-schema-0.7.3.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def schema_0_7_2() -> dict:
    return json.loads((SCHEMAS_DIR / "fluid-schema-0.7.2.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def validator(schema_0_7_3) -> Draft7Validator:
    return Draft7Validator(schema_0_7_3)


def _load(name: str) -> dict:
    return yaml.safe_load((FIXTURES_DIR / name).read_text(encoding="utf-8"))


def test_0_7_3_schema_file_exists(schema_0_7_3):
    assert schema_0_7_3.get("title", "").startswith("FLUID 0.7.3"), schema_0_7_3.get("title")


def test_inline_openapi_dict_validates(validator):
    contract = _load("inline-openapi-dict.yaml")
    errors = list(validator.iter_errors(contract))
    assert not errors, _format_errors(errors)


def test_inline_openapi_string_validates(validator):
    contract = _load("inline-openapi-string.yaml")
    errors = list(validator.iter_errors(contract))
    assert not errors, _format_errors(errors)


def test_inline_openapi_and_openapiRef_conflict_rejected(validator):
    contract = _load("conflict-openapi-and-openapiRef.yaml")
    errors = list(validator.iter_errors(contract))
    assert errors, "expected schema to reject expose with both openapi and contract.openapiRef"


def test_source_sentinel_in_sql_validates(validator):
    contract = _load("source-sentinel-in-sql.yaml")
    errors = list(validator.iter_errors(contract))
    assert not errors, _format_errors(errors)


def test_source_sentinel_malformed_path_rejected(validator):
    contract = _load("source-sentinel-malformed-path.yaml")
    errors = list(validator.iter_errors(contract))
    assert errors, "expected schema to reject $source path outside sources/{sql,openapi,policy}/"


def test_expose_mcp_overrides_validates(validator):
    contract = _load("expose-mcp-overrides.yaml")
    errors = list(validator.iter_errors(contract))
    assert not errors, _format_errors(errors)


def test_existing_0_7_2_contract_still_validates_under_0_7_2(schema_0_7_2):
    """Backward compat: a 0.7.2 contract still loads under 0.7.2."""
    contract = {
        "fluidVersion": "0.7.2",
        "kind": "DataProduct",
        "id": "gold.test.legacy_v1",
        "name": "Legacy 0.7.2",
        "metadata": {
            "layer": "Gold",
            "owner": {"team": "qa", "email": "qa@example.com"},
        },
        "exposes": [
            {
                "exposeId": "test_table",
                "kind": "table",
                "contract": {
                    "schema": [{"name": "id", "type": "STRING"}],
                },
                "binding": {
                    "platform": "local",
                    "format": "parquet",
                    "location": {"path": "/tmp/test.parquet"},
                },
            }
        ],
    }
    Draft7Validator(schema_0_7_2).validate(contract)


def test_0_7_3_strictly_extends_0_7_2_for_legacy_contracts(validator):
    """Same legacy contract bumped to 0.7.3 still validates."""
    contract = {
        "fluidVersion": "0.7.3",
        "kind": "DataProduct",
        "id": "gold.test.upgrade_v1",
        "name": "Upgraded 0.7.3",
        "metadata": {
            "layer": "Gold",
            "owner": {"team": "qa", "email": "qa@example.com"},
        },
        "exposes": [
            {
                "exposeId": "test_table",
                "kind": "table",
                "contract": {
                    "schema": [{"name": "id", "type": "STRING"}],
                },
                "binding": {
                    "platform": "local",
                    "format": "parquet",
                    "location": {"path": "/tmp/test.parquet"},
                },
            }
        ],
    }
    errors = list(validator.iter_errors(contract))
    assert not errors, _format_errors(errors)


def test_source_ref_definition_present(schema_0_7_3):
    assert "SourceRef" in schema_0_7_3["$defs"]
    pattern = schema_0_7_3["$defs"]["SourceRef"]["properties"]["$source"]["pattern"]
    assert pattern == "^sources/(sql|openapi|policy)/.+"


def test_mcp_output_port_definition_present(schema_0_7_3):
    assert "mcpOutputPort" in schema_0_7_3["$defs"]
    properties = schema_0_7_3["$defs"]["mcpOutputPort"]["properties"]
    for required_key in ("toolNames", "sampling", "classification", "allowFreeFormSql", "auth"):
        assert required_key in properties, f"mcpOutputPort missing {required_key}"


def test_expose_openapi_property_advertised(schema_0_7_3):
    expose_props = schema_0_7_3["$defs"]["expose"]["properties"]
    assert "openapi" in expose_props
    assert "mcp" in expose_props


def test_expose_conflict_uses_if_then(schema_0_7_3):
    """Make sure the conflict guard is the documented form, not the
    accidental no-op (`not` on a property that doesn't exist at this
    level)."""
    expose = schema_0_7_3["$defs"]["expose"]
    assert "allOf" in expose, "expose should carry the if/then conflict guard"
    guards = expose["allOf"]
    assert any("if" in entry and "then" in entry for entry in guards)


def test_schema_manager_lists_0_7_3():
    from fluid_build.schema_manager import _discover_bundled_versions

    versions = _discover_bundled_versions()
    assert "0.7.3" in versions
    assert "0.7.2" in versions  # legacy still listed


def test_inline_openapi_string_round_trips_with_yaml_loader():
    """The inline string form must survive YAML parsing as a plain
    string (not auto-deserialised into an OpenAPI object) so the
    bundle extractor can write it verbatim to sources/openapi/<file>."""
    contract = _load("inline-openapi-string.yaml")
    expose = contract["exposes"][0]
    assert isinstance(expose["openapi"], str), "inline openapi string was auto-deserialised"
    assert "openapi: \"3.1.0\"" in expose["openapi"]


def _format_errors(errors) -> str:
    return "\n".join(f"- {error.message} (path={list(error.path)})" for error in errors)
