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

"""Extended tests for ContractValidator in cli/contract_validation.py.

Tests _validate_single_expose, _validate_exposes, _validate_binding (deeper),
_validate_schema_definition (deeper) via a stubbed validator.
"""

import logging
from pathlib import Path
from unittest.mock import patch

from fluid_build.cli.contract_validation import ContractValidator, ValidationReport


def _make_validator(contract_dict=None):
    """Create a ContractValidator with minimal mocking to avoid file I/O."""
    with patch.object(ContractValidator, "__init__", lambda self, *a, **kw: None):
        v = ContractValidator.__new__(ContractValidator)
    v.contract_path = Path("/fake/contract.yaml")
    v.contract = contract_dict or {}
    v.env = "dev"
    v.provider_name = None
    v.project = None
    v.region = None
    v.server = None
    v.check_data = False
    v.use_cache = False
    v.cache = None
    v.track_history = False
    v.check_drift = False
    v.history = None
    v.validation_provider = None
    v.logger = logging.getLogger("test")
    v.report = ValidationReport(
        contract_path="/fake/contract.yaml",
        contract_id="test",
        contract_version="1.0.0",
        validation_time=__import__("datetime").datetime.now(),
        duration=0.0,
    )
    return v


class TestValidateSingleExpose:
    def test_all_required_fields_present(self):
        v = _make_validator()
        expose = {
            "id": "out",
            "type": "table",
            "binding": {"platform": "local", "location": {"format": "csv", "properties": {}}},
            "schema": [{"name": "x", "type": "STRING"}],
        }
        v._validate_single_expose(expose, 0)
        errors = [i for i in v.report.issues if i.severity == "error"]
        assert len(errors) == 0

    def test_missing_required_fields(self):
        v = _make_validator()
        v._validate_single_expose({}, 0)
        errors = [i for i in v.report.issues if i.severity == "error"]
        # Should flag missing id, type, binding, schema
        assert len(errors) >= 4

    def test_missing_id_only(self):
        v = _make_validator()
        expose = {"type": "table", "binding": {"platform": "local"}, "schema": []}
        v._validate_single_expose(expose, 0)
        error_msgs = [i.message for i in v.report.issues if i.severity == "error"]
        assert any("id" in m for m in error_msgs)

    def test_binding_validation_called(self):
        v = _make_validator()
        expose = {
            "id": "out",
            "type": "table",
            "binding": {"platform": "local", "location": {"format": "parquet", "properties": {}}},
            "schema": [],
        }
        v._validate_single_expose(expose, 0)
        # Binding is valid so no binding errors
        binding_errors = [
            i for i in v.report.issues if i.category == "binding" and i.severity == "error"
        ]
        assert len(binding_errors) == 0


class TestValidateExposes:
    def test_no_exposes(self):
        v = _make_validator({"exposes": []})
        v._validate_exposes()
        warnings = [i for i in v.report.issues if i.severity == "warning"]
        assert any("No data products" in w.message for w in warnings)

    def test_multiple_exposes(self):
        v = _make_validator(
            {
                "exposes": [
                    {"id": "a", "type": "table", "binding": {"platform": "local"}, "schema": []},
                    {"id": "b", "type": "view", "binding": {"platform": "local"}, "schema": []},
                ]
            }
        )
        v._validate_exposes()
        assert v.report.exposes_validated == 2


class TestValidateBindingDeeper:
    def test_missing_platform(self):
        v = _make_validator()
        v._validate_binding({}, "exposes[0].binding", "test")
        errors = [i for i in v.report.issues if i.severity == "error"]
        assert any("platform" in e.message for e in errors)

    def test_missing_location(self):
        v = _make_validator()
        v._validate_binding({"platform": "local"}, "exposes[0].binding", "test")
        errors = [i for i in v.report.issues if i.severity == "error"]
        assert any("location" in e.message for e in errors)

    def test_location_missing_format(self):
        v = _make_validator()
        v._validate_binding(
            {"platform": "local", "location": {"properties": {}}}, "exposes[0].binding", "test"
        )
        warnings = [i for i in v.report.issues if i.severity == "warning"]
        assert any("format" in w.message for w in warnings)

    def test_location_missing_properties(self):
        v = _make_validator()
        v._validate_binding(
            {"platform": "local", "location": {"format": "parquet"}}, "exposes[0].binding", "test"
        )
        warnings = [i for i in v.report.issues if i.severity == "warning"]
        assert any("properties" in w.message for w in warnings)

    def test_gcp_missing_required_props(self):
        v = _make_validator()
        v.provider_name = "gcp"
        v._validate_binding(
            {"platform": "gcp", "location": {"format": "parquet", "properties": {"project": "p"}}},
            "path",
            "test",
        )
        errors = [i for i in v.report.issues if i.severity == "error"]
        # Missing dataset and table
        assert any("dataset" in e.message for e in errors)
        assert any("table" in e.message for e in errors)

    def test_gcp_all_props_present(self):
        v = _make_validator()
        v.provider_name = "gcp"
        v._validate_binding(
            {
                "platform": "gcp",
                "location": {
                    "format": "parquet",
                    "properties": {"project": "p", "dataset": "d", "table": "t"},
                },
            },
            "path",
            "test",
        )
        errors = [i for i in v.report.issues if i.severity == "error"]
        assert len(errors) == 0

    def test_valid_binding(self):
        v = _make_validator()
        v._validate_binding(
            {
                "platform": "local",
                "location": {"format": "parquet", "properties": {"path": "/data"}},
            },
            "path",
            "test",
        )
        assert len(v.report.issues) == 0


class TestValidateSchemaDefinitionDeeper:
    def test_empty_schema(self):
        v = _make_validator()
        v._validate_schema_definition([], "path", "test")
        warnings = [i for i in v.report.issues if i.severity == "warning"]
        assert any("Empty schema" in w.message for w in warnings)

    def test_valid_columns(self):
        v = _make_validator()
        schema = [
            {"name": "id", "type": "INTEGER"},
            {"name": "value", "type": "FLOAT"},
            {"name": "label", "type": "STRING"},
        ]
        v._validate_schema_definition(schema, "path", "test")
        assert len(v.report.issues) == 0

    def test_missing_name(self):
        v = _make_validator()
        v._validate_schema_definition([{"type": "STRING"}], "path", "test")
        errors = [i for i in v.report.issues if i.severity == "error"]
        assert any("name" in e.message for e in errors)

    def test_missing_type(self):
        v = _make_validator()
        v._validate_schema_definition([{"name": "col"}], "path", "test")
        errors = [i for i in v.report.issues if i.severity == "error"]
        assert any("type" in e.message for e in errors)

    def test_nonstandard_type(self):
        v = _make_validator()
        v._validate_schema_definition([{"name": "col", "type": "SUPERTYPE"}], "path", "test")
        warnings = [i for i in v.report.issues if i.severity == "warning"]
        assert any("Non-standard" in w.message for w in warnings)

    def test_all_standard_types(self):
        """Verify all standard types pass without warnings."""
        v = _make_validator()
        standard = [
            "VARCHAR",
            "STRING",
            "TEXT",
            "INT",
            "INTEGER",
            "BIGINT",
            "FLOAT",
            "DOUBLE",
            "DECIMAL",
            "BOOL",
            "BOOLEAN",
            "DATE",
            "DATETIME",
            "TIMESTAMP",
            "JSON",
            "BYTES",
        ]
        schema = [{"name": f"c{i}", "type": t} for i, t in enumerate(standard)]
        v._validate_schema_definition(schema, "path", "test")
        warnings = [i for i in v.report.issues if i.severity == "warning"]
        assert len(warnings) == 0
