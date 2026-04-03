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

"""Tests for fluid_build.blueprints.validators."""

from pathlib import Path
from unittest.mock import MagicMock, PropertyMock

import pytest
import yaml

from fluid_build.blueprints.validators import BlueprintValidator


def _make_blueprint(tmp_path, **overrides):
    """Create a mock Blueprint object with sensible defaults."""
    bp = MagicMock()
    bp.contract_path = tmp_path / "contract.yaml"
    bp.dbt_path = tmp_path / "dbt_project"
    bp.tests_path = tmp_path / "tests"
    bp.sample_data_path = tmp_path / "sample_data"
    bp.docs_path = tmp_path / "docs"
    bp.validate.return_value = []

    metadata = MagicMock()
    metadata.runtimes = overrides.get("runtimes", [])
    metadata.has_tests = overrides.get("has_tests", False)
    metadata.has_sample_data = overrides.get("has_sample_data", False)
    metadata.has_docs = overrides.get("has_docs", False)
    bp.metadata = metadata

    return bp


class TestValidateStructure:
    def test_delegates_to_blueprint(self, tmp_path):
        bp = _make_blueprint(tmp_path)
        bp.validate.return_value = ["error1"]
        v = BlueprintValidator(bp)
        assert v.validate_structure() == ["error1"]


class TestValidateContract:
    def test_missing_contract_file(self, tmp_path):
        bp = _make_blueprint(tmp_path)
        v = BlueprintValidator(bp)
        errors = v.validate_contract()
        assert "Contract file missing" in errors

    def test_valid_contract(self, tmp_path):
        bp = _make_blueprint(tmp_path)
        contract = {
            "version": "1.0",
            "metadata": {"name": "test"},
            "products": {
                "my_product": {"metadata": {"owner": "me"}, "schema": {"columns": []}}
            },
        }
        bp.contract_path.write_text(yaml.dump(contract))
        v = BlueprintValidator(bp)
        errors = v.validate_contract()
        assert errors == []

    def test_missing_required_fields(self, tmp_path):
        bp = _make_blueprint(tmp_path)
        bp.contract_path.write_text(yaml.dump({"version": "1.0"}))
        v = BlueprintValidator(bp)
        errors = v.validate_contract()
        assert any("metadata" in e for e in errors)
        assert any("products" in e for e in errors)

    def test_product_missing_metadata(self, tmp_path):
        bp = _make_blueprint(tmp_path)
        contract = {
            "version": "1.0",
            "metadata": {},
            "products": {"p1": {"schema": {}}},
        }
        bp.contract_path.write_text(yaml.dump(contract))
        v = BlueprintValidator(bp)
        errors = v.validate_contract()
        assert any("p1" in e and "metadata" in e for e in errors)

    def test_product_missing_schema(self, tmp_path):
        bp = _make_blueprint(tmp_path)
        contract = {
            "version": "1.0",
            "metadata": {},
            "products": {"p1": {"metadata": {}}},
        }
        bp.contract_path.write_text(yaml.dump(contract))
        v = BlueprintValidator(bp)
        errors = v.validate_contract()
        assert any("p1" in e and "schema" in e for e in errors)

    def test_invalid_yaml(self, tmp_path):
        bp = _make_blueprint(tmp_path)
        bp.contract_path.write_text("{{invalid yaml")
        v = BlueprintValidator(bp)
        errors = v.validate_contract()
        assert any("syntax error" in e.lower() or "validation error" in e.lower() for e in errors)


class TestValidateDbtProject:
    def test_skips_if_no_dbt_runtime(self, tmp_path):
        bp = _make_blueprint(tmp_path, runtimes=[])
        v = BlueprintValidator(bp)
        assert v.validate_dbt_project() == []

    def test_missing_dbt_directory(self, tmp_path):
        bp = _make_blueprint(tmp_path, runtimes=["dbt"])
        v = BlueprintValidator(bp)
        errors = v.validate_dbt_project()
        assert "dbt_project directory missing" in errors

    def test_missing_dbt_project_yml(self, tmp_path):
        bp = _make_blueprint(tmp_path, runtimes=["dbt"])
        bp.dbt_path.mkdir()
        v = BlueprintValidator(bp)
        errors = v.validate_dbt_project()
        assert "dbt_project.yml missing" in errors

    def test_valid_dbt_project(self, tmp_path):
        bp = _make_blueprint(tmp_path, runtimes=["dbt"])
        bp.dbt_path.mkdir()
        (bp.dbt_path / "dbt_project.yml").write_text(
            yaml.dump({"name": "test", "version": "1.0", "profile": "default"})
        )
        (bp.dbt_path / "models").mkdir()
        v = BlueprintValidator(bp)
        assert v.validate_dbt_project() == []

    def test_dbt_project_missing_fields(self, tmp_path):
        bp = _make_blueprint(tmp_path, runtimes=["dbt"])
        bp.dbt_path.mkdir()
        (bp.dbt_path / "dbt_project.yml").write_text(yaml.dump({"name": "test"}))
        (bp.dbt_path / "models").mkdir()
        v = BlueprintValidator(bp)
        errors = v.validate_dbt_project()
        assert any("version" in e for e in errors)
        assert any("profile" in e for e in errors)


class TestValidateTests:
    def test_skips_if_no_tests_flag(self, tmp_path):
        bp = _make_blueprint(tmp_path, has_tests=False)
        v = BlueprintValidator(bp)
        assert v.validate_tests() == []

    def test_missing_tests_directory(self, tmp_path):
        bp = _make_blueprint(tmp_path, has_tests=True)
        v = BlueprintValidator(bp)
        errors = v.validate_tests()
        assert "tests directory missing" in errors

    def test_empty_tests_directory(self, tmp_path):
        bp = _make_blueprint(tmp_path, has_tests=True)
        bp.tests_path.mkdir()
        v = BlueprintValidator(bp)
        errors = v.validate_tests()
        assert any("No test files" in e for e in errors)

    def test_tests_present(self, tmp_path):
        bp = _make_blueprint(tmp_path, has_tests=True)
        bp.tests_path.mkdir()
        (bp.tests_path / "test_something.py").write_text("pass")
        v = BlueprintValidator(bp)
        assert v.validate_tests() == []


class TestValidateSampleData:
    def test_skips_if_no_sample_data_flag(self, tmp_path):
        bp = _make_blueprint(tmp_path, has_sample_data=False)
        v = BlueprintValidator(bp)
        assert v.validate_sample_data() == []

    def test_missing_sample_data_directory(self, tmp_path):
        bp = _make_blueprint(tmp_path, has_sample_data=True)
        v = BlueprintValidator(bp)
        errors = v.validate_sample_data()
        assert "sample_data directory missing" in errors


class TestValidateDocumentation:
    def test_skips_if_no_docs_flag(self, tmp_path):
        bp = _make_blueprint(tmp_path, has_docs=False)
        v = BlueprintValidator(bp)
        assert v.validate_documentation() == []

    def test_missing_docs_directory(self, tmp_path):
        bp = _make_blueprint(tmp_path, has_docs=True)
        v = BlueprintValidator(bp)
        errors = v.validate_documentation()
        assert "docs directory missing" in errors

    def test_docs_without_readme(self, tmp_path):
        bp = _make_blueprint(tmp_path, has_docs=True)
        bp.docs_path.mkdir()
        v = BlueprintValidator(bp)
        errors = v.validate_documentation()
        assert any("README" in e for e in errors)

    def test_docs_with_readme(self, tmp_path):
        bp = _make_blueprint(tmp_path, has_docs=True)
        bp.docs_path.mkdir()
        (bp.docs_path / "README.md").write_text("# Docs")
        v = BlueprintValidator(bp)
        assert v.validate_documentation() == []


class TestValidateAll:
    def test_returns_all_categories(self, tmp_path):
        bp = _make_blueprint(tmp_path)
        v = BlueprintValidator(bp)
        result = v.validate_all()
        assert set(result.keys()) == {
            "structure",
            "contract",
            "dbt_project",
            "tests",
            "sample_data",
            "documentation",
        }
