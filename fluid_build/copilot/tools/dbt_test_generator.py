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

"""Heuristic dbt schema.yml test generator.

Pure-Python, no LLM calls. Mirrors the canonical dbt schema-test
shape (built-in ``unique`` / ``not_null`` / ``accepted_values`` /
``relationships``) plus the calogica/dbt-expectations
``expect_column_values_to_be_between`` test for numeric ranges.

Prior art surveyed:
* dbt-labs/dbt-core schema-test conventions
  (https://docs.getdbt.com/reference/resource-properties/data-tests)
* calogica/dbt-expectations
  (https://github.com/calogica/dbt-expectations) — exact test name
  ``dbt_expectations.expect_column_values_to_be_between`` (underscore
  prefix, not hyphen) with ``min_value`` / ``max_value`` arguments.

The BuilderAgent should call :func:`generate_dbt_tests` after it
has resolved column-level metadata for a model; the returned dict
serialises directly to a valid dbt ``schema.yml``.
"""

from __future__ import annotations

import logging
from typing import Any

# The single shared contract → dbt-test mapping. Reached via module attribute
# access (``_tm.<fn>``) so a ``patch("...engines.dbt._test_mapping.<fn>")``
# flows through to every call site. This module owns the ``relationships`` and
# ``dbt_expectations`` range emitters — both now live in the shared module so
# the engine + exporter paths reuse the identical shape.
from ...engines.dbt import _test_mapping as _tm

_LOG = logging.getLogger(__name__)


# Numeric-type recognition lives in the shared mapping module
# (_test_mapping.is_numeric_type) so all three generator surfaces agree.
# The old local prefix table mis-matched BIGINT/SMALLINT/TINYINT
# ("BIGINT".startswith("INT") is False) and silently dropped their range tests.
def _is_numeric(sql_type: str | None) -> bool:
    return _tm.is_numeric_type(sql_type)


def _column_tests(col: dict[str, Any]) -> list[Any]:
    """Build the dbt ``tests:`` list for a single column.

    Heuristics (applied in deterministic order):

    1. ``primary_key`` true  → ``unique`` + ``not_null``
    2. ``nullable`` false (and not PK) → ``not_null``
    3. ``foreign_key`` set → ``relationships``
    4. ``enum`` set → ``accepted_values``
    5. ``min`` / ``max`` (or ``minimum`` / ``maximum``) set on a numeric —
       or untyped — column →
       ``dbt_expectations.expect_column_values_to_be_between``
    """
    tests: list[Any] = []

    is_pk = bool(col.get("primary_key"))
    if is_pk:
        tests.append(_tm.unique_test())
        tests.append(_tm.not_null_test())
    else:
        # nullable defaults to True if unspecified — only emit not_null
        # when the schema explicitly says the column is non-nullable.
        nullable = col.get("nullable", True)
        if nullable is False:
            tests.append(_tm.not_null_test())

    rel = _tm.column_relationship(col)
    if rel is not None:
        tests.append(_tm.relationships_test(*rel))

    enum = col.get("enum")
    if isinstance(enum, list) and enum:
        tests.append(_tm.accepted_values_test(enum))

    col_type = col.get("type")
    bounds = _tm.column_range(col)  # accepts min/max AND minimum/maximum
    if bounds:
        if col_type is None or _tm.is_numeric_type(col_type):
            range_test = _tm.numeric_range_test(**bounds)
            if range_test is not None:
                tests.append(range_test)
        else:
            # Don't silently emit on string types — surface a WARN so the
            # BuilderAgent / Validator can decide whether to coerce the
            # column type or drop the min/max.
            _LOG.warning(
                "skipping range test on non-numeric column %r (type=%r)",
                col.get("name"),
                col_type,
            )

    return tests


def generate_dbt_tests(
    schema: dict[str, Any],
    *,
    dialect: str = "snowflake",
) -> dict[str, Any]:
    """Generate a dbt schema.yml-compatible dict from a column schema.

    Parameters
    ----------
    schema
        Lenient column-schema dict. Expected keys: ``model_name``,
        ``columns`` (list of column dicts). Missing keys yield a
        minimally-populated output rather than raising — the caller
        is the agent loop and should not crash on incomplete input.
    dialect
        Reserved for future per-warehouse divergences (e.g. type
        normalisation). Currently informational only.

    Returns
    -------
    dict
        Shape::

            {
              "version": 2,
              "models": [{
                "name": <model_name>,
                "columns": [
                  {"name": <col>, "description": <desc>,
                   "tests": [<test>, ...]},
                  ...
                ],
              }],
            }
    """
    # ``dialect`` is accepted but not yet used — kept on the signature
    # so callers don't break when we later add per-warehouse rules.
    del dialect

    model_name = str(schema.get("model_name") or "unnamed_model")
    columns_in = schema.get("columns") or []
    if not isinstance(columns_in, list):
        _LOG.warning("schema.columns is not a list — coercing to empty")
        columns_in = []

    columns_out: list[dict[str, Any]] = []
    for raw in columns_in:
        if not isinstance(raw, dict):
            continue
        name = raw.get("name")
        if not name:
            continue
        col_dict: dict[str, Any] = {"name": str(name)}
        desc = raw.get("description")
        if desc:
            col_dict["description"] = str(desc)
        tests = _column_tests(raw)
        if tests:
            col_dict["tests"] = tests
        columns_out.append(col_dict)

    return {
        "version": 2,
        "models": [
            {
                "name": model_name,
                "columns": columns_out,
            }
        ],
    }


__all__ = ["generate_dbt_tests"]
