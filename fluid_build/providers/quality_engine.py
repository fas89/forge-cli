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

"""
Data quality check engine for live data validation.

Executes DQ rules declared in ``contract.dq.rules`` against actual data
via provider-specific SQL engines.  Supports five rule types:

- **completeness** — fraction of non-null values for a column
- **uniqueness**   — fraction of distinct values for a column
- **accuracy**     — comparison of column values against a threshold
- **validity**     — column values must be within an allowed set
- **freshness**    — maximum age of the most recent timestamp in a column
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from fluid_build.providers.validation_provider import ValidationIssue

LOG = logging.getLogger("fluid.providers.quality_engine")

# Safe SQL identifier regex (letters, digits, underscores only)
_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_ident(name: str) -> str:
    if not _SAFE_IDENT.match(name):
        raise ValueError("Invalid SQL identifier: {!r}".format(name))
    return name


@dataclass
class QualityCheckResult:
    """Result of a single data quality check."""
    rule_id: str
    rule_type: str
    selector: str
    passed: bool
    severity: str  # error | warning | info
    message: str
    expected: Optional[Any] = None
    actual: Optional[Any] = None


# ------------------------------------------------------------------
# SQL generators per rule type
# ------------------------------------------------------------------

def _completeness_sql(table_ref: str, column: str) -> str:
    """Generate SQL to compute non-null fraction for a column."""
    col = _validate_ident(column)
    return (
        "SELECT "
        "CAST(SUM(CASE WHEN \"{col}\" IS NOT NULL THEN 1 ELSE 0 END) AS DOUBLE PRECISION) "
        "/ NULLIF(COUNT(*), 0) AS completeness_ratio "
        "FROM {tbl}"
    ).format(col=col, tbl=table_ref)


def _uniqueness_sql(table_ref: str, column: str) -> str:
    """Generate SQL to compute distinct-value fraction for a column."""
    col = _validate_ident(column)
    return (
        "SELECT "
        "CAST(COUNT(DISTINCT \"{col}\") AS DOUBLE PRECISION) "
        "/ NULLIF(COUNT(\"{col}\"), 0) AS uniqueness_ratio "
        "FROM {tbl}"
    ).format(col=col, tbl=table_ref)


def _accuracy_min_sql(table_ref: str, column: str) -> str:
    """Generate SQL to get the minimum value of a numeric column."""
    col = _validate_ident(column)
    return 'SELECT MIN("{col}") AS min_val FROM {tbl}'.format(col=col, tbl=table_ref)


def _freshness_sql(table_ref: str, column: str, dialect: str = "ansi") -> str:
    """Generate SQL to get age of most-recent timestamp value (in seconds)."""
    col = _validate_ident(column)
    if dialect == "snowflake":
        return (
            "SELECT DATEDIFF('second', MAX(\"{col}\"), CURRENT_TIMESTAMP()) AS age_seconds "
            "FROM {tbl}"
        ).format(col=col, tbl=table_ref)
    elif dialect == "bigquery":
        return (
            "SELECT TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(`{col}`), SECOND) AS age_seconds "
            "FROM {tbl}"
        ).format(col=col, tbl=table_ref)
    else:
        # ANSI / DuckDB / Athena (Presto)
        return (
            "SELECT EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(\"{col}\"))) AS age_seconds "
            "FROM {tbl}"
        ).format(col=col, tbl=table_ref)


# ------------------------------------------------------------------
# Comparator
# ------------------------------------------------------------------

_OPERATORS = {
    ">=": lambda a, b: a >= b,
    ">":  lambda a, b: a > b,
    "<=": lambda a, b: a <= b,
    "<":  lambda a, b: a < b,
    "==": lambda a, b: a == b,
    "=":  lambda a, b: a == b,
    "!=": lambda a, b: a != b,
}


def _compare(actual, threshold, operator: str) -> bool:
    fn = _OPERATORS.get(operator)
    if fn is None:
        raise ValueError("Unsupported operator: {!r}".format(operator))
    try:
        return fn(float(actual), float(threshold))
    except (TypeError, ValueError):
        return False


# ------------------------------------------------------------------
# Freshness duration parser  (e.g. "1h", "30m", "7d", "3600s")
# ------------------------------------------------------------------

_DURATION_RE = re.compile(r"^(\d+)\s*([smhd])$", re.IGNORECASE)

_DURATION_MULTIPLIERS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def _parse_duration_seconds(value: str) -> Optional[int]:
    """Parse a human duration string like '1h' or '7d' into seconds."""
    m = _DURATION_RE.match(value.strip())
    if not m:
        return None
    return int(m.group(1)) * _DURATION_MULTIPLIERS[m.group(2).lower()]


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------

def execute_quality_checks(
    rules: List[Dict[str, Any]],
    table_ref: str,
    execute_fn,
    dialect: str = "ansi",
) -> List[QualityCheckResult]:
    """
    Execute a list of DQ rules against a live table.

    Parameters
    ----------
    rules : list[dict]
        DQ rules from ``contract.dq.rules``.
    table_ref : str
        Fully-qualified (and quoted if needed) table reference for SQL.
    execute_fn : callable
        ``execute_fn(sql) -> list[tuple]`` — runs a SQL statement and
        returns the result rows.
    dialect : str
        SQL dialect hint: ``"ansi"``, ``"snowflake"``, ``"bigquery"``.

    Returns
    -------
    list[QualityCheckResult]
    """
    results: List[QualityCheckResult] = []

    for rule in rules:
        rule_id = rule.get("id", "unnamed")
        rule_type = rule.get("type", "").lower()
        selector = rule.get("selector", "")
        severity = rule.get("severity", "warning")
        description = rule.get("description", "")
        threshold = rule.get("threshold")
        operator = rule.get("operator", ">=")

        if not selector:
            results.append(QualityCheckResult(
                rule_id=rule_id, rule_type=rule_type, selector="",
                passed=False, severity=severity,
                message="Rule '{}' missing 'selector' (column name)".format(rule_id),
            ))
            continue

        try:
            result = _execute_single_rule(
                rule_id=rule_id,
                rule_type=rule_type,
                selector=selector,
                severity=severity,
                description=description,
                threshold=threshold,
                operator=operator,
                valid_values=rule.get("validValues", []),
                window=rule.get("window", rule.get("freshness")),
                table_ref=table_ref,
                execute_fn=execute_fn,
                dialect=dialect,
            )
            results.append(result)
        except Exception as e:
            results.append(QualityCheckResult(
                rule_id=rule_id, rule_type=rule_type, selector=selector,
                passed=False, severity=severity,
                message="Error executing rule '{}': {}".format(rule_id, e),
            ))

    return results


def quality_results_to_issues(
    results: List[QualityCheckResult],
    path_prefix: str = "contract.dq.rules",
) -> List[ValidationIssue]:
    """Convert QualityCheckResults into ValidationIssues for the report."""
    issues: List[ValidationIssue] = []
    for r in results:
        if not r.passed:
            issues.append(ValidationIssue(
                severity=r.severity,
                category="quality",
                message=r.message,
                path="{}.{}".format(path_prefix, r.rule_id),
                expected=r.expected,
                actual=r.actual,
            ))
    return issues


# ------------------------------------------------------------------
# Internal dispatch
# ------------------------------------------------------------------

def _execute_single_rule(
    *,
    rule_id: str,
    rule_type: str,
    selector: str,
    severity: str,
    description: str,
    threshold,
    operator: str,
    valid_values: List[str],
    window: Optional[str],
    table_ref: str,
    execute_fn,
    dialect: str,
) -> QualityCheckResult:

    if rule_type == "completeness":
        return _check_completeness(
            rule_id, selector, severity, description,
            threshold, operator, table_ref, execute_fn,
        )
    elif rule_type == "uniqueness":
        return _check_uniqueness(
            rule_id, selector, severity, description,
            threshold, operator, table_ref, execute_fn,
        )
    elif rule_type == "accuracy":
        return _check_accuracy(
            rule_id, selector, severity, description,
            threshold, operator, table_ref, execute_fn,
        )
    elif rule_type == "validity":
        return _check_validity(
            rule_id, selector, severity, description,
            valid_values, table_ref, execute_fn,
        )
    elif rule_type == "freshness":
        return _check_freshness(
            rule_id, selector, severity, description,
            window, table_ref, execute_fn, dialect,
        )
    else:
        return QualityCheckResult(
            rule_id=rule_id, rule_type=rule_type, selector=selector,
            passed=False, severity="warning",
            message="Unknown DQ rule type '{}' for rule '{}'".format(rule_type, rule_id),
        )


# ------------------------------------------------------------------
# Individual check implementations
# ------------------------------------------------------------------

def _check_completeness(
    rule_id, selector, severity, description,
    threshold, operator, table_ref, execute_fn,
) -> QualityCheckResult:
    sql = _completeness_sql(table_ref, selector)
    rows = execute_fn(sql)
    ratio = rows[0][0] if rows and rows[0][0] is not None else 0.0
    passed = _compare(ratio, threshold, operator) if threshold is not None else ratio == 1.0
    return QualityCheckResult(
        rule_id=rule_id, rule_type="completeness", selector=selector,
        passed=passed, severity=severity,
        message=(
            "{desc} — completeness for '{col}' is {ratio:.2%}".format(
                desc=description or rule_id, col=selector, ratio=float(ratio))
            if not passed else
            "Completeness OK for '{}'".format(selector)
        ),
        expected="{op} {t}".format(op=operator, t=threshold) if threshold is not None else "1.0",
        actual="{:.4f}".format(float(ratio)),
    )


def _check_uniqueness(
    rule_id, selector, severity, description,
    threshold, operator, table_ref, execute_fn,
) -> QualityCheckResult:
    sql = _uniqueness_sql(table_ref, selector)
    rows = execute_fn(sql)
    ratio = rows[0][0] if rows and rows[0][0] is not None else 0.0
    passed = _compare(ratio, threshold, operator) if threshold is not None else ratio == 1.0
    return QualityCheckResult(
        rule_id=rule_id, rule_type="uniqueness", selector=selector,
        passed=passed, severity=severity,
        message=(
            "{desc} — uniqueness for '{col}' is {ratio:.2%}".format(
                desc=description or rule_id, col=selector, ratio=float(ratio))
            if not passed else
            "Uniqueness OK for '{}'".format(selector)
        ),
        expected="{op} {t}".format(op=operator, t=threshold) if threshold is not None else "1.0",
        actual="{:.4f}".format(float(ratio)),
    )


def _check_accuracy(
    rule_id, selector, severity, description,
    threshold, operator, table_ref, execute_fn,
) -> QualityCheckResult:
    sql = _accuracy_min_sql(table_ref, selector)
    rows = execute_fn(sql)
    min_val = rows[0][0] if rows and rows[0][0] is not None else None
    if min_val is None:
        return QualityCheckResult(
            rule_id=rule_id, rule_type="accuracy", selector=selector,
            passed=False, severity=severity,
            message="No data to check accuracy for '{}'".format(selector),
        )
    passed = _compare(min_val, threshold, operator) if threshold is not None else True
    return QualityCheckResult(
        rule_id=rule_id, rule_type="accuracy", selector=selector,
        passed=passed, severity=severity,
        message=(
            "{desc} — min value of '{col}' is {val}".format(
                desc=description or rule_id, col=selector, val=min_val)
            if not passed else
            "Accuracy OK for '{}'".format(selector)
        ),
        expected="{op} {t}".format(op=operator, t=threshold) if threshold is not None else "pass",
        actual=str(min_val),
    )


def _check_validity(
    rule_id, selector, severity, description,
    valid_values, table_ref, execute_fn,
) -> QualityCheckResult:
    if not valid_values:
        return QualityCheckResult(
            rule_id=rule_id, rule_type="validity", selector=selector,
            passed=False, severity="warning",
            message="Rule '{}' is type 'validity' but has no 'validValues' list".format(rule_id),
        )
    col = _validate_ident(selector)
    # Build SQL with quoted string literals for valid values
    escaped = ", ".join("'{}'".format(v.replace("'", "''")) for v in valid_values)
    sql = (
        'SELECT COUNT(*) AS invalid_count FROM {tbl} '
        'WHERE "{col}" IS NOT NULL AND "{col}" NOT IN ({vals})'
    ).format(tbl=table_ref, col=col, vals=escaped)
    rows = execute_fn(sql)
    invalid_count = rows[0][0] if rows and rows[0][0] is not None else 0
    passed = invalid_count == 0
    return QualityCheckResult(
        rule_id=rule_id, rule_type="validity", selector=selector,
        passed=passed, severity=severity,
        message=(
            "{desc} — {n} invalid value(s) in '{col}'".format(
                desc=description or rule_id, n=invalid_count, col=selector)
            if not passed else
            "Validity OK for '{}'".format(selector)
        ),
        expected="0 invalid values",
        actual="{} invalid value(s)".format(invalid_count),
    )


def _check_freshness(
    rule_id, selector, severity, description,
    window, table_ref, execute_fn, dialect,
) -> QualityCheckResult:
    sql = _freshness_sql(table_ref, selector, dialect)
    rows = execute_fn(sql)
    age_seconds = rows[0][0] if rows and rows[0][0] is not None else None

    if age_seconds is None:
        return QualityCheckResult(
            rule_id=rule_id, rule_type="freshness", selector=selector,
            passed=False, severity=severity,
            message="No data to check freshness for '{}'".format(selector),
        )

    max_age_seconds = None
    if window:
        max_age_seconds = _parse_duration_seconds(str(window))

    if max_age_seconds is None:
        # No threshold specified — just report the age
        return QualityCheckResult(
            rule_id=rule_id, rule_type="freshness", selector=selector,
            passed=True, severity="info",
            message="Freshness for '{}': data is {:.0f}s old (no threshold set)".format(
                selector, float(age_seconds)),
            actual="{:.0f}s".format(float(age_seconds)),
        )

    passed = float(age_seconds) <= float(max_age_seconds)
    return QualityCheckResult(
        rule_id=rule_id, rule_type="freshness", selector=selector,
        passed=passed, severity=severity,
        message=(
            "{desc} — data is {age:.0f}s old, max allowed is {max}s".format(
                desc=description or rule_id,
                age=float(age_seconds),
                max=max_age_seconds,
            )
            if not passed else
            "Freshness OK for '{}' ({:.0f}s old)".format(selector, float(age_seconds))
        ),
        expected="<= {}s".format(max_age_seconds),
        actual="{:.0f}s".format(float(age_seconds)),
    )
