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

"""Engine-driver abstraction for the consumer MCP output-port server.

A driver knows how to:

* Resolve a fully-qualified, dialect-quoted table reference from an
  expose's ``binding.location`` block (so the query compiler can
  treat it as opaque text).
* Execute a parameterised SQL statement against the engine and
  return rows.
* Apply column-level masking before returning results — restricted
  columns are dropped from the projection and from each row.

The base class enforces the contract; concrete drivers in
``drivers/duckdb.py``, ``drivers/bigquery.py``, ``drivers/snowflake.py``
implement the engine-specific bits.

Driver discovery is keyed by ``(binding.platform, binding.format)``;
unknown combinations raise :class:`UnsupportedBindingError`.
"""

from __future__ import annotations

import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple


class UnsupportedBindingError(RuntimeError):
    """Raised when no driver supports the (platform, format) pair."""


# ---------------------------------------------------------------------
# Statement-level injection guard, shared across drivers.
# ---------------------------------------------------------------------

_INJECTION_MARKERS: Tuple[str, ...] = (";", "--", "/*", "*/")

# Body keywords blocked AFTER the leading ``SELECT`` is consumed.
# Catches a regression in :func:`compile_free_form_sql` that would
# slip a DDL/DML / set-operator past the body allowlist (the
# whitespace-bypass class of bug). ``select`` is included because a
# body SELECT (e.g. ``... UNION ALL SELECT secret FROM …``) is the
# canonical exfiltration pattern.
_TAIL_BLOCKED_TOKENS = re.compile(
    r"(?i)\b("
    r"alter|call|copy|create|delete|drop|execute|grant|insert|merge|put|remove|"
    r"revoke|select|show|truncate|update|use|union|intersect|except"
    r")\b"
)


def guard_against_injection_markers(sql: str) -> None:
    """Reject a rendered SQL statement that contains injection
    markers or banned body keywords.

    Drivers see only compiler output; the compiler is the primary
    safety net. This guard is defence-in-depth against a regression
    that introduces ``;`` / ``--`` / ``/*`` / ``*/`` or a banned
    keyword (UNION / DROP / …) into the body."""
    if any(marker in sql for marker in _INJECTION_MARKERS):
        raise ValueError(f"Refusing to execute statement with injection marker: {sql!r}")
    parts = sql.split(None, 1)
    if len(parts) >= 2 and _TAIL_BLOCKED_TOKENS.search(parts[1]):
        raise ValueError(
            "Refusing to execute statement: rendered SQL body contains a "
            f"banned keyword (compiler safety regression). sql={sql!r}"
        )


@dataclass(frozen=True)
class DriverDescriptor:
    """Metadata returned by :meth:`EngineDriver.descriptor`.

    Surfaced via the MCP ``describe`` tool and the ``resources/list``
    advertisement so an LLM client can reason about the engine
    without hitting it.
    """

    platform: str
    format: str
    table_reference: str
    """Fully-qualified, dialect-quoted table identifier the engine
    accepts in ``FROM`` clauses."""

    dialect: str
    """Dialect token recognised by
    :meth:`fluid_build.output_ports.mcp.query_compiler.CompiledQuery.render_sql_for_dialect`.
    """

    capabilities: Dict[str, bool] = field(default_factory=dict)
    """Per-feature flags, e.g. ``{"sample": True, "query": True,
    "lineage": False}``. Tools whose capabilities are absent or
    ``False`` are still advertised by the server but a call returns a
    typed error explaining the gap.
    """


@dataclass(frozen=True)
class SampleResult:
    """Result of a :meth:`EngineDriver.sample` call.

    Rows are dicts of column → value. Restricted columns have already
    been dropped by the time the result reaches the caller.
    """

    columns: Tuple[str, ...]
    rows: Tuple[Dict[str, Any], ...]
    truncated: bool
    """True when the engine returned exactly ``limit`` rows; the data
    set may have more rows than the cap permitted us to show."""


@dataclass(frozen=True)
class QueryResult:
    """Result of a :meth:`EngineDriver.query` call.

    ``columns`` is the projection (validated identifiers); ``rows``
    is one dict per row keyed by those columns.
    """

    columns: Tuple[str, ...]
    rows: Tuple[Dict[str, Any], ...]


class EngineDriver(ABC):
    """Abstract base for engine drivers.

    Drivers are stateless from the server's perspective: the server
    creates one instance per stdio process and re-uses it for every
    tools/call. Concrete drivers are free to maintain a pool / cached
    client internally; the only invariant the server relies on is
    that the methods below can be called repeatedly without producing
    side-effects beyond the audited query.
    """

    #: Driver registry key — set on the subclass.
    name: str = "base"

    def __init__(
        self,
        *,
        expose: Mapping[str, Any],
        contract: Mapping[str, Any],
        logger: Optional[logging.Logger] = None,
        connection_options: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.expose: Mapping[str, Any] = expose
        self.contract: Mapping[str, Any] = contract
        self.logger = logger or logging.getLogger(f"fluid.output_port.mcp.{self.name}")
        self.connection_options: Mapping[str, Any] = connection_options or {}
        self._restricted_columns: Set[str] = self._compute_restricted_columns(expose)

    # ------------------------------------------------------------------
    # Abstract surface — concrete drivers must override these.
    # ------------------------------------------------------------------

    @abstractmethod
    def descriptor(self) -> DriverDescriptor:
        """Return engine metadata for ``describe`` / ``resources/list``."""

    @abstractmethod
    def execute(
        self,
        *,
        sql: str,
        params: Sequence[Any] = (),
        timeout_seconds: Optional[float] = None,
    ) -> QueryResult:
        """Execute a parameterised SQL statement and return its rows.

        ``sql`` is the ``CompiledQuery.sql`` after dialect rendering;
        the driver MUST treat ``params`` as values to be bound by the
        engine's parameter mechanism, NOT as substrings to interpolate.

        ``timeout_seconds`` is advisory — drivers that can enforce a
        statement timeout should do so; ones that can't can ignore the
        argument. The server still wraps the call in its own deadline
        guard so a misbehaving driver can't hang the stdio loop.
        """

    @abstractmethod
    def health_check(self) -> Dict[str, Any]:
        """Cheap liveness probe — drivers should round-trip a trivial
        query (``SELECT 1`` or equivalent) and return a small dict
        with ``"status"`` ∈ ``{"ok", "degraded", "unavailable"}``,
        ``"detail"`` (string), and any driver-specific telemetry.

        Used by the ``health`` tool (Phase-2) and the cert script.
        """

    # ------------------------------------------------------------------
    # Default implementations driver authors typically don't override.
    # ------------------------------------------------------------------

    @property
    def restricted_columns(self) -> Set[str]:
        """Read-only view of columns dropped from any returned row.

        Computed once at construction time from
        ``expose.policy.authz.columnRestrictions`` so subsequent
        tool-calls don't pay the parsing cost.
        """
        return frozenset(self._restricted_columns)

    def project(
        self,
        rows: Iterable[Mapping[str, Any]],
        *,
        columns: Optional[Sequence[str]] = None,
    ) -> Tuple[Tuple[str, ...], Tuple[Dict[str, Any], ...]]:
        """Apply column masking + projection to engine rows.

        The driver-specific ``execute`` returns raw rows; this helper
        runs the universal masking/projection step so no driver can
        forget to do it. Returns ``(visible_columns, masked_rows)``.

        Snowflake (and BigQuery in some configurations) folds
        unquoted aliases to UPPERCASE, while the compiler emits
        lowercase aliases driven by the contract. The lookup is
        case-insensitive so the visible-column key (taken from
        ``columns``) matches the engine's echo regardless of case.
        Restricted-column matching is also case-insensitive — a
        contract that lists ``email`` denies ``EMAIL`` too.
        """
        rows_list = list(rows)
        if not rows_list:
            return tuple(columns or ()), ()
        all_columns = list(columns) if columns else list(rows_list[0].keys())
        restricted_lower = {c.lower() for c in self._restricted_columns}
        visible_columns = tuple(
            column for column in all_columns if column.lower() not in restricted_lower
        )
        masked_rows = tuple(self._mask_row(row, visible_columns) for row in rows_list)
        return visible_columns, masked_rows

    @staticmethod
    def _mask_row(
        row: Mapping[str, Any], visible_columns: Sequence[str]
    ) -> Dict[str, Any]:
        """Return a row keyed by the requested ``visible_columns``,
        looking values up case-insensitively against the engine's
        echo.

        Performance note: builds a per-row case-fold map. With
        Phase-1 sample/query result sets capped at ~100 rows the
        cost is negligible; if a future code path returns 100k+
        rows this can be lifted out and amortised once per result
        set."""
        case_fold_map = {key.lower(): key for key in row}
        out: Dict[str, Any] = {}
        for column in visible_columns:
            engine_key = case_fold_map.get(column.lower())
            out[column] = row.get(engine_key) if engine_key else row.get(column)
        return out

    def sample(self, *, limit: int) -> SampleResult:
        """Default ``sample`` implementation — ``SELECT * FROM <table> LIMIT n``.

        Drivers can override if the engine has a cheaper sampling
        primitive (BigQuery's ``TABLESAMPLE``, Snowflake's
        ``SAMPLE``).
        """
        if not isinstance(limit, int) or limit < 1 or limit > 1_000_000:
            raise ValueError("limit must be an integer in [1, 1_000_000]")
        descriptor = self.descriptor()
        sql = f"SELECT * FROM {descriptor.table_reference} LIMIT {limit}"
        result = self.execute(sql=sql, params=())
        visible_columns, rows = self.project(result.rows, columns=result.columns)
        truncated = len(rows) >= limit
        return SampleResult(columns=visible_columns, rows=rows, truncated=truncated)

    def query(
        self,
        *,
        sql: str,
        params: Sequence[Any],
        projection: Sequence[str],
    ) -> QueryResult:
        """Default ``query`` implementation — execute + project.

        ``projection`` is the validated column list from the compiler
        so we never trust the engine's column-name echo blindly.
        """
        result = self.execute(sql=sql, params=params)
        visible_columns, rows = self.project(result.rows, columns=projection)
        return QueryResult(columns=visible_columns, rows=rows)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_restricted_columns(expose: Mapping[str, Any]) -> Set[str]:
        """Return the set of column names that must never be returned.

        Reads ``expose.policy.authz.columnRestrictions`` and the
        privacy block. Phase-1 strategy: any column listed with
        ``access: deny`` for ANY principal is dropped wholesale —
        the consumer-side server doesn't yet know which principal it's
        serving (Phase-3 OAuth lands that), so the safe default is
        the union of all denials.

        Privacy ``masking`` rules with strategy ``mask`` / ``hash`` /
        ``tokenize`` / ``encrypt`` likewise drop the column today;
        Phase-2 will render the actual masking expression in the
        engine. This is safer than returning the raw value with a
        comment-only mask annotation.
        """
        restricted: Set[str] = set()
        policy = expose.get("policy") or {}
        authz = policy.get("authz") or {}
        for restriction in authz.get("columnRestrictions") or []:
            if not isinstance(restriction, Mapping):
                continue
            if str(restriction.get("access") or "").lower() != "deny":
                continue
            for column in restriction.get("columns") or []:
                if isinstance(column, str) and column:
                    restricted.add(column)
        privacy = policy.get("privacy") or {}
        for masking_rule in privacy.get("masking") or []:
            if not isinstance(masking_rule, Mapping):
                continue
            column = masking_rule.get("column")
            if isinstance(column, str) and column:
                restricted.add(column)
        return restricted


# ---------------------------------------------------------------------
# Helpers for driver implementations
# ---------------------------------------------------------------------


def get_binding(expose: Mapping[str, Any]) -> Tuple[str, str, Mapping[str, Any]]:
    """Return ``(platform, format, location)`` from an expose's binding.

    Raises :class:`UnsupportedBindingError` if the expose has no
    binding or the binding is malformed. Concrete drivers call this
    in ``__init__`` to fail fast.
    """
    binding = expose.get("binding") or {}
    platform = binding.get("platform")
    fmt = binding.get("format")
    location = binding.get("location") or {}
    if not isinstance(platform, str) or not platform:
        raise UnsupportedBindingError("binding.platform missing or empty")
    if not isinstance(fmt, str) or not fmt:
        raise UnsupportedBindingError("binding.format missing or empty")
    if not isinstance(location, Mapping):
        raise UnsupportedBindingError("binding.location must be a mapping")
    return platform, fmt, location
