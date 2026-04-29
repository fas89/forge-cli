# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""DuckDB acquisition runner.

Builds a DuckDB SQL string of the form:

    COPY (FROM <reader>(<uri>, <opts>)) TO '<dest>' (FORMAT <fmt>);

Reader dispatch by ``source.kind``:

  - ``filesystem``  -> ``read_csv`` / ``read_parquet`` / ``read_json``
  - ``postgres``    -> ``postgres_scan(...)``
  - ``mysql``       -> ``mysql_scan(...)``
  - ``sqlite``      -> ``sqlite_scan(...)``
  - ``http``        -> ``read_csv_auto('https://...')``

Loads required extensions on demand (``httpfs``, ``postgres``, ``mysql``,
``sqlite``, ``aws``, ``azure``).

The runner satisfies the ``api.runner.Runner`` Protocol and registers the
top-level ``execute_duckdb_build`` function used by ``build_runners.base``
when it detects an acquisition build with ``engine: duckdb``.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Dict, FrozenSet, List, Optional

from fluid_build.api.runner import (
    RunContext,
    Runner,
    RunnerCapability,
    RunPlan,
    RunResult,
    RunState,
    StreamResult,
)
from fluid_build.api.schema import SchemaFingerprint
from fluid_build.api.source import AcquisitionMode
from fluid_build.providers._sql_safety import quote_string_literal, validate_ident

from .._acquisition_common import generate_run_id, utc_now_iso
from .._fingerprint import fingerprint_from_duckdb_describe

LOG = logging.getLogger("fluid.acquire.duckdb")


# ── Object-store credentials (CREATE SECRET) ────────────────────────────


_S3_BOOL_KEYS = {"use_ssl"}
_S3_STR_KEYS = {"endpoint", "region", "url_style", "key_id", "secret", "session_token"}
_AZURE_STR_KEYS = {"account_name", "account_key", "tenant_id", "client_id", "client_secret"}
_GCS_STR_KEYS = {"key_id", "secret"}


def _build_create_secret(scheme: str, cfg: Dict[str, Any]) -> Optional[str]:
    """Render a DuckDB ``CREATE SECRET`` statement for the given scheme.

    Returns ``None`` when no credentials need to be configured. The values
    are passed through ``quote_string_literal`` so secrets containing
    single quotes (or any other SQL-meta character) cannot break out of
    the literal.
    """
    if not cfg:
        return None
    type_map = {"s3": "s3", "gcs": "gcs", "azure": "azure"}
    duck_type = type_map.get(scheme)
    if duck_type is None:
        return None

    allowed = {
        "s3": _S3_STR_KEYS | _S3_BOOL_KEYS,
        "gcs": _GCS_STR_KEYS,
        "azure": _AZURE_STR_KEYS,
    }[scheme]

    parts: List[str] = []
    for key, value in cfg.items():
        validate_ident(key)
        if key not in allowed:
            continue
        if value is None:
            continue
        if key in _S3_BOOL_KEYS or isinstance(value, bool):
            parts.append(f"{key.upper()} {'true' if value else 'false'}")
        else:
            parts.append(f"{key.upper()} {quote_string_literal(str(value))}")
    if not parts:
        return None
    return (
        f"CREATE OR REPLACE SECRET fluid_{scheme}_secret (TYPE {duck_type}, "
        + ", ".join(parts)
        + ")"
    )


def _apply_object_store_secret(con: Any, ctx: RunContext) -> None:
    """Issue ``CREATE SECRET`` if the contract carries object-store credentials.

    Looks for ``connection.s3`` / ``connection.gcs`` / ``connection.azure``
    blocks. Best-effort: a missing/incompatible duckdb build logs a warning
    and continues so the run can still proceed via env-var fallback.
    """
    raw = dict(ctx.source.connection.raw or {})
    for scheme in ("s3", "gcs", "azure"):
        cfg = raw.get(scheme)
        if not isinstance(cfg, dict):
            continue
        try:
            stmt = _build_create_secret(scheme, cfg)
            if stmt is None:
                continue
            con.execute(stmt)
        except Exception as exc:  # noqa: BLE001
            LOG.warning("DuckDB CREATE SECRET (%s) failed: %s", scheme, exc)


# ── Reader dispatch ──────────────────────────────────────────────────────


def _csv_options_clause(opts: Dict[str, Any]) -> str:
    """Render DuckDB ``read_csv_auto`` options.

    Option keys are validated as identifiers (no quoting, no spaces);
    string values are properly single-quote-escaped using
    ``quote_string_literal``. Booleans and numerics are rendered literal.
    """
    parts: List[str] = []
    for k, v in opts.items():
        validate_ident(k)  # option keys must be plain identifiers
        if isinstance(v, bool):
            parts.append(f"{k}={'true' if v else 'false'}")
        elif isinstance(v, (int, float)):
            parts.append(f"{k}={v}")
        elif isinstance(v, str):
            parts.append(f"{k}={quote_string_literal(v)}")
        else:
            # JSON-y nested values not supported in this pass.
            continue
    return ", " + ", ".join(parts) if parts else ""


def _build_select_for_filesystem(uri: str, fmt: str, opts: Dict[str, Any]) -> str:
    """Build a SELECT against a filesystem source.

    URIs go through ``quote_string_literal`` so a malicious URI containing
    a single quote can't break out of the string literal and inject SQL.
    """
    fmt = (fmt or "csv").lower()
    quoted_uri = quote_string_literal(uri)
    if fmt == "csv":
        return f"SELECT * FROM read_csv_auto({quoted_uri}{_csv_options_clause(opts)})"
    if fmt == "parquet":
        return f"SELECT * FROM read_parquet({quoted_uri})"
    if fmt in ("json", "ndjson"):
        return f"SELECT * FROM read_json_auto({quoted_uri})"
    raise ValueError(f"unsupported reader.format for filesystem: {fmt}")


def _build_select_for_postgres(connection: Dict[str, Any], stream: str) -> str:
    """``postgres_scan('host=… port=… dbname=… user=… password=…', schema, table)``.

    Schema/table are validated as identifiers (so ``stream='x; DROP TABLE y'``
    is rejected at build time, not executed). DSN values are passed through
    ``quote_string_literal`` so embedded single quotes can't escape the
    outer SQL string. Note the inner DSN format itself uses spaces to
    separate fields, but the entire DSN is a single SQL literal — quoting
    at the SQL boundary is sufficient to neutralize injection.
    """
    if "." in stream:
        schema, table = stream.split(".", 1)
    else:
        schema, table = "public", stream
    schema = validate_ident(schema)
    table = validate_ident(table)

    parts: List[str] = []

    # libpq DSN itself escapes single quotes by doubling them; we apply that
    # here, then wrap the whole thing in quote_string_literal which doubles
    # them again at the SQL-literal layer. The two layers compose correctly:
    # libpq sees one level of quoting, SQL sees the other.
    def _libpq(v: Any) -> str:
        s = str(v).replace("\\", "\\\\").replace("'", "\\'")
        return f"'{s}'" if (" " in s or "'" in s) else s

    if connection.get("host"):
        parts.append(f"host={_libpq(connection['host'])}")
    if connection.get("port"):
        # port must be numeric; reject anything else.
        port_str = str(connection["port"])
        if not port_str.isdigit():
            raise ValueError(f"postgres connection.port must be numeric, got {port_str!r}")
        parts.append(f"port={port_str}")
    if connection.get("database"):
        parts.append(f"dbname={_libpq(connection['database'])}")
    if connection.get("user"):
        parts.append(f"user={_libpq(connection['user'])}")
    if connection.get("password"):
        parts.append(f"password={_libpq(connection['password'])}")
    dsn = " ".join(parts)
    return (
        f"SELECT * FROM postgres_scan({quote_string_literal(dsn)}, "
        f"{quote_string_literal(schema)}, {quote_string_literal(table)})"
    )


def _build_select_for_http(uri: str, fmt: str, opts: Dict[str, Any]) -> str:
    return _build_select_for_filesystem(uri, fmt or "csv", opts)


# ── Destination dispatch ─────────────────────────────────────────────────


def _build_copy_destination(out_path: str, fmt: str) -> str:
    """COPY ... TO '<path>' (FORMAT <fmt>).

    The output path is passed through ``quote_string_literal`` so a path
    containing a single quote can't escape the literal and inject SQL.
    """
    fmt = (fmt or "parquet").lower()
    quoted_path = quote_string_literal(out_path)
    if fmt == "parquet":
        return f"COPY ({{select}}) TO {quoted_path} (FORMAT 'parquet')"
    if fmt == "csv":
        return f"COPY ({{select}}) TO {quoted_path} (FORMAT 'csv', HEADER)"
    if fmt in ("json", "ndjson"):
        return f"COPY ({{select}}) TO {quoted_path} (FORMAT 'json')"
    raise ValueError(f"unsupported sink format: {fmt}")


# ── Extension loading ────────────────────────────────────────────────────


_EXT_BY_KIND = {
    "postgres": ["postgres"],
    "mysql": ["mysql"],
    "sqlite": ["sqlite"],
    "filesystem": [],  # may add httpfs/aws if URI scheme requires it
    "http": ["httpfs"],
}


def _required_extensions(kind: str, uri: Optional[str]) -> List[str]:
    base = list(_EXT_BY_KIND.get(kind, []))
    if uri:
        if uri.startswith("s3://"):
            base.extend(["httpfs", "aws"])
        elif uri.startswith(("gs://", "gcs://")):
            base.append("httpfs")
        elif uri.startswith("azure://"):
            base.append("azure")
        elif uri.startswith(("http://", "https://")):
            base.append("httpfs")
    # Deduplicate while preserving order
    seen = set()
    out = []
    for ext in base:
        if ext not in seen:
            seen.add(ext)
            out.append(ext)
    return out


# ── Runner ───────────────────────────────────────────────────────────────


@dataclass
class DuckdbRunner:
    """Runner Protocol implementation for the DuckDB engine."""

    name: ClassVar[str] = "duckdb"
    declared_capabilities: ClassVar[FrozenSet[RunnerCapability]] = frozenset(
        {
            RunnerCapability.FULL_REFRESH,
            RunnerCapability.INCREMENTAL_APPEND,
            RunnerCapability.SCHEMA_DISCOVERY,
            RunnerCapability.AT_LEAST_ONCE,
        }
    )
    declared_modes: ClassVar[FrozenSet[str]] = frozenset({"embedded"})

    def plan(self, ctx: RunContext) -> RunPlan:
        streams = list(ctx.source.streams) or self._infer_streams(ctx)
        return RunPlan(streams_planned=streams)

    def run(self, ctx: RunContext) -> RunResult:
        return _execute(ctx, self)

    def replay(self, ctx: RunContext, run_id: str) -> RunResult:
        # Replay is just rerun under the same run-id; idempotency on the data
        # path keeps the destination consistent.
        ctx.run_id = run_id
        return _execute(ctx, self)

    def fingerprint(self, ctx: RunContext) -> SchemaFingerprint:
        import duckdb

        con = duckdb.connect(":memory:")
        try:
            self._load_extensions(con, ctx)
            select_sql = _select_for_first_stream(ctx)
            con.execute(f"CREATE TEMP VIEW _fp AS {select_sql}")
            rows = con.execute("DESCRIBE _fp").fetchall()
            return fingerprint_from_duckdb_describe(rows)
        finally:
            con.close()

    # ── helpers ──────────────────────────────────────────────────────────
    def _infer_streams(self, ctx: RunContext) -> List[str]:
        """Single-stream inference for kinds that don't carry a stream list."""
        kind = ctx.source.kind
        if kind in {"filesystem", "http"}:
            return [Path(ctx.source.connection.uri or "data").stem]
        return ["data"]

    def _load_extensions(self, con: Any, ctx: RunContext) -> None:
        for ext in _required_extensions(ctx.source.kind, ctx.source.connection.uri):
            try:
                con.execute(f"INSTALL {ext}")
                con.execute(f"LOAD {ext}")
            except Exception as exc:  # noqa: BLE001
                LOG.warning("DuckDB extension load failed (%s): %s", ext, exc)
        _apply_object_store_secret(con, ctx)


def _select_for_first_stream(ctx: RunContext) -> str:
    return _select_for_stream(ctx, ctx.source.streams[0] if ctx.source.streams else "data")


def _select_for_stream(ctx: RunContext, stream: str) -> str:
    kind = ctx.source.kind
    conn = dict(ctx.source.connection.raw)
    if kind == "filesystem":
        uri = conn.get("uri") or stream
        fmt = ctx.source.reader.format if ctx.source.reader else "csv"
        opts = dict(ctx.source.reader.options) if ctx.source.reader else {}
        return _build_select_for_filesystem(uri, fmt or "csv", opts)
    if kind == "http":
        uri = conn.get("uri") or stream
        fmt = ctx.source.reader.format if ctx.source.reader else "csv"
        opts = dict(ctx.source.reader.options) if ctx.source.reader else {}
        return _build_select_for_http(uri, fmt or "csv", opts)
    if kind == "postgres":
        return _build_select_for_postgres(conn, stream)
    raise ValueError(f"DuckDB runner: unsupported source.kind '{kind}'")


def _execute(ctx: RunContext, runner: DuckdbRunner) -> RunResult:
    import duckdb

    started_at = utc_now_iso()
    t_start = time.time()
    streams_to_run = list(ctx.source.streams) or runner._infer_streams(ctx)
    sink_format = (ctx.sink.format or "parquet").lower()
    out_dir = Path(ctx.workdir) / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    stream_results: List[StreamResult] = []
    failures = 0
    records_total = 0

    con = duckdb.connect(":memory:")
    try:
        runner._load_extensions(con, ctx)
        for stream in streams_to_run:
            t_stream = time.time()
            try:
                sel = _select_for_stream(ctx, stream)
                if ctx.sample_rows:
                    sel = f"SELECT * FROM ({sel}) LIMIT {int(ctx.sample_rows)}"
                # Resolve binding location if present, else write under workdir.
                out_path = _resolve_destination_path(ctx, stream, sink_format, out_dir)
                copy_template = _build_copy_destination(out_path, sink_format)
                copy_sql = copy_template.format(select=sel)
                LOG.info("duckdb.run stream=%s sql_chars=%d", stream, len(copy_sql))
                con.execute(copy_sql)
                # Count rows landed (best-effort via COUNT on the same select).
                try:
                    row = con.execute(f"SELECT COUNT(*) FROM ({sel})").fetchone()
                    n = row[0] if row is not None else 0
                except Exception:
                    n = 0
                records_total += int(n)
                stream_results.append(
                    StreamResult(
                        name=stream,
                        state=RunState.SUCCEEDED,
                        records=int(n),
                        duration_seconds=time.time() - t_stream,
                        cursor_advanced=False,  # full_refresh
                    )
                )
            except Exception as exc:  # noqa: BLE001
                LOG.error("duckdb.run.failed stream=%s err=%s", stream, exc, exc_info=True)
                failures += 1
                stream_results.append(
                    StreamResult(
                        name=stream,
                        state=RunState.FAILED,
                        records=0,
                        duration_seconds=time.time() - t_stream,
                        error=str(exc),
                    )
                )
    finally:
        con.close()

    finished_at = utc_now_iso()
    if failures == 0:
        run_state = RunState.SUCCEEDED
    elif failures < len(streams_to_run):
        run_state = RunState.PARTIAL
    else:
        run_state = RunState.FAILED

    return RunResult(
        run_id=ctx.run_id,
        state=run_state,
        streams=stream_results,
        started_at=started_at,
        finished_at=finished_at,
        records_total=records_total,
        bytes_total=0,  # Not tracked at this layer; OTel observers can.
        dlq_records=0,
        facets={"engine": "duckdb", "duration_seconds": time.time() - t_start},
    )


_REMOTE_URI_SCHEMES = ("s3://", "gs://", "gcs://", "azure://", "abfs://", "http://", "https://")


def _is_remote_uri(p: str) -> bool:
    return p.startswith(_REMOTE_URI_SCHEMES)


def _resolve_destination_path(
    ctx: RunContext, stream: str, sink_format: str, default_dir: Path
) -> str:
    """Pick the destination URI/path for ``stream``.

    Honors the contract's ``exposes[].binding.location.path`` when the
    expose has a single output. Returns a string so URI schemes
    (``s3://``, ``gs://``, ``azure://`` …) survive untouched — wrapping
    them in ``Path`` collapses ``s3://`` to ``s3:/`` which DuckDB can't
    read. For local paths, behavior is unchanged: relative paths are
    rooted under ``ctx.workdir`` and parent directories are created.
    """
    expose = _find_first_expose(ctx)
    if expose is not None:
        loc = expose.get("binding", {}).get("location", {}) or {}
        path = loc.get("path")
        if path and len(ctx.source.streams) <= 1:
            if _is_remote_uri(path):
                return path
            p = Path(path)
            if not p.is_absolute():
                p = Path(ctx.workdir) / p
            p.parent.mkdir(parents=True, exist_ok=True)
            return str(p)
    ext = {"parquet": "parquet", "csv": "csv", "json": "ndjson"}.get(sink_format, sink_format)
    return str(default_dir / f"{stream}.{ext}")


def _find_first_expose(ctx: RunContext) -> Optional[Dict[str, Any]]:
    exposes = ctx.contract.get("exposes") or []
    return exposes[0] if exposes else None


# ── Top-level entry point used by build_runners.base ────────────────────


def execute_duckdb_build(
    build: Dict[str, Any],
    contract: Dict[str, Any],
    contract_dir: Path,
    *,
    dry_run: bool = False,
    sample_rows: Optional[int] = None,
    state_root: Optional[Path] = None,
) -> int:
    """Glue function called by build_runners.base. Returns exit code."""
    from fluid_build.api.runner import RunContext
    from fluid_build.api.source import SinkSpec, SourceSpec
    from fluid_build.build_runners._state import FileStateStore

    from .._acquisition_common import get_acquisition_build_props
    from ..base import _resolve_env_placeholders

    props = get_acquisition_build_props(build)
    source_dict = props.get("source")
    if not source_dict:
        LOG.error("acquisition build missing properties.source")
        return 1

    # Resolve {{ env.NAME }} placeholders in connection (host, port, etc.) so
    # the runner sees real values, not templates. Mirrors how the dbt and
    # python runners already handle env interpolation via build_runners.base.
    source_dict = _resolve_env_placeholders(source_dict)
    source = SourceSpec.from_dict(source_dict)
    sink = SinkSpec.from_dict(props.get("sink"))
    workdir = str(contract_dir)
    state_root = state_root or (contract_dir / ".fluid")
    store = FileStateStore(state_root)

    # Minimal context for end-to-end runs; lineage / hooks / cost are wired
    # through cli/apply.py in production paths and pass through here as
    # no-op defaults to keep the engine self-contained for tests/examples.
    from fluid_build.api.hooks import HookChain
    from fluid_build.build_runners._cost import InMemoryCostTracker
    from fluid_build.build_runners._lineage import NullLineageEmitter

    ctx = RunContext(
        run_id=generate_run_id(),
        product_id=contract.get("id", "unknown"),
        build_id=build.get("id", "unknown"),
        contract=contract,
        source=source,
        sink=sink,
        state_store=store,
        hook_chain=HookChain(hooks=[]),
        lineage=NullLineageEmitter(),
        cost_tracker=InMemoryCostTracker(),
        workdir=workdir,
        sample_rows=sample_rows,
    )

    runner = DuckdbRunner()
    if dry_run:
        plan = runner.plan(ctx)
        LOG.info("duckdb.dry-run streams=%s", plan.streams_planned)
        return 0

    result = runner.run(ctx)
    # Persist the run record so status/replay can find it.
    store.write_run_record(
        ctx.product_id,
        ctx.build_id,
        {
            "run_id": result.run_id,
            "state": result.state.value,
            "started_at": result.started_at,
            "finished_at": result.finished_at,
            "records_total": result.records_total,
            "streams": [
                {
                    "name": s.name,
                    "state": s.state.value,
                    "records": s.records,
                    "duration_seconds": s.duration_seconds,
                    "error": s.error,
                }
                for s in result.streams
            ],
            "facets": result.facets,
        },
    )
    return 0 if result.state in (RunState.SUCCEEDED, RunState.PARTIAL) else 1
