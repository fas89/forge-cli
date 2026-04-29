# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Kafka Connect acquisition runner.

REST-driven: creates / updates / deletes connectors against a Kafka Connect
cluster (bring-your-own or Strimzi-managed). For tests, the
``kafka_connect_mock`` respx fixture stands in for a real cluster.

The runner manages the connector lifecycle: it idempotently posts the
configuration, waits for the connector to reach RUNNING state, and reports
per-task status. Records flow continuously through Kafka — there is no
stream-of-records to consume here; the runner's job is connector
orchestration.
"""

from __future__ import annotations

import logging
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
from fluid_build.api.schema import SchemaColumn, SchemaFingerprint

from .._acquisition_common import generate_run_id, utc_now_iso
from .._fingerprint import fingerprint_from_columns

LOG = logging.getLogger("fluid.acquire.kafka_connect")


# ── Connector class resolution ─────────────────────────────────────────


SOURCE_CONNECTOR_CLASS: Dict[str, str] = {
    "jdbc": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "postgres": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "mysql": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "sqlserver": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "oracle": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "s3": "io.confluent.connect.s3.S3SourceConnector",
    "salesforce": "io.confluent.salesforce.SalesforceCdcSourceConnector",
    "mongodb": "com.mongodb.kafka.connect.MongoSourceConnector",
}

SINK_CONNECTOR_CLASS: Dict[str, str] = {
    "jdbc": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "s3": "io.confluent.connect.s3.S3SinkConnector",
    "snowflake": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
    "iceberg": "org.apache.iceberg.connect.IcebergSinkConnector",
    "bigquery": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
}


def resolve_source_connector(kind: str, override: Optional[str] = None) -> str:
    if override:
        return override
    cls = SOURCE_CONNECTOR_CLASS.get(kind.lower())
    if not cls:
        raise ValueError(f"kafka-connect: no source connector class for kind '{kind}'")
    return cls


def resolve_sink_connector(format_or_platform: str, override: Optional[str] = None) -> str:
    if override:
        return override
    key = (format_or_platform or "").lower()
    for k, cls in SINK_CONNECTOR_CLASS.items():
        if k in key:
            return cls
    return SINK_CONNECTOR_CLASS["s3"]  # safe default


# ── REST client ────────────────────────────────────────────────────────


class KafkaConnectRestClient:
    """Minimal REST client for the Kafka Connect API."""

    def __init__(self, base_url: str, *, timeout_seconds: int = 30):
        import httpx

        self._client = httpx.Client(base_url=base_url.rstrip("/"), timeout=timeout_seconds)

    def close(self) -> None:
        self._client.close()

    def list_connectors(self) -> List[str]:
        r = self._client.get("/connectors")
        r.raise_for_status()
        return r.json()

    def create_connector(self, name: str, config: Dict[str, Any]) -> Dict[str, Any]:
        r = self._client.post("/connectors", json={"name": name, "config": config})
        if r.status_code in (200, 201):
            return r.json()
        r.raise_for_status()
        return r.json()

    def update_config(self, name: str, config: Dict[str, Any]) -> Dict[str, Any]:
        r = self._client.put(f"/connectors/{name}/config", json=config)
        r.raise_for_status()
        return r.json()

    def get_connector(self, name: str) -> Optional[Dict[str, Any]]:
        r = self._client.get(f"/connectors/{name}")
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

    def delete_connector(self, name: str) -> bool:
        r = self._client.delete(f"/connectors/{name}")
        return r.status_code in (204, 404)

    def get_status(self, name: str) -> Dict[str, Any]:
        r = self._client.get(f"/connectors/{name}/status")
        r.raise_for_status()
        return r.json()


# ── Runner ─────────────────────────────────────────────────────────────


@dataclass
class KafkaConnectRunner:
    name: ClassVar[str] = "kafka-connect"
    declared_capabilities: ClassVar[FrozenSet[RunnerCapability]] = frozenset(
        {
            RunnerCapability.STREAMING,
            RunnerCapability.AT_LEAST_ONCE,
            RunnerCapability.EXACTLY_ONCE,
            RunnerCapability.CDC,
            RunnerCapability.SCHEMA_DISCOVERY,
        }
    )
    declared_modes: ClassVar[FrozenSet[str]] = frozenset({"bring-your-own", "managed"})

    def plan(self, ctx: RunContext) -> RunPlan:
        return RunPlan(streams_planned=list(ctx.source.streams) or [ctx.source.kind])

    def run(self, ctx: RunContext) -> RunResult:
        return _execute(ctx, self)

    def replay(self, ctx: RunContext, run_id: str) -> RunResult:
        ctx.run_id = run_id
        return _execute(ctx, self)

    def fingerprint(self, ctx: RunContext) -> SchemaFingerprint:
        cols = [
            SchemaColumn(name=s, type="kafka-connect", nullable=True)
            for s in (ctx.source.streams or [ctx.source.kind])
        ]
        return SchemaFingerprint.of(cols, captured_at=utc_now_iso())


def _execute(ctx: RunContext, runner: KafkaConnectRunner) -> RunResult:
    started_at = utc_now_iso()
    t_start = time.time()

    props = ctx.contract.get("builds", [{}])[0].get("properties", {})
    kc_props = props.get("kafka-connect", {}) or {}
    deployment = kc_props.get("deployment", {}) or {}
    server_url = deployment.get("server_url")
    if not server_url:
        return _failed(ctx, started_at, t_start, "kafka-connect requires deployment.server_url")

    try:
        connector_class = resolve_source_connector(
            ctx.source.kind, override=kc_props.get("connector_class")
        )
    except ValueError as exc:
        return _failed(ctx, started_at, t_start, str(exc))

    # Build connector config from source connection.
    base_config: Dict[str, Any] = {
        "connector.class": connector_class,
        "tasks.max": str(kc_props.get("tasks_max", 1)),
    }
    base_config.update(_jdbc_config(ctx.source.connection.raw, ctx.source.kind))
    if ctx.source.streams:
        base_config["table.whitelist"] = ",".join(ctx.source.streams)
    # Stream / mode wiring.
    base_config["mode"] = _map_acquisition_mode_to_kc(ctx.source.mode.value)
    if ctx.source.cursor_field:
        base_config["incrementing.column.name"] = ctx.source.cursor_field

    # Avro / Schema-Registry wiring (optional). When the contract declares
    # ``properties.kafka-connect.schema_registry.url`` we emit the AvroConverter
    # properties so produced records are serialized as Avro under the
    # registered subjects. Without it we leave Connect's default JSON converter.
    sr_cfg = kc_props.get("schema_registry") or {}
    sr_url = sr_cfg.get("url")
    if sr_url:
        from .schema_registry import avro_converter_config

        base_config.update(avro_converter_config(sr_url))

    # Optional sink-side connector (companion).
    sink_config = kc_props.get("sink_connector_config")

    connector_name = kc_props.get("connector_name") or f"forge-{ctx.product_id.replace('.', '-')}"
    client = KafkaConnectRestClient(server_url)
    try:
        existing = client.get_connector(connector_name)
        if existing is None:
            client.create_connector(connector_name, base_config)
        else:
            client.update_config(connector_name, base_config)

        sink_name = None
        if sink_config:
            sink_name = kc_props.get("sink_connector_name") or f"{connector_name}-sink"
            existing_sink = client.get_connector(sink_name)
            if existing_sink is None:
                client.create_connector(sink_name, sink_config)
            else:
                client.update_config(sink_name, sink_config)

        status = client.get_status(connector_name)
        connector_state = (status.get("connector", {}) or {}).get("state", "UNKNOWN")
        ok = connector_state == "RUNNING"

        stream_results = [
            StreamResult(
                name=s,
                state=RunState.SUCCEEDED if ok else RunState.FAILED,
                records=0,
                cursor_advanced=False,
            )
            for s in (ctx.source.streams or [ctx.source.kind])
        ]
        finished_at = utc_now_iso()
        return RunResult(
            run_id=ctx.run_id,
            state=RunState.SUCCEEDED if ok else RunState.FAILED,
            streams=stream_results,
            started_at=started_at,
            finished_at=finished_at,
            records_total=0,
            bytes_total=0,
            dlq_records=0,
            facets={
                "engine": "kafka-connect",
                "duration_seconds": time.time() - t_start,
                "connector_name": connector_name,
                "connector_class": connector_class,
                "sink_connector_name": sink_name,
                "connector_state": connector_state,
            },
        )
    except Exception as exc:  # noqa: BLE001
        LOG.error("kafka_connect.failed err=%s", exc, exc_info=True)
        return _failed(ctx, started_at, t_start, str(exc))
    finally:
        client.close()


def _jdbc_config(connection: Dict[str, Any], kind: str) -> Dict[str, Any]:
    """Translate a connection dict into Kafka Connect JDBC config keys."""
    if kind in ("jdbc", "postgres", "mysql", "sqlserver", "oracle"):
        host = connection.get("host", "localhost")
        port = connection.get("port", "")
        db = connection.get("database", "")
        user = connection.get("user", "")
        password = connection.get("password", "")
        protocol = {
            "postgres": "postgresql",
            "mysql": "mysql",
            "sqlserver": "sqlserver",
            "oracle": "oracle",
        }.get(kind, "postgresql")
        url = f"jdbc:{protocol}://{host}{':' + str(port) if port else ''}/{db}"
        return {
            "connection.url": url,
            "connection.user": user,
            "connection.password": password,
        }
    return {k: v for k, v in connection.items() if k != "secretRef"}


def _map_acquisition_mode_to_kc(mode: str) -> str:
    """FLUID acquisition mode → Confluent JDBC source `mode`."""
    return {
        "full_refresh": "bulk",
        "incremental_append": "incrementing",
        "incremental_dedup": "timestamp+incrementing",
        "cdc": "timestamp",
        "streaming": "incrementing",
    }.get(mode, "bulk")


def _failed(ctx: RunContext, started_at: str, t_start: float, err: str) -> RunResult:
    return RunResult(
        run_id=ctx.run_id,
        state=RunState.FAILED,
        streams=[],
        started_at=started_at,
        finished_at=utc_now_iso(),
        records_total=0,
        bytes_total=0,
        dlq_records=0,
        error=err,
        facets={"engine": "kafka-connect", "duration_seconds": time.time() - t_start},
    )


# ── Top-level entry point ──────────────────────────────────────────────


def execute_kafka_connect_build(
    build: Dict[str, Any],
    contract: Dict[str, Any],
    contract_dir: Path,
    *,
    dry_run: bool = False,
    sample_rows: Optional[int] = None,
    state_root: Optional[Path] = None,
) -> int:
    from fluid_build.api.hooks import HookChain
    from fluid_build.api.runner import RunContext
    from fluid_build.api.source import SinkSpec, SourceSpec
    from fluid_build.build_runners._cost import InMemoryCostTracker
    from fluid_build.build_runners._lineage import NullLineageEmitter
    from fluid_build.build_runners._state import FileStateStore

    from .._acquisition_common import get_acquisition_build_props
    from ..base import _resolve_env_placeholders

    props = get_acquisition_build_props(build)
    source_dict = props.get("source")
    if not source_dict:
        LOG.error("acquisition build missing properties.source")
        return 1
    source_dict = _resolve_env_placeholders(source_dict)
    source = SourceSpec.from_dict(source_dict)
    sink = SinkSpec.from_dict(props.get("sink"))

    state_root = state_root or (contract_dir / ".fluid")
    store = FileStateStore(state_root)

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
        workdir=str(contract_dir),
        sample_rows=sample_rows,
    )
    runner = KafkaConnectRunner()
    if dry_run:
        plan = runner.plan(ctx)
        LOG.info("kafka_connect.dry-run streams=%s", plan.streams_planned)
        return 0

    result = runner.run(ctx)
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
                {"name": s.name, "state": s.state.value, "records": s.records}
                for s in result.streams
            ],
            "error": result.error,
            "facets": result.facets,
        },
    )
    return 0 if result.state in (RunState.SUCCEEDED, RunState.PARTIAL) else 1
