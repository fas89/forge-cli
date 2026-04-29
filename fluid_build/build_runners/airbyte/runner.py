# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Airbyte acquisition runner.

REST mode (default for production deployments): drives an Airbyte OSS / Cloud
server. Embedded mode (PyAirbyte) runs connectors in-process — kept for
parity but exercised via the REST path in tests since PyAirbyte requires a
heavyweight install.

Image-signature verification: when ``properties.airbyte.image_signature`` is
set, the connector image is verified via the configured ``ImageSignatureVerifier``
before any sync triggers.
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
from fluid_build.api.security import ImageSignatureVerifier

from .._acquisition_common import generate_run_id, utc_now_iso
from .._fingerprint import fingerprint_from_columns

LOG = logging.getLogger("fluid.acquire.airbyte")


# ── REST client ─────────────────────────────────────────────────────────


class AirbyteRestClient:
    """Minimal REST client for the Airbyte API surface the runner uses.

    Wraps httpx.Client; tolerates Airbyte Cloud auth (Bearer token) when
    ``properties.airbyte.deployment.auth.secretRef`` resolves to a token.
    """

    def __init__(
        self,
        server_url: str,
        *,
        api_token: Optional[str] = None,
        timeout_seconds: int = 30,
    ):
        import httpx

        headers: Dict[str, str] = {}
        if api_token:
            headers["Authorization"] = f"Bearer {api_token}"
        self._client = httpx.Client(
            base_url=server_url.rstrip("/"),
            headers=headers,
            timeout=timeout_seconds,
        )

    def close(self) -> None:
        self._client.close()

    def list_sources(self, workspace_id: str) -> List[Dict[str, Any]]:
        r = self._client.post("/api/v1/sources/list", json={"workspaceId": workspace_id})
        r.raise_for_status()
        return r.json().get("sources", [])

    def create_source(self, body: Dict[str, Any]) -> Dict[str, Any]:
        r = self._client.post("/api/v1/sources/create", json=body)
        r.raise_for_status()
        return r.json()

    def create_destination(self, body: Dict[str, Any]) -> Dict[str, Any]:
        r = self._client.post("/api/v1/destinations/create", json=body)
        r.raise_for_status()
        return r.json()

    def create_connection(self, body: Dict[str, Any]) -> Dict[str, Any]:
        r = self._client.post("/api/v1/connections/create", json=body)
        r.raise_for_status()
        return r.json()

    def trigger_sync(self, connection_id: str) -> Dict[str, Any]:
        r = self._client.post("/api/v1/connections/sync", json={"connectionId": connection_id})
        r.raise_for_status()
        return r.json()

    def get_job(self, job_id: int) -> Dict[str, Any]:
        r = self._client.post("/api/v1/jobs/get", json={"id": job_id})
        r.raise_for_status()
        return r.json()


# ── Connector kind → image map ─────────────────────────────────────────


KIND_TO_IMAGE: Dict[str, str] = {
    "salesforce": "airbyte/source-salesforce",
    "stripe": "airbyte/source-stripe",
    "github": "airbyte/source-github",
    "postgres": "airbyte/source-postgres",
    "mysql": "airbyte/source-mysql",
    "mongodb": "airbyte/source-mongodb-v2",
    "s3": "airbyte/source-s3",
    "snowflake": "airbyte/source-snowflake",
    "faker": "airbyte/source-faker",
}


def resolve_connector_image(kind: str, override: Optional[str] = None) -> str:
    """Return ``image:tag`` for a given source kind. Override wins."""
    if override:
        return override
    img = KIND_TO_IMAGE.get(kind)
    if not img:
        raise ValueError(f"airbyte: no connector image for kind '{kind}'")
    return img + ":latest"


# ── Mode mapping ───────────────────────────────────────────────────────


def map_mode_to_sync_mode(mode: str) -> str:
    """FLUID acquisition mode → Airbyte syncMode."""
    return {
        "full_refresh": "full_refresh",
        "incremental_append": "incremental",
        "incremental_dedup": "incremental",
        "cdc": "incremental",
    }.get(mode, "full_refresh")


# ── Runner ─────────────────────────────────────────────────────────────


@dataclass
class AirbyteRunner:
    """Runner Protocol implementation for the Airbyte engine."""

    name: ClassVar[str] = "airbyte"
    declared_capabilities: ClassVar[FrozenSet[RunnerCapability]] = frozenset(
        {
            RunnerCapability.FULL_REFRESH,
            RunnerCapability.INCREMENTAL_APPEND,
            RunnerCapability.INCREMENTAL_DEDUP,
            RunnerCapability.CDC,
            RunnerCapability.SCHEMA_DISCOVERY,
            RunnerCapability.AT_LEAST_ONCE,
        }
    )
    declared_modes: ClassVar[FrozenSet[str]] = frozenset({"embedded", "bring-your-own", "managed"})

    image_verifier: Optional[ImageSignatureVerifier] = None

    def plan(self, ctx: RunContext) -> RunPlan:
        return RunPlan(streams_planned=list(ctx.source.streams) or [ctx.source.kind])

    def run(self, ctx: RunContext) -> RunResult:
        return _execute(ctx, self)

    def replay(self, ctx: RunContext, run_id: str) -> RunResult:
        ctx.run_id = run_id
        return _execute(ctx, self)

    def fingerprint(self, ctx: RunContext) -> SchemaFingerprint:
        cols = [
            SchemaColumn(name=s, type="airbyte", nullable=True)
            for s in (ctx.source.streams or [ctx.source.kind])
        ]
        return SchemaFingerprint.of(cols, captured_at=utc_now_iso())


def _execute(ctx: RunContext, runner: AirbyteRunner) -> RunResult:
    started_at = utc_now_iso()
    t_start = time.time()

    props = ctx.contract.get("builds", [{}])[0].get("properties", {})
    airbyte_props = props.get("airbyte", {}) or {}
    deployment = airbyte_props.get("deployment", {}) or {}
    mode = deployment.get("mode", "bring-your-own")

    try:
        image_ref = resolve_connector_image(
            ctx.source.kind, override=airbyte_props.get("connector_image")
        )
    except ValueError as exc:
        return _failed(ctx, started_at, t_start, str(exc))
    sig_block = airbyte_props.get("image_signature") or {}
    if sig_block and runner.image_verifier is not None:
        sig_result = runner.image_verifier.verify(
            image_ref,
            public_key=sig_block.get("publicKey", ""),
            require_slsa_provenance=(sig_block.get("slsaProvenance") == "required"),
        )
        if not sig_result.signed:
            return _failed(
                ctx,
                started_at,
                t_start,
                f"connector image signature verification failed: {sig_result.error}",
            )

    if mode in ("bring-your-own", "managed"):
        return _execute_rest_mode(
            ctx, runner, deployment, airbyte_props, image_ref, started_at, t_start
        )
    if mode == "embedded":
        return _execute_embedded_mode(ctx, runner, airbyte_props, image_ref, started_at, t_start)
    return _failed(ctx, started_at, t_start, f"airbyte: unknown deployment.mode '{mode}'")


def _execute_rest_mode(
    ctx: RunContext,
    runner: AirbyteRunner,
    deployment: Dict[str, Any],
    airbyte_props: Dict[str, Any],
    image_ref: str,
    started_at: str,
    t_start: float,
) -> RunResult:
    server_url = deployment.get("server_url")
    if not server_url:
        return _failed(ctx, started_at, t_start, "airbyte REST mode requires deployment.server_url")
    auth = deployment.get("auth", {}) or {}
    api_token = auth.get("token") or auth.get("secretRef")  # secretRef resolved upstream

    workspace_id = airbyte_props.get("workspace_id", "default-workspace")
    expose = (ctx.contract.get("exposes") or [{}])[0]
    binding = expose.get("binding", {}) or {}

    client = AirbyteRestClient(server_url, api_token=api_token)
    try:
        # 1. Create source.
        source_body = {
            "workspaceId": workspace_id,
            "name": f"forge-{ctx.product_id}",
            "sourceDefinitionId": image_ref,
            "connectionConfiguration": dict(ctx.source.connection.raw),
        }
        source = client.create_source(source_body)
        source_id = source["sourceId"]

        # 2. Create destination.
        dest_body = {
            "workspaceId": workspace_id,
            "name": f"forge-dst-{ctx.product_id}",
            "destinationDefinitionId": _destination_image_for_binding(binding),
            "connectionConfiguration": dict(binding.get("location") or {}),
        }
        destination = client.create_destination(dest_body)
        destination_id = destination["destinationId"]

        # 3. Create connection.
        sync_mode = map_mode_to_sync_mode(ctx.source.mode.value)
        streams = ctx.source.streams or [ctx.source.kind]
        conn_body = {
            "name": f"forge-conn-{ctx.product_id}",
            "sourceId": source_id,
            "destinationId": destination_id,
            "namespaceDefinition": "destination",
            "syncCatalog": {
                "streams": [
                    {
                        "stream": {"name": s},
                        "config": {"syncMode": sync_mode, "destinationSyncMode": "append"},
                    }
                    for s in streams
                ]
            },
        }
        conn = client.create_connection(conn_body)
        connection_id = conn["connectionId"]

        # 4. Trigger sync.
        sync = client.trigger_sync(connection_id)
        job_id = sync.get("job", {}).get("id")
        status = sync.get("job", {}).get("status", "succeeded")

        # 5. Build per-stream results.
        stream_results = [
            StreamResult(name=s, state=RunState.SUCCEEDED, records=0, cursor_advanced=False)
            for s in streams
        ]

        finished_at = utc_now_iso()
        return RunResult(
            run_id=ctx.run_id,
            state=RunState.SUCCEEDED if status == "succeeded" else RunState.FAILED,
            streams=stream_results,
            started_at=started_at,
            finished_at=finished_at,
            records_total=0,
            bytes_total=0,
            dlq_records=0,
            facets={
                "engine": "airbyte",
                "mode": "rest",
                "duration_seconds": time.time() - t_start,
                "connection_id": connection_id,
                "job_id": job_id,
                "image_ref": image_ref,
            },
        )
    except Exception as exc:  # noqa: BLE001
        LOG.error("airbyte.rest.failed err=%s", exc, exc_info=True)
        return _failed(ctx, started_at, t_start, str(exc))
    finally:
        client.close()


def _execute_embedded_mode(
    ctx: RunContext,
    runner: AirbyteRunner,
    airbyte_props: Dict[str, Any],
    image_ref: str,
    started_at: str,
    t_start: float,
) -> RunResult:
    """PyAirbyte-based in-process mode. Skipped if PyAirbyte is not installed."""
    try:
        import airbyte as ab  # type: ignore[import-untyped]
    except ImportError:
        return _failed(
            ctx,
            started_at,
            t_start,
            "PyAirbyte not installed; install with `pip install airbyte` "
            "or use deployment.mode=bring-your-own with a server URL.",
        )

    try:
        source = ab.get_source(
            f"source-{ctx.source.kind}",
            config=dict(ctx.source.connection.raw),
        )
        if ctx.source.streams:
            source.select_streams(list(ctx.source.streams))
        else:
            source.select_all_streams()

        cache = ab.new_local_cache(cache_name=f"forge_{ctx.product_id.replace('.', '_')}")
        result = source.read(cache=cache)
        records_total = (
            sum(len(stream) for stream in result.streams.values())
            if hasattr(result, "streams")
            else 0
        )

        return RunResult(
            run_id=ctx.run_id,
            state=RunState.SUCCEEDED,
            streams=[
                StreamResult(name=s, state=RunState.SUCCEEDED, records=0)
                for s in (ctx.source.streams or [ctx.source.kind])
            ],
            started_at=started_at,
            finished_at=utc_now_iso(),
            records_total=records_total,
            bytes_total=0,
            dlq_records=0,
            facets={
                "engine": "airbyte",
                "mode": "embedded",
                "duration_seconds": time.time() - t_start,
                "image_ref": image_ref,
            },
        )
    except Exception as exc:  # noqa: BLE001
        return _failed(ctx, started_at, t_start, f"PyAirbyte run failed: {exc}")


def _destination_image_for_binding(binding: Dict[str, Any]) -> str:
    """Map binding.platform/format → Airbyte destination image."""
    fmt = binding.get("format", "")
    platform = binding.get("platform", "")
    if "snowflake" in fmt or platform == "snowflake":
        return "airbyte/destination-snowflake"
    if "bigquery" in fmt or platform == "gcp":
        return "airbyte/destination-bigquery"
    if "postgres" in fmt:
        return "airbyte/destination-postgres"
    return "airbyte/destination-jsonl"


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
        facets={"engine": "airbyte", "duration_seconds": time.time() - t_start},
    )


# ── Top-level entry point ──────────────────────────────────────────────


def execute_airbyte_build(
    build: Dict[str, Any],
    contract: Dict[str, Any],
    contract_dir: Path,
    *,
    dry_run: bool = False,
    sample_rows: Optional[int] = None,
    state_root: Optional[Path] = None,
    image_verifier: Optional[ImageSignatureVerifier] = None,
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
    runner = AirbyteRunner(image_verifier=image_verifier)
    if dry_run:
        plan = runner.plan(ctx)
        LOG.info("airbyte.dry-run streams=%s", plan.streams_planned)
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
