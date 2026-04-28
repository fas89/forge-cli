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

"""Consumer-side MCP output-port server — JSON-RPC dispatcher.

Each instance is bound to one ``expose`` from one FLUID contract.
The dispatcher answers four MCP methods:

* ``initialize`` — protocol handshake.
* ``tools/list`` — advertises the four tools (``describe``,
  ``sample``, ``query``, optional ``query_sql``).
* ``tools/call`` — runs a tool with permission checks + audit
  logging.
* ``resources/list`` + ``resources/read`` — exposes the contract
  YAML and semantic JSON as MCP resources so an LLM can browse
  metadata without spending a tool call.

The dispatcher is transport-agnostic; the entry point
:func:`run_stdio` wires it to the line-delimited stdio transport.
A future :func:`run_streamable_http` will share the dispatcher.

Audit:

* Every ``tools/call`` writes one JSON document to
  ``~/.fluid/store/audit/`` via
  :func:`fluid_build.copilot.store.audit_trail.write_audit_event`.
* Argument values are not redacted today — the consumer-side wire
  carries no secrets (credentials live in env vars / keyring) so
  arguments are safe to log. If a consumer ever passes a sensitive
  literal in a filter, the existing
  :class:`fluid_build.observability.SecretRedactingFilter` (installed
  by the CLI bootstrap) catches it on the log path.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

import yaml

from fluid_build.copilot.store.audit_trail import write_audit_event

from .drivers import EngineDriver, build_driver
from .drivers.base import UnsupportedBindingError
from .policy import OutputPortPolicy
from .query_compiler import compile_free_form_sql, compile_semantic_query
from .tools import (
    OUTPUT_PORT_TOOL_CAPABILITIES,
    check_tool_permission,
    derive_advertised_tools,
)

MCP_PROTOCOL_VERSION = "2025-06-18"
SERVER_NAME = "forge-cli-output-port-mcp"
SERVER_VERSION = "0.1.0"
DEFAULT_QUERY_TIMEOUT_SECONDS = 60.0


class OutputPortMcpServer:
    """JSON-RPC dispatcher for the consumer MCP output-port server."""

    def __init__(
        self,
        *,
        contract: Mapping[str, Any],
        expose: Mapping[str, Any],
        policy: OutputPortPolicy,
        logger: Optional[logging.Logger] = None,
        driver: Optional[EngineDriver] = None,
        query_timeout_seconds: float = DEFAULT_QUERY_TIMEOUT_SECONDS,
    ) -> None:
        self.contract: Mapping[str, Any] = contract
        self.expose: Mapping[str, Any] = expose
        self.policy: OutputPortPolicy = policy
        self.logger = logger or logging.getLogger("fluid.output_port.mcp.server")
        self.query_timeout_seconds = query_timeout_seconds
        self._driver: Optional[EngineDriver] = driver

    # ------------------------------------------------------------------
    # Public dispatch surface
    # ------------------------------------------------------------------

    def handle_request(self, request: Mapping[str, Any]) -> Dict[str, Any]:
        """Dispatch one JSON-RPC request and return the full response.

        Never raises — every exception is mapped to a JSON-RPC error
        envelope so the stdio loop stays alive across malformed
        upstream traffic.
        """
        request_id = request.get("id")
        method = request.get("method")
        response: Dict[str, Any] = {"jsonrpc": "2.0", "id": request_id}
        try:
            if method == "initialize":
                response["result"] = self._handle_initialize(request)
            elif method == "tools/list":
                response["result"] = {"tools": self._render_advertised_tools()}
            elif method == "tools/call":
                response["result"] = self._handle_tools_call(request)
            elif method == "resources/list":
                response["result"] = {"resources": self._render_resources()}
            elif method == "resources/read":
                response["result"] = self._handle_resources_read(request)
            elif method == "ping":
                response["result"] = {}
            elif method in {"notifications/initialized", "notifications/cancelled"}:
                # MCP "notifications" carry no response — but we still
                # need to write an envelope when an id is present so
                # the upstream client doesn't time-out waiting.
                if request_id is None:
                    return {}
                response["result"] = {}
            else:
                response["error"] = {
                    "code": -32601,
                    "message": f"Method {method!r} not implemented",
                }
        except PermissionError as exc:
            self.logger.debug("permission_denied: %s", exc)
            response["error"] = {"code": -32001, "message": f"Permission denied: {exc}"}
        except UnsupportedBindingError as exc:
            self.logger.debug("unsupported_binding: %s", exc)
            response["error"] = {"code": -32002, "message": f"Unsupported binding: {exc}"}
        except ValueError as exc:
            self.logger.debug("invalid_arguments: %s", exc)
            response["error"] = {"code": -32602, "message": f"Invalid arguments: {exc}"}
        except Exception as exc:  # noqa: BLE001
            self.logger.debug("internal_error: %s", exc)
            response["error"] = {
                "code": -32000,
                "message": _annotate_engine_error(exc, expose=self.expose),
            }
        return response

    # ------------------------------------------------------------------
    # Method handlers
    # ------------------------------------------------------------------

    def _handle_initialize(self, request: Mapping[str, Any]) -> Dict[str, Any]:
        params = request.get("params") or {}
        return {
            "protocolVersion": params.get("protocolVersion") or MCP_PROTOCOL_VERSION,
            "capabilities": {
                "tools": {"listChanged": False},
                "resources": {"subscribe": False, "listChanged": False},
            },
            "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
        }

    def _handle_tools_call(self, request: Mapping[str, Any]) -> Dict[str, Any]:
        params = request.get("params") or {}
        tool_name = str(params.get("name") or "")
        arguments = params.get("arguments") or {}
        if not isinstance(arguments, Mapping):
            raise ValueError("tools/call.arguments must be an object")
        check_tool_permission(
            tool_name,
            allowed_tools=self.policy.allowed_tools,
            denied_tools=self.policy.denied_tools,
            allow_free_form_sql=self.policy.allow_free_form_sql,
        )
        started = time.monotonic()
        payload = self._dispatch_tool(tool_name, arguments)
        elapsed_ms = round((time.monotonic() - started) * 1000, 2)
        self._audit_tools_call(tool_name=tool_name, arguments=arguments, elapsed_ms=elapsed_ms)
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(payload, indent=2, default=str),
                }
            ],
            "isError": False,
            "structuredContent": payload,
        }

    def _handle_resources_read(self, request: Mapping[str, Any]) -> Dict[str, Any]:
        params = request.get("params") or {}
        uri = str(params.get("uri") or "")
        if not uri:
            raise ValueError("resources/read.uri is required")
        for resource in self._render_resources():
            if resource["uri"] == uri:
                return {
                    "contents": [
                        {
                            "uri": uri,
                            "mimeType": resource["mimeType"],
                            "text": self._read_resource_body(resource),
                        }
                    ]
                }
        raise ValueError(f"Unknown resource uri: {uri}")

    # ------------------------------------------------------------------
    # Tool dispatch
    # ------------------------------------------------------------------

    def _dispatch_tool(self, tool: str, arguments: Mapping[str, Any]) -> Dict[str, Any]:
        if tool == "describe":
            return self._tool_describe()
        if tool == "sample":
            limit = self._normalised_limit(arguments.get("limit"))
            return self._tool_sample(limit=limit)
        if tool == "query":
            return self._tool_query(arguments)
        if tool == "query_sql":
            return self._tool_query_sql(arguments)
        raise RuntimeError(f"Unknown tool {tool}")

    def _tool_describe(self) -> Dict[str, Any]:
        descriptor = self._driver_descriptor_with_fallback()
        expose = self.expose
        return {
            "exposeId": expose.get("exposeId"),
            "title": expose.get("title"),
            "description": expose.get("description"),
            "version": expose.get("version"),
            "kind": expose.get("kind"),
            "binding": expose.get("binding"),
            "contract": expose.get("contract"),
            "qos": expose.get("qos"),
            "policy": _strip_secrets_from_policy(expose.get("policy")),
            "semantics": expose.get("semantics"),
            "lifecycle": expose.get("lifecycle"),
            "observability": expose.get("observability"),
            "engine": {
                "platform": descriptor.platform,
                "format": descriptor.format,
                "dialect": descriptor.dialect,
                "tableReference": descriptor.table_reference,
                "capabilities": descriptor.capabilities,
            },
            "restrictedColumns": sorted(self._driver_or_build().restricted_columns)
            if self._can_load_driver()
            else [],
        }

    def _tool_sample(self, *, limit: int) -> Dict[str, Any]:
        driver = self._driver_or_build()
        result = driver.sample(limit=limit)
        return {
            "columns": list(result.columns),
            "rows": [dict(row) for row in result.rows],
            "rowCount": len(result.rows),
            "truncated": result.truncated,
        }

    def _tool_query(self, arguments: Mapping[str, Any]) -> Dict[str, Any]:
        driver = self._driver_or_build()
        descriptor = driver.descriptor()
        compiled = compile_semantic_query(
            expose=self.expose,
            metric=arguments.get("metric"),
            measure=arguments.get("measure"),
            dimensions=list(arguments.get("dimensions") or []),
            filters=arguments.get("filters") or {},
            limit=self._normalised_limit(arguments.get("limit")),
            table_reference=descriptor.table_reference,
        )
        rendered_sql = compiled.render_sql_for_dialect(descriptor.dialect)
        result = driver.query(
            sql=rendered_sql,
            params=compiled.params,
            projection=compiled.columns,
        )
        return {
            "columns": list(result.columns),
            "rows": [dict(row) for row in result.rows],
            "rowCount": len(result.rows),
            "compiledSql": rendered_sql,
            "boundParameters": list(compiled.params),
        }

    def _tool_query_sql(self, arguments: Mapping[str, Any]) -> Dict[str, Any]:
        driver = self._driver_or_build()
        descriptor = driver.descriptor()
        # Pass the driver's restricted-column set into the compiler so
        # it rejects free-form SQL that references a denied column —
        # closes the alias-based bypass where ``SELECT email AS x``
        # would otherwise slip past the result-set masking step
        # (which only matches engine-reported column names).
        compiled = compile_free_form_sql(
            sql=str(arguments.get("sql") or ""),
            table_reference=descriptor.table_reference,
            limit=self._normalised_limit(arguments.get("limit")),
            restricted_columns=driver.restricted_columns,
        )
        result = driver.execute(
            sql=compiled.render_sql_for_dialect(descriptor.dialect),
            params=compiled.params,
            timeout_seconds=self.query_timeout_seconds,
        )
        visible_columns, rows = driver.project(result.rows, columns=result.columns)
        return {
            "columns": list(visible_columns),
            "rows": [dict(row) for row in rows],
            "rowCount": len(rows),
            "compiledSql": compiled.render_sql_for_dialect(descriptor.dialect),
        }

    # ------------------------------------------------------------------
    # Resources
    # ------------------------------------------------------------------

    def _render_resources(self) -> List[Dict[str, Any]]:
        expose_id = self.expose.get("exposeId") or "expose"
        resources: List[Dict[str, Any]] = []
        contract_path = self.policy.contract_path
        if contract_path is not None:
            resources.append(
                {
                    "uri": f"fluid://contract/{expose_id}",
                    "name": "FLUID Contract",
                    "description": (
                        "The full FLUID contract YAML this server was started "
                        "against. Use this to inspect every expose, build, "
                        "and policy block — not just the bound expose."
                    ),
                    "mimeType": "application/x-yaml",
                    "_source": "contract_path",
                }
            )
        resources.append(
            {
                "uri": f"fluid://expose/{expose_id}",
                "name": "Expose JSON",
                "description": (
                    "The bound expose's contract block, semantic model, "
                    "QoS, and policy as a JSON document. Cheaper than "
                    "calling 'describe' when you just want metadata."
                ),
                "mimeType": "application/json",
                "_source": "expose_json",
            }
        )
        if self.expose.get("semantics"):
            resources.append(
                {
                    "uri": f"fluid://semantics/{expose_id}",
                    "name": "Semantic Model",
                    "description": (
                        "Semantic model entities, measures, dimensions, and "
                        "metrics for this expose. Drives the 'query' tool."
                    ),
                    "mimeType": "application/json",
                    "_source": "semantics_json",
                }
            )
        return resources

    def _read_resource_body(self, resource: Mapping[str, Any]) -> str:
        source = resource.get("_source")
        if source == "contract_path":
            path = self.policy.contract_path
            if path is None:
                raise ValueError("contract_path resource is unavailable")
            return Path(path).read_text(encoding="utf-8")
        if source == "expose_json":
            return json.dumps(_jsonable(self.expose), indent=2, default=str)
        if source == "semantics_json":
            return json.dumps(_jsonable(self.expose.get("semantics") or {}), indent=2, default=str)
        raise ValueError(f"unsupported resource source: {source}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _render_advertised_tools(self) -> List[Dict[str, Any]]:
        return derive_advertised_tools(
            expose=self.expose,
            allow_free_form_sql=self.policy.allow_free_form_sql,
            extra_denied=self.policy.denied_tools,
        )

    def _normalised_limit(self, raw: Any) -> int:
        cap = self.policy.max_sample_rows
        if raw is None:
            return cap
        if not isinstance(raw, int) or raw < 1:
            raise ValueError("limit must be a positive integer")
        return min(raw, cap)

    def _can_load_driver(self) -> bool:
        try:
            self._driver_or_build()
            return True
        except Exception:  # noqa: BLE001
            return False

    def _driver_or_build(self) -> EngineDriver:
        if self._driver is not None:
            return self._driver
        # Relative ``binding.location.path`` references resolve
        # against the contract's parent directory so an example
        # contract can ship a ``./customers.csv`` next to it without
        # forcing every operator to author absolute paths.
        expose_for_driver = self._expose_with_resolved_paths()
        driver = build_driver(
            expose=expose_for_driver, contract=self.contract, logger=self.logger
        )
        self._driver = driver
        return driver

    def _expose_with_resolved_paths(self) -> Mapping[str, Any]:
        contract_dir = (
            self.policy.contract_path.parent if self.policy.contract_path else None
        )
        return resolve_expose_paths(self.expose, contract_dir=contract_dir)

    def _driver_descriptor_with_fallback(self):
        try:
            return self._driver_or_build().descriptor()
        except UnsupportedBindingError as exc:
            from .drivers.base import DriverDescriptor

            self.logger.debug("describe_without_driver: %s", exc)
            binding = self.expose.get("binding") or {}
            return DriverDescriptor(
                platform=str(binding.get("platform") or ""),
                format=str(binding.get("format") or ""),
                table_reference="",
                dialect="unknown",
                capabilities={
                    "describe": True,
                    "sample": False,
                    "query": False,
                    "query_sql": False,
                },
            )

    def _audit_tools_call(
        self,
        *,
        tool_name: str,
        arguments: Mapping[str, Any],
        elapsed_ms: float,
    ) -> None:
        try:
            write_audit_event(
                "mcp_output_port_tools_call",
                payload={
                    "tool": tool_name,
                    "exposeId": self.expose.get("exposeId"),
                    "contractPath": str(self.policy.contract_path)
                    if self.policy.contract_path is not None
                    else None,
                    "elapsedMs": elapsed_ms,
                    "argumentSummary": _summarise_arguments(arguments),
                },
            )
        except Exception as exc:  # noqa: BLE001
            # Audit logging should never crash the dispatcher. Keep
            # the failure observable on the debug log so an operator
            # can spot a misconfigured ~/.fluid directory.
            self.logger.debug("audit_log_failed: %s", exc)


# ---------------------------------------------------------------------
# Module-level conveniences
# ---------------------------------------------------------------------


def run_stdio(
    *,
    contract: Mapping[str, Any],
    expose: Mapping[str, Any],
    policy: OutputPortPolicy,
    logger: Optional[logging.Logger] = None,
) -> int:
    """Build a server and run it on the stdio transport.

    Wired by :mod:`fluid_build.cli.mcp_output_port`.
    """
    from .transports import stdio as stdio_transport

    server_logger = logger or logging.getLogger("fluid.output_port.mcp.server")
    server = OutputPortMcpServer(
        contract=contract,
        expose=expose,
        policy=policy,
        logger=server_logger,
    )
    return stdio_transport.serve(
        handle_request=server.handle_request,
        logger=server_logger,
    )


def find_expose(
    contract: Mapping[str, Any], expose_id: Optional[str]
) -> Mapping[str, Any]:
    """Return an ``expose`` block from the contract.

    When ``expose_id`` is ``None`` and the contract has exactly one
    expose, that expose is returned automatically — the common case
    for single-expose contracts. When the contract has multiple
    exposes and no id is given, a ``ValueError`` lists them so the
    operator can pick.

    When an id IS given but doesn't match, the error message lists
    the ids that DO exist so an operator can fix the typo without
    re-reading the contract.
    """
    available: List[str] = []
    matches: List[Mapping[str, Any]] = []
    for expose in contract.get("exposes") or []:
        if not isinstance(expose, Mapping):
            continue
        candidate = expose.get("exposeId")
        if isinstance(candidate, str):
            available.append(candidate)
            if expose_id is not None and candidate == expose_id:
                return expose
            matches.append(expose)
    if expose_id is None:
        if len(matches) == 1:
            return matches[0]
        if not matches:
            raise ValueError("Contract has no exposes")
        raise ValueError(
            f"Contract has {len(matches)} exposes; pass --expose-id to "
            f"pick one. Available: {available}"
        )
    raise ValueError(
        f"exposeId {expose_id!r} not found in contract; available: {available or 'none'}"
    )


def resolve_expose_paths(
    expose: Mapping[str, Any], *, contract_dir: Optional[Path]
) -> Mapping[str, Any]:
    """Return a copy of ``expose`` with relative ``binding.location.path``
    resolved against ``contract_dir``.

    Lets example contracts ship a ``path: ./customers.csv`` next to
    the YAML without forcing operators to author absolute paths.
    Absolute paths and missing paths pass through unchanged.
    """
    if contract_dir is None:
        return expose
    binding = expose.get("binding") or {}
    location = binding.get("location") or {}
    raw_path = location.get("path")
    if not isinstance(raw_path, str) or not raw_path:
        return expose
    candidate = Path(raw_path).expanduser()
    if candidate.is_absolute():
        return expose
    resolved = (contract_dir / candidate).resolve()
    new_location = dict(location)
    new_location["path"] = str(resolved)
    new_binding = dict(binding)
    new_binding["location"] = new_location
    new_expose = dict(expose)
    new_expose["binding"] = new_binding
    return new_expose


def list_exposes(contract: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Return a lightweight summary of every expose in ``contract``.

    Used by ``fluid mcp output-port list`` to show operators what's
    available without parsing the full YAML themselves. Each entry
    carries the bare minimum a human needs to pick: ``exposeId``,
    ``kind``, ``title``, the binding's ``platform``/``format``, and
    a short engine-readable ``tableReference``.
    """
    out: List[Dict[str, Any]] = []
    for expose in contract.get("exposes") or []:
        if not isinstance(expose, Mapping):
            continue
        binding = expose.get("binding") or {}
        location = binding.get("location") or {}
        table_reference = _format_table_reference(binding, location)
        out.append(
            {
                "exposeId": expose.get("exposeId"),
                "kind": expose.get("kind"),
                "title": expose.get("title"),
                "platform": binding.get("platform"),
                "format": binding.get("format"),
                "tableReference": table_reference,
                "hasSemantics": bool(expose.get("semantics")),
                "hasMcpOverrides": bool(expose.get("mcp")),
            }
        )
    return out


def _format_table_reference(
    binding: Mapping[str, Any], location: Mapping[str, Any]
) -> str:
    """Best-effort, human-readable engine reference for the
    ``list`` summary. Driver-specific quoting is NOT applied — this
    string is for display, not execution."""
    fmt = str(binding.get("format") or "")
    if fmt == "bigquery_table":
        parts = [location.get("project"), location.get("dataset"), location.get("table")]
        return ".".join(p for p in parts if p) or "<unknown>"
    if fmt == "snowflake_table":
        parts = [
            location.get("database") or location.get("dataset"),
            location.get("schema"),
            location.get("table"),
        ]
        return ".".join(p for p in parts if p) or "<unknown>"
    if fmt in {"csv", "parquet", "json"}:
        path = location.get("path") or location.get("table") or "<unknown>"
        return str(path)
    return str(location.get("table") or location.get("name") or "<unknown>")


# ---------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------


_ENGINE_HINT_PATTERNS: List[Tuple[str, str]] = [
    (
        "does not exist or not authorized",
        "engine reports the table is missing or the role can't see it. "
        "Check binding.location.{database,schema,table} (Snowflake / "
        "BigQuery: project.dataset.table) and confirm the connection's "
        "role/credentials grant SELECT on the bound table.",
    ),
    (
        "Object not found",
        "engine reports the bound object does not exist. Verify the "
        "binding.location values against the contract's expose block.",
    ),
    (
        "Access Denied",
        "engine refused the query for permissions. Check that the "
        "connection's role/service-account has SELECT on the bound "
        "table; column-level grants may also be required when the "
        "expose carries policy.authz.columnRestrictions.",
    ),
    (
        "Authentication failed",
        "Snowflake authentication failed. Verify SNOWFLAKE_ACCOUNT, "
        "SNOWFLAKE_USER, and one of SNOWFLAKE_PASSWORD / "
        "SNOWFLAKE_PRIVATE_KEY_PATH in the server's environment.",
    ),
    (
        "could not be authenticated",
        "BigQuery authentication failed. Verify "
        "GOOGLE_APPLICATION_CREDENTIALS or the active gcloud auth "
        "context can call the bound project.",
    ),
    (
        "Catalog Error",
        "DuckDB couldn't resolve the bound table. Verify "
        "binding.location.path points at a readable file and "
        "binding.location.table matches the file's logical name.",
    ),
]


def _annotate_engine_error(exc: BaseException, *, expose: Mapping[str, Any]) -> str:
    """Attach an actionable hint to a raw engine exception.

    Driver clients (Snowflake, BigQuery, DuckDB) return long,
    technical error strings that an LLM agent or operator has to
    decode. We pattern-match the most common shapes and append a
    one-line "what to fix" hint with the relevant binding fields,
    so the wire response is structured help instead of raw stack
    text.
    """
    raw = str(exc)
    expose_id = str(expose.get("exposeId") or "<unknown>")
    binding = expose.get("binding") or {}
    binding_summary = (
        f"platform={binding.get('platform')} format={binding.get('format')}"
    )
    for needle, hint in _ENGINE_HINT_PATTERNS:
        if needle.lower() in raw.lower():
            return f"{raw}\n\nHint: {hint} (expose={expose_id}, {binding_summary})"
    return raw


def _summarise_arguments(arguments: Mapping[str, Any]) -> Dict[str, Any]:
    """Strip large or sensitive blobs from an argument dict before
    audit logging.

    Today only ``sql`` is summarised — long SQL bodies are truncated
    to keep the audit document small. Filter values are kept in full
    because they're typed scalars from a predeclared dimension.
    """
    summary: Dict[str, Any] = {}
    for key, value in arguments.items():
        if key == "sql" and isinstance(value, str) and len(value) > 256:
            summary[key] = value[:256] + "... [truncated]"
        else:
            summary[key] = value
    return summary


def _strip_secrets_from_policy(policy: Optional[Any]) -> Optional[Any]:
    """Return a copy of ``expose.policy`` safe to ship to the LLM.

    Today the schema doesn't put secrets under ``expose.policy`` —
    authn is just an enum, authz lists principals, classifications
    are strings. This function is here as a defence-in-depth point to
    edit when the schema gains a credential-bearing field. The
    no-op return is intentional today.
    """
    return policy


def _jsonable(value: Any) -> Any:
    """Coerce a contract fragment into JSON-serialisable form.

    YAML loads can return ``OrderedDict`` / ``datetime`` /
    ``decimal.Decimal``; ``json.dumps(default=str)`` handles them, but
    we apply an explicit pass for stable test output.
    """
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    return value
