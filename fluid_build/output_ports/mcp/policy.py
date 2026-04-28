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

"""Access-control policy for the consumer-side MCP output-port server.

The output-port server is distinct from the authoring-side ``fluid mcp
serve`` (whose policy lives in :mod:`fluid_build.cli.mcp`). Authoring
mutates filesystem paths and store namespaces; the consumer-side
server queries production data, so its threat model and policy
surface differ:

* No writable filesystem roots — the server is read-only against the
  underlying engine and never writes consumer-supplied paths.
* No store namespaces — there is no logical/sidecar to mutate.
* New surface: ``allow_free_form_sql`` (default OFF) gates the
  free-form ``query_sql`` tool; without it, callers must use the
  predeclared semantic ``query`` tool.
* New surface: ``max_sample_rows`` (default 100) caps the row count
  any single ``sample`` call can return.
* Reused: ``allowed_tools`` / ``denied_tools`` / ``readable_paths``
  carry the same semantics as the authoring server so operators can
  apply consistent policies across both surfaces.

Column-level masking comes from ``expose.policy.authz.columnRestrictions``
inside the contract — it is not a server-side policy field, because the
restrictions belong to the data product, not the runtime.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Tuple


@dataclass(frozen=True)
class OutputPortPolicy:
    """Consumer-side MCP server policy.

    ``allowed_tools = None`` means "all tools allowed"; an empty tuple
    means "no tools allowed" — useful when an operator wants the
    server to advertise itself but reject every call (e.g. during a
    change freeze).

    Defaults are conservative: every option that could widen the
    server's surface is OFF until explicitly enabled.
    """

    read_only: bool = True
    """Reject any tool that mutates the underlying engine.

    All Phase-1 tools are read-only by design; this flag is reserved
    for forward-compat with future write tools and for symmetry with
    the authoring server. Default ``True`` so that any future write
    tool stays disabled until the operator opts in.
    """

    allowed_tools: Optional[Tuple[str, ...]] = None
    """Allowlist of tool names; ``None`` allows every advertised tool.

    Tools not in the allowlist are also hidden from ``tools/list`` so
    upstream agents (Claude Code, Cursor) do not advertise calls
    doomed to fail.
    """

    denied_tools: Tuple[str, ...] = ()
    """Blocklist of tool names. Evaluated before ``allowed_tools`` so
    denial wins."""

    readable_paths: Tuple[Path, ...] = field(
        default_factory=lambda: (Path.cwd().resolve(),)
    )
    """Filesystem roots the server may read from.

    Today the only path-based read is the contract YAML itself; the
    consumer-side server never opens caller-supplied paths. The field
    exists to keep the policy shape symmetric with authoring and to
    leave room for future tools (e.g. an OpenAPI doc resolver).
    """

    allow_free_form_sql: bool = False
    """Permit the optional ``query_sql`` tool to execute caller-
    supplied SQL.

    Default OFF because free-form SQL bypasses the semantic-layer
    safety net (predeclared measures + dimensions). When ON, the
    query compiler still calls
    :func:`fluid_build.providers._sql_safety.validate_sql_expression_allowlist`
    on every untrusted string, so the surface is bounded but wider.

    Operators should leave this OFF for LLM-driven agents and only
    enable it for trusted internal copilots that have to handle
    ad-hoc analyst questions.
    """

    max_sample_rows: int = 100
    """Hard cap for the ``sample`` tool's row count.

    A consumer can request fewer rows but never more. Defends against
    a curious agent that asks for ``limit: 100_000_000`` against a
    petabyte-scale lake. Default ``100`` is enough for "show me what
    this looks like" without exposing meaningful data volumes.
    """

    expose_id: Optional[str] = None
    """The exposeId this server is bound to.

    Phase-1 servers are single-expose. Phase-2 will widen this to
    ``Optional[Tuple[str, ...]]`` so one server can multiplex many
    exposes; the field is named in singular form today and migrates
    cleanly.
    """

    contract_path: Optional[Path] = None
    """Absolute path to the FLUID contract this server was started
    against.

    Used for audit logging and for the ``describe`` tool's
    ``contract_path`` reference. The path is resolved at startup, so
    a working-directory change later does not invalidate it.
    """

    def is_tool_allowed(self, tool: str) -> bool:
        """True iff ``tool`` is currently dispatchable by this policy."""
        if tool in self.denied_tools:
            return False
        if self.allowed_tools is None:
            return True
        return tool in self.allowed_tools
