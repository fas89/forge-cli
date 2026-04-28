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

"""Consumer-side MCP output-port server.

Public API:

* :class:`OutputPortMcpServer` — JSON-RPC dispatcher bound to one
  expose. Transport-agnostic; pair with the stdio transport via
  :func:`run_stdio` or with a future streamable-HTTP transport.
* :class:`OutputPortPolicy` — access-control policy applied to every
  ``tools/call`` request.
* :func:`build_driver` — factory that resolves the right
  :class:`EngineDriver` for an expose's ``binding``.
* :func:`derive_advertised_tools` — render the ``tools/list``
  advertisement for a given expose + policy.
* :func:`compile_semantic_query` / :func:`compile_free_form_sql` —
  the two query-compilation paths (predeclared semantic / explicit
  SQL).

The CLI wrapper at :mod:`fluid_build.cli.mcp_output_port` builds an
:class:`OutputPortPolicy` from argparse, finds the bound expose, and
calls :func:`run_stdio`.
"""

from __future__ import annotations

from .drivers import (
    BigQueryDriver,
    DuckDBDriver,
    EngineDriver,
    SnowflakeDriver,
    UnsupportedBindingError,
    build_driver,
    register_driver,
    supported_keys,
)
from .policy import OutputPortPolicy
from .query_compiler import (
    CompiledQuery,
    compile_free_form_sql,
    compile_semantic_query,
)
from .server import (
    MCP_PROTOCOL_VERSION,
    SERVER_NAME,
    SERVER_VERSION,
    OutputPortMcpServer,
    find_expose,
    list_exposes,
    resolve_expose_paths,
    run_stdio,
)
from .tools import (
    OUTPUT_PORT_TOOL_CAPABILITIES,
    ToolCapability,
    check_tool_permission,
    derive_advertised_tools,
)

__all__ = [
    # Server
    "MCP_PROTOCOL_VERSION",
    "OutputPortMcpServer",
    "SERVER_NAME",
    "SERVER_VERSION",
    "find_expose",
    "list_exposes",
    "resolve_expose_paths",
    "run_stdio",
    # Policy + tools
    "OUTPUT_PORT_TOOL_CAPABILITIES",
    "OutputPortPolicy",
    "ToolCapability",
    "check_tool_permission",
    "derive_advertised_tools",
    # Query compilation
    "CompiledQuery",
    "compile_free_form_sql",
    "compile_semantic_query",
    # Drivers
    "BigQueryDriver",
    "DuckDBDriver",
    "EngineDriver",
    "SnowflakeDriver",
    "UnsupportedBindingError",
    "build_driver",
    "register_driver",
    "supported_keys",
]
