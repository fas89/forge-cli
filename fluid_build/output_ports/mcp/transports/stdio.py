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

"""Line-delimited JSON-RPC over stdio for the MCP output-port server.

The transport is deliberately small: it owns the read-line / write-
JSON-RPC envelope only. Method dispatch, error mapping, and
permission checks live in
:mod:`fluid_build.output_ports.mcp.server` so the upcoming streamable-
HTTP transport can plug into the same dispatcher with no duplication.

A line is dropped silently when:

* it is empty / whitespace-only;
* it does not parse as JSON.

This matches the authoring server's behaviour and keeps the loop
robust against an upstream LLM that occasionally writes a stray
heartbeat line.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any, Callable, Dict, IO, Optional


def serve(
    *,
    handle_request: Callable[[Dict[str, Any]], Dict[str, Any]],
    logger: logging.Logger,
    stdin: Optional[IO[str]] = None,
    stdout: Optional[IO[str]] = None,
) -> int:
    """Run the stdio loop, calling ``handle_request`` per JSON-RPC
    message.

    ``handle_request`` returns the full JSON-RPC response envelope —
    including ``"jsonrpc": "2.0"``, ``"id": <request-id>``, and
    either ``"result"`` or ``"error"``. The transport never edits
    the envelope; if the caller emits a malformed envelope, the
    upstream MCP client surfaces it directly, which makes
    troubleshooting easier than silently re-wrapping.
    """
    in_stream = stdin or sys.stdin
    out_stream = stdout or sys.stdout
    for raw in in_stream:
        line = raw.strip()
        if not line:
            continue
        try:
            request = json.loads(line)
        except json.JSONDecodeError:
            logger.debug("dropping non-JSON stdin line: %s", line[:120])
            continue
        if not isinstance(request, dict):
            logger.debug("dropping non-object JSON stdin line: %s", line[:120])
            continue
        response = handle_request(request)
        # ``default=str`` coerces date / datetime / Decimal / UUID and
        # other non-JSON-native scalars into stable string forms so
        # the engine drivers can return rich types without each one
        # having to remember to coerce.
        out_stream.write(json.dumps(response, default=str) + "\n")
        out_stream.flush()
    return 0
