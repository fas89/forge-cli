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

"""Workspace-level ``AGENTS.md`` loader for the forge copilot agent loop.

``AGENTS.md`` (sibling of ``CLAUDE.md``) is the team's free-form guide
for AI agents working in the repo — the canonical place to encode
conventions ("we always use Snowflake Tasks, never MWAA"), preferred
defaults ("prefer ``amend`` over ``replace``"), and "always" / "never"
rules that don't fit the schema-bound ``agent-policy`` block in the
contract.  Loading the file into the agent loop's system prompt lets
the copilot honor those conventions without code or prompt changes.

Security
--------

* The file is read **only** from ``workspace_root``.  Nested
  ``AGENTS.md`` files in sub-directories are not auto-loaded.
* Symlinks whose target resolves outside ``workspace_root`` are
  rejected (mirrors the workspace-confinement pattern from
  ``cli/security.py``).
* Content is truncated at :data:`MAX_AGENTS_MD_BYTES` so a runaway
  file cannot dominate the LLM context window.  Truncation is
  signalled to the LLM via an explicit trailing marker so it doesn't
  treat the cut as malformed input.
"""

from __future__ import annotations

__all__ = [
    "AGENTS_MD_FILENAME",
    "MAX_AGENTS_MD_BYTES",
    "load_agents_md",
]

import logging
from pathlib import Path
from typing import Optional

LOG = logging.getLogger("fluid.cli.forge_copilot.agents_md")

AGENTS_MD_FILENAME = "AGENTS.md"

# 16 KB ceiling — fits the vast majority of real-world AGENTS.md
# files (forge-cli's own is 24 KB but that's an outlier full of
# repo-architecture docs the copilot loop doesn't need). The
# system prompt is rebuilt once per loop and Anthropic-style
# prompt caching makes the per-iteration cost negligible, so the
# ceiling is bounded primarily for hygiene rather than cost.
MAX_AGENTS_MD_BYTES = 16 * 1024


def load_agents_md(
    workspace_root: Path,
    *,
    max_bytes: int = MAX_AGENTS_MD_BYTES,
) -> Optional[str]:
    """Read ``AGENTS.md`` from *workspace_root* if present and safe.

    Returns the file contents (UTF-8 decoded, optionally truncated)
    or ``None`` if the file is absent, empty, or fails the
    workspace-confinement check.

    A truncated file is annotated with a trailing marker so the LLM
    can see the cut was deliberate rather than a parse error.
    """
    try:
        ws_root = workspace_root.resolve()
    except (OSError, RuntimeError):
        return None

    candidate = ws_root / AGENTS_MD_FILENAME
    try:
        if not candidate.is_file():
            return None
        # ``resolve(strict=True)`` follows symlinks. The
        # ``relative_to`` check then confirms the *real* target stays
        # inside the workspace — a symlink pointing at /etc/passwd
        # raises ``ValueError`` here and we return ``None``.
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(ws_root)
    except (OSError, ValueError):
        LOG.debug(
            "Skipping AGENTS.md outside workspace_root %s",
            ws_root,
        )
        return None

    try:
        # Read up to ``max_bytes + 1`` so we know whether truncation
        # was needed without slurping the entire file when it's large.
        with resolved.open("rb") as fh:
            raw = fh.read(max_bytes + 1)
    except OSError as exc:
        LOG.debug("Could not read AGENTS.md: %s", exc)
        return None

    if not raw:
        return None

    truncated = len(raw) > max_bytes
    if truncated:
        raw = raw[:max_bytes]

    text = raw.decode("utf-8", errors="replace").strip()
    if not text:
        return None

    if truncated:
        text = f"{text}\n\n[truncated — AGENTS.md exceeds {max_bytes} bytes]"

    return text
