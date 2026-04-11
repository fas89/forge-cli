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

"""CI state file — the committed record of what produced committed CI files.

``ci-state.json`` is the **one exception** to "everything under
``.fluid/`` is engineer-personal state".  It lives under
``<product>/.fluid/ci-state.json`` (same hidden directory as the
per-engineer receipts and copilot memory) but it is **committed to
git** because:

1. The CI files it describes (``.github/workflows/*.yml``,
   ``.gitlab-ci.yml``, ``Jenkinsfile``, etc.) are themselves committed,
   and the state that describes "what inputs produced these files"
   must travel with them for cross-machine drift detection to work.

2. When Alice runs ``fluid forge --ci github_actions`` on her laptop
   and commits both the workflow file and ``ci-state.json``, Bob on
   another clone running ``fluid forge`` can read the recorded sha256
   and decide whether Alice's file is pristine (safe to regenerate
   silently) or hand-edited (prompt before clobbering).

3. Gitignoring ``ci-state.json`` would silently break drift detection
   across machines — the sha256 from Alice's run would never reach
   Bob, so Bob's forge run would skip every CI file out of caution.

The file's consumers in slices 7 and 8:

* **Slice 7** (this slice): ``_scaffold_ci_pipeline`` writes
  ``ci-state.json`` immediately after ``write_pipeline_files`` succeeds,
  recording the provider, complexity, environments, options, and the
  sha256 of every generated file.

* **Slice 8** (next): ``_resolve_ci_choice`` reads the recorded provider
  ahead of personal memory, and the collision check in
  ``_scaffold_ci_pipeline`` uses the recorded sha256 to distinguish
  pristine files (silent overwrite) from hand-edited ones (prompt).
"""

from __future__ import annotations

__all__ = [
    "CIStateDocument",
    "build_ci_state_payload",
    "write_ci_state",
]

import hashlib
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from fluid_build.cli.artifact_envelope import dump_json_with_envelope
from fluid_build.cli.artifact_paths import product_ci_state_path

LOG = logging.getLogger("fluid.cli.artifact_ci_state")


@dataclass
class CIStateDocument:
    """In-memory representation of the ci-state payload."""

    provider: str
    complexity: str
    environments: List[str] = field(default_factory=list)
    options: Dict[str, Any] = field(default_factory=dict)
    files: List[Dict[str, Any]] = field(default_factory=list)

    def to_payload(self) -> Dict[str, Any]:
        return {
            "provider": self.provider,
            "complexity": self.complexity,
            "environments": list(self.environments),
            "options": dict(self.options),
            "files": list(self.files),
        }


def build_ci_state_payload(
    *,
    provider: str,
    complexity: str,
    environments: Optional[List[str]] = None,
    options: Optional[Dict[str, Any]] = None,
    written_files: List[Path],
    product_root: Path,
) -> CIStateDocument:
    """Build a :class:`CIStateDocument` from the files that were just written.

    *written_files* is the list returned by
    :func:`pipeline_generator.write_pipeline_files`.  Each path is
    re-anchored relative to *product_root* so the state file stays
    portable across clones.  sha256 is computed from the on-disk bytes
    (matching the provenance header's prefix injected during write).
    """
    files: List[Dict[str, Any]] = []
    root = Path(product_root).resolve()
    for path in written_files:
        abs_path = Path(path).resolve()
        try:
            rel = str(abs_path.relative_to(root))
        except ValueError:
            rel = str(abs_path)
        sha: Optional[str] = None
        try:
            sha = hashlib.sha256(abs_path.read_bytes()).hexdigest()
        except OSError:
            sha = None
        entry: Dict[str, Any] = {"path": rel}
        if sha is not None:
            entry["sha256"] = sha
        try:
            entry["size"] = abs_path.stat().st_size
        except OSError:
            pass
        files.append(entry)

    return CIStateDocument(
        provider=provider,
        complexity=complexity,
        environments=list(environments or []),
        options=dict(options or {}),
        files=files,
    )


def write_ci_state(
    doc: CIStateDocument,
    product_root: Path,
    *,
    command: str,
    tool_version: str,
    logger: Optional[logging.Logger] = None,
) -> Optional[Path]:
    """Serialise *doc* to ``<product>/.fluid/ci-state.json``.

    Returns the written path on success or ``None`` on filesystem error
    (write failures never propagate — CI state is best-effort, same
    discipline as the receipts).
    """
    log = logger or LOG
    try:
        target = product_ci_state_path(product_root)
        target.parent.mkdir(parents=True, exist_ok=True)
        body = dump_json_with_envelope(
            doc.to_payload(),
            kind="CIState",
            command=command,
            tool_version=tool_version,
        )
        target.write_text(body, encoding="utf-8")
        log.debug("ci_state_written", extra={"path": str(target)})
        return target
    except OSError as exc:
        log.debug("ci_state_write_failed", extra={"error": str(exc)})
        return None
