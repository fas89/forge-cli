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

"""Personal memory for individual engineers — persists preferences across projects.

Stores lightweight preferences in ``~/.fluid/engineer_memory.json``.
This is a plain JSON file the user can edit or delete directly
(like ``~/.dbt/profiles.yml`` or ``~/.config/gh/hosts.yml``).

No external dependencies — mem0 can be added later as a provider adapter.
"""

from __future__ import annotations

__all__ = [
    "load_personal_memory",
    "save_personal_memory",
]

import json
import logging
import stat
from pathlib import Path
from typing import Any, Dict, Optional

LOG = logging.getLogger("fluid.cli.forge.personal_memory")

_MEMORY_FILE = Path.home() / ".fluid" / "engineer_memory.json"
_MAX_ITEMS = 5  # Max items per list (recent domains, engines, etc.)


def load_personal_memory() -> Optional[Dict[str, Any]]:
    """Load personal preferences from ``~/.fluid/engineer_memory.json``.

    Returns ``None`` if no memory file exists.
    """
    try:
        if not _MEMORY_FILE.exists():
            return None
        data = json.loads(_MEMORY_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            LOG.debug("Loaded personal memory from %s", _MEMORY_FILE)
            return data
        return None
    except (json.JSONDecodeError, OSError):
        return None


def save_personal_memory(context: Dict[str, Any], console: Any = None) -> bool:
    """Update personal memory from the latest successful forge run.

    Merges new preferences into existing memory (doesn't overwrite).
    Shows a hint on first save telling the user where the file is.
    """
    existing = load_personal_memory() or {}
    is_first_save = not existing

    # Extract preferences from context
    prefs = {
        "preferred_provider": context.get("provider") or existing.get("preferred_provider"),
        "preferred_engine": context.get("build_engine") or existing.get("preferred_engine"),
        "preferred_domain": context.get("domain") or existing.get("preferred_domain"),
        "owner_team": context.get("owner_team") or existing.get("owner_team"),
    }

    # Track recent domains (FIFO, max _MAX_ITEMS)
    recent_domains = list(existing.get("recent_domains") or [])
    domain = context.get("domain")
    if domain and domain not in recent_domains:
        recent_domains.insert(0, domain)
        recent_domains = recent_domains[:_MAX_ITEMS]
    prefs["recent_domains"] = recent_domains

    # Track recent use cases
    recent_use_cases = list(existing.get("recent_use_cases") or [])
    use_case = context.get("use_case")
    if use_case and use_case not in recent_use_cases:
        recent_use_cases.insert(0, use_case)
        recent_use_cases = recent_use_cases[:_MAX_ITEMS]
    prefs["recent_use_cases"] = recent_use_cases

    # Remove None values
    prefs = {k: v for k, v in prefs.items() if v is not None}

    try:
        _MEMORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        _MEMORY_FILE.write_text(json.dumps(prefs, indent=2), encoding="utf-8")
        _MEMORY_FILE.chmod(stat.S_IRUSR | stat.S_IWUSR)
        LOG.debug("Saved personal memory to %s", _MEMORY_FILE)

        # Show hint on first save
        if is_first_save and console:
            try:
                console.print(
                    f"\n[dim]Your preferences saved to {_MEMORY_FILE}[/dim]\n"
                    "[dim](Edit or delete this file to reset your preferences.)[/dim]"
                )
            except Exception as exc:  # noqa: BLE001
                LOG.debug("Could not print first-save hint: %s", exc)

        return True
    except OSError as exc:
        LOG.debug("Could not save personal memory: %s", exc)
        return False
