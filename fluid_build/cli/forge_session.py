# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Lightweight session-state container for the forge REPL/launcher UX.

This is *additive* — no existing command uses ForgeSession today. The REPL
and the onboarding wizard populate it so that slash commands have a single
place to read state (provider, mode, memory-on flag, etc.).

Persistence is session-scoped (``~/.fluid/session.json``) and intentionally
separate from the project-scoped copilot memory at
``runtime/.state/copilot-memory.json`` — that remains the authoritative
store for per-project preferences.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

LOG = logging.getLogger("fluid.cli.forge_session")


def _default_config_dir() -> Path:
    override = os.environ.get("FLUID_CONFIG_DIR")
    if override:
        return Path(override).expanduser()
    return Path.home() / ".fluid"


@dataclass
class ForgeSession:
    """Transient state shared across a single forge REPL/launcher invocation."""

    provider: Optional[str] = None
    mode: str = "copilot"
    config_dir: Path = field(default_factory=_default_config_dir)
    project_root: Path = field(default_factory=Path.cwd)
    memory_on: bool = True
    last_result: Optional[str] = None
    history: List[str] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)
    # Populated lazily by refresh_discovery/load_memory to keep imports cheap.
    discovery: Optional[Dict[str, Any]] = None
    memory_snapshot: Optional[Dict[str, Any]] = None

    # ----- serialization --------------------------------------------------

    def _session_file(self) -> Path:
        return self.config_dir / "session.json"

    def save(self) -> None:
        try:
            self.config_dir.mkdir(parents=True, exist_ok=True)
            payload = {
                "provider": self.provider,
                "mode": self.mode,
                "memory_on": self.memory_on,
                "last_result": self.last_result,
                "history": self.history[-50:],
                "started_at": self.started_at,
            }
            self._session_file().write_text(json.dumps(payload, indent=2))
        except Exception as exc:  # noqa: BLE001
            LOG.debug("session save failed: %s", exc)

    @classmethod
    def load(cls) -> "ForgeSession":
        sess = cls()
        path = sess._session_file()
        if not path.exists():
            return sess
        try:
            data = json.loads(path.read_text())
            sess.provider = data.get("provider")
            sess.mode = data.get("mode", "copilot")
            sess.memory_on = bool(data.get("memory_on", True))
            sess.last_result = data.get("last_result")
            sess.history = list(data.get("history") or [])
            sess.started_at = float(data.get("started_at", time.time()))
        except Exception as exc:  # noqa: BLE001
            LOG.debug("session load failed: %s", exc)
        return sess

    # ----- mutation helpers -----------------------------------------------

    def switch_provider(self, provider: str) -> None:
        self.provider = provider
        self.save()

    def switch_mode(self, mode: str) -> None:
        self.mode = mode
        self.save()

    def record(self, entry: str) -> None:
        self.history.append(entry)
        if len(self.history) > 200:
            self.history = self.history[-200:]

    # ----- discovery / memory hooks (lazy) --------------------------------

    def refresh_discovery(self) -> Dict[str, Any]:
        """Re-scan the project for metadata hints. Reuses forge_copilot_discovery."""
        try:
            from .forge_copilot_runtime import discover_local_context

            report = discover_local_context(
                None,
                discover=True,
                workspace_root=self.project_root,
                logger=LOG,
            )
            # Best-effort extraction; the report is a dataclass in most paths.
            self.discovery = {
                "project_root": str(self.project_root),
                "summary": getattr(report, "summary", None)
                or getattr(report, "__dict__", {}),
            }
        except Exception as exc:  # noqa: BLE001
            LOG.debug("discovery refresh failed: %s", exc)
            self.discovery = {"project_root": str(self.project_root), "summary": None}
        return self.discovery or {}

    def refresh_memory(self) -> Dict[str, Any]:
        """Load the project-scoped copilot memory summary."""
        try:
            from .forge_copilot_memory import (
                CopilotMemoryStore,
                summarize_copilot_memory,
            )

            store = CopilotMemoryStore(self.project_root, logger=LOG)
            snapshot = store.load() if hasattr(store, "load") else None
            self.memory_snapshot = (
                summarize_copilot_memory(snapshot) if snapshot else None
            )
        except Exception as exc:  # noqa: BLE001
            LOG.debug("memory refresh failed: %s", exc)
            self.memory_snapshot = None
        return self.memory_snapshot or {}

    # ----- display --------------------------------------------------------

    def to_status_dict(self) -> Dict[str, Any]:
        return {
            "provider": self.provider or "—",
            "mode": self.mode,
            "memory": "on" if self.memory_on else "off",
            "project": self.project_root.name,
        }


__all__ = ["ForgeSession"]
