# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Shared helpers for acquisition runners.

- ``RunIdGenerator`` — ULID-style monotonic ids that survive replay.
- ``build_run_context`` — assemble a ``RunContext`` from a contract + build.
- ``utc_now_iso`` — single source of truth for timestamps.
"""

from __future__ import annotations

import os
import secrets
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_run_id() -> str:
    """Lightweight monotonic id without external deps. Format: HHMMM-XXXXXX
    where the prefix is a millisecond timestamp (base32) and the suffix is
    6 random chars. Sortable and unique per process.
    """
    ts_ms = int(time.time() * 1000)
    ts_b32 = _to_base32(ts_ms, width=10)
    rand = _rand_b32(6)
    return f"01{ts_b32}{rand}"


_BASE32_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _to_base32(n: int, width: int) -> str:
    out = []
    while n > 0:
        out.append(_BASE32_ALPHABET[n & 31])
        n >>= 5
    while len(out) < width:
        out.append("0")
    return "".join(reversed(out))


def _rand_b32(n: int) -> str:
    return "".join(_BASE32_ALPHABET[secrets.randbelow(32)] for _ in range(n))


def get_acquisition_build_props(build: Dict[str, Any]) -> Dict[str, Any]:
    """Return the ``properties`` dict for an acquisition build, defaulting to {}."""
    return dict(build.get("properties") or {})


def is_acquisition_build(build: Dict[str, Any]) -> bool:
    return build.get("pattern") == "acquisition"
