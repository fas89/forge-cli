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

# fluid_build/loader.py
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None  # YAML support optional; JSON still works


__all__ = ["load_contract", "load_with_overlay"]


def _read_text(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Contract/overlay not found: {path}")
    try:
        return path.read_text(encoding="utf-8")
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"Failed to read file {path}: {e}") from e


def _parse_file(path: Path) -> Dict[str, Any]:
    """
    Parse a JSON or YAML file based on extension.
    YAML requires PyYAML; if missing, raise a helpful error.
    """
    suffix = path.suffix.lower()
    text = _read_text(path)
    try:
        if suffix in (".json",):
            return json.loads(text)
        if suffix in (".yaml", ".yml"):
            if yaml is None:
                raise RuntimeError(
                    "YAML parsing requires PyYAML. Install with: pip install pyyaml "
                    "or use JSON (.json) contracts."
                )
            obj = yaml.safe_load(text)
            if obj is None:
                return {}
            if not isinstance(obj, dict):
                raise ValueError(f"YAML root must be an object/dict: {path}")
            return obj
        # Fallback: try JSON first, then YAML (if available)
        try:
            return json.loads(text)
        except Exception:
            if yaml is None:
                raise
            obj = yaml.safe_load(text)
            if obj is None:
                return {}
            if not isinstance(obj, dict):
                raise ValueError(f"YAML root must be an object/dict: {path}")
            return obj
    except Exception as e:
        raise RuntimeError(f"Failed to parse {path}: {e}") from e


def _deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively merge overlay into base. Lists are replaced (not extended) to
    avoid accidental duplication; dicts are merged key-by-key.
    """
    for k, v in overlay.items():
        if k in base and isinstance(base[k], dict) and isinstance(v, dict):
            base[k] = _deep_merge(base[k], v)
        else:
            base[k] = v
    return base


def _overlay_candidates(contract_path: Path, env: str) -> Tuple[Path, ...]:
    """
    Given a contract path and env name, return likely overlay file candidates.
    Search order (first match wins):
      1) <dir>/overlays/<env>.yaml
      2) <dir>/overlays/<env>.yml
      3) <dir>/overlays/<env>.json
      4) <dir>/<env>.yaml
      5) <dir>/<env>.yml
      6) <dir>/<env>.json
      7) <same-filename>.<env>.yaml (e.g., contract.fluid.<env>.yaml)
      8) <same-filename>.<env>.yml
      9) <same-filename>.<env>.json
    """
    d = contract_path.parent
    stem = contract_path.stem  # e.g., "contract.fluid"
    return (
        d / "overlays" / f"{env}.yaml",
        d / "overlays" / f"{env}.yml",
        d / "overlays" / f"{env}.json",
        d / f"{env}.yaml",
        d / f"{env}.yml",
        d / f"{env}.json",
        d / f"{stem}.{env}.yaml",
        d / f"{stem}.{env}.yml",
        d / f"{stem}.{env}.json",
    )


def load_contract(path: str | Path) -> Dict[str, Any]:
    """
    Load a single FLUID contract file (JSON or YAML).
    """
    p = Path(path)
    return _parse_file(p)


def load_with_overlay(
    contract_path: str | Path,
    env: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    """
    Load a contract and, if env is provided, deep-merge a matching overlay.

    Example:
      base: examples/customer360/contract.fluid.yaml
      overlay search (env=dev):
        examples/customer360/overlays/dev.yaml  (etc...)
    """
    log = logger or logging.getLogger("fluid.loader")
    base_path = Path(contract_path)

    # Load base
    base = load_contract(base_path)

    # Apply overlay if requested
    if env:
        for cand in _overlay_candidates(base_path, env):
            if cand.exists():
                try:
                    overlay = _parse_file(cand)
                    if not isinstance(overlay, dict):
                        raise ValueError(f"Overlay root must be an object/dict: {cand}")
                    merged = _deep_merge(dict(base), overlay)
                    log.info("overlay_applied", extra={"overlay": str(cand)})
                    return merged
                except Exception as e:
                    raise RuntimeError(f"Failed to apply overlay {cand}: {e}") from e
        # No overlay found – log at INFO (not ERROR) to avoid noisy runs
        log.debug("overlay_not_found", extra={"env": env})
        return base

    # No env → return base as-is
    return base
