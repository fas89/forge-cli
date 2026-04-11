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

"""Shared contract scaffolding for blank and guided Forge modes.

All contract generation goes through ``build_minimal_contract()`` which
returns a Python dict.  The dict is serialised to YAML via ``yaml.dump``
so that user-supplied values (descriptions containing colons, quotes,
etc.) are escaped correctly.
"""

from __future__ import annotations

__all__ = [
    "build_minimal_contract",
    "create_and_validate_contract",
    "write_contract",
    "validate_contract_file",
]

import logging
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from fluid_build.cli.artifact_paths import CONTRACT_FILENAME

LOG = logging.getLogger("fluid.cli.forge.contract_factory")

DOCS_URL = "https://fluid-build.dev/docs/contracts"


def build_minimal_contract(
    *,
    product_id: str = "my-data-product",
    name: Optional[str] = None,
    domain: str = "analytics",
    owner: str = "data-team",
    description: str = "TODO: Describe your data product",
    engine: str = "sql",
    tags: Optional[list] = None,
) -> Dict[str, Any]:
    """Build a minimal but valid FLUID contract as a Python dict.

    This is the single source of truth for scaffold contracts used by
    both ``--blank`` and guided mode.
    """
    return {
        "fluidVersion": "0.7.1",
        "kind": "DataProduct",
        "id": product_id,
        "name": name or product_id.replace("-", " ").title(),
        "metadata": {
            "domain": domain,
            "owner": owner,
            "description": description,
            "tags": tags or [],
        },
        "builds": [
            {
                "id": "main",
                "pattern": "command",
                "engine": engine,
                "properties": {
                    "sql": "SELECT 1 AS placeholder",
                },
            },
        ],
        "exposes": [
            {
                "id": "output",
                "kind": "table",
                "from_build": "main",
            },
        ],
    }


def write_contract(contract: Dict[str, Any], path: Path) -> None:
    """Serialise *contract* to YAML at *path* using ``yaml.dump``.

    Uses ``yaml.dump`` instead of f-strings so that user-supplied values
    containing special YAML characters (``:``, ``"``, ``\\n``) are
    escaped correctly.
    """
    header = f"# FLUID Data Product Contract\n# Docs: {DOCS_URL}\n"
    body = yaml.dump(contract, default_flow_style=False, sort_keys=False, allow_unicode=True)
    path.write_text(header + body, encoding="utf-8")
    LOG.debug("Wrote contract to %s", path)


def create_and_validate_contract(
    contract: Dict[str, Any],
    target_dir: Path,
    logger: logging.Logger,
    console: Any = None,
) -> Optional[Path]:
    """Write *contract* to ``target_dir/contract.fluid.yaml`` and validate.

    Returns the contract path on success or ``None`` on failure.
    Logs and optionally prints errors via *console*.
    """
    target_dir.mkdir(parents=True, exist_ok=True)
    contract_path = target_dir / CONTRACT_FILENAME
    write_contract(contract, contract_path)

    error = validate_contract_file(contract_path)
    if error:
        logger.error("Generated contract failed validation: %s", error)
        if console:
            try:
                console.print(f"[red]Generated contract is invalid: {error}[/red]")
            except Exception:  # noqa: BLE001
                pass
        return None
    return contract_path


def validate_contract_file(path: Path) -> Optional[str]:
    """Quick-validate the YAML contract at *path*.

    Returns ``None`` on success or an error message string on failure.
    Does **not** run full schema validation -- only checks that the file
    is parseable YAML with the required top-level keys.
    """
    required_keys = {"fluidVersion", "kind", "id"}
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return "Contract file is not a YAML mapping"
        missing = required_keys - set(data.keys())
        if missing:
            return f"Contract is missing required keys: {', '.join(sorted(missing))}"
        return None
    except yaml.YAMLError as exc:
        return f"Invalid YAML: {exc}"
    except OSError as exc:
        return f"Cannot read contract file: {exc}"
