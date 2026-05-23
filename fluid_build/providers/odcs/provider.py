# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""OdcsProvider — thin orchestrator over the :mod:`mappers` pipeline."""

from __future__ import annotations

import copy
import logging
import os
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from fluid_build.providers.base import ApplyResult, BaseProvider, ProviderError

from .io import read_input, write_output
from .mappers import EXPORT_PIPELINE, IMPORT_PIPELINE
from .mappers.base import ExportCtx, ImportCtx, fluid_id
from .validation import load_schema, roundtrip_check, validate


class OdcsProvider(BaseProvider):
    """ODCS v3.1.0 bidirectional provider.

    Public surface:
      - :meth:`render`            — FLUID → ODCS (one expose or whole contract)
      - :meth:`render_all_ports`  — one ODCS per FLUID expose
      - :meth:`import_contract`   — ODCS → FLUID (lossless when paired with export)
      - :meth:`validate_contract` — JSON Schema check
      - :meth:`roundtrip_check`   — diff helper for round-trip tests

    All real work lives in :mod:`fluid_build.providers.odcs.mappers`.
    """

    def __init__(self) -> None:
        super().__init__()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.odcs_version = "v3.1.0"
        self.odcs_spec_url = "https://github.com/bitol-io/open-data-contract-standard"
        self.schema = load_schema()
        self.include_quality_checks = (
            os.getenv("ODCS_INCLUDE_QUALITY", "true").lower() == "true"
        )
        self.include_sla = os.getenv("ODCS_INCLUDE_SLA", "true").lower() == "true"

    @property
    def name(self) -> str:
        return "odcs"

    def capabilities(self) -> Mapping[str, bool]:
        caps = dict(super().capabilities())
        caps.update(
            {
                "planning": False,
                "apply": False,
                "render": True,
                "validate": True,
                "import": True,
                "supports_batch": False,
            }
        )
        return caps

    # ---- unsupported lifecycle hooks ------------------------------------

    def plan(self, contract: Mapping[str, Any]) -> List[Dict[str, Any]]:
        raise ProviderError(
            "ODCS provider does not support plan(). Use render() for export or "
            "import_contract() for ODCS → FLUID."
        )

    def apply(self, actions: Iterable[Mapping[str, Any]]) -> ApplyResult:
        raise ProviderError(
            "ODCS provider does not support apply(). Use render() for export or "
            "import_contract() for ODCS → FLUID."
        )

    # ---- export ---------------------------------------------------------

    def render(
        self,
        src: Union[Mapping[str, Any], Sequence[Mapping[str, Any]]],
        *,
        out: Optional[Union[Path, str]] = None,
        fmt: Optional[str] = "yaml",
        expose_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Render a FLUID contract to ODCS v3.1.0.

        When ``expose_id`` is provided the rendered ODCS scopes its ``id`` to
        ``{product_id}.{expose_id}`` and reads ``status`` from that expose's
        lifecycle — exactly what the Bitol ODPS provider needs when emitting
        per-port contracts.
        """
        if isinstance(src, list):
            raise ProviderError(
                "ODCS export does not support batch processing. "
                "Each contract should be exported separately."
            )

        fluid = self._scope_to_expose(src, expose_id) if expose_id else src
        odcs: Dict[str, Any] = {}
        ctx = ExportCtx(
            fluid=fluid,
            odcs=odcs,
            logger=self.logger,
            options={
                "include_sla": self.include_sla,
                "include_quality_checks": self.include_quality_checks,
                "expose_id": expose_id,
            },
        )
        for mapper in EXPORT_PIPELINE:
            mapper.to_odcs(ctx)

        if self.schema and os.getenv("ODCS_VALIDATE", "false").lower() == "true":
            validate(odcs, self.schema)

        if out is not None and out != "-":
            write_output(odcs, out, fmt or "yaml")
            self.logger.info("Exported ODCS contract: %s", out)
        return odcs

    def render_all_ports(
        self,
        fluid: Mapping[str, Any],
        *,
        out_dir: Optional[Union[Path, str]] = None,
        fmt: Optional[str] = "yaml",
    ) -> List[Tuple[str, Dict[str, Any]]]:
        """One ODCS contract per FLUID expose. Files named ``product.odcs.<exposeId>.<fmt>``."""
        results: List[Tuple[str, Dict[str, Any]]] = []
        for expose in fluid.get("exposes", []) or []:
            if not isinstance(expose, Mapping):
                continue
            eid = expose.get("exposeId") or expose.get("id")
            if not eid:
                self.logger.warning("Expose missing exposeId — skipping")
                continue
            out_path = None
            if out_dir is not None:
                out_path = Path(out_dir) / f"product.odcs.{eid}.{fmt}"
            results.append((eid, self.render(fluid, out=out_path, fmt=fmt, expose_id=eid)))
        return results

    # ---- import ---------------------------------------------------------

    def import_contract(
        self, odcs: Union[Mapping[str, Any], str, Path]
    ) -> Dict[str, Any]:
        odcs_data = read_input(odcs) if isinstance(odcs, (str, Path)) else dict(odcs)
        if self.schema:
            try:
                self.validate_contract(odcs_data)
            except ProviderError as exc:
                # validation failures during import are non-fatal — log and
                # continue so partially-malformed contracts can still be
                # inspected. Strict-mode callers should call validate_contract
                # themselves first.
                self.logger.warning("ODCS import validation warning: %s", exc)

        fluid: Dict[str, Any] = {}
        ctx = ImportCtx(odcs=odcs_data, fluid=fluid, logger=self.logger)
        for mapper in IMPORT_PIPELINE:
            mapper.to_fluid(ctx)
        return fluid

    # ---- validation / round-trip ---------------------------------------

    def validate_contract(self, odcs: Mapping[str, Any]) -> None:
        """Validate an ODCS contract payload against the vendored JSON Schema."""
        if not self.schema:
            self.logger.warning("ODCS schema not available, skipping validation")
            return
        validate(odcs, self.schema)

    def roundtrip_check(self, odcs: Mapping[str, Any]) -> Dict[str, Any]:
        """Convert ODCS → FLUID → ODCS and return a structured diff.

        Returns ``{"equal": bool, "missing": [...], "extra": [...],
        "changed": [...]}``. Pure; no I/O.
        """
        fluid = self.import_contract(odcs)
        reconstructed = self.render(fluid)
        return roundtrip_check(odcs, reconstructed)

    # ---- internal helpers ----------------------------------------------

    def _scope_to_expose(
        self, fluid: Mapping[str, Any], expose_id: str
    ) -> Dict[str, Any]:
        """Filter a FLUID contract to a single output port for per-port export."""
        scoped = copy.deepcopy(dict(fluid))
        exposes = [
            e
            for e in scoped.get("exposes", [])
            if isinstance(e, dict)
            and (e.get("exposeId") == expose_id or e.get("id") == expose_id)
        ]
        if not exposes:
            available = [
                e.get("exposeId") or e.get("id")
                for e in fluid.get("exposes", [])
                if isinstance(e, dict)
            ]
            raise ProviderError(
                f"Expose '{expose_id}' not found in contract. "
                f"Available exposeIds: {available}"
            )
        scoped["exposes"] = exposes
        product_id = fluid_id(fluid)
        if product_id:
            scoped["_scoped_id"] = f"{product_id}.{expose_id}"
        lifecycle = exposes[0].get("lifecycle") or {}
        scoped["_scoped_status"] = lifecycle.get("state", "active")
        return scoped
