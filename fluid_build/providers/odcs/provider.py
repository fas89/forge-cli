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
from .validation import (
    DEFAULT_API_VERSION,
    SUPPORTED_API_VERSIONS,
    load_schema,
    normalize_api_version,
    roundtrip_check,
    validate,
    validate_via_vowl,
)


class OdcsProvider(BaseProvider):
    """ODCS bidirectional provider (vendored schemas: v3.2.0, v3.1.0).

    Public surface:
      - :meth:`render`            — FLUID → ODCS (one expose or whole contract)
      - :meth:`render_all_ports`  — one ODCS per FLUID expose
      - :meth:`import_contract`   — ODCS → FLUID (lossless when paired with export)
      - :meth:`validate_contract` — JSON Schema check, version-selected from ``apiVersion``
      - :meth:`roundtrip_check`   — diff helper for round-trip tests
      - :meth:`to_model`          — optional typed view (``open-data-contract-standard``)

    ``api_version`` (or ``ODCS_API_VERSION``) sets the version *fresh* exports
    declare. Re-exports of imported contracts preserve the document's own
    ``apiVersion`` unless an explicit version is given.

    All real work lives in :mod:`fluid_build.providers.odcs.mappers`.
    """

    def __init__(self, api_version: Optional[str] = None) -> None:
        super().__init__()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        explicit = api_version or os.getenv("ODCS_API_VERSION") or None
        self._explicit_api_version = normalize_api_version(explicit) if explicit else None
        self.odcs_version = self._explicit_api_version or DEFAULT_API_VERSION
        self.supported_versions = SUPPORTED_API_VERSIONS
        self.odcs_spec_url = "https://github.com/bitol-io/open-data-contract-standard"
        self.schema = load_schema(self.odcs_version)
        self.include_quality_checks = (
            os.getenv("ODCS_INCLUDE_QUALITY", "true").lower() == "true"
        )
        self.include_sla = os.getenv("ODCS_INCLUDE_SLA", "true").lower() == "true"
        # Whether ``render()`` runs vowl's parser as a second-pass validator
        # on the emitted ODCS. Off by default so the vowl dependency stays
        # opt-in; flipped on by ``BitolOdpsProvider`` when ``strict_validation``
        # is set so the per-port ODCS contracts go through vowl too.
        self._vowl_validate_on_export = (
            os.getenv("ODCS_VOWL_VALIDATE", "false").lower() == "true"
        )

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
        """Render a FLUID contract to ODCS.

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
                "api_version": self._explicit_api_version,
            },
        )
        for mapper in EXPORT_PIPELINE:
            mapper.to_odcs(ctx)

        if os.getenv("ODCS_VALIDATE", "false").lower() == "true":
            # Version-selected from the emitted apiVersion (may be a preserved
            # older version for re-exports).
            validate(odcs)

        # Second-pass validation through ``vowl`` (optional). Off by default
        # so the dependency is opt-in; flip ``ODCS_VOWL_VALIDATE=true`` (or
        # pass ``strict_validation=True`` callers that compose providers like
        # ``BitolOdpsProvider``) to run vowl's parser on the way out.
        if (
            self._vowl_validate_on_export
            or os.getenv("ODCS_VOWL_VALIDATE", "false").lower() == "true"
        ):
            diag = validate_via_vowl(odcs)
            if diag is not None:
                self.logger.info(
                    "vowl: ODCS v%s parsed; %d checks across %d schema(s)",
                    diag["api_version"],
                    diag["total_checks"],
                    len(diag["schemas"]),
                )

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
        """Validate against the vendored JSON Schema matching ``odcs["apiVersion"]``.

        A contract declaring ``v3.1.0`` is judged by the v3.1.0 schema, one
        declaring ``v3.2.0`` by v3.2.0; older-than-vendored and unknown
        versions fall back to the newest vendored schema.
        """
        validate(odcs)

    def to_model(self, odcs: Mapping[str, Any]) -> Any:
        """Typed view via the optional ``open-data-contract-standard`` package.

        Returns an ``OpenDataContractStandard`` pydantic model — handy for
        callers that want attribute access and readable field-level
        validation errors. Requires the ``odcs-strict`` extra; raises
        :class:`ProviderError` if the package is not installed.
        """
        try:
            from open_data_contract_standard.model import (  # type: ignore[import-not-found]
                OpenDataContractStandard,
            )
        except ImportError as exc:
            raise ProviderError(
                "open-data-contract-standard is not installed; "
                "pip install 'fluid-forge[odcs-strict]'"
            ) from exc
        return OpenDataContractStandard.model_validate(dict(odcs))

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
