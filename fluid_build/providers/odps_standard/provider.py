# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""BitolOdpsProvider — thin orchestrator over the :mod:`mappers` pipeline.

Phase 2 adds the **export** side: render a FLUID contract to one ODPS product
document plus N sibling ODCS contracts (one per output port). The plan
guarantees ``port.contractId == odcs_contract.id`` for every port, so the
sibling files line up with the references inside the ODPS doc.

Phase 3 will add :meth:`import_contract` and :meth:`import_directory`.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from fluid_build.providers.base import ApplyResult, BaseProvider, ProviderError
from fluid_build.providers.odcs.provider import OdcsProvider

from .io import write_output
from .mappers import EXPORT_PIPELINE
from .mappers.base import ExportCtx
from .validation import load_schema, validate


class BitolOdpsProvider(BaseProvider):
    """Bitol Open Data Product Standard v1.0.0 — bidirectional provider.

    Phase 2 surface:
      - :meth:`render` — FLUID → ODPS product + per-port ODCS contracts.
      - :meth:`validate_product` — JSON Schema validation.

    Phase 3 will add ``import_contract`` and ``import_directory``.

    Per-port ODCS rendering is delegated to :class:`OdcsProvider` (calling
    ``render(fluid, expose_id=...)``); the per-expose scoped id it produces
    (``{product_id}.{expose_id}``) equals the ODPS port's ``contractId``
    by construction.
    """

    def __init__(self) -> None:
        super().__init__()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.api_version = "v1.0.0"
        self.spec_url = "https://github.com/bitol-io/open-data-product-standard"
        self.schema = load_schema()
        self.include_custom_properties = (
            os.getenv("ODPS_INCLUDE_CUSTOM", "true").lower() == "true"
        )
        # Strict by default — a contract that fails the schema is a bug.
        self.strict_validation = os.getenv("ODPS_STRICT", "true").lower() == "true"
        self._odcs = OdcsProvider()

    @property
    def name(self) -> str:
        return "odps_bitol"

    def capabilities(self) -> Mapping[str, bool]:
        caps = dict(super().capabilities())
        caps.update(
            {
                "planning": False,
                "apply": False,
                "render": True,
                "validate": True,
                "import": True,  # implemented in Phase 3
                "supports_batch": False,
            }
        )
        return caps

    # ---- unsupported lifecycle hooks ------------------------------------

    def plan(self, contract: Mapping[str, Any]) -> List[Dict[str, Any]]:
        raise ProviderError(
            "Bitol ODPS provider does not support plan(). Use render() for export."
        )

    def apply(self, actions: Iterable[Mapping[str, Any]]) -> ApplyResult:
        raise ProviderError(
            "Bitol ODPS provider does not support apply(). Use render() for export."
        )

    # ---- render ---------------------------------------------------------

    def render(
        self,
        src: Union[Mapping[str, Any], Sequence[Mapping[str, Any]]],
        *,
        out: Optional[Union[Path, str]] = None,
        fmt: Optional[str] = "yaml",
        out_dir: Optional[Union[Path, str]] = None,
    ) -> Dict[str, Any]:
        """Render a FLUID contract to one ODPS product + N per-port ODCS contracts.

        Returns ``{"product": dict, "contracts": {contractId: dict, ...}}``.

        When ``out`` is provided the product is written there; when ``out_dir``
        is provided the product **and** every contract are written there with
        the layout ``<product>.odps.<fmt>`` + ``<contractId>.odcs.<fmt>``.
        ``out`` and ``out_dir`` can be combined.
        """
        if isinstance(src, list):
            raise ProviderError(
                "Bitol ODPS export does not support batch processing. "
                "Each data product should be exported separately."
            )

        product: Dict[str, Any] = {}
        ctx = ExportCtx(
            fluid=src,
            odps=product,
            logger=self.logger,
            options={
                "include_custom_properties": self.include_custom_properties,
            },
        )
        for mapper in EXPORT_PIPELINE:
            mapper.to_odps(ctx)

        # Emit per-port ODCS contracts, keyed by contractId.
        contracts: Dict[str, Dict[str, Any]] = {}
        seen_port_names: set[str] = set()
        for port in product.get("outputPorts") or []:
            port_name = port.get("name")
            contract_id = port.get("contractId")
            if port_name in seen_port_names:
                raise ProviderError(
                    f"Duplicate output port name '{port_name}' — each FLUID expose "
                    "must have a unique exposeId/id."
                )
            seen_port_names.add(port_name)
            odcs = self._odcs.render(src, expose_id=port_name)
            if odcs.get("id") != contract_id:
                # Defensive — these should be equal by construction
                raise ProviderError(
                    f"Linking invariant violated: port.contractId={contract_id!r} "
                    f"≠ odcs.id={odcs.get('id')!r}"
                )
            contracts[contract_id] = odcs

        # Strict-mode validation (fail-loud)
        if self.strict_validation and self.schema:
            validate(product, self.schema)
        for contract_id, odcs in contracts.items():
            if self.strict_validation and self._odcs.schema:
                self._odcs.validate_contract(odcs)

        # Write artefacts
        if out is not None and out != "-":
            write_output(product, out, fmt or "yaml")
            self.logger.info("Exported ODPS product: %s", out)
        if out_dir is not None:
            self._write_bundle(product, contracts, Path(out_dir), fmt or "yaml")

        return {"product": product, "contracts": contracts}

    def _write_bundle(
        self,
        product: Mapping[str, Any],
        contracts: Mapping[str, Mapping[str, Any]],
        out_dir: Path,
        fmt: str,
    ) -> None:
        """Emit the canonical layout: 1 ``<product>.odps.<fmt>`` + N ``<contractId>.odcs.<fmt>``."""
        out_dir.mkdir(parents=True, exist_ok=True)
        product_name = product.get("id") or product.get("name") or "product"
        product_path = out_dir / f"{product_name}.odps.{fmt}"
        write_output(dict(product), product_path, fmt)
        self.logger.info("Exported ODPS product: %s", product_path)
        for contract_id, odcs in contracts.items():
            contract_path = out_dir / f"{contract_id}.odcs.{fmt}"
            write_output(dict(odcs), contract_path, fmt)
            self.logger.info("Exported ODCS contract: %s", contract_path)

    # ---- validation -----------------------------------------------------

    def validate_product(self, odps: Mapping[str, Any]) -> None:
        if not self.schema:
            self.logger.warning("ODPS schema not available, skipping validation")
            return
        validate(odps, self.schema)
