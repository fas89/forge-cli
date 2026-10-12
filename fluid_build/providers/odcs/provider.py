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
from .mappers.base import ExportCtx, ImportCtx, fluid_id, get_expose_passthrough, get_stash
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
        self.include_quality_checks = os.getenv("ODCS_INCLUDE_QUALITY", "true").lower() == "true"
        self.include_sla = os.getenv("ODCS_INCLUDE_SLA", "true").lower() == "true"
        # Whether ``render()`` runs vowl's parser as a second-pass validator
        # on the emitted ODCS. Off by default so the vowl dependency stays
        # opt-in; flipped on by ``BitolOdpsProvider`` when ``strict_validation``
        # is set so the per-port ODCS contracts go through vowl too.
        self._vowl_validate_on_export = os.getenv("ODCS_VOWL_VALIDATE", "false").lower() == "true"

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
        contract_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Render a FLUID contract to ODCS.

        When ``expose_id`` is provided the rendered ODCS scopes its ``id`` to
        ``contract_id`` (default ``{product_id}.{expose_id}``) and reads
        ``status`` from that expose's lifecycle — exactly what the Bitol ODPS
        provider needs when emitting per-port contracts. ``contract_id`` lets
        the caller keep an imported port's real ``contractId`` (a UUID, say)
        as the emitted ODCS ``id`` instead of minting the convention id.

        An expose imported from an ODPS bundle carries its per-port ODCS
        document state (``expose.odcs_passthrough._document``); the scoped
        render then starts from that document so the re-emitted contract
        reproduces the source (servers, team, SLA, apiVersion, top-level
        fields) and only the expose's own edits are overlaid.
        """
        if isinstance(src, list):
            raise ProviderError(
                "ODCS export does not support batch processing. "
                "Each contract should be exported separately."
            )

        fluid = self._scope_to_expose(src, expose_id, contract_id=contract_id) if expose_id else src
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
        self,
        odcs: Union[Mapping[str, Any], str, Path],
        *,
        lenient: bool = False,
    ) -> Dict[str, Any]:
        """ODCS (file or dict) → FLUID.

        The document is validated against the vendored JSON Schema matching
        its ``apiVersion`` first; a schema-invalid contract raises
        :class:`ProviderError` (the historical behaviour). ``lenient=True``
        downgrades that failure to a warning so partially-malformed
        contracts can still be inspected.
        """
        odcs_data = read_input(odcs) if isinstance(odcs, (str, Path)) else dict(odcs)
        try:
            self.validate_contract(odcs_data)
        except ProviderError as exc:
            if not lenient:
                raise
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
        validation errors. Requires the ``odcs-strict`` extra (Python 3.10+:
        ``open-data-contract-standard`` does not support 3.9); raises
        :class:`ProviderError` if the package is not installed.
        """
        try:
            from open_data_contract_standard.model import (  # type: ignore[import-not-found]
                OpenDataContractStandard,
            )
        except ImportError as exc:
            raise ProviderError(
                "open-data-contract-standard is not installed (it requires "
                "Python >= 3.10); pip install 'fluid-forge[odcs-strict]'"
            ) from exc
        return OpenDataContractStandard.model_validate(dict(odcs))

    def roundtrip_check(self, odcs: Mapping[str, Any]) -> Dict[str, Any]:
        """Convert ODCS → FLUID → ODCS and return a structured diff.

        Returns ``{"equal": bool, "missing": [...], "extra": [...],
        "changed": [...]}``. Pure; no I/O.
        """
        fluid = self.import_contract(odcs, lenient=True)
        reconstructed = self.render(fluid)
        return roundtrip_check(odcs, reconstructed)

    # ---- internal helpers ----------------------------------------------

    def _scope_to_expose(
        self,
        fluid: Mapping[str, Any],
        expose_id: str,
        contract_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Filter a FLUID contract to a single output port for per-port export.

        ``contract_id`` (when given) becomes the scoped ODCS ``id``; otherwise
        the ``{product_id}.{expose_id}`` convention is minted.
        """
        exposes = [
            e
            for e in fluid.get("exposes", [])
            if isinstance(e, Mapping)
            and (e.get("exposeId") == expose_id or e.get("id") == expose_id)
        ]
        if not exposes:
            available = [
                e.get("exposeId") or e.get("id")
                for e in fluid.get("exposes", [])
                if isinstance(e, Mapping)
            ]
            raise ProviderError(
                f"Expose '{expose_id}' not found in contract. " f"Available exposeIds: {available}"
            )
        document = get_expose_passthrough(exposes[0]).get(DOCUMENT_KEY)
        if isinstance(document, Mapping):
            scoped = _port_document_view(document, exposes[0], fluid)
        else:
            scoped = copy.deepcopy(dict(fluid))
            scoped["exposes"] = [copy.deepcopy(dict(e)) for e in exposes]
            lifecycle = exposes[0].get("lifecycle") or {}
            scoped["_scoped_status"] = lifecycle.get("state", "active")
        product_id = fluid_id(fluid)
        if contract_id:
            scoped["_scoped_id"] = contract_id
        elif product_id:
            scoped["_scoped_id"] = f"{product_id}.{expose_id}"
        return scoped


# Key (inside ``expose.odcs_passthrough``) holding the per-port ODCS document
# state captured when an ODPS bundle is imported; see
# :meth:`BitolOdpsProvider._merge_odcs_into_expose`.
DOCUMENT_KEY = "_document"
# Keys (inside a ``_document`` snapshot) holding the importing product's
# FLUID ``owner`` / ``expects`` at import time; the per-port view merges the
# product's edits since then into the document's own.
PRODUCT_OWNER_KEY = "product_owner"
PRODUCT_EXPECTS_KEY = "product_expects"

# FLUID ``expects`` keys the ODCS servers mapper reads.
EXPECT_SERVER_KEYS = ("provider", "location", "binding")


def _port_document_view(
    document: Mapping[str, Any],
    expose: Mapping[str, Any],
    fluid: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Rebuild the FLUID view a per-port ODCS document was imported from.

    ``document`` holds the sub-import's document-level FLUID (``metadata``
    with its ``odcs_passthrough`` stash of top-level fields, servers, team,
    SLA and quality; ``owner``; ``contract``), any extra SchemaObjects, and
    the expose's FLUID-native ``name``/``description`` at import time. The
    current ``expose`` supplies the schema (and every edit made since).
    Because the expose is shared with the ODPS port, its native name and
    description are mapped back to the SchemaObject's own values unless the
    user changed them after import.

    The product (``fluid``) owns ``owner`` and ``expects``: edits made to
    them since import (against the snapshot's ``product_owner`` /
    ``product_expects``) are merged into the document's own owner and
    expects, so they reach the per-port ``team`` and ``servers``. An
    unedited product leaves the document untouched.
    """
    doc = copy.deepcopy(dict(document))
    extra = doc.pop("extra_exposes", None) or []
    imported = doc.pop("expose_native", None) or {}
    has_product_state = PRODUCT_OWNER_KEY in doc or PRODUCT_EXPECTS_KEY in doc
    owner_then = doc.pop(PRODUCT_OWNER_KEY, None)
    expects_then = doc.pop(PRODUCT_EXPECTS_KEY, None)
    if has_product_state and fluid is not None:
        _merge_product_owner(doc, owner_then, product_owner(fluid))
        _merge_product_expects(doc, expects_then or [], fluid.get("expects") or [])
    view_expose = copy.deepcopy(dict(expose))
    st = get_stash(get_expose_passthrough(view_expose))
    if st is not None:
        name_now = view_expose.get("exposeId") or view_expose.get("id")
        if name_now == imported.get("name") and st.get("name"):
            view_expose["id"] = view_expose["exposeId"] = st["name"]
        if (view_expose.get("description") or None) == (imported.get("description") or None):
            if st.get("description"):
                view_expose["description"] = st["description"]
            else:
                view_expose.pop("description", None)
    if "binding" in imported and view_expose.get("binding") == imported.get("binding"):
        # Unedited since import: hand the servers mapper the binding the ODCS
        # importer derived, so it sees no binding edit and replays ``servers``.
        odcs_binding = imported.get("odcs_binding")
        if isinstance(odcs_binding, Mapping):
            view_expose["binding"] = copy.deepcopy(dict(odcs_binding))
        else:
            view_expose.pop("binding", None)
    doc["exposes"] = [view_expose] + [copy.deepcopy(e) for e in extra if isinstance(e, Mapping)]
    doc.setdefault("expects", [])
    lifecycle = view_expose.get("lifecycle")
    if isinstance(lifecycle, Mapping) and lifecycle.get("state"):
        doc["_scoped_status"] = lifecycle["state"]
    else:
        doc["_scoped_status"] = (doc.get("metadata") or {}).get("status", "active")
    return doc


def product_owner(fluid: Mapping[str, Any]) -> Any:
    """A FLUID contract's ``owner`` (root, else ``metadata.owner``), as the
    ODCS team mapper reads it."""
    metadata = fluid.get("metadata")
    nested = metadata.get("owner") if isinstance(metadata, Mapping) else None
    return fluid.get("owner") or nested or None


def _merge_product_owner(doc: Dict[str, Any], then: Any, now: Any) -> None:
    """Overlay the product's owner edit (``now`` − ``then``) on ``doc["owner"]``."""
    if now == then:
        return
    if not isinstance(now, Mapping) or not isinstance(then, (Mapping, type(None))):
        if now is None:
            doc.pop("owner", None)
        else:
            doc["owner"] = copy.deepcopy(now)
        return
    then_map: Mapping[str, Any] = then or {}
    current = doc.get("owner")
    owner: Dict[str, Any] = dict(current) if isinstance(current, Mapping) else {}
    for key in set(then_map) | set(now):
        if then_map.get(key) == now.get(key):
            continue
        if now.get(key) is None:
            owner.pop(key, None)
        else:
            owner[key] = copy.deepcopy(now[key])
    if owner:
        doc["owner"] = owner
    else:
        doc.pop("owner", None)


def _merge_product_expects(doc: Dict[str, Any], then: List[Any], now: List[Any]) -> None:
    """Merge the product's expects edits since import into ``doc["expects"]``.

    Expects pair up by ``id``. A product expect whose server-relevant keys
    (provider / location / binding) changed updates the document's expect of
    the same id; a removed one removes it; a new one is added. Untouched
    expects leave the document (and so its verbatim ``servers``) as it was.
    """

    def by_id(items: List[Any]) -> Dict[str, Mapping[str, Any]]:
        out: Dict[str, Mapping[str, Any]] = {}
        for item in items:
            if isinstance(item, Mapping):
                out.setdefault(str(item.get("id")), item)
        return out

    then_by, now_by = by_id(then), by_id(now)
    expects = [dict(e) for e in doc.get("expects") or [] if isinstance(e, Mapping)]
    changed = False
    for eid, then_e in then_by.items():
        now_e = now_by.get(eid)
        if now_e is None:
            kept = [e for e in expects if str(e.get("id")) != eid]
            changed = changed or len(kept) != len(expects)
            expects = kept
            continue
        delta = [k for k in EXPECT_SERVER_KEYS if then_e.get(k) != now_e.get(k)]
        if not delta:
            continue
        for expect in expects:
            if str(expect.get("id")) != eid:
                continue
            changed = True
            for key in delta:
                if now_e.get(key) is None:
                    expect.pop(key, None)
                else:
                    expect[key] = copy.deepcopy(now_e[key])
    for eid, now_e in now_by.items():
        if eid in then_by or any(str(e.get("id")) == eid for e in expects):
            continue
        added = {k: copy.deepcopy(now_e[k]) for k in ("id",) + EXPECT_SERVER_KEYS if k in now_e}
        expects.append(added)
        changed = True
    if changed:
        doc["expects"] = expects
