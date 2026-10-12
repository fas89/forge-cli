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

import copy
import logging
import os
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from fluid_build.providers.base import ApplyResult, BaseProvider, ProviderError
from fluid_build.providers.odcs.mappers.base import PASSTHROUGH_KEY as ODCS_PASSTHROUGH_KEY
from fluid_build.providers.odcs.provider import DOCUMENT_KEY as ODCS_DOCUMENT_KEY
from fluid_build.providers.odcs.provider import OdcsProvider

from .io import read_input, write_output
from .mappers import EXPORT_PIPELINE, IMPORT_PIPELINE
from .mappers.base import ExportCtx, ImportCtx
from .resolver import ContractResolver, ResolvedContract
from .validation import (
    DEFAULT_API_VERSION,
    SUPPORTED_API_VERSIONS,
    load_schema,
    normalize_api_version,
    validate,
)

# Sub-import keys never kept in a port's ODCS document snapshot: the
# SchemaObjects travel as the expose itself (+ ``extra_exposes``) and the
# server-derived expects are redundant with the verbatim ``servers`` stash.
_SUB_SKIP_KEYS = frozenset({"exposes", "expects"})
# Key of the product file in :meth:`BitolOdpsProvider._bundle_paths` (cannot
# collide with a contractId, which is never empty).
_PRODUCT_PATH_KEY = ""


class BitolOdpsProvider(BaseProvider):
    """Bitol Open Data Product Standard — bidirectional provider
    (vendored schemas: v1.1.0, v1.0.0).

    Surface:
      - :meth:`render` — FLUID → ODPS product + per-port ODCS contracts.
      - :meth:`import_contract` / :meth:`import_directory` — ODPS → FLUID.
      - :meth:`validate_product` — JSON Schema validation, version-selected
        from the document's ``apiVersion``.

    ``api_version`` (or ``ODPS_API_VERSION``) sets the version *fresh* exports
    declare. Re-exports of imported products preserve the document's own
    ``apiVersion`` unless an explicit version is given. The per-port ODCS
    version is an independent axis (see :class:`OdcsProvider`).

    Per-port ODCS rendering is delegated to :class:`OdcsProvider` (calling
    ``render(fluid, expose_id=..., contract_id=port.contractId)``), so the
    emitted ODCS ``id`` equals the ODPS port's ``contractId`` by
    construction — the ``{product_id}.{expose_id}`` convention for fresh
    FLUID, the original id (often a UUID) for imported products. A port
    without a ``contractId`` (allowed by v1.1.0) gets no sibling ODCS.

    Importing a bundle keeps each port's whole ODCS import state on its
    expose (see :meth:`_merge_odcs_into_expose`), so import → export
    reproduces every per-port ODCS document as well as the product.
    """

    def __init__(self, api_version: Optional[str] = None) -> None:
        super().__init__()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        explicit = api_version or os.getenv("ODPS_API_VERSION") or None
        self._explicit_api_version = normalize_api_version(explicit) if explicit else None
        self.api_version = self._explicit_api_version or DEFAULT_API_VERSION
        self.supported_versions = SUPPORTED_API_VERSIONS
        self.spec_url = "https://github.com/bitol-io/open-data-product-standard"
        self.schema = load_schema(self.api_version)
        self.include_custom_properties = os.getenv("ODPS_INCLUDE_CUSTOM", "true").lower() == "true"
        # ``version`` of fresh output ports whose expose declares none.
        self.default_port_version = os.getenv("ODPS_DEFAULT_PORT_VERSION", "1")
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
        raise ProviderError("Bitol ODPS provider does not support plan(). Use render() for export.")

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
                "api_version": self._explicit_api_version,
                "default_port_version": self.default_port_version,
            },
        )
        for mapper in EXPORT_PIPELINE:
            mapper.to_odps(ctx)

        # Propagate strict mode to the per-port OdcsProvider dynamically so
        # late toggles (``provider.strict_validation = False`` in tests) take
        # effect for the same render call.
        self._odcs._vowl_validate_on_export = self.strict_validation

        # Emit per-port ODCS contracts, keyed by contractId. The ports mapper
        # emits exactly one port per named expose, in order, so the two
        # sequences pair up positionally.
        named_exposes = [
            e
            for e in (src.get("exposes") or [])
            if isinstance(e, Mapping) and (e.get("exposeId") or e.get("id"))
        ]
        contracts: Dict[str, Dict[str, Any]] = {}
        seen_expose_ids: set = set()
        for expose, port in zip(named_exposes, product.get("outputPorts") or []):
            expose_id = expose.get("exposeId") or expose.get("id")
            if expose_id in seen_expose_ids:
                raise ProviderError(
                    f"Duplicate output port name '{expose_id}' — each FLUID expose "
                    "must have a unique exposeId/id."
                )
            seen_expose_ids.add(expose_id)
            contract_id = port.get("contractId")
            if not contract_id:
                # ODPS v1.1.0 allows a port without a contract; it references
                # no ODCS document, so there is no sibling to emit.
                self.logger.debug("Output port %r has no contractId; no ODCS emitted", expose_id)
                continue
            odcs = self._odcs.render(src, expose_id=expose_id, contract_id=contract_id)
            if odcs.get("id") != contract_id:
                # Defensive — these are equal by construction
                raise ProviderError(
                    f"Linking invariant violated: port.contractId={contract_id!r} "
                    f"≠ odcs.id={odcs.get('id')!r}"
                )
            contracts[contract_id] = odcs

        # Strict-mode validation (fail-loud), version-selected from each
        # emitted document's own apiVersion.
        if self.strict_validation:
            validate(product)
            for odcs in contracts.values():
                self._odcs.validate_contract(odcs)

        # Write artefacts. Bundle paths are checked before anything is
        # written so an unsafe id fails the whole export.
        bundle_paths = None
        if out_dir is not None:
            bundle_paths = self._bundle_paths(product, contracts, Path(out_dir), fmt or "yaml")
        if out is not None and out != "-":
            write_output(product, out, fmt or "yaml")
            self.logger.info("Exported ODPS product: %s", out)
        if out_dir is not None:
            self._write_bundle(product, contracts, Path(out_dir), fmt or "yaml", paths=bundle_paths)

        return {"product": product, "contracts": contracts}

    def _write_bundle(
        self,
        product: Mapping[str, Any],
        contracts: Mapping[str, Mapping[str, Any]],
        out_dir: Path,
        fmt: str,
        *,
        paths: Optional[Dict[str, Path]] = None,
    ) -> None:
        """Emit the canonical layout: 1 ``<product>.odps.<fmt>`` + N ``<contractId>.odcs.<fmt>``."""
        if paths is None:
            paths = self._bundle_paths(product, contracts, out_dir, fmt)
        out_dir.mkdir(parents=True, exist_ok=True)
        product_path = paths[_PRODUCT_PATH_KEY]
        write_output(dict(product), product_path, fmt)
        self.logger.info("Exported ODPS product: %s", product_path)
        for contract_id, odcs in contracts.items():
            contract_path = paths[contract_id]
            write_output(dict(odcs), contract_path, fmt)
            self.logger.info("Exported ODCS contract: %s", contract_path)

    @staticmethod
    def _bundle_paths(
        product: Mapping[str, Any],
        contracts: Mapping[str, Mapping[str, Any]],
        out_dir: Path,
        fmt: str,
    ) -> Dict[str, Path]:
        """Compute (and vet) every bundle file path before anything is written.

        File names come from ids an imported document controls, so an id
        that is absolute or carries a path component would escape
        ``out_dir``; such ids raise :class:`ProviderError`.
        """
        product_name = product.get("id") or product.get("name") or "product"
        paths: Dict[str, Path] = {
            _PRODUCT_PATH_KEY: _safe_bundle_path(out_dir, str(product_name), f".odps.{fmt}")
        }
        for contract_id in contracts:
            paths[contract_id] = _safe_bundle_path(out_dir, str(contract_id), f".odcs.{fmt}")
        return paths

    # ---- import (Phase 3) ----------------------------------------------

    def import_contract(
        self,
        odps_doc: Union[Mapping[str, Any], str, Path],
        *,
        base_path: Optional[Union[str, Path]] = None,
        allow_remote: bool = True,
        resolver: Optional[ContractResolver] = None,
        lenient: bool = False,
    ) -> Dict[str, Any]:
        """ODPS product (file or dict) → one FLUID contract.

        Walks every port, resolves the referenced ODCS contract via the
        :class:`ContractResolver`, and merges per-port schema/quality/qos
        back into FLUID exposes/expects. The verbatim ODPS payload is
        preserved via ``metadata.odps_passthrough.*`` so re-export reproduces
        it exactly.

        ``lenient=True`` downgrades resolution failures to warnings (the
        resulting expose/expect stub will lack a populated contract.schema).
        """
        # Load doc and capture its base path for the resolver
        odps_data, doc_base_path = self._load_odps_doc(odps_doc, base_path)

        if resolver is None:
            resolver = ContractResolver(
                base_path=doc_base_path,
                allow_remote=allow_remote,
                odcs_provider=self._odcs,
            )

        # Run the mapper pipeline to build the FLUID skeleton (metadata, team,
        # port stubs, support pass-through)
        fluid: Dict[str, Any] = {}
        ctx = ImportCtx(odps=odps_data, fluid=fluid, logger=self.logger)
        for mapper in IMPORT_PIPELINE:
            mapper.to_fluid(ctx)

        # Resolve every port's contract and merge it into the stub
        self._resolve_and_merge_ports(fluid, odps_data, resolver, lenient=lenient)

        # Preserve the original ODPS doc verbatim for round-trip
        from .mappers.base import metadata_passthrough

        metadata_passthrough(fluid)["source"] = dict(odps_data)
        return fluid

    def import_directory(
        self,
        dir_path: Union[str, Path],
        *,
        allow_remote: bool = False,
        lenient: bool = False,
    ) -> Dict[str, Any]:
        """Import a directory containing an ODPS doc + sibling ODCS files.

        Layout cases handled:

        - **Standard**: exactly one ``*.odps.yaml`` / ``*.odps.yml`` /
          ``*.odps.json`` (or any file with ``kind: DataProduct``) plus N
          sibling ODCS files. The resolver
          gets the directory's file index, so per-port contracts always
          hit local first.
        - **ODCS-only**: no ODPS doc, just one or more ODCS files. We
          return a FLUID contract with one expose per ODCS file and no
          product wrapper.

        Raises if the directory contains zero or more than one ODPS doc.
        """
        root = Path(dir_path)
        if not root.is_dir():
            raise ProviderError(f"import_directory: {root} is not a directory")

        odps_files: List[Path] = []
        odcs_files: List[Path] = []
        for child in sorted(root.rglob("*")):
            if not child.is_file() or not _is_within(child, root):
                # Symlinks pointing outside the bundle are never read.
                continue
            suf = child.suffix.lower()
            if suf not in (".yaml", ".yml", ".json"):
                continue
            kind = _sniff_kind(child)
            if kind == "DataProduct":
                odps_files.append(child)
            elif kind == "DataContract":
                odcs_files.append(child)

        if len(odps_files) > 1:
            raise ProviderError(
                f"import_directory: found {len(odps_files)} ODPS docs in {root}; "
                "expected exactly one (or zero for ODCS-only mode). "
                f"Found: {[str(p) for p in odps_files]}"
            )

        if not odps_files:
            return self._import_odcs_only(root, odcs_files, lenient=lenient)

        # Standard path — build a resolver pre-indexed with the dir's ODCS files
        resolver = ContractResolver(
            base_path=root,
            allow_remote=allow_remote,
            odcs_provider=self._odcs,
            additional_files=odcs_files,
        )
        resolver.index_directory(root)
        return self.import_contract(
            odps_files[0],
            base_path=root,
            allow_remote=allow_remote,
            resolver=resolver,
            lenient=lenient,
        )

    # ---- import internals ----------------------------------------------

    def _load_odps_doc(
        self,
        odps_doc: Union[Mapping[str, Any], str, Path],
        base_path: Optional[Union[str, Path]],
    ) -> tuple:
        if isinstance(odps_doc, (str, Path)):
            doc_path = Path(odps_doc)
            data = read_input(doc_path)
            if not isinstance(data, Mapping):
                raise ProviderError(f"ODPS doc {doc_path} did not parse as a mapping")
            return dict(data), Path(base_path) if base_path else doc_path.parent
        if not isinstance(odps_doc, Mapping):
            raise ProviderError("import_contract: odps_doc must be a path or a mapping")
        return dict(odps_doc), Path(base_path) if base_path else None

    def _resolve_and_merge_ports(
        self,
        fluid: Dict[str, Any],
        odps_data: Mapping[str, Any],
        resolver: ContractResolver,
        *,
        lenient: bool,
    ) -> None:
        # The mapper pipeline already created an expose stub per output port
        # and an expect stub per input port (with contractId attached via the
        # pass-through bucket). We now resolve each contractId and merge the
        # imported ODCS fields into the stub.
        #
        # Resolution policy:
        # - **output ports** must resolve. They're the data this product
        #   produces; an unresolved output is a broken bundle. The ``lenient``
        #   flag still applies here.
        # - **input ports** reference upstream contracts that often live in
        #   other repos / catalogues. Failure to find them is normal — we
        #   warn and leave the stub with just its contractId reference.
        #
        # Stubs pair with ports by position (the ports mapper appends exactly
        # one stub per mapping port, in order): two ports may share a name
        # and differ only in version.
        expose_stubs = [e for e in fluid.get("exposes", []) if isinstance(e, Mapping)]
        output_ports = [p for p in odps_data.get("outputPorts") or [] if isinstance(p, Mapping)]
        server_expects: Dict[str, Dict[str, Any]] = {}
        for stub, port in zip(expose_stubs, output_ports):
            cid = port.get("contractId")
            if not cid:
                continue
            resolved = self._safe_resolve(resolver, cid, lenient=lenient)
            if resolved is None:
                continue
            for expect in self._merge_odcs_into_expose(stub, resolved, port_name=port.get("name")):
                server_expects.setdefault(str(expect.get("id")), expect)

        expect_stubs = [e for e in fluid.get("expects", []) if isinstance(e, Mapping)]
        input_ports = [p for p in odps_data.get("inputPorts") or [] if isinstance(p, Mapping)]
        for stub, port in zip(expect_stubs, input_ports):
            cid = port.get("contractId")
            if cid:
                # Always lenient for input ports — upstream contracts may live elsewhere
                resolved = self._safe_resolve(resolver, cid, lenient=True)
                if resolved is not None:
                    self._merge_odcs_into_expect(stub, resolved)
            # The output-port contracts describe the upstream sources they
            # read as ODCS servers; give the matching expect its provider /
            # location back when it has none of its own.
            server = server_expects.get(str(port.get("name")))
            if server is not None:
                for key in ("provider", "location"):
                    if key in server and key not in stub:
                        stub[key] = copy.deepcopy(server[key])

    def _safe_resolve(
        self, resolver: ContractResolver, contract_id: str, lenient: bool
    ) -> Optional[ResolvedContract]:
        try:
            return resolver.resolve(contract_id)
        except ProviderError as exc:
            # ContractNotFound, RemoteFetchDisabled and ContractValidationError
            # all mean "this port's contract is unavailable".
            if lenient:
                self.logger.warning("Skipping unresolved contract %s: %s", contract_id, exc)
                return None
            raise

    def _merge_odcs_into_expose(
        self,
        stub: Dict[str, Any],
        resolved: ResolvedContract,
        *,
        port_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Run the ODCS importer to get FLUID fields, then merge into the stub.

        The OdcsProvider returns a FLUID skeleton with one expose per
        SchemaObject. For per-port contracts we expect one, so we merge its
        ``contract`` and ``qos`` into the port stub, keeping the port's
        id/version/description/tags/pass-through.

        The port's complete ODCS import state is kept on the stub so a
        re-export reproduces the per-port document:

        - ``expose.odcs_passthrough._verbatim`` — the SchemaObject stash;
        - ``expose.odcs_passthrough._document`` — the sub-import's
          document-level FLUID (``metadata`` with its ``odcs_passthrough``
          stash of top-level fields/apiVersion, servers, team, SLA and
          quality; ``owner``; ``contract``), extra SchemaObjects, and the
          stub's FLUID-native name/description at import time.

        Returns the expects the ODCS importer derived from ``servers`` so the
        caller can backfill matching input ports.
        """
        # The resolver already validated the document (strictly).
        sub_fluid = self._odcs.import_contract(resolved.odcs, lenient=True)
        sub_exposes = [e for e in sub_fluid.get("exposes") or [] if isinstance(e, Mapping)]
        sub_expects = [e for e in sub_fluid.get("expects") or [] if isinstance(e, Mapping)]
        if not sub_exposes:
            return sub_expects
        primary = sub_exposes[0]
        # Carry over the contract block (schema, quality, relationships, etc.)
        if primary.get("contract"):
            stub["contract"] = primary["contract"]
        if "binding" not in stub:
            binding = self._binding_from_servers(sub_expects, stub, primary, port_name)
            if binding:
                stub["binding"] = binding
        if primary.get("qos") and "qos" not in stub:
            stub["qos"] = primary["qos"]
        # The SchemaObject description stays in the stash: copying it onto
        # the stub would add a description to the ODPS port on re-export.

        document = {k: copy.deepcopy(v) for k, v in sub_fluid.items() if k not in _SUB_SKIP_KEYS}
        # The sub-import's expects are the FLUID projection of this document's
        # ``servers``; the ODCS servers mapper three-way-merges against them,
        # so they must travel with the snapshot or every server reads as
        # "removed in FLUID".
        document["expects"] = copy.deepcopy(sub_expects)
        if len(sub_exposes) > 1:
            document["extra_exposes"] = copy.deepcopy(sub_exposes[1:])
        document["expose_native"] = {
            "name": stub.get("exposeId") or stub.get("id"),
            "description": stub.get("description") or None,
            # The binding the port got here vs. the one the ODCS importer
            # derived from ``servers``: an unedited port binding is mapped back
            # to the latter on render so the per-port servers replay verbatim.
            "binding": copy.deepcopy(stub.get("binding")),
            "odcs_binding": copy.deepcopy(primary.get("binding")),
        }
        odcs_pt = stub.setdefault(ODCS_PASSTHROUGH_KEY, {})
        odcs_pt.update(copy.deepcopy(dict(primary.get(ODCS_PASSTHROUGH_KEY) or {})))
        odcs_pt[ODCS_DOCUMENT_KEY] = document
        return sub_expects

    @staticmethod
    def _binding_from_servers(
        sub_expects: List[Dict[str, Any]],
        stub: Mapping[str, Any],
        primary: Mapping[str, Any],
        port_name: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        """FLUID-native binding for a port: the ODCS server named like the
        port (or its SchemaObject), else the ODCS importer's own guess."""
        names = {
            n
            for n in (
                port_name,
                stub.get("exposeId"),
                stub.get("id"),
                primary.get("exposeId"),
                primary.get("id"),
            )
            if n
        }
        for expect in sub_expects:
            if expect.get("id") in names and expect.get("provider"):
                binding: Dict[str, Any] = {"platform": expect["provider"]}
                if isinstance(expect.get("location"), Mapping) and expect["location"]:
                    binding["location"] = copy.deepcopy(dict(expect["location"]))
                return binding
        if isinstance(primary.get("binding"), Mapping) and primary["binding"]:
            return copy.deepcopy(dict(primary["binding"]))
        return None

    def _merge_odcs_into_expect(self, stub: Dict[str, Any], resolved: ResolvedContract) -> None:
        sub_fluid = self._odcs.import_contract(resolved.odcs, lenient=True)
        sub_exposes = sub_fluid.get("exposes") or []
        if not sub_exposes:
            return
        primary = sub_exposes[0]
        if primary.get("contract"):
            stub["contract"] = primary["contract"]
        if primary.get("binding") and "binding" not in stub:
            stub["binding"] = primary["binding"]

    def _import_odcs_only(
        self, root: Path, odcs_files: List[Path], *, lenient: bool = False
    ) -> Dict[str, Any]:
        """Build a wrapper-less FLUID contract from a directory of ODCS files.

        Each ODCS file becomes one expose under a synthetic product. Useful
        when a team publishes raw ODCS contracts without an ODPS wrapper.
        """
        if not odcs_files:
            raise ProviderError(f"import_directory: {root} contains no ODPS doc and no ODCS files")

        # ``Path(".").name`` is "" — name the product after the real directory.
        product_name = root.resolve().name or "product"
        fluid: Dict[str, Any] = {
            "metadata": {
                "name": product_name,
                "version": "1.0.0",
                "status": "active",
            },
            "contract": {"id": product_name},
            "exposes": [],
            "expects": [],
        }
        for odcs_file in odcs_files:
            sub = self._odcs.import_contract(odcs_file, lenient=lenient)
            for expose in sub.get("exposes") or []:
                fluid["exposes"].append(expose)

        from .mappers.base import metadata_passthrough

        metadata_passthrough(fluid)["odcs_only_directory"] = True
        metadata_passthrough(fluid)["odcs_only_warning"] = (
            f"Directory {root} contained no ODPS doc; "
            f"FLUID built from {len(odcs_files)} ODCS file(s)."
        )
        self.logger.warning(
            "import_directory: no ODPS doc in %s — building FLUID from %d ODCS file(s)",
            root,
            len(odcs_files),
        )
        return fluid

    # ---- validation -----------------------------------------------------

    def validate_product(self, odps: Mapping[str, Any]) -> None:
        """Validate against the vendored JSON Schema matching ``odps["apiVersion"]``."""
        validate(odps)


def _sniff_kind(path: Path) -> Optional[str]:
    """Return ``kind`` from a YAML/JSON file without raising.

    FLUID contracts also declare ``kind: DataProduct``; a document carrying
    ``fluidVersion`` is FLUID, neither ODPS nor ODCS, and yields ``None``.
    """
    try:
        data = read_input(path)
    except Exception:
        return None
    if not isinstance(data, Mapping):
        return None
    if "fluidVersion" in data:
        return None
    return data.get("kind")


def _is_within(path: Path, root: Path) -> bool:
    """True when ``path`` (symlinks resolved) stays inside ``root``."""
    try:
        return path.resolve().is_relative_to(root.resolve())
    except (OSError, RuntimeError):
        return False


def _safe_bundle_path(out_dir: Path, stem: str, suffix: str) -> Path:
    """``out_dir / (stem + suffix)``, refusing stems that would escape ``out_dir``."""
    if (
        not stem
        or ".." in stem
        or "/" in stem
        or "\\" in stem
        or os.sep in stem
        or (os.altsep is not None and os.altsep in stem)
        or os.path.isabs(stem)
        or Path(stem).drive
    ):
        raise ProviderError(
            f"Refusing to write bundle file for id {stem!r}: an id used as a file "
            "name must not be absolute or contain path separators or '..'."
        )
    candidate = out_dir / f"{stem}{suffix}"
    if candidate.resolve().parent != out_dir.resolve():
        raise ProviderError(
            f"Refusing to write bundle file {candidate}: it resolves outside {out_dir}."
        )
    return candidate
