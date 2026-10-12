# Changelog

All notable changes to FLUID Forge CLI will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **Breaking: `fluid odps export` / `fluid opds export` default to Bitol
  ODPS v1.0.0** instead of ODPI v4.1 (Linux Foundation). The output is a
  Bitol product document plus one ODCS contract per output port: `--out-dir`
  writes the canonical bundle (`<product>.odps.<fmt>` + `<contractId>.odcs.<fmt>`),
  `--out FILE` writes the product to FILE (format inferred from its suffix
  unless `-f/--format` is given) and the per-port contracts next to it, and
  stdout receives the product. Get the 0.7.8 output with `--spec odpi-4.1`
  (or the deprecated `--version 4.1`). Strict validation is on by default;
  `--no-validate-strict` still validates but reports problems as warnings.
  Without an explicit `--spec` the version follows `ODPS_API_VERSION`, then
  an imported document's own `apiVersion`, then v1.0.0; the success message
  reports the version actually written. `opds` is an alias of `odps`.
  `--spec odpi-4.1` writes the 0.7.8 single JSON document and rejects
  `--out-dir` / `-f yaml` with a clear error instead of ignoring them.
- **Breaking: `fluid dmm publish` and `fluid publish --catalog
  datamesh-manager` send Bitol ODPS by default** (previously the legacy Data
  Product Specification 0.0.1). Current Entropy Data / Entropy CE
  organisations accept only `odps` and reject DPS with HTTP 400
  (`Specification type 'dps' is not supported`), so the old default was a
  guaranteed failure there. Opt back into DPS with `--data-product-spec
  0.0.1`, `provider_hint="dps"`, the new `DMM_DATA_PRODUCT_SPEC` env var, or
  (catalog path) catalog config `data_product_spec: "0.0.1"` /
  `CatalogTarget.catalog_hints`. Resolution order: explicit value → provider
  hint (`odps`/`opds` → ODPS, `dps` → 0.0.1) → `DMM_DATA_PRODUCT_SPEC` →
  ODPS; the token `dps` means `0.0.1` everywhere a spec is accepted. The
  ODPS `team.name` is the team id the publish auto-creates (the `--team-id`
  value, or the owner team slugged, e.g. `customer-platform`). The catalog
  path keeps its 0.7.8 defaults (a missing `metadata.status` publishes as
  `active`; `metadata.domain` / `version` fall back to the top-level
  fields) and now publishes one data contract per expose
  (`{productId}.{exposeId}`) instead of one per product. Verified live
  against Entropy CE 2.1.x on both publish paths at ODCS v3.1.0 and v3.2.0
  (`scripts/dmm_live_e2e.sh`).
- `BaseCatalogProvider.publish` / `update` take `(fluid, target=None)`;
  `target` defaults to `CatalogTarget.from_fluid(fluid)`. The 0.7.8
  `publish(asset)` API keeps working for one release (see Deprecated):
  `fluid publish` treats a provider as using the new API only when its
  `publish` declares `(fluid, target)` (second parameter named `target` or
  annotated `CatalogTarget`); every other provider, including duck-typed
  plugins, takes the 0.7.8 `map_contract_to_asset` → `validate_asset` →
  `publish(asset)` route. Missing `domain` / owner still fall back to
  `general` / `unknown`; files that are not FLUID contracts (no
  `name` or `metadata.name`, e.g. fragments, overlays, config files) are
  rejected with a clear error.
- **Mapper round-trip architecture: verbatim stash + native overlay.** On
  import every ODCS/ODPS section (document, schema object, property, port)
  is stashed verbatim; on export the stash is the base layer and only the
  FLUID-native fields that actually changed are overlaid (three-way merge,
  `overlay_native`). This replaces the hand-maintained per-field
  pass-through tuples and means fields added by future spec versions
  round-trip with no code. FLUID edits to `owner`, `expects`, an expose
  `binding` and `qos` (on any expose) of an imported contract reach the
  stashed team / servers / `slaProperties`, for standalone ODCS imports and
  for every per-port contract of an imported ODPS bundle; a server an
  expose binding still selects is kept when its `expects` entry is removed.
  Unchanged contracts still round-trip verbatim, and imported FLUID never
  contains YAML anchors.
  Validated lossless against verbatim upstream Bitol examples, including
  the 192 KB v3.0.0 AdventureWorks contract.
- `OdcsProvider.validate_contract` / `BitolOdpsProvider.validate_product`
  and `fluid odcs validate` select the JSON Schema from the document's
  `apiVersion` rather than the provider's configured emit version. YAML
  dates are validated as the ISO strings JSON would carry, so an unquoted
  `2024-01-01` no longer fails a `type: string` check.
- `validate_via_vowl` skips with a warning (instead of failing the export)
  when the installed `vowl` predates the document's `apiVersion` (`vowl`
  0.0.5 supports up to v3.1.0; 0.0.6 adds v3.2.0).
- The `odcs-strict` extra now also installs `open-data-contract-standard`
  and `deepdiff`. `vowl` and `open-data-contract-standard` require Python
  3.10+, so on Python 3.9 the extra installs only `deepdiff` (the vowl pass
  is skipped and `OdcsProvider.to_model()` reports the missing package).
  `deepdiff` is also in the `dev` extra; it backs `roundtrip_check()`.
- `requests` is now a core dependency, so `fluid dmm publish` works on a
  plain `pip install fluid-forge`.
- **ODCS v3.1.0 import is now lossless.** `team` in the v3.1.0 object form
  and the v3.0.x array form, `required` (not the non-existent
  `isNullable`), and the `slaProperties`, contract/object/property-level
  `quality`, `relationships`, `support`, `price`, `customProperties`,
  `authoritativeDefinitions` and `roles` sections are preserved end-to-end.
  ODCS → FLUID → ODCS round-trips zero-diff on the full v3.1.0 fixture.
  Import still rejects schema-invalid ODCS (`lenient=True` downgrades that
  to a warning) and derives an expose `binding` from the server that
  unambiguously backs each SchemaObject — never from `physicalType`.
- ODCS export of fresh FLUID: falls back to top-level `name`,
  `description`, `tags`, `domain` and `lifecycle.state` when `metadata.*`
  does not set them; field-level `quality` items are appended to the
  auto-generated checks; `map` / `vector` and the v3.2.0-only server engines
  are emitted only when the target is v3.2.0+ (below that they fall back to
  `object` / `array` and `custom`, and an imported v3.2.0 contract exported
  at an older version has its v3.2.0-only content downgraded, with a
  warning, so it validates against the version it declares); each
  `server.type` emits only the fields
  its ODCS `ServerSource` allows, and a FLUID `binding.format` reaches the
  server (local-platform contracts, examples 01–06 and every `fluid init`
  template, export valid ODCS).
- **ODCS provider refactored into modular bidirectional mappers.** The
  monolithic 1441-line `odcs.py` split into `provider.py`, `validation.py`,
  `io.py`, and `mappers/{metadata,team,schema,quality,sla,servers,types}.py`.
  Each section module exposes paired `to_fluid` / `to_odcs` functions. All
  pass-through values live under a single `odcs_passthrough` namespace at
  each level (metadata / expose / field). `fluid_build.providers.odcs.odcs`
  remains as a compatibility shim.
- **Naming disambiguation.** The provider registry has three distinct
  names: `odcs` (Open Data Contract Standard), `odps` (ODPI v4.1, Linux
  Foundation; export-only), `odps_bitol` (Bitol Open Data Product Standard;
  bidirectional). `fluid --help` labels `odps` as Bitol ODPS (default) /
  ODPI v4.1 via `--spec`.
- Config: `~/.fluid/config.yaml` is read as an additional, lowest-priority
  user layer; the first-found legacy file (`~/.fluidrc.yaml`, `~/.fluidrc`,
  `~/.config/fluid/config.yaml`) is still read exactly as before and wins.
  Config values may reference environment variables as `${VAR}`; they are
  expanded when read and stored unexpanded, so `save_user_config()` writes
  the placeholder, never the secret.

### Added
- **ODCS v3.2.0 and Bitol ODPS v1.1.0** vendored alongside v3.1.0 / v1.0.0.
  Validation is *version-selected* from the document's own `apiVersion`
  (`resolve_schema_version`); older-than-vendored versions (e.g. v3.0.0)
  validate against the newest vendored schema, a strict superset. Emit
  defaults are unchanged (v3.1.0 / v1.0.0); opt into the newer version with
  `OdcsProvider(api_version=…)` / `BitolOdpsProvider(api_version=…)`, the
  `ODCS_API_VERSION` / `ODPS_API_VERSION` env vars, or
  `fluid opds export --spec bitol-1.1.0`. Re-exports of imported documents
  preserve the source's `apiVersion`.
- ODCS v3.2.0 content: `map` / `vector` logical types; `context`,
  `synonyms`, `enum`, `semanticType`, `deprecated`, stable `id`s; ten new
  `Server.type` engines (iceberg, hana, teradata, exasol, ingres,
  vectorwise, versant, zen, btrieve, poet/fastobjects) with the
  allowed-field tables the schema's `unevaluatedProperties: false` requires.
- ODPS v1.1.0 content: first-class product `type` (≥ v1.1.0 exports emit it
  natively; v1.0.0 exports keep the legacy `customProperties` encoding),
  `deprecated`, `context`, `synonyms`; relaxed `status` / port `version` /
  `contractId` requirements honoured on import. A product rendered below
  v1.1.0 is fitted to the vendored v1.0.0 schema, nested objects included
  (management ports, SBOM entries, input contracts, support channels, team
  members): a key v1.0.0 does not allow moves into that object's
  `customProperties` when it has them, otherwise it is dropped, each with a
  warning, so the document validates against the version it declares.
- **Bitol ODPS — bidirectional provider** (`BitolOdpsProvider`, registered
  as `odps_bitol`). Export emits the canonical fragments layout: 1 ODPS doc
  + N sibling `<contractId>.odcs.yaml` files; each per-port ODCS `id`
  follows the port's `contractId` (UUIDs included), minting
  `{productId}.{port}` only when a port has none. Bundle import keeps each
  port's complete ODCS state, so `fluid opds import` then `fluid opds
  export` reproduces the product and every per-port ODCS document (servers,
  team, SLA, top-level fields, per-document `apiVersion`) under strict
  validation, schema-less per-port contracts included. Same-name ports that
  differ only in version import as distinct exposes. Fresh output ports
  carry the `customProperties` (server, environment, containsPii,
  autoApprove, status) and the product the `type` / `domain` /
  `fluidVersion` properties the 0.7.8 ODPS exporter emitted, and honour
  `expose.contract_id` and `ODPS_DEFAULT_PORT_VERSION`. Output-port
  `contractId`s are minted as `{productId}.{port}` (the id of the sibling
  per-port ODCS), also by the legacy `OdpsStandardProvider` / `fluid
  odps-bitol export`, which used `{exposeId}_contract` in 0.7.8; set
  `contract_id` on an expose to keep a specific id.
- **Three import entry points** — single ODPS file, directory (ODPS +
  sibling ODCS, or just ODCS files), or a lone ODCS file — via
  `fluid opds import <path>`. Inputs are schema-validated; the output is a
  FLUID contract skeleton that may need edits (builds, bindings) before
  `fluid validate` passes. An ODCS-only directory import keeps each
  contract's id and document-level fields. `--lenient` downgrades
  output-port resolution failures, malformed `contractId`s and
  schema-invalid ODCS inputs to warnings (input ports are always lenient).
  `-o FILE` picks the format from the suffix unless `-f` is given. FLUID
  contracts passed as input are rejected with a clear error rather than
  mistaken for ODPS.
- **`ContractResolver`** — resolves Bitol ODPS port `contractId`
  references to ODCS documents through local-file probes and http(s) fetch,
  with caching, JSON Schema validation, HTML-with-200 refusal, and
  `--no-remote` hermetic mode. A local file is used only if its `id`
  matches the requested contractId.
- **`fluid opds validate|info --spec bitol-1.0.0|bitol-1.1.0|odpi-4.1`.**
  Without `--spec`, validate auto-detects Bitol ODPS documents (apiVersion
  + `kind: DataProduct`) and validates them against the schema matching
  their own apiVersion; any other document is validated as ODPI v4.1 as
  before. YAML and JSON input are both accepted.
- **`fluid forge --seed-from <path>`** — copilot accepts an ODCS contract,
  a Bitol ODPS product file, or a directory bundle as a structural seed.
  The seed's schema/qos are ground truth the LLM must not mutate; per-field
  ODCS quality is not carried into FLUID, and the LLM fills in builds,
  execution and the expose kind/binding/semantics. The ground truth is a
  FLUID 0.7.2-valid view (round-trip bookkeeping and ODCS-only field keys
  removed; each `exposeId` normalised to a FLUID identifier, e.g.
  `Daily Orders` → `daily_orders`) used for both the prompt and the
  `diff_against_seed()` guard, so a model that copies the seed verbatim
  passes strict validation. FLUID contracts, unresolved output-port
  contracts and seeds with no ground truth fail with a clear error; a run
  that fails only on ground-truth violations reports
  `failure_class: seed_mismatch`.
- **`OdcsProvider.roundtrip_check(odcs)`** — public diff primitive
  returning `{equal, missing, extra, changed}`; `OdcsProvider.render(...,
  contract_id=...)` and `OdcsProvider.import_contract(..., lenient=True)`
  keyword options; `OdcsProvider.to_model()` — typed view via the
  `open-data-contract-standard` pydantic model (Python 3.10+).
- `tests/fixtures/upstream/` — verbatim Bitol example documents (Apache-2.0)
  used as gold-standard fixtures, and `tests/test_spec_version_matrix.py`
  asserting lossless round-trip, version fidelity and correct schema
  selection across every fixture.
- `scripts/dmm_live_e2e.sh` — opt-in, manual end-to-end check against a
  real Entropy Data CE (Data Mesh Manager) instance: brings the CE compose
  stack up, publishes the multi-port fixture through both
  `fluid publish --catalog datamesh-manager` and `fluid dmm publish
  --with-contract`, at ODCS v3.1.0 and v3.2.0, and asserts via the REST API
  that the product and one contract per port landed with the expected
  `apiVersion`. Runs `fluid` under an isolated `HOME` so the user's config
  is untouched. Not run by CI (needs Docker and a DMM API key).
- New docs: `docs/BITOL_INTEROP.md` covering canonical-model architecture,
  the `contractId` convention, resolver behaviour, and the forge
  `--seed-from` flow.

### Deprecated
- `fluid odps export --version 4.1` — use `--spec odpi-4.1` (warning logged).
- `fluid odps-bitol export|validate|info` — use `fluid opds … --spec
  bitol-1.0.0`. A deprecation notice is printed on every invocation; kept
  working for one release.
- `CatalogAsset`, `BaseCatalogProvider.map_contract_to_asset`,
  `validate_asset`, `publish_asset` / `update_asset`, and calling
  `publish(asset)` / `update(asset)` with a `CatalogAsset`. These keep
  working for one release with a `DeprecationWarning` and publish the
  asset's own fields (including edits made after `map_contract_to_asset`,
  e.g. `sensitivity` → `is_public`), in positional and keyword form
  (`publish(asset=...)`). Third-party catalog providers written against the
  0.7.8 API keep working through `fluid publish`, and `map_contract_to_asset`
  / `validate_asset` overrides are honoured, including on subclasses of the
  built-in providers, where they reach the published payload. Migrate to
  `publish(fluid, CatalogTarget.from_fluid(fluid))`.
- `verify(asset_id=...)` / `delete(asset_id=...)` on catalog providers: the
  parameter is now `contract_id`; `asset_id=` keeps working for one release
  with a `DeprecationWarning`.
- `fluid_build.providers.odcs.mappers.types.physical_type_to_platform` is
  no longer used by the importer and is kept only for API compatibility.

### Fixed
- ODCS/ODPS round-trip lossiness surfaced by real upstream documents:
  explicit `required: false` / `primaryKey: false` and empty `tags: []` were
  dropped (800+ diffs on AdventureWorks); property-level `relationships` and
  `transformSourceObjects` / `transformLogic` / `transformDescription` were
  never passed through; `logicalType: map` was coerced to `string`;
  re-export synthesised `schema[].logicalType`, a top-level `name`, and a
  `servers[]` block for imported documents that had none; `apiVersion` was
  not preserved on re-export; SchemaObjects without `properties` were
  dropped.
- FLUID `number`, `float64`, `int64`, `smallint`, `timestamp_ntz` and
  similar column types exported as ODCS `logicalType: string`.
- ODCS export dropped `name`, `description` and `domain` for schema-valid
  FLUID contracts that set them at the top level.
- `OdpsStandardProvider.render` (the ODPS path behind `fluid dmm publish`)
  raised `KeyError: 'id'` on exposes that use the FLUID 0.7.x `exposeId`
  key, i.e. every shipped example contract.
- `fluid opds export` (ODPI stdout path) printed the payload through Rich,
  wrapping lines at 80 columns and stripping `[bracket]` text; JSON/YAML
  payloads now go to stdout raw.
- `FluidConfig` leaked file config into the module-level defaults across
  instances.
- `tests/fixtures/odcs/contract-full.yaml` (relationship `from` must be
  `table.column`) and `contract-minimal.yaml` (a `local` server requires
  `format`) were invalid against the version they declare.

### Security
- `FLUID_API_KEY` / `FLUID_CC_ENDPOINT` environment overrides are no longer
  written into the stored config, so `save_user_config()` cannot persist
  them to disk.
- ODPS `contractId`s that are absolute or contain path separators / `..`
  are never used as local file names, symlinks that resolve outside a bundle
  are ignored by `fluid opds import` and `fluid forge --seed-from`, and
  bundle export refuses product/contract ids that would write outside
  `--out-dir` (or next to `--out FILE`).
- Remote `contractId` fetches are size-capped (10 MiB), time-boxed (30 s
  total, covering connect, slow response headers and chunked bodies) and
  follow only http(s) redirects.
- YAML contracts (local ODCS/ODPS files and remote `contractId` fetches) are
  refused when their aliases would expand beyond 1,000,000 nodes or are
  recursive ("billion laughs"); JSON-Schema validation no longer duplicates
  aliased subtrees in memory.
- `scripts/dmm_live_e2e.sh` uses a private `mktemp -d` work dir and passes
  the API key through a 0600 header file instead of the curl command line.

## [0.7.8] — 2026-04-03

### Changed
- **Release version consistency** — aligned the runtime CLI version with package metadata so `fluid --version` and the published package both report `0.7.8`.
- **Release notes continuity** — added the `0.7.8` changelog section and compare link so the release history matches the version bump.

### Fixed
- **Windows timeout enforcement** — corrected the non-`SIGALRM` security timeout path so operations time out promptly instead of waiting for the worker to finish.
- **Regression coverage** — added a targeted test for the Windows timeout path to keep the fallback behavior stable.

## [0.7.7] — 2026-04-01

### Added
- **Forge copilot architecture refresh** — modularized the `fluid forge` copilot flow into focused runtime, context, UI, mode, and agent layers for easier iteration and maintenance.
- **Declarative domain-agent specs** — built-in YAML-backed agent specs now power domain guidance without hard-coding every interview path in Python.
- **Project-memory-aware copilot flow** — copilot generation now supports project-scoped memory and post-generation clarification loops to refine outputs with more context.

### Changed
- **Release version bump** — promoted the Forge CLI and companion Claude plugin assets to `0.7.7` to reflect the sizable copilot feature set landing in this release.

### Fixed
- **Test suite alignment with PR #8 merge** — updated tests to match new `_extract_sla_properties()` list-of-dicts return format, `_publish_odcs_per_expose()` keyword argument signature, and `dataContractId` format (`{product_id}.{expose_id}`)
- **Result URL domain** — test assertions updated to match `api.entropy-data.com` (from `app.entropy-data.com`)
- **License headers** — added Apache 2.0 headers to `tests/test_datamesh_manager_publish_spec.py` and `tests/test_odcs_sla_properties.py`

## [0.7.6] — 2026-03-31

### Fixed
- **Data Product payload conformance** — `PUT /api/dataproducts/{id}` now sends `dataProductSpecification: "0.0.1"`, root-level `id`, `info.title` (not `info.name`), and `info.owner` per Data Product Specification v0.0.1 schema
- **Output port server object** — output ports now send structured `server` objects (`account`, `database`, `table`) instead of flat `location` strings, matching the DPS schema
- **Result URLs** — publish result URLs now use the configured `api_url` instead of hardcoded `app.entropy-data.com`
- **`_cmd_publish()` signature bug** — all five `_cmd_*` functions in `datamesh_manager.py` now accept `(args, logger=None)` to match the CLI dispatcher in `__init__.py:432`

### Added
- **ODCS v3.1.0 data contract support** — `_build_data_contract_odcs()` generates Open Data Contract Standard v3.1.0 payloads (`apiVersion`, `kind: DataContract`, `team.name`, `description.purpose`, array-based `schema` with `logicalType` mapping)
- **`--contract-format {odcs,dcs}` CLI flag** — choose between ODCS v3.1.0 (default) and deprecated DCS 0.9.3 when publishing companion data contracts
- **`dataContractId` wiring** — output ports automatically include `dataContractId` linking to the companion contract when `--with-contract` is used
- **Archetype inference** — `info.archetype` auto-inferred from `metadata.layer` (Bronze→source-aligned, Silver→consumer-aligned, Gold→aggregate) when not explicitly set
- **SQL-to-ODCS type mapping** — `_odcs_logical_type()` maps 25+ SQL/FLUID types to ODCS logical types
- **86 tests** for `fluid dmm` subcommand (up from 40), covering DPS conformance, ODCS/DCS builders, server objects, format dispatch, and dataContractId wiring

### Deprecated
- DCS 0.9.3 data contract format — use `--contract-format dcs` for backward compatibility. Entropy Data removing DCS support after 2026-12-31.

## [Unreleased]

### Added
- **Multi-file contract composition via `$ref`** — split contracts across multiple files using `$ref` pointers (OpenAPI-style). Refs resolve transparently in all commands (validate, plan, apply, execute). Supports external file refs, JSON pointer fragments (`file.yaml#/section`), nested/transitive refs, and subdirectory resolution. Circular reference detection and depth limiting protect against malformed contracts.
- **`fluid compile` command** — bundle a multi-file contract into a single resolved document. Supports `--out` for file output, `--env` for overlay application, and `--format json|yaml` for output format control. Equivalent to `swagger-cli bundle` for OpenAPI.
- **`compile_contract()` API** — programmatic entry point for $ref resolution in `fluid_build.loader`.
- **`RefResolutionError` exception** — clear error type for $ref failures (missing files, circular refs, invalid pointers).
- Multi-file contract example: `examples/0.7.1/bitcoin-multifile/` demonstrating team-owned fragments.
- "What is FLUID?" section in README
- CI status badge in README
- `.secrets.baseline` for detect-secrets scanning
- `.pre-commit-config.yaml` with detect-secrets, black, and ruff hooks
- `.github/CODEOWNERS` for code review ownership
- `THIRD_PARTY_LICENSES.md` documenting all dependency licenses
- `CHANGELOG.md` (this file)
- CLI smoke tests (`tests/cli/test_cli_smoke.py` — 28 tests)
- Config manager tests (`tests/test_config_manager.py` — 45 tests)
- `docs/HOW_IT_WORKS.md` — architecture overview of SDK + CLI

### Fixed
- Removed hardcoded default password (`'admin'`) in Apache Atlas connector — now requires explicit config or `ATLAS_USERNAME`/`ATLAS_PASSWORD` env vars
- Replaced 17 occurrences of hardcoded AWS account ID `123456789012` with `YOUR_AWS_ACCOUNT_ID` placeholder
- AWS Glue/SageMaker fallback role ARNs now use `self.account_id` instead of hardcoded values

### Removed
- `fluid_build/cli/admin.py` — internal admin/diagnostics command (not for public use)
- `fluid_build/cli/analytics.py` — internal telemetry module (dead code)

### Changed
- Bandit security scan in CI now blocks on failures (removed `|| true` fallback)

## [0.7.1] — 2026-01-30

### Added
- Multi-provider Airflow DAG generation (`fluid generate-airflow`)
- GCP code generators: Airflow, Dagster, Prefect
- Snowflake code generators: Airflow, Dagster, Prefect
- AWS code generators: Airflow, Dagster, Prefect
- Circular dependency detection in contract validation
- Provider SDK extraction (`fluid-provider-sdk` v0.1.0)
- Plugin-based provider discovery via entry points
- Local provider with DuckDB backend
- Policy engine (check, compile, apply)
- Blueprint system for data product templates
- ODPS/ODCS standard export support
- Marketplace and catalog connectors
- Comprehensive GitHub Actions CI (Python 3.9–3.12, ruff, black, bandit, coverage)
- Apache 2.0 license headers on all source files
- CONTRIBUTING.md, CODE_OF_CONDUCT.md, SECURITY.md
- Issue templates (bug report, feature request, provider request)
- PR template

### Performance
- Contract generation: 0.29–2.54ms per contract
- 828 tests passing at release

## [0.5.7] — 2025-08-15

### Added
- Initial public release
- Core CLI: `validate`, `plan`, `apply`
- GCP BigQuery provider
- Contract schema v0.5.7
- Basic Airflow DAG export

[Unreleased]: https://github.com/Agentics-Rising/forge-cli/compare/v0.7.8...HEAD
[0.7.8]: https://github.com/Agentics-Rising/forge-cli/compare/v0.7.7...v0.7.8
[0.7.7]: https://github.com/Agentics-Rising/forge-cli/compare/v0.7.6...v0.7.7
[0.7.6]: https://github.com/Agentics-Rising/forge-cli/compare/v0.7.1...v0.7.6
[0.7.1]: https://github.com/Agentics-Rising/forge-cli/compare/v0.5.7...v0.7.1
[0.5.7]: https://github.com/Agentics-Rising/forge-cli/releases/tag/v0.5.7
