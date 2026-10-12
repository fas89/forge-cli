# Changelog

All notable changes to FLUID Forge CLI will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `scripts/dmm_live_e2e.sh` — opt-in, manual end-to-end check against a
  real Entropy Data CE (Data Mesh Manager) instance: brings the CE compose
  stack up, publishes the multi-port fixture through both
  `fluid publish --catalog datamesh-manager` and `fluid dmm publish
  --with-contract`, at ODCS v3.1.0 and v3.2.0, and asserts via the REST
  API that the product and one contract per port landed with the expected
  `apiVersion`. Not run by CI (needs Docker and a DMM API key).
- **ODCS v3.2.0 and Bitol ODPS v1.1.0** vendored alongside v3.1.0 / v1.0.0.
  Validation is now *version-selected* from the document's own `apiVersion`
  (`resolve_schema_version`): a v3.2.0 / v1.1.0 document is judged by its
  own schema instead of failing the older schema's `apiVersion` enum;
  older-than-vendored versions (e.g. v3.0.0) validate against the newest
  vendored schema, which is a strict superset. Emit defaults are unchanged
  (v3.1.0 / v1.0.0); opt into the newer version with
  `OdcsProvider(api_version=…)` / `BitolOdpsProvider(api_version=…)`, the
  `ODCS_API_VERSION` / `ODPS_API_VERSION` env vars, or
  `fluid opds export --spec bitol-1.1.0`. Re-exports of imported documents
  preserve the source's `apiVersion`.
- ODCS v3.2.0 content: `map` / `vector` logical types; `context`, `synonyms`,
  `enum`, `semanticType`, `deprecated`, stable `id`s; ten new
  `Server.type` engines (iceberg, hana, teradata, exasol, ingres, vectorwise,
  versant, zen, btrieve, poet/fastobjects) with the allowed-field tables the
  schema's `unevaluatedProperties: false` requires.
- ODPS v1.1.0 content: first-class product `type` (fresh ≥ v1.1.0 exports
  emit it natively; v1.0.0 exports keep the legacy `customProperties`
  encoding), `deprecated`, `context`, `synonyms`; relaxed `status` /
  port `version` / `contractId` requirements honoured on import.
- `OdcsProvider.to_model()` — typed view via the `open-data-contract-standard`
  pydantic model (now part of the `odcs-strict` extra) for attribute access
  and readable field-level errors.
- `tests/fixtures/upstream/` — verbatim Bitol example documents (Apache-2.0)
  used as gold-standard fixtures, and `tests/test_spec_version_matrix.py`
  (46 cases) asserting lossless round-trip, version fidelity and correct
  schema selection across every fixture.
- **Bitol ODPS v1.0.0 — bidirectional provider** (`BitolOdpsProvider`,
  registered as `odps_bitol`). Export emits the canonical fragments layout:
  1 ODPS doc + N sibling `<contractId>.odcs.yaml` files; the linking
  invariant `port.contractId == odcs.id` is enforced by construction and
  asserted in tests.
- **Three import entry points**, all converging on one validated FLUID
  contract: single ODPS file, directory (ODPS + sibling ODCS or just ODCS
  files), or a lone ODCS file. Exposed via `fluid opds import <path>`.
- **`ContractResolver`** — resolves Bitol ODPS port `contractId` references
  to ODCS documents through local-file probes and http(s) fetch, with
  caching, JSON Schema validation, HTML-with-200 refusal, and `--no-remote`
  hermetic mode.
- **`fluid opds export --spec bitol-1.0.0|odpi-4.1`** — `--spec` dispatches
  between Bitol ODPS v1.0.0 (new default) and the existing ODPI v4.1.
  `--out-dir`, `--validate-strict`, and `--format` flags added.
- **`fluid forge --seed-from <path>`** — copilot accepts an ODCS contract,
  a Bitol ODPS product file, or a directory bundle as a structural seed.
  The seed's schema/quality/qos are ground truth the LLM must not mutate.
  Pre-processor in `fluid_build.cli.forge_copilot_seed`, with provenance
  and `diff_against_seed()` guard primitive.
- **`OdcsProvider.roundtrip_check(odcs)`** — public diff primitive
  returning `{equal, missing, extra, changed}` for round-trip tests and
  the forge ground-truth guard.
- New docs: `docs/BITOL_INTEROP.md` covering canonical-model architecture,
  the `contractId` convention, resolver behaviour, and the forge
  `--seed-from` flow.

### Changed
- **`fluid dmm publish` now sends Bitol ODPS by default** (previously the
  legacy Data Product Specification 0.0.1). Current Entropy Data / Entropy
  CE organisations accept only `odps` and reject DPS with HTTP 400
  (`Specification type 'dps' is not supported`), so the old default was a
  guaranteed failure; `fluid publish --catalog datamesh-manager` already
  defaulted to ODPS. Opt back into DPS with `--data-product-spec 0.0.1`,
  `provider_hint="dps"`, or the new `DMM_DATA_PRODUCT_SPEC` env var.
  Resolution order: explicit flag → provider hint → `kind: DataProduct` →
  env var → ODPS. Verified live against Entropy CE 2.1.x on both publish
  paths at ODCS v3.1.0 and v3.2.0 (`scripts/dmm_live_e2e.sh`).
- **Mapper round-trip architecture: verbatim stash + native overlay.** On
  import every ODCS/ODPS section (document, schema object, property, port)
  is stashed verbatim; on export the stash is the base layer and only the
  FLUID-native fields that actually changed are overlaid (three-way merge,
  `overlay_native`). This replaces the hand-maintained per-field
  pass-through tuples — 104 lines of per-field copy code removed — and
  means fields added by future spec versions round-trip with no code.
  Validated lossless against verbatim upstream Bitol examples, including
  the 192 KB v3.0.0 AdventureWorks contract.
- `OdcsProvider.validate_contract` / `BitolOdpsProvider.validate_product`
  select the JSON Schema from the document's `apiVersion` rather than the
  provider's configured emit version.
- `validate_via_vowl` now skips with a warning (instead of failing the
  export) when the installed `vowl` predates the document's `apiVersion`
  (`vowl` 0.0.5 supports up to v3.1.0; 0.0.6 adds v3.2.0).
- The `odcs-strict` extra now also installs `open-data-contract-standard`
  and `deepdiff`; `deepdiff` is also declared in the `dev` extra — it backs
  `roundtrip_check()` and was previously an undeclared import, so a fresh
  `pip install -e .[dev]` could not run the ODCS/ODPS round-trip tests.
- **ODCS v3.1.0 import is now lossless.** Two import bugs fixed
  (`_odcs_team_to_fluid_owner` now accepts the v3.1.0 object form;
  `_odcs_schema_to_field` now reads `required` instead of the non-existent
  `isNullable`). New sections preserved end-to-end: `slaProperties`,
  contract/object/property-level `quality`, `relationships`, `support`,
  `price`, `customProperties`, `authoritativeDefinitions`, `roles`. ODCS
  → FLUID → ODCS round-trips zero-diff on the full v3.1.0 fixture.
- **ODCS provider refactored into modular bidirectional mappers.** The
  monolithic 1441-line `odcs.py` split into `provider.py`, `validation.py`,
  `io.py`, and `mappers/{metadata,team,schema,quality,sla,servers,types}.py`.
  Each section module exposes paired `to_fluid` / `to_odcs` functions. All
  pass-through values live under a single `odcs_passthrough` namespace at
  each level (metadata / expose / field) for auditability.
- **`fluid opds`** now has `opds` as an alias for `odps`; both go through
  the `--spec` dispatcher. Default spec changed to `bitol-1.0.0`.
- **Naming disambiguation.** Provider registry now has three clearly
  distinct names: `odcs` (Open Data Contract Standard v3.1.0), `odps`
  (ODPI v4.1, Linux Foundation; export-only), `odps_bitol` (Bitol Open
  Data Product Standard v1.0.0; bidirectional). Docstrings and
  `features.yaml` updated.
- **ODCS server export now type-aware.** Each `server.type` only emits
  the fields its ODCS v3.1.0 `ServerSource` allows (snowflake, bigquery,
  s3, kafka, postgres, mysql, databricks, redshift, local, athena, plus
  the `custom` union). The previous behaviour leaked snowflake's `table`
  (which belongs in `contract.schema`) and tripped strict validation.

### Deprecated
- `fluid odps export --version 4.1` — use `--spec odpi-4.1` (warning logged).
- `fluid odps-bitol export` — use `fluid opds export --spec bitol-1.0.0`
  (warning printed on every invocation). Kept working for one release.

### Fixed
- ODCS/ODPS round-trip lossiness surfaced by real upstream documents:
  explicit `required: false` / `primaryKey: false` and empty `tags: []`
  were dropped (truthiness checks; 800+ diffs on AdventureWorks);
  property-level `relationships` and `transformSourceObjects` /
  `transformLogic` / `transformDescription` were never passed through;
  `logicalType: map` was coerced to `string`; re-export synthesised
  `schema[].logicalType`, a top-level `name`, and a `servers[]` block for
  imported documents that had none (the synthesised server could itself be
  schema-invalid — `servers` is not required in any vendored version);
  ODPS output-port `contractId` was overwritten with the
  `{productId}.{port}` convention even when the import captured the
  original; `apiVersion` was not preserved on re-export.
- `tests/fixtures/odcs/contract-full.yaml` (relationship `from` must be
  `table.column`) and `contract-minimal.yaml` (a `local` server requires
  `format`) were invalid against the version they declare; import
  validation is warn-only so this went unnoticed.
- `fluid forge --seed-from`: the ground-truth block sent to the LLM and
  the `diff_against_seed` guard included the `*_passthrough` round-trip
  stash (and never filtered `odps_passthrough`, which holds the whole
  source ODPS document for bundle seeds). Both now operate on the
  FLUID-native projection only — leaner prompt, and a model that omits
  round-trip bookkeeping is no longer flagged as mutating ground truth.
- `tests/test_forge_seed_real_llm.py` indexed `content[0].text`; Claude 5
  models lead with a `ThinkingBlock`, so it now selects the first text
  block and dogfoods the production prompt block. Set
  `FLUID_REAL_LLM_MODEL` to choose the model.

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
