# Changelog

All notable changes to FLUID Forge CLI will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Removed
- **ODPI v4.1 (Open Data Product Initiative) export path.** The legacy
  `fluid_build/providers/odps/` provider, the `--spec odpi-4.1` flag, the
  `fluid export-opds` command, and the `--version 4.1` deprecation alias
  are all gone. Bitol ODPS v1.0.0 is now the only supported data-product
  specification. Users who still need ODPI v4.1 output should pin to a
  release before this change.

### Changed
- **ODCS schema validation is now default-on with warn-on-fail.** The
  vendored ODCS v3.1.0 JSON Schema runs on every export by default
  (`ODCS_VALIDATE=true`); schema failures log a warning rather than raise.
  Callers wanting hard failure can set `ODCS_VALIDATE_STRICT=true` or call
  `OdcsProvider.validate_contract()` directly.
- **DMM publish defaults to ODPS shape.** `fluid datamesh-manager publish`
  (and `DataMeshManagerProvider.apply()`) now produce an ODPS-v1.0.0 shape
  payload by default — `apiVersion: v1.0.0` + `kind: DataProduct`, no `info`
  wrapper. The legacy DPS `0.0.1` shape is rejected by current Entropy Data
  releases with HTTP 400, so it's now opt-in via explicit
  `data_product_specification="0.0.1"` or `provider_hint="dps"` (the
  symmetric inverse of the existing `provider_hint="odps"`). Contracts that
  set `kind: DataProduct` see no behavioural change.
- **`fluid datamesh-manager list` reads ODPS top-level fields.** The list
  command's table renderer previously only read `id`/`name`/`status` from
  the legacy DPS `info` envelope, leaving every cell as `?` when DMM
  returned the ODPS shape (the new default). It now reads top-level
  fields first and falls back to the `info` envelope + `teamId` for
  backward-compat with older DMM responses.

### Added
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
- **`fluid opds export --spec bitol-1.0.0`** — Bitol ODPS v1.0.0 dispatcher
  with `--out-dir`, `--validate-strict`, and `--format` flags. `--spec`
  retained on the surface for forward-compatibility with future spec
  additions.
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
