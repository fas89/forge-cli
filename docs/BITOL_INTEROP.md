# Bitol Interoperability

FLUID plugs into the wider Bitol data ecosystem through two open standards.

## The three standards under one roof

| Standard | What it describes | Authority | FLUID provider | CLI selector |
|---|---|---|---|---|
| **ODCS v3.1.0 / v3.2.0** | Per-dataset contract (schema, quality, SLA, governance) | [bitol-io/open-data-contract-standard](https://github.com/bitol-io/open-data-contract-standard) | `odcs` | `fluid odcs ...` |
| **Bitol ODPS v1.0.0 / v1.1.0** | Data product wrapper referencing ODCS contracts via `contractId` | [bitol-io/open-data-product-standard](https://github.com/bitol-io/open-data-product-standard) | `odps_bitol` | `fluid opds --spec bitol-1.0.0` (default) / `bitol-1.1.0` |
| **ODPI v4.1** | Open Data Product Initiative v4.1 (Linux Foundation) — single JSON document | [Open-Data-Product-Initiative/v4.1](https://github.com/Open-Data-Product-Initiative/v4.1) | `odps` | `fluid opds --spec odpi-4.1` |

**Bitol ODPS** and **ODPI v4.1** are unrelated despite sharing the "ODPS" three-letter slug. Phase 5 of this work made the disambiguation explicit in every layer (provider names, CLI flag, docstrings, features.yaml).

## Canonical-model architecture

FLUID is the canonical hub. Every export goes FLUID → standard; every import goes standard → FLUID. The Bitol ODPS provider delegates per-port ODCS rendering to the ODCS provider — there's exactly one place that knows how to translate schema/quality/SLA across the boundary.

```
                                  FLUID (canonical)
                                       ↓
                       ┌───────────────┼───────────────┐
                       ↓               ↓               ↓
                 OdcsProvider   BitolOdpsProvider   OdpsProvider
                       │               │  ↓               │
                       │               │  └── per-port ODCS
                       │               │       (via OdcsProvider)
                       ↓               ↓               ↓
                  *.odcs.yaml    1 ODPS doc        single
                                 + N *.odcs.yaml   ODPI-v4.1 JSON
```

## Export pattern (Bitol ODPS)

One FLUID contract with N output ports always emits:

- 1 `<productId>.odps.yaml` — the Bitol ODPS product doc.
- N `<contractId>.odcs.yaml` siblings — one per output port, referenced by `contractId`.

`contractId` follows the convention `{productId}.{portName}` and matches the `id` of the corresponding ODCS contract exactly. This linking invariant is enforced inside the provider (`BitolOdpsProvider.render` raises `ProviderError` on mismatch) and asserted in tests.

```bash
fluid opds export contract.fluid.yaml --spec bitol-1.0.0 --out-dir ./out
# ./out/<productId>.odps.yaml
# ./out/<productId>.<port1>.odcs.yaml
# ./out/<productId>.<port2>.odcs.yaml

# --out FILE writes the product doc to FILE and the per-port ODCS contracts
# next to it; the format follows the suffix unless -f/--format is given.
# Use a different directory than --out-dir: a directory import accepts only
# one ODPS doc.
fluid opds export contract.fluid.yaml --out ./single/product.json
# ./single/product.json
# ./single/<productId>.<port1>.odcs.json
# ./single/<productId>.<port2>.odcs.json

# No --out / --out-dir: the product doc is printed to stdout as JSON.
fluid opds export contract.fluid.yaml > product.json
```

With `--spec` omitted the emitted `apiVersion` follows `ODPS_API_VERSION`, else the imported document's own `apiVersion` (re-exports of imports), else `v1.0.0`; an explicit `--spec bitol-1.0.0` / `bitol-1.1.0` pins it. Exports are validated against the vendored schemas; `--no-validate-strict` still validates but reports problems as warnings on stderr instead of failing.

`--out-dir` and `-f yaml` apply to the Bitol specs only; `--spec odpi-4.1` always writes a single JSON document to `--out` (or stdout) and rejects `--out-dir` and `-f yaml` with an error (`-f json` is accepted).

No inline-embedding mode. The fragments layout is the only export pattern — it matches Bitol convention and `datacontract-cli` expectations.

## Import — three entry points, one FLUID skeleton

All three converge on the same FLUID skeleton. The input documents are schema-validated, but the output is a **skeleton, not yet a schema-valid FLUID contract**: add `fluidVersion` / `kind` / `id` / `name`, each expose's `kind`, and binding `format` / `location` (and map the platform to a supported provider) before `fluid validate` / `plan` / `apply` will accept it. The `*_passthrough` keys it carries are what make re-export lossless.

```bash
# 1. Single ODPS file — resolver finds sibling ODCS by contractId.
fluid opds import out/<productId>.odps.yaml -o product.fluid.yaml

# 2. Directory — auto-discovers the ODPS doc + all ODCS files inside.
#    Also handles ODCS-only directories (no ODPS doc → multi-expose FLUID
#    with no product wrapper, warning emitted).
fluid opds import out/ -o product.fluid.yaml

# 3. Lone ODCS file — produces a single-expose FLUID.
fluid opds import out/<productId>.<port1>.odcs.yaml -o port1.fluid.yaml
```

With `-o` the format follows the file suffix (`.json` → JSON, anything else → YAML) unless `-f/--format` is given. Without `-o` the FLUID document is written to stdout (YAML, or JSON with `-f json`).

Flags:

- `--no-remote` — refuse http(s) `contractId` resolution (hermetic CI).
- `--lenient` — downgrade output-port resolution failures to warnings. Input ports are always lenient because their contracts often live in other repos / registries.

## Validate

```bash
fluid opds validate out/<productId>.odps.yaml                    # auto-detected as Bitol
fluid opds validate out/<productId>.odps.yaml --spec bitol-1.1.0 # pin the schema version

fluid opds export contract.fluid.yaml --spec odpi-4.1 --out odpi.json
fluid opds validate odpi.json --spec odpi-4.1                    # ODPI v4.1 (also: --version 4.1)
```

YAML and JSON are both accepted. With `--spec` omitted, a document declaring `apiVersion` and `kind: DataProduct` is validated as Bitol ODPS against the vendored schema matching its own `apiVersion`; anything else is validated as ODPI v4.1, exactly as before. `fluid opds info --spec <spec>` shows the details of one specification.

## The `contractId` resolver

`fluid_build.providers.odps_standard.resolver.ContractResolver` resolves `contractId` references with this priority:

1. **Explicit hint** — when a caller passes a file path or URL alongside the id.
2. **Pre-indexed file** — files passed in via `additional_files=` or discovered by `index_directory()`. The directory import path uses this so local files always beat anything else.
3. **Local file probes** — across the cross-product of `{<base>, contracts/}` × `{<full-id>, <last-segment>}` × `{.odcs.yaml, .odcs.yml, .odcs.json, .yaml}` (16 candidates for a dotted id). Other layouts (an `odcs/` subdirectory, plain `.yml` / `.json` names) are found by directory import (`fluid opds import <dir>/`), which indexes every `.yaml` / `.yml` / `.json` file under the directory by its ODCS `id`.
4. **HTTP(S) URL** — when the `contractId` itself is a URL and `allow_remote=True`. Refuses HTML-with-200 responses and non-http(s) schemes.

Every resolved document is validated against the vendored ODCS JSON Schema matching its own `apiVersion` (v3.2.0 and v3.1.0 are vendored; older versions are checked against the newest, which is a strict superset); `ContractValidationError` raises with the offending field name.

## Round-trip guarantees

- **ODCS → FLUID → ODCS** is structurally lossless. Pass-through namespace (`metadata.odcs_passthrough.*`, `expose.odcs_passthrough.*`, `field.odcs_passthrough.*`) preserves every section the FLUID model doesn't natively represent.
- **FLUID → Bitol ODPS → FLUID → Bitol ODPS** reproduces the product doc and every per-port ODCS contract zero-diff (tested in `tests/test_odps_bitol_import.py::TestBundleRoundTrip`).
- `OdcsProvider.roundtrip_check(odcs)` is a public primitive returning a structured `{equal, missing, extra, changed}` diff — used by tests and the forge ground-truth guard.

## `fluid forge --seed-from` — standards as structural seed

The copilot accepts the same three input shapes as `opds import` as a **structural seed**:

```bash
# Lone ODCS — single-expose FLUID skeleton
fluid forge --seed-from existing-contract.odcs.yaml --context '{"goal":"add daily refresh"}'

# Single Bitol ODPS file — multi-expose skeleton with sibling ODCS resolved
fluid forge --seed-from product.odps.yaml

# Directory bundle — same as above; resolves sibling ODCS files by contractId
fluid forge --seed-from ./contracts/
```

Each expose's schema and qos from the seed are **ground truth** the LLM must not modify, and every seed expose must be kept, in order, under its `exposeId`. They are presented to the LLM as valid FLUID 0.7.2: an ODCS schema object or ODPS port name that is not a FLUID identifier is normalised deterministically (lower-cased, other characters replaced by `_`, so `Daily Orders` becomes `daily_orders`), and ODCS-only keys such as per-field `quality` rules are **not carried** into the FLUID output; add them to the generated contract yourself if you need them. The pre-processor (`fluid_build.cli.forge_copilot_seed.load_seed`) emits a `SeedResult` carrying:

- `fluid` — the imported skeleton the LLM augments.
- `provenance` — every contract source consumed (paths + URLs).
- `ground_truth_paths` — dotted paths inside `fluid` that must round-trip.
- `diff_against_seed(seed, candidate)` — primitive returning `{path, seed, candidate}` mismatches whenever the candidate mutated a ground-truth value (schema, qos or an `exposeId`).

The LLM fills in builds (with their execution) and each expose's kind, binding and semantics — the things the standards don't model. A directory seed reads only files inside the directory: symlinks that resolve outside it are ignored.

## File map

| Concern | Lives in |
|---|---|
| ODCS provider orchestrator | `fluid_build/providers/odcs/provider.py` |
| ODCS section mappers (1 per section) | `fluid_build/providers/odcs/mappers/*.py` |
| ODCS validation + round-trip diff | `fluid_build/providers/odcs/validation.py` |
| Bitol ODPS provider orchestrator | `fluid_build/providers/odps_standard/provider.py` |
| Bitol ODPS section mappers | `fluid_build/providers/odps_standard/mappers/*.py` |
| Bitol ODPS schemas | `fluid_build/providers/odps_standard/schemas/odps-product-v1.1.0.json`, `…-v1.0.0.json` |
| ODCS schemas | `fluid_build/providers/odcs/odcs-schema-v3.2.0.json`, `…-v3.1.0.json` |
| Upstream example fixtures (gold standard) | `tests/fixtures/upstream/{odcs,odps}/` — verbatim Bitol examples, exercised by `tests/test_spec_version_matrix.py` |
| `contractId` resolver | `fluid_build/providers/odps_standard/resolver.py` |
| CLI dispatcher | `fluid_build/cli/opds.py` |
| Forge `--seed-from` pre-processor | `fluid_build/cli/forge_copilot_seed.py` |
| Tests | `tests/test_odcs_roundtrip.py`, `tests/test_odps_bitol_export.py`, `tests/test_odps_bitol_import.py`, `tests/test_odps_resolver.py`, `tests/test_opds_cli_spec.py`, `tests/test_forge_copilot_seed.py` |
