# Bitol Interoperability

FLUID plugs into the wider Bitol data ecosystem through two open standards.

## The three standards under one roof

| Standard | What it describes | Authority | FLUID provider | CLI selector |
|---|---|---|---|---|
| **ODCS v3.1.0** | Per-dataset contract (schema, quality, SLA, governance) | [bitol-io/open-data-contract-standard](https://github.com/bitol-io/open-data-contract-standard) | `odcs` | `fluid odcs ...` |
| **Bitol ODPS v1.0.0** | Data product wrapper referencing ODCS contracts via `contractId` | [bitol-io/open-data-product-standard](https://github.com/bitol-io/open-data-product-standard) | `odps_bitol` | `fluid opds --spec bitol-1.0.0` (default) |
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
# ./out/product.odps.yaml
# ./out/product.port1.odcs.yaml
# ./out/product.port2.odcs.yaml
```

No inline-embedding mode. The fragments layout is the only export pattern — it matches Bitol convention and `datacontract-cli` expectations.

## Import — three entry points, one validated FLUID

All three converge on the same FLUID skeleton:

```bash
# 1. Single ODPS file — resolver finds sibling ODCS by contractId.
fluid opds import out/product.odps.yaml

# 2. Directory — auto-discovers the ODPS doc + all ODCS files inside.
#    Also handles ODCS-only directories (no ODPS doc → multi-expose FLUID
#    with no product wrapper, warning emitted).
fluid opds import out/

# 3. Lone ODCS file — produces a single-expose FLUID.
fluid opds import out/product.port1.odcs.yaml
```

Flags:

- `--no-remote` — refuse http(s) `contractId` resolution (hermetic CI).
- `--lenient` — downgrade output-port resolution failures to warnings. Input ports are always lenient because their contracts often live in other repos / registries.

## The `contractId` resolver

`fluid_build.providers.odps_standard.resolver.ContractResolver` resolves `contractId` references with this priority:

1. **Explicit hint** — when a caller passes a file path or URL alongside the id.
2. **Pre-indexed file** — files passed in via `additional_files=` or discovered by `index_directory()`. The directory import path uses this so local files always beat anything else.
3. **Local file probes** — across the cross-product of `{<base>, contracts/, odcs/, odcs/contracts/}` × `{<full-id>, <last-segment>}` × `{.odcs.yaml, .odcs.yml, .odcs.json, .yaml, .yml, .json}`.
4. **HTTP(S) URL** — when the `contractId` itself is a URL and `allow_remote=True`. Refuses HTML-with-200 responses and non-http(s) schemes.

Every resolved document is validated against the vendored ODCS v3.1.0 JSON Schema; `ContractValidationError` raises with the offending field name.

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

The seed's schema/quality/qos are **ground truth** the LLM must not modify. The pre-processor (`fluid_build.cli.forge_copilot_seed.load_seed`) emits a `SeedResult` carrying:

- `fluid` — the imported skeleton the LLM augments.
- `provenance` — every contract source consumed (paths + URLs).
- `ground_truth_paths` — dotted paths inside `fluid` that must round-trip.
- `diff_against_seed(seed, candidate)` — primitive returning `{path, seed, candidate}` mismatches whenever the candidate mutated a ground-truth value.

The LLM fills in builds, executes, and governance — the things the standards don't model.

## File map

| Concern | Lives in |
|---|---|
| ODCS provider orchestrator | `fluid_build/providers/odcs/provider.py` |
| ODCS section mappers (1 per section) | `fluid_build/providers/odcs/mappers/*.py` |
| ODCS validation + round-trip diff | `fluid_build/providers/odcs/validation.py` |
| Bitol ODPS provider orchestrator | `fluid_build/providers/odps_standard/provider.py` |
| Bitol ODPS section mappers | `fluid_build/providers/odps_standard/mappers/*.py` |
| Bitol ODPS schema | `fluid_build/providers/odps_standard/schemas/odps-product-v1.0.0.json` |
| `contractId` resolver | `fluid_build/providers/odps_standard/resolver.py` |
| CLI dispatcher | `fluid_build/cli/opds.py` |
| Forge `--seed-from` pre-processor | `fluid_build/cli/forge_copilot_seed.py` |
| Tests | `tests/test_odcs_roundtrip.py`, `tests/test_odps_bitol_export.py`, `tests/test_odps_bitol_import.py`, `tests/test_odps_resolver.py`, `tests/test_opds_cli_spec.py`, `tests/test_forge_copilot_seed.py` |
