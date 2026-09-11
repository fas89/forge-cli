# Upstream Bitol example fixtures (gold standard)

Verbatim copies of the example documents shipped by the Bitol specification
repositories, vendored so our import/export/validate paths are exercised
against real, maintainer-authored documents — not just hand-rolled fixtures.

| File | Source | Declares |
|------|--------|----------|
| `odcs/full-example.v3.2.0.odcs.yaml` | bitol-io/open-data-contract-standard `docs/examples/all/full-example.odcs.yaml` | `apiVersion: v3.2.0` |
| `odcs/postgresql-adventureworks.v3.0.0.odcs.yaml` | bitol-io/open-data-contract-standard `docs/examples/all/postgresql-adventureworks-contract.odcs.yaml` | `apiVersion: v3.0.0` (older-than-vendored; exercises the superset fallback) |
| `odps/customer-data-product.v1.1.0.odps.yaml` | bitol-io/open-data-product-standard `docs/examples/customer-data-product.odps.yaml` | `v1.1.0` — uses `context`, `synonyms`, top-level `type` |
| `odps/deprecated-data-product.v1.1.0.odps.yaml` | … `docs/examples/deprecated-data-product.odps.yaml` | `v1.1.0` — uses `deprecated` |
| `odps/simple-data-product.v1.1.0.odps.yaml` | … `docs/examples/simple-data-product.odps.yaml` | `v1.1.0` — minimal (relaxed `required`) |
| `odps/variables.v1.1.0.odps.yaml` | … `docs/examples/variables.odps.yaml` | `v1.1.0` — `${VAR}` references (RFC 0050) |
| `odps/vendor-custom-properties.v1.1.0.odps.yaml` | … `docs/examples/vendor-custom-properties.odps.yaml` | `v1.1.0` — `customProperties[].vendor` |

Both upstream repositories are Apache-2.0 licensed. Files are unmodified;
the `.vX.Y.Z.` infix in the filename is ours, recording the declared
`apiVersion` at vendoring time. Refresh with the fetch snippet in
`tests/test_spec_version_matrix.py`.
