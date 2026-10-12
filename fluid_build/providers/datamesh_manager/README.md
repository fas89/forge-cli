# Data Mesh Manager / Entropy Data Provider

Publish FLUID contracts as **data products** and **data contracts** to [Entropy Data](https://www.entropy-data.com/) (formerly Data Mesh Manager).

## Quick Start

```bash
# 1. Set your API key
export DMM_API_KEY="your-secret-api-key"

# 2. Publish a data product
fluid datamesh-manager publish contract.fluid.yaml

# 3. Preview without publishing
fluid dmm publish contract.fluid.yaml --dry-run

# 4. Publish with companion data contract
fluid dmm publish contract.fluid.yaml --with-contract
```

## Features

- **Publish data products** via `PUT /api/dataproducts/{id}`
- **Publish data contracts** via `PUT /api/datacontracts/{id}`
- **Auto-create teams** when they don't exist
- **Input/output port mapping** from FLUID expects/exposes
- **PII detection** from schema field classification
- **Multi-provider location** mapping (BigQuery, Snowflake, S3, Kafka, Redshift, etc.)
- **Dry-run mode** for previewing API payloads
- **Retry with backoff** for transient failures (429, 5xx)
- **Catalog adapter** — also works via `fluid publish --catalog datamesh-manager`

## CLI Commands

```bash
# Publish
fluid dmm publish contract.fluid.yaml
fluid dmm publish contract.fluid.yaml --dry-run
fluid dmm publish contract.fluid.yaml --with-contract
fluid dmm publish contract.fluid.yaml --team-id my-team

# List data products
fluid dmm list
fluid dmm list --format json

# Get a specific product
fluid dmm get search-queries-all

# Delete a product
fluid dmm delete search-queries-all

# List teams
fluid dmm teams
```

## Authentication

Generate an API key at: **Profile → Organization → Settings → API Keys**

```bash
export DMM_API_KEY="your-secret-api-key"

# Optional: custom API endpoint
export DMM_API_URL="https://api.entropy-data.com"
```

Or pass inline:
```bash
fluid dmm publish contract.yaml --api-key "your-key"
```

## FLUID → Entropy Data Mapping

### Default: Bitol ODPS

Data products are sent as **Bitol Open Data Product Standard (ODPS)**
documents (`apiVersion: v1.0.0`, `kind: DataProduct`) by default — current
Entropy Data / Entropy CE organisations accept only this type.

| FLUID Field | Entropy Data (ODPS) Field |
|---|---|
| `id` / `metadata.id` | `id` |
| `name` / `metadata.name` | `name` |
| `description` | `description.purpose` |
| `metadata.status` | `status` |
| `domain` | `domain` |
| `metadata.owner` / `owner.team` | `team.name` + `team.members[]` (`--team-id` overrides `team.name`) |
| `expects[]` | `inputPorts[]` |
| `exposes[]` | `outputPorts[]` (each with `contractId` = `{productId}.{exposeId}`) |
| `metadata.tags` / `tags` | `tags[]` |

### Legacy: Data Product Specification 0.0.1 (opt-out)

Organisations that still expect the legacy DPS 0.0.1 shape can opt out of ODPS
with `--data-product-spec 0.0.1`, `DMM_DATA_PRODUCT_SPEC=0.0.1`, or (for
`fluid publish --catalog datamesh-manager`) `data_product_spec: "0.0.1"` /
`provider_hint: dps` in the catalog config. Resolution order: explicit spec →
provider hint (`odps`/`opds` → ODPS, `dps` → 0.0.1) → `DMM_DATA_PRODUCT_SPEC`
→ ODPS.

```bash
fluid dmm publish contract.fluid.yaml --data-product-spec 0.0.1
```

| FLUID Field | Entropy Data (DPS 0.0.1) Field |
|---|---|
| — | `dataProductSpecification: "0.0.1"` |
| `id` / `metadata.id` | `id` |
| `metadata.name` | `info.title` |
| `metadata.description` | `info.description` |
| `metadata.status` (production→active) | `info.status` |
| `owner.team` | `teamId` + `info.owner` (`--team-id` overrides `teamId`) |
| `metadata.archetype` | `info.archetype` |
| `metadata.maturity` | `info.maturity` |
| `expects[]` | `inputPorts[]` |
| `exposes[]` | `outputPorts[]` (each with `dataContractId`) |
| `metadata.tags` | `tags[]` |
| `metadata.domain`, `.version`, etc. | `custom{}` |

### Port Mapping

Input/output ports are mapped with:
- **type** — provider name → platform type (gcp→BigQuery, snowflake→Snowflake, etc.)
- **location** — assembled from provider-specific config (project.dataset.table, s3://bucket/key, etc.)
- **containsPii** — detected from schema field `classification: pii` or `pii: true`
- **sourceSystemId** — from `source_system` or `sourceSystem` field

## Using via `fluid publish`

The provider also integrates with the generic `fluid publish` command via the catalogs framework:

```bash
# Configure in ~/.fluid/config.yaml
catalogs:
  datamesh-manager:
    endpoint: https://api.entropy-data.com
    auth:
      api_key: ${DMM_API_KEY}
    enabled: true
    # data_product_spec: "0.0.1"   # optional: legacy DPS instead of ODPS

# Then publish
fluid publish contract.fluid.yaml --catalog datamesh-manager
```

## API Reference

- **Swagger**: https://api.entropy-data.com/swagger/index.html
- **Docs**: https://docs.datamesh-manager.com/dataproducts
- **Auth**: https://docs.datamesh-manager.com/authentication
- **Data Contracts**: https://docs.datamesh-manager.com/datacontracts

## Requirements

- Python 3.9+
- `requests` library (`pip install requests`)
- Entropy Data API key (free tier available)
