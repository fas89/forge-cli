# Semantic Customer Model with Multi-Version Builds

## Overview

This blueprint demonstrates two advanced FLUID patterns:

1. **Multi-Version Builds**: Multiple concurrent output versions (v1, v2, v3) from the same data product
2. **Semantic Modeling**: Business-friendly semantic layer with entities, metrics, and dimensions

## Architecture

### Multi-Version Build Strategy

```
Source Data (Bronze)
  ├── customer_raw
  ├── orders
  ├── products
  └── clickstream
        ↓
Dimensions Build (SCD Type 2)
  ├── dim_customer (with history)
  ├── dim_product
  ├── dim_date
  └── dim_geography
        ↓
Facts Build
  ├── fact_transactions
  └── fact_customer_interactions
        ↓
╔════════════════════════════════════════╗
║    Multi-Version Output Layer          ║
╠════════════════════════════════════════╣
║ v1: customer_profile_v1                ║
║     └─ DEPRECATED (legacy format)      ║
║                                         ║
║ v2: customer_intelligence_v2           ║
║     └─ CURRENT (enhanced metrics)      ║
║                                         ║
║ v3: customer_metrics_v3_streaming      ║
║     └─ STREAMING (real-time)           ║
║                                         ║
║ Semantic: semantic_customer_model      ║
║     └─ BUSINESS LAYER (entities+metrics)║
╚════════════════════════════════════════╝
```

### Semantic Layer Architecture

```
Physical Layer                 Semantic Layer
┌─────────────────┐           ┌──────────────────┐
│ dim_customer    │◄──────────│ Customer Entity  │
│ (SCD Type 2)    │           │ - customer_id    │
└─────────────────┘           │ - profile        │
                              │ - segment        │
┌─────────────────┐           └──────────────────┘
│ fact_trans.     │◄──────────┐
└─────────────────┘           │ ┌──────────────────┐
                              ├─│ Metrics          │
┌─────────────────┐           │ │ - LTV            │
│ fact_interact.  │◄──────────┘ │ - Churn Risk     │
└─────────────────┘             │ - AOV            │
                                │ - Frequency      │
                                └──────────────────┘
```

## Key Features

### 1. Multi-Version Builds

**Why Multi-Version?**
- **Backward Compatibility**: Keep v1 running while consumers migrate
- **Gradual Migration**: Test v2 in parallel before switching
- **A/B Testing**: Compare performance between versions
- **Zero Downtime**: No breaking changes for consumers
- **Consumer Choice**: Different SLAs for different needs

**Version Comparison:**

| Feature | v1 (Legacy) | v2 (Enhanced) | v3 (Streaming) | Semantic |
|---------|-------------|---------------|----------------|----------|
| **Latency** | Daily batch | Daily batch | Real-time (1min) | 6 hours |
| **Freshness** | 24 hours | 12 hours | 5 minutes | 6 hours |
| **Metrics** | Basic (8) | Enhanced (20+) | Real-time (10) | All |
| **SCD History** | ❌ No | ✅ Yes | ❌ No | ✅ Yes |
| **SLA** | 95% | 99.9% | 99.5% | 99.9% |
| **Status** | Deprecated | Current | Real-time | Business |
| **Sunset** | 2026-06-01 | Active | Active | Active |

### 2. Semantic Modeling

**Business-Friendly Abstractions:**

```yaml
# Instead of SQL joins:
SELECT 
  c.customer_id,
  SUM(t.amount) as total_spent
FROM dim_customer c
JOIN fact_transactions t ON c.customer_key = t.customer_key
WHERE c.is_current = TRUE
GROUP BY c.customer_id

# Use semantic metrics:
SELECT 
  customer_lifetime_value
FROM semantic_customer_model
WHERE segment = 'VIP'
```

**Semantic Components:**

1. **Entities**: Business objects (Customer, Product, Transaction)
2. **Metrics**: Pre-calculated KPIs (LTV, Churn Risk, AOV)
3. **Dimensions**: Slicing and dicing (Time, Geography, Segment)

### 3. Slowly Changing Dimensions (SCD Type 2)

Track historical changes to customer attributes:

```sql
customer_key  | customer_id | segment  | valid_from | valid_to   | is_current
--------------|-------------|----------|------------|------------|------------
abc-2024-01   | C001        | Standard | 2024-01-01 | 2024-06-30 | FALSE
abc-2024-07   | C001        | Premium  | 2024-07-01 | 9999-12-31 | TRUE
```

**Benefits:**
- Historical analysis: "What was customer segment on 2024-03-15?"
- Trend analysis: "How many customers upgraded this quarter?"
- Audit compliance: Complete change history

### 4. Star Schema Design

**Fact Tables** (Transactions):
- `fact_transactions`: Purchase events
- `fact_customer_interactions`: Web/app interactions

**Dimension Tables** (Context):
- `dim_customer`: Customer profiles (SCD Type 2)
- `dim_product`: Product catalog
- `dim_date`: Time dimension
- `dim_geography`: Location hierarchy

**Benefits:**
- Query performance (star joins)
- Business-friendly (conformed dimensions)
- Consistent metrics across analyses

## Quick Start

### Prerequisites

```bash
pip install dbt-core dbt-snowflake dbt-semantic-interfaces
```

### 1. Initialize from Blueprint

```bash
fluid init --blueprint semantic-customer-model --output ./my-customer-model
cd my-customer-model
```

### 2. Configure Environment

```bash
# .env.dev
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your-user
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_ROLE=TRANSFORMER
SNOWFLAKE_WAREHOUSE=DEV_WH
SNOWFLAKE_DATABASE=DEV_ANALYTICS
SNOWFLAKE_SCHEMA=CUSTOMER_INTELLIGENCE
```

### 3. Deploy All Builds

```bash
# Deploy dimensions first
fluid apply contract.fluid.yaml \
  --build dimensions \
  --environment dev

# Deploy facts (depends on dimensions)
fluid apply contract.fluid.yaml \
  --build facts \
  --environment dev

# Deploy all versions in parallel
fluid apply contract.fluid.yaml \
  --build v1_legacy \
  --build v2_enhanced \
  --build v3_streaming \
  --build semantic_layer \
  --environment dev
```

### 4. Query Different Versions

```sql
-- v1: Simple legacy format
SELECT * FROM DEV_ANALYTICS.CUSTOMER_INTELLIGENCE.CUSTOMER_PROFILE_V1
WHERE customer_id = 'C001';

-- v2: Enhanced with LTV and churn
SELECT 
  customer_id,
  lifetime_value,
  churn_risk_score,
  rfm_score
FROM DEV_ANALYTICS.CUSTOMER_INTELLIGENCE.CUSTOMER_INTELLIGENCE_V2
WHERE is_current = TRUE
  AND customer_segment = 'VIP';

-- v3: Real-time metrics
SELECT 
  customer_id,
  orders_last_30d,
  revenue_last_30d,
  days_since_last_order
FROM DEV_ANALYTICS.CUSTOMER_INTELLIGENCE.CUSTOMER_METRICS_V3_RT
WHERE orders_last_30d > 0;

-- Semantic: Business-friendly
SELECT 
  customer_lifetime_value,
  churn_risk_score,
  average_order_value
FROM DEV_ANALYTICS.CUSTOMER_INTELLIGENCE.SEMANTIC_CUSTOMER_MODEL
WHERE customer_segment = 'At-Risk';
```

## Build Dependencies

The builds execute in order based on dependencies:

```
1. dimensions (no dependencies)
   ↓
2. facts (depends on: dimensions)
   ↓
3. v1_legacy, v2_enhanced, v3_streaming (depends on: facts)
   ↓
4. semantic_layer (depends on: facts, dimensions)
```

**Configuration:**

```yaml
builds:
  - id: "facts"
    execution:
      dependencies:
        - buildId: "dimensions"
          condition: "success"
```

## Migration Guide: v1 → v2

### Step 1: Audit v1 Consumers

```sql
-- Find consumers querying v1
SELECT 
  user_name,
  query_text,
  COUNT(*) as query_count
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_text ILIKE '%customer_profile_v1%'
  AND start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 3 DESC;
```

### Step 2: Test v2 in Parallel

```sql
-- Compare results
WITH v1 AS (
  SELECT customer_id, total_orders, total_spent
  FROM customer_profile_v1
),
v2 AS (
  SELECT customer_id, total_orders, total_revenue as total_spent
  FROM customer_intelligence_v2
  WHERE is_current = TRUE
)
SELECT 
  v1.*,
  v2.total_orders as v2_orders,
  v2.total_spent as v2_spent,
  ABS(v1.total_orders - v2.total_orders) as orders_diff,
  ABS(v1.total_spent - v2.total_spent) as spent_diff
FROM v1
FULL OUTER JOIN v2 ON v1.customer_id = v2.customer_id
WHERE orders_diff > 0 OR spent_diff > 0;
```

### Step 3: Update Consumer Code

```python
# Before (v1)
df = spark.read.table("customer_profile_v1")
ltv = df.select("customer_id", "total_spent")

# After (v2)
df = spark.read.table("customer_intelligence_v2") \
    .filter(col("is_current") == True)
ltv = df.select("customer_id", "lifetime_value")  # More accurate LTV
```

### Step 4: Monitor Adoption

```sql
-- Track v1 vs v2 usage
SELECT 
  CASE 
    WHEN query_text ILIKE '%customer_profile_v1%' THEN 'v1_legacy'
    WHEN query_text ILIKE '%customer_intelligence_v2%' THEN 'v2_enhanced'
    ELSE 'other'
  END as version,
  COUNT(*) as query_count,
  COUNT(DISTINCT user_name) as unique_users
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 2 DESC;
```

### Step 5: Deprecate v1

Once v2 adoption reaches 100%, remove v1:

```bash
# Remove v1 from contract
fluid apply contract.fluid.yaml --exclude-build v1_legacy --environment prod

# Drop v1 view
DROP VIEW IF EXISTS PROD_ANALYTICS.CUSTOMER_INTELLIGENCE.CUSTOMER_PROFILE_V1;
```

## Semantic Layer Usage

### Define Metrics in dbt

```yaml
# dbt_project/models/semantic/semantic_models.yml
semantic_models:
  - name: customer_entity
    model: ref('customer_intelligence_v2')
    
    entities:
      - name: customer
        type: primary
        expr: customer_id
    
    dimensions:
      - name: customer_segment
        type: categorical
      - name: region
        type: categorical
      - name: valid_from
        type: time
        type_params:
          time_granularity: day
    
    measures:
      - name: customer_lifetime_value
        agg: sum
        expr: lifetime_value
      
      - name: average_order_value
        agg: avg
        expr: average_order_value
      
      - name: customer_count
        agg: count_distinct
        expr: customer_id

metrics:
  - name: total_ltv
    type: simple
    label: Total Customer Lifetime Value
    type_params:
      measure: customer_lifetime_value
  
  - name: avg_ltv_by_segment
    type: simple
    label: Average LTV by Segment
    type_params:
      measure: customer_lifetime_value
    dimensions:
      - customer_segment
```

### Query Metrics via API

```python
from dbt_semantic_interfaces import SemanticLayerClient

client = SemanticLayerClient(connection_params)

# Simple metric query
result = client.query(
    metrics=["total_ltv", "customer_count"],
    group_by=["customer_segment"],
    where=["region = 'US'"]
)

# Time series
result = client.query(
    metrics=["customer_lifetime_value"],
    group_by=["valid_from__month"],
    where=["customer_segment = 'VIP'"],
    order_by=["valid_from__month"]
)
```

### Self-Service Analytics

Business users query metrics without SQL:

```python
# Business analysts can write:
ltv_by_segment = semantic_model.get_metric(
    "customer_lifetime_value",
    group_by="customer_segment"
)

# Instead of:
SELECT 
  customer_segment,
  SUM(lifetime_value) as customer_lifetime_value
FROM customer_intelligence_v2
WHERE is_current = TRUE
GROUP BY customer_segment;
```

## Data Quality Validation

### Cross-Version Consistency

```yaml
# tests/cross_version_consistency.yml
tests:
  - name: "v1_v2_customer_count_match"
    description: "Customer counts should match between v1 and v2"
    sql: |
      WITH v1_count AS (
        SELECT COUNT(DISTINCT customer_id) as cnt FROM customer_profile_v1
      ),
      v2_count AS (
        SELECT COUNT(DISTINCT customer_id) as cnt 
        FROM customer_intelligence_v2 
        WHERE is_current = TRUE
      )
      SELECT 
        ABS(v1.cnt - v2.cnt) as difference
      FROM v1_count v1
      CROSS JOIN v2_count v2
      HAVING difference > 10  -- Allow small variance
```

### SCD Integrity

```yaml
tests:
  - name: "scd_no_gaps"
    description: "SCD Type 2 should have no gaps in validity periods"
    sql: |
      SELECT 
        customer_id,
        valid_to,
        LEAD(valid_from) OVER (PARTITION BY customer_id ORDER BY valid_from) as next_valid_from
      FROM customer_intelligence_v2
      WHERE valid_to < '9999-12-31'
      HAVING valid_to != next_valid_from
  
  - name: "scd_one_current_per_customer"
    description: "Each customer should have exactly one current record"
    sql: |
      SELECT 
        customer_id,
        COUNT(*) as current_count
      FROM customer_intelligence_v2
      WHERE is_current = TRUE
      GROUP BY customer_id
      HAVING current_count > 1
```

## Performance Optimization

### Clustering Strategy

```sql
-- v2: Cluster by segment and region
ALTER TABLE customer_intelligence_v2
CLUSTER BY (customer_segment, region);

-- v3: Cluster by recent activity
ALTER TABLE customer_metrics_v3_rt
CLUSTER BY (days_since_last_order, customer_segment);
```

### Materialized Views for Streaming

```sql
-- v3 uses materialized views for real-time refresh
CREATE MATERIALIZED VIEW customer_metrics_v3_rt AS
SELECT ...
FROM dim_customer c
LEFT JOIN fact_transactions t ...;

-- Auto-refresh every minute
ALTER MATERIALIZED VIEW customer_metrics_v3_rt 
SET refresh_interval = '1 MINUTE';
```

### Query Performance Comparison

```sql
-- Measure query performance by version
SELECT 
  CASE 
    WHEN query_text ILIKE '%customer_profile_v1%' THEN 'v1'
    WHEN query_text ILIKE '%customer_intelligence_v2%' THEN 'v2'
    WHEN query_text ILIKE '%customer_metrics_v3_rt%' THEN 'v3'
  END as version,
  AVG(total_elapsed_time)/1000 as avg_elapsed_sec,
  MEDIAN(total_elapsed_time)/1000 as median_elapsed_sec,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_elapsed_time)/1000 as p95_elapsed_sec,
  COUNT(*) as query_count
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND execution_status = 'SUCCESS'
GROUP BY 1
ORDER BY 5 DESC;
```

## Cost Optimization

### Version-Specific Warehouses

```yaml
# Lower cost warehouse for deprecated v1
v1_legacy:
  warehouse: "DEPRECATED_WH"  # X-SMALL
  auto_suspend: 60
  
# Production warehouse for v2
v2_enhanced:
  warehouse: "PROD_WH"  # MEDIUM
  auto_suspend: 600
  
# Dedicated streaming warehouse for v3
v3_streaming:
  warehouse: "STREAMING_WH"  # SMALL
  auto_suspend: 300
  min_cluster_count: 1
  max_cluster_count: 3
```

### Query Tag for Cost Attribution

```sql
-- Tag queries by version
ALTER SESSION SET QUERY_TAG = 'version:v2,team:analytics';
SELECT * FROM customer_intelligence_v2;

-- Analyze cost by version
SELECT 
  query_tag,
  SUM(credits_used_cloud_services) as credits,
  COUNT(*) as query_count
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 2 DESC;
```

## Monitoring & Alerts

### Version Adoption Tracking

```sql
CREATE OR REPLACE VIEW version_adoption_metrics AS
SELECT 
  DATE_TRUNC('day', start_time) as date,
  CASE 
    WHEN query_text ILIKE '%customer_profile_v1%' THEN 'v1_legacy'
    WHEN query_text ILIKE '%customer_intelligence_v2%' THEN 'v2_enhanced'
    WHEN query_text ILIKE '%customer_metrics_v3%' THEN 'v3_streaming'
    WHEN query_text ILIKE '%semantic_customer_model%' THEN 'semantic'
  END as version,
  COUNT(*) as query_count,
  COUNT(DISTINCT user_name) as unique_users,
  SUM(total_elapsed_time)/1000/60 as total_minutes,
  AVG(total_elapsed_time)/1000 as avg_seconds
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD(day, -90, CURRENT_TIMESTAMP())
  AND version IS NOT NULL
GROUP BY 1, 2;
```

### Streaming Lag Alert

```sql
-- Alert if v3 streaming lag exceeds 10 minutes
SELECT 
  DATEDIFF(second, MAX(calculated_at), CURRENT_TIMESTAMP()) as lag_seconds
FROM customer_metrics_v3_rt
HAVING lag_seconds > 600;  -- > 10 minutes
```

## Best Practices

### 1. Version Naming Convention

```
{output_name}_v{major_version}_{variant}

Examples:
- customer_profile_v1          (original)
- customer_intelligence_v2      (major update)
- customer_metrics_v3_streaming (real-time variant)
- semantic_customer_model       (semantic layer)
```

### 2. Deprecation Policy

```yaml
1. Announce deprecation: 6 months before sunset
2. Add deprecation warnings in schema/docs
3. Monitor adoption of replacement version
4. Provide migration support
5. Set sunset date (target: 100% migration)
6. Remove deprecated version
```

### 3. Testing Strategy

```bash
# Test all versions together
fluid test contract.fluid.yaml --all-builds

# Test specific version
fluid test contract.fluid.yaml --build v2_enhanced

# Cross-version consistency tests
fluid test contract.fluid.yaml --test-suite cross_version
```

### 4. Documentation

- Document differences between versions
- Provide migration guides
- Show example queries for each version
- Explain use cases for each variant

## Troubleshooting

### Issue: v1 and v2 Row Counts Don't Match

```sql
-- Debug row count differences
WITH v1_customers AS (
  SELECT DISTINCT customer_id FROM customer_profile_v1
),
v2_customers AS (
  SELECT DISTINCT customer_id FROM customer_intelligence_v2 WHERE is_current = TRUE
)
SELECT 
  COUNT(*) as v1_count,
  (SELECT COUNT(*) FROM v2_customers) as v2_count,
  COUNT(*) - (SELECT COUNT(*) FROM v2_customers) as difference;

-- Find customers in v1 but not v2
SELECT customer_id FROM v1_customers
EXCEPT
SELECT customer_id FROM v2_customers;
```

### Issue: SCD Type 2 Has Overlapping Periods

```sql
-- Find overlapping validity periods
WITH overlaps AS (
  SELECT 
    c1.customer_id,
    c1.valid_from as period1_start,
    c1.valid_to as period1_end,
    c2.valid_from as period2_start,
    c2.valid_to as period2_end
  FROM customer_intelligence_v2 c1
  JOIN customer_intelligence_v2 c2
    ON c1.customer_id = c2.customer_id
    AND c1.customer_key != c2.customer_key
    AND c1.valid_from < c2.valid_to
    AND c1.valid_to > c2.valid_from
)
SELECT * FROM overlaps;
```

### Issue: Streaming View Not Refreshing

```sql
-- Check materialized view refresh status
SHOW MATERIALIZED VIEWS LIKE 'customer_metrics_v3_rt';

-- Force manual refresh
ALTER MATERIALIZED VIEW customer_metrics_v3_rt REFRESH;

-- Check refresh history
SELECT 
  materialization_time,
  credits_used,
  bytes_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY
WHERE table_name = 'CUSTOMER_METRICS_V3_RT'
ORDER BY materialization_time DESC
LIMIT 10;
```

## Examples

See the `/examples` directory for:
- Sample data generation scripts
- dbt model implementations
- Test suites
- Migration scripts
- Dashboard configurations

## Support

- **Documentation**: [docs/semantic-customer-model](./docs/)
- **Slack**: #customer-intelligence
- **Email**: customer-intelligence@company.com

## License

MIT
