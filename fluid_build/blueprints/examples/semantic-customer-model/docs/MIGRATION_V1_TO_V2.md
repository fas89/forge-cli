# Migration Guide: v1 → v2

This guide helps you migrate from `customer_profile_v1` (legacy) to `customer_intelligence_v2` (enhanced).

## Overview

**Timeline:**
- **Deprecation Announced**: 2025-12-01
- **Migration Window**: 6 months
- **Sunset Date**: 2026-06-01
- **Support**: Migration support available until 2026-03-01

## Key Differences

### Schema Changes

| Field | v1 | v2 | Notes |
|-------|----|----|-------|
| `customer_id` | ✅ | ✅ | No change |
| `customer_key` | ❌ | ✅ | New surrogate key for SCD |
| `email` | ✅ | ✅ | No change |
| `first_name` | ✅ | ✅ | No change |
| `last_name` | ✅ | ✅ | No change |
| `customer_segment` | ✅ | ✅ | No change |
| `total_orders` | ✅ | ✅ | No change |
| `total_spent` | ✅ | `total_revenue` | **Renamed** |
| `last_order_date` | ✅ | ✅ | No change |
| `lifetime_value` | ❌ | ✅ | **New** - Predictive LTV |
| `churn_risk_score` | ❌ | ✅ | **New** - ML prediction |
| `rfm_score` | ❌ | ✅ | **New** - Recency/Frequency/Monetary |
| `valid_from` | ❌ | ✅ | **New** - SCD Type 2 |
| `valid_to` | ❌ | ✅ | **New** - SCD Type 2 |
| `is_current` | ❌ | ✅ | **New** - SCD Type 2 |

### Data Model Changes

**v1**: Simple view with current state only
```sql
SELECT * FROM customer_profile_v1
WHERE customer_id = 'C001'
-- Returns: 1 row (current state)
```

**v2**: Table with historical tracking (SCD Type 2)
```sql
SELECT * FROM customer_intelligence_v2
WHERE customer_id = 'C001'
-- Returns: Multiple rows (full history)

SELECT * FROM customer_intelligence_v2
WHERE customer_id = 'C001' AND is_current = TRUE
-- Returns: 1 row (current state)
```

## Migration Steps

### Step 1: Audit Current Usage

Identify all consumers of v1:

```sql
-- Find queries using v1
SELECT 
  user_name,
  query_text,
  COUNT(*) as query_count,
  MAX(start_time) as last_used
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_text ILIKE '%customer_profile_v1%'
  AND start_time >= DATEADD(day, -90, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 3 DESC;
```

### Step 2: Update SQL Queries

**Before (v1):**
```sql
SELECT 
  customer_id,
  email,
  customer_segment,
  total_orders,
  total_spent
FROM customer_profile_v1
WHERE customer_segment = 'VIP';
```

**After (v2):**
```sql
SELECT 
  customer_id,
  email,
  customer_segment,
  total_orders,
  total_revenue,  -- RENAMED from total_spent
  lifetime_value,  -- NEW
  churn_risk_score  -- NEW
FROM customer_intelligence_v2
WHERE is_current = TRUE  -- IMPORTANT: Filter for current records only
  AND customer_segment = 'VIP';
```

### Step 3: Update Application Code

#### Python Example

**Before:**
```python
import pandas as pd
from snowflake.connector import connect

conn = connect(...)
query = """
    SELECT customer_id, email, total_spent
    FROM customer_profile_v1
"""
df = pd.read_sql(query, conn)
```

**After:**
```python
import pandas as pd
from snowflake.connector import connect

conn = connect(...)
query = """
    SELECT customer_id, email, total_revenue, lifetime_value
    FROM customer_intelligence_v2
    WHERE is_current = TRUE
"""
df = pd.read_sql(query, conn)

# Handle renamed column
df['total_spent'] = df['total_revenue']  # For backward compatibility
```

#### PySpark Example

**Before:**
```python
df = spark.read \
    .format("snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", "customer_profile_v1") \
    .load()
```

**After:**
```python
df = spark.read \
    .format("snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", "customer_intelligence_v2") \
    .load() \
    .filter(col("is_current") == True)  # Filter for current records

# Rename for compatibility
df = df.withColumnRenamed("total_revenue", "total_spent")
```

### Step 4: Update BI Dashboards

#### Tableau

1. Open workbook using `customer_profile_v1`
2. Data Source → Edit Connection
3. Change table to `customer_intelligence_v2`
4. Add filter: `is_current = TRUE`
5. Update calculated fields:
   - Replace `total_spent` with `total_revenue`
   - Add new fields: `lifetime_value`, `churn_risk_score`

#### Power BI

1. Power Query Editor → Source step
2. Update table name to `customer_intelligence_v2`
3. Add filter row: `is_current = true`
4. Rename column: `total_revenue` → `total_spent` (if needed for compatibility)

### Step 5: Validate Data Consistency

```sql
-- Compare row counts
WITH v1_stats AS (
  SELECT 
    COUNT(*) as customer_count,
    SUM(total_spent) as total_revenue,
    AVG(total_orders) as avg_orders
  FROM customer_profile_v1
),
v2_stats AS (
  SELECT 
    COUNT(*) as customer_count,
    SUM(total_revenue) as total_revenue,
    AVG(total_orders) as avg_orders
  FROM customer_intelligence_v2
  WHERE is_current = TRUE
)
SELECT 
  v1.customer_count as v1_customers,
  v2.customer_count as v2_customers,
  v1.customer_count - v2.customer_count as customer_diff,
  v1.total_revenue as v1_revenue,
  v2.total_revenue as v2_revenue,
  v1.total_revenue - v2.total_revenue as revenue_diff,
  v1.avg_orders as v1_avg_orders,
  v2.avg_orders as v2_avg_orders
FROM v1_stats v1
CROSS JOIN v2_stats v2;
```

### Step 6: Parallel Testing

Run both versions side-by-side:

```sql
-- Create comparison view
CREATE OR REPLACE VIEW migration_comparison AS
WITH v1 AS (
  SELECT 
    customer_id,
    email,
    total_orders as v1_total_orders,
    total_spent as v1_total_spent
  FROM customer_profile_v1
),
v2 AS (
  SELECT 
    customer_id,
    email,
    total_orders as v2_total_orders,
    total_revenue as v2_total_revenue
  FROM customer_intelligence_v2
  WHERE is_current = TRUE
)
SELECT 
  COALESCE(v1.customer_id, v2.customer_id) as customer_id,
  COALESCE(v1.email, v2.email) as email,
  v1.v1_total_orders,
  v2.v2_total_orders,
  ABS(COALESCE(v1.v1_total_orders, 0) - COALESCE(v2.v2_total_orders, 0)) as orders_diff,
  v1.v1_total_spent,
  v2.v2_total_revenue,
  ABS(COALESCE(v1.v1_total_spent, 0) - COALESCE(v2.v2_total_revenue, 0)) as revenue_diff,
  CASE 
    WHEN v1.customer_id IS NULL THEN 'Only in v2'
    WHEN v2.customer_id IS NULL THEN 'Only in v1'
    WHEN orders_diff > 0 OR revenue_diff > 0.01 THEN 'Mismatch'
    ELSE 'Match'
  END as status
FROM v1
FULL OUTER JOIN v2 ON v1.customer_id = v2.customer_id;

-- Check for issues
SELECT 
  status,
  COUNT(*) as customer_count,
  SUM(orders_diff) as total_orders_diff,
  SUM(revenue_diff) as total_revenue_diff
FROM migration_comparison
GROUP BY status;
```

### Step 7: Monitor Adoption

Track migration progress:

```sql
CREATE OR REPLACE VIEW migration_progress AS
SELECT 
  DATE_TRUNC('day', start_time) as date,
  COUNT_IF(query_text ILIKE '%customer_profile_v1%') as v1_queries,
  COUNT_IF(query_text ILIKE '%customer_intelligence_v2%') as v2_queries,
  DIV0(v2_queries, v1_queries + v2_queries) * 100 as v2_adoption_pct,
  COUNT(DISTINCT CASE WHEN query_text ILIKE '%customer_profile_v1%' THEN user_name END) as v1_users,
  COUNT(DISTINCT CASE WHEN query_text ILIKE '%customer_intelligence_v2%' THEN user_name END) as v2_users
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD(day, -90, CURRENT_TIMESTAMP())
  AND (query_text ILIKE '%customer_profile_v1%' OR query_text ILIKE '%customer_intelligence_v2%')
GROUP BY 1
ORDER BY 1 DESC;
```

## New Features in v2

### 1. Lifetime Value (LTV)

Predictive customer value calculation:

```sql
SELECT 
  customer_id,
  customer_segment,
  total_revenue as historical_revenue,
  lifetime_value as predicted_ltv,
  lifetime_value - total_revenue as future_value_prediction
FROM customer_intelligence_v2
WHERE is_current = TRUE
ORDER BY lifetime_value DESC
LIMIT 100;
```

### 2. Churn Risk Score

ML-based churn probability (0.0 - 1.0):

```sql
SELECT 
  customer_segment,
  COUNT(*) as customers,
  AVG(churn_risk_score) as avg_churn_risk,
  COUNT_IF(churn_risk_score > 0.7) as high_risk_count,
  COUNT_IF(churn_risk_score > 0.7) / COUNT(*) * 100 as high_risk_pct
FROM customer_intelligence_v2
WHERE is_current = TRUE
GROUP BY customer_segment
ORDER BY avg_churn_risk DESC;
```

### 3. RFM Segmentation

Recency, Frequency, Monetary scoring:

```sql
SELECT 
  CASE 
    WHEN rfm_score >= 444 THEN 'Champions'
    WHEN rfm_score >= 333 THEN 'Loyal'
    WHEN rfm_score >= 222 THEN 'At Risk'
    ELSE 'Lost'
  END as rfm_segment,
  COUNT(*) as customers,
  AVG(lifetime_value) as avg_ltv,
  SUM(total_revenue) as total_revenue
FROM customer_intelligence_v2
WHERE is_current = TRUE
GROUP BY 1
ORDER BY 2 DESC;
```

### 4. Historical Tracking (SCD Type 2)

Analyze customer segment changes over time:

```sql
-- Customers who upgraded to VIP
SELECT 
  customer_id,
  valid_from as upgrade_date,
  customer_segment,
  LAG(customer_segment) OVER (PARTITION BY customer_id ORDER BY valid_from) as prev_segment
FROM customer_intelligence_v2
WHERE customer_segment = 'VIP'
  AND LAG(customer_segment) OVER (PARTITION BY customer_id ORDER BY valid_from) != 'VIP';
```

## Common Issues

### Issue 1: Duplicate Records

**Problem**: Query returns multiple rows per customer

**Solution**: Add `WHERE is_current = TRUE` filter

```sql
-- ❌ Wrong: Returns all history
SELECT * FROM customer_intelligence_v2
WHERE customer_id = 'C001'

-- ✅ Correct: Returns only current record
SELECT * FROM customer_intelligence_v2
WHERE customer_id = 'C001' AND is_current = TRUE
```

### Issue 2: Column Name Mismatch

**Problem**: `total_spent` column not found

**Solution**: Use `total_revenue` instead

```sql
-- ❌ Wrong
SELECT total_spent FROM customer_intelligence_v2

-- ✅ Correct
SELECT total_revenue FROM customer_intelligence_v2

-- Or use alias for compatibility
SELECT total_revenue as total_spent FROM customer_intelligence_v2
```

### Issue 3: Performance Degradation

**Problem**: Queries slower on v2

**Solution**: v2 is a table (not view), ensure clustering is enabled

```sql
-- Check clustering
SELECT SYSTEM$CLUSTERING_INFORMATION('customer_intelligence_v2');

-- Re-cluster if needed
ALTER TABLE customer_intelligence_v2 
CLUSTER BY (customer_segment, region);
```

## Rollback Plan

If issues arise, temporary rollback:

```sql
-- Create compatibility view (temporary)
CREATE OR REPLACE VIEW customer_profile_v1_compat AS
SELECT 
  customer_id,
  email,
  first_name,
  last_name,
  customer_segment,
  total_orders,
  total_revenue as total_spent,  -- Rename for compatibility
  last_order_date
FROM customer_intelligence_v2
WHERE is_current = TRUE;

-- Use compatibility view
SELECT * FROM customer_profile_v1_compat;
```

## Support

- **Slack**: #customer-intelligence-migration
- **Email**: customer-intelligence@company.com
- **Office Hours**: Tuesdays 2-3pm, Thursdays 10-11am
- **Documentation**: [docs/migration](../docs/migration)

## Checklist

- [ ] Audit current v1 usage
- [ ] Update SQL queries with `is_current = TRUE`
- [ ] Rename `total_spent` → `total_revenue`
- [ ] Update application code
- [ ] Update BI dashboards
- [ ] Validate data consistency
- [ ] Parallel testing (7 days minimum)
- [ ] Monitor adoption metrics
- [ ] Cutover to v2
- [ ] Remove v1 references
- [ ] Confirm 100% migration
