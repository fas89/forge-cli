-- tests/cross_version_consistency.sql
-- Test: Customer counts should match across v1, v2, and v3

{{ config(severity='error') }}

WITH v1_count AS (
  SELECT COUNT(DISTINCT customer_id) as customer_count
  FROM {{ ref('customer_profile_v1') }}
),

v2_count AS (
  SELECT COUNT(DISTINCT customer_id) as customer_count
  FROM {{ ref('customer_intelligence_v2') }}
  WHERE is_current = TRUE
),

v3_count AS (
  SELECT COUNT(DISTINCT customer_id) as customer_count
  FROM {{ ref('customer_metrics_v3_streaming') }}
),

comparison AS (
  SELECT 
    v1.customer_count as v1_count,
    v2.customer_count as v2_count,
    v3.customer_count as v3_count,
    ABS(v1.customer_count - v2.customer_count) as v1_v2_diff,
    ABS(v2.customer_count - v3.customer_count) as v2_v3_diff
  FROM v1_count v1
  CROSS JOIN v2_count v2
  CROSS JOIN v3_count v3
)

SELECT *
FROM comparison
WHERE v1_v2_diff > 100  -- Allow variance of 100 customers
   OR v2_v3_diff > 100
