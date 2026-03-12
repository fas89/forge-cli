-- tests/scd_integrity_test.sql
-- Test: SCD Type 2 integrity checks

{{ config(severity='error') }}

-- Test 1: Each customer should have exactly one current record
WITH current_check AS (
  SELECT 
    customer_id,
    COUNT(*) as current_count
  FROM {{ ref('dim_customer') }}
  WHERE is_current = TRUE
  GROUP BY customer_id
  HAVING COUNT(*) > 1
),

-- Test 2: No gaps in validity periods
gap_check AS (
  SELECT 
    customer_id,
    valid_from,
    valid_to,
    LEAD(valid_from) OVER (PARTITION BY customer_id ORDER BY valid_from) as next_valid_from
  FROM {{ ref('dim_customer') }}
  WHERE valid_to < '9999-12-31'::TIMESTAMP
  QUALIFY valid_to != next_valid_from
),

-- Test 3: No overlapping periods
overlap_check AS (
  SELECT 
    c1.customer_id,
    c1.valid_from as period1_start,
    c1.valid_to as period1_end,
    c2.valid_from as period2_start,
    c2.valid_to as period2_end
  FROM {{ ref('dim_customer') }} c1
  JOIN {{ ref('dim_customer') }} c2
    ON c1.customer_id = c2.customer_id
    AND c1.customer_key != c2.customer_key
    AND c1.valid_from < c2.valid_to
    AND c1.valid_to > c2.valid_from
),

all_issues AS (
  SELECT customer_id, 'multiple_current' as issue_type FROM current_check
  UNION ALL
  SELECT customer_id, 'gap_in_history' as issue_type FROM gap_check
  UNION ALL
  SELECT customer_id, 'overlapping_periods' as issue_type FROM overlap_check
)

SELECT *
FROM all_issues
