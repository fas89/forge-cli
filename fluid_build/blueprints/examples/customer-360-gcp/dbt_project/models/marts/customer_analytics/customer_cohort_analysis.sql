{{
  config(
    materialized='table',
    partition_by={
      'field': 'cohort_month',
      'data_type': 'date'
    },
    description='Advanced customer cohort analysis for retention and lifetime value tracking'
  )
}}

SELECT 
  cohort_month,
  cohort_size,
  period_number,
  customers_active,
  retention_rate,
  ROUND(retention_rate * 100, 2) AS retention_percentage,
  revenue_per_customer AS avg_revenue_per_customer,
  cumulative_revenue,
  ltv_estimate,
  cohort_performance_tier,
  cohort_health_score,
  cohort_month_name,
  period_label,
  
  -- Add additional calculated fields for the final mart
  ROUND(cumulative_revenue / cohort_size, 2) AS cumulative_revenue_per_customer,
  ROUND(total_revenue, 2) AS period_revenue,
  ROUND(avg_order_value, 2) AS avg_order_value,
  total_transactions,
  
  -- Retention benchmarks
  CASE 
    WHEN period_number = 1 AND retention_rate >= 0.2 THEN 'above_benchmark'
    WHEN period_number = 1 AND retention_rate >= 0.1 THEN 'at_benchmark'
    WHEN period_number = 3 AND retention_rate >= 0.15 THEN 'above_benchmark'
    WHEN period_number = 3 AND retention_rate >= 0.08 THEN 'at_benchmark'
    WHEN period_number = 6 AND retention_rate >= 0.1 THEN 'above_benchmark'
    WHEN period_number = 6 AND retention_rate >= 0.05 THEN 'at_benchmark'
    WHEN period_number = 12 AND retention_rate >= 0.05 THEN 'above_benchmark'
    WHEN period_number = 12 AND retention_rate >= 0.03 THEN 'at_benchmark'
    ELSE 'below_benchmark'
  END AS retention_benchmark_status,
  
  -- Metadata
  CURRENT_DATE() AS last_updated,
  CURRENT_TIMESTAMP() AS dbt_loaded_at

FROM {{ ref('int_customer_cohorts') }}
ORDER BY cohort_month, period_number