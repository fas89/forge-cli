{{
  config(
    materialized='view',
    description='Customer cohort analysis for retention and lifetime value tracking'
  )
}}

WITH customer_first_purchase AS (
  SELECT 
    customer_id,
    MIN(transaction_date) AS first_purchase_date,
    DATE_TRUNC(MIN(transaction_date), MONTH) AS cohort_month
  FROM {{ ref('stg_transactions') }}
  WHERE transaction_status = 'Completed'
  GROUP BY customer_id
),

customer_purchases AS (
  SELECT 
    t.customer_id,
    t.transaction_date,
    t.amount_usd,
    cfp.cohort_month,
    cfp.first_purchase_date,
    
    -- Calculate period number (months since first purchase)
    DATE_DIFF(
      DATE_TRUNC(t.transaction_date, MONTH), 
      cfp.cohort_month, 
      MONTH
    ) AS period_number
    
  FROM {{ ref('stg_transactions') }} t
  INNER JOIN customer_first_purchase cfp
    ON t.customer_id = cfp.customer_id
  WHERE t.transaction_status = 'Completed'
),

cohort_data AS (
  SELECT
    cohort_month,
    period_number,
    
    -- Customer counts
    COUNT(DISTINCT customer_id) AS customers_active,
    
    -- Revenue metrics
    SUM(amount_usd) AS total_revenue,
    AVG(amount_usd) AS avg_order_value,
    SUM(amount_usd) / COUNT(DISTINCT customer_id) AS revenue_per_customer,
    
    -- Transaction metrics
    COUNT(*) AS total_transactions,
    COUNT(*) / COUNT(DISTINCT customer_id) AS transactions_per_customer
    
  FROM customer_purchases
  GROUP BY cohort_month, period_number
),

cohort_sizes AS (
  SELECT 
    cohort_month,
    COUNT(DISTINCT customer_id) AS cohort_size,
    SUM(amount_usd) AS cohort_first_month_revenue
  FROM customer_purchases
  WHERE period_number = 0  -- First month only
  GROUP BY cohort_month
),

cohort_table AS (
  SELECT 
    cd.cohort_month,
    cs.cohort_size,
    cs.cohort_first_month_revenue,
    cd.period_number,
    cd.customers_active,
    cd.total_revenue,
    cd.avg_order_value,
    cd.revenue_per_customer,
    cd.total_transactions,
    cd.transactions_per_customer,
    
    -- Calculate retention rate
    cd.customers_active / cs.cohort_size AS retention_rate,
    
    -- Calculate cumulative metrics
    SUM(cd.total_revenue) OVER (
      PARTITION BY cd.cohort_month 
      ORDER BY cd.period_number 
      ROWS UNBOUNDED PRECEDING
    ) AS cumulative_revenue,
    
    SUM(cd.total_transactions) OVER (
      PARTITION BY cd.cohort_month 
      ORDER BY cd.period_number 
      ROWS UNBOUNDED PRECEDING  
    ) AS cumulative_transactions
    
  FROM cohort_data cd
  INNER JOIN cohort_sizes cs
    ON cd.cohort_month = cs.cohort_month
),

ltv_estimates AS (
  SELECT
    *,
    -- Simple LTV estimate based on cumulative revenue
    cumulative_revenue / cohort_size AS ltv_estimate,
    
    -- Revenue trend (change from previous period)
    revenue_per_customer - LAG(revenue_per_customer) OVER (
      PARTITION BY cohort_month 
      ORDER BY period_number
    ) AS revenue_trend,
    
    -- Retention trend (change from previous period)  
    retention_rate - LAG(retention_rate) OVER (
      PARTITION BY cohort_month 
      ORDER BY period_number
    ) AS retention_trend,
    
    -- Predict future retention (simple linear projection)
    CASE 
      WHEN period_number >= 3 THEN
        retention_rate + (
          (retention_rate - LAG(retention_rate, 2) OVER (
            PARTITION BY cohort_month 
            ORDER BY period_number
          )) / 2
        )
      ELSE NULL
    END AS predicted_next_retention
    
  FROM cohort_table
)

SELECT
  cohort_month,
  cohort_size,
  cohort_first_month_revenue,
  period_number,
  customers_active,
  total_revenue,
  avg_order_value,
  revenue_per_customer,
  total_transactions,
  transactions_per_customer,
  retention_rate,
  cumulative_revenue,
  cumulative_transactions,
  ltv_estimate,
  revenue_trend,
  retention_trend,
  predicted_next_retention,
  
  -- Additional calculated metrics
  ROUND(retention_rate * 100, 2) AS retention_percentage,
  
  -- Cohort health score (composite metric)
  (retention_rate * 0.4 + (revenue_per_customer / 100) * 0.3 + 
   (transactions_per_customer / 5) * 0.3) AS cohort_health_score,
   
  -- Categorize cohort performance
  CASE
    WHEN retention_rate >= 0.3 AND revenue_per_customer >= 50 THEN 'high_performing'
    WHEN retention_rate >= 0.15 AND revenue_per_customer >= 25 THEN 'moderate_performing'
    WHEN retention_rate >= 0.05 OR revenue_per_customer >= 10 THEN 'low_performing'
    ELSE 'poor_performing'
  END AS cohort_performance_tier,
  
  -- Month names for better readability
  FORMAT_DATE('%Y-%m', cohort_month) AS cohort_month_name,
  CASE period_number
    WHEN 0 THEN 'Month 0 (Acquisition)'
    WHEN 1 THEN 'Month 1'
    WHEN 2 THEN 'Month 2'
    WHEN 3 THEN 'Month 3'
    WHEN 6 THEN 'Month 6'
    WHEN 12 THEN 'Month 12'
    ELSE CONCAT('Month ', CAST(period_number AS STRING))
  END AS period_label

FROM ltv_estimates
ORDER BY cohort_month, period_number