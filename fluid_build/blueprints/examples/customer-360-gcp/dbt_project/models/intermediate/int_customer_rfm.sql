{{
  config(
    materialized='view',
    description='RFM (Recency, Frequency, Monetary) analysis for customer segmentation'
  )
}}

WITH customer_transactions AS (
  SELECT 
    customer_id,
    transaction_date,
    amount_usd,
    transaction_id
  FROM {{ ref('stg_transactions') }}
  WHERE transaction_status = 'Completed'
    AND transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('rfm_analysis_months') }} MONTH)
),

customer_metrics AS (
  SELECT
    customer_id,
    
    -- Recency: Days since last purchase
    DATE_DIFF(CURRENT_DATE(), MAX(transaction_date), DAY) AS recency_days,
    
    -- Frequency: Number of transactions
    COUNT(DISTINCT transaction_id) AS frequency_count,
    
    -- Monetary: Total amount spent
    SUM(amount_usd) AS monetary_value,
    
    -- Additional metrics for analysis
    AVG(amount_usd) AS avg_order_value,
    MIN(transaction_date) AS first_purchase_date,
    MAX(transaction_date) AS last_purchase_date,
    DATE_DIFF(MAX(transaction_date), MIN(transaction_date), DAY) AS customer_lifespan_days
    
  FROM customer_transactions
  GROUP BY customer_id
),

rfm_quartiles AS (
  SELECT
    customer_id,
    recency_days,
    frequency_count,
    monetary_value,
    avg_order_value,
    first_purchase_date,
    last_purchase_date,
    customer_lifespan_days,
    
    -- Calculate quartiles for scoring (1-5 scale, 5 being best)
    NTILE(5) OVER (ORDER BY recency_days DESC) AS recency_score,  -- Lower recency = higher score
    NTILE(5) OVER (ORDER BY frequency_count ASC) AS frequency_score,  -- Higher frequency = higher score  
    NTILE(5) OVER (ORDER BY monetary_value ASC) AS monetary_score   -- Higher monetary = higher score
    
  FROM customer_metrics
),

rfm_scores AS (
  SELECT
    *,
    -- Combine scores into RFM string
    CONCAT(
      CAST(recency_score AS STRING),
      CAST(frequency_score AS STRING), 
      CAST(monetary_score AS STRING)
    ) AS rfm_score,
    
    -- Calculate overall RFM value
    (recency_score + frequency_score + monetary_score) / 3.0 AS rfm_value
    
  FROM rfm_quartiles
),

customer_segments AS (
  SELECT
    *,
    -- Advanced RFM segmentation based on individual scores
    CASE
      WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 
        THEN 'champions'
      WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 4
        THEN 'loyal_customers'
      WHEN recency_score >= 4 AND frequency_score >= 2 AND monetary_score >= 2
        THEN 'potential_loyalists'
      WHEN recency_score >= 4 AND frequency_score <= 2 AND monetary_score <= 2
        THEN 'new_customers'
      WHEN recency_score >= 3 AND frequency_score <= 2 AND monetary_score <= 2
        THEN 'promising'
      WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3
        THEN 'need_attention'
      WHEN recency_score <= 2 AND frequency_score >= 2 AND monetary_score >= 2
        THEN 'about_to_sleep'
      WHEN recency_score <= 3 AND frequency_score >= 2 AND monetary_score >= 4
        THEN 'at_risk'
      WHEN recency_score <= 2 AND frequency_score >= 4 AND monetary_score >= 4
        THEN 'cannot_lose'
      ELSE 'hibernating'
    END AS customer_segment,
    
    -- Value tier based on monetary score
    CASE
      WHEN monetary_score = 5 THEN 'high_value'
      WHEN monetary_score >= 3 THEN 'medium_value'
      ELSE 'low_value'
    END AS value_tier,
    
    -- Engagement level based on recency and frequency
    CASE
      WHEN recency_score >= 4 AND frequency_score >= 4 THEN 'highly_engaged'
      WHEN recency_score >= 3 AND frequency_score >= 3 THEN 'moderately_engaged'
      WHEN recency_score >= 2 OR frequency_score >= 2 THEN 'low_engagement'
      ELSE 'inactive'
    END AS engagement_level,
    
    -- Purchase behavior classification
    CASE
      WHEN frequency_count = 1 THEN 'one_time_buyer'
      WHEN frequency_count BETWEEN 2 AND 5 THEN 'occasional_buyer'
      WHEN frequency_count BETWEEN 6 AND 15 THEN 'regular_buyer'
      ELSE 'frequent_buyer'
    END AS purchase_behavior
    
  FROM rfm_scores
)

SELECT
  customer_id,
  recency_days,
  frequency_count,
  monetary_value,
  avg_order_value,
  first_purchase_date,
  last_purchase_date,
  customer_lifespan_days,
  recency_score,
  frequency_score,
  monetary_score,
  rfm_score,
  rfm_value,
  customer_segment,
  value_tier,
  engagement_level,
  purchase_behavior,
  
  -- Additional calculated fields
  CASE 
    WHEN customer_lifespan_days > 0 
    THEN monetary_value / customer_lifespan_days 
    ELSE NULL 
  END AS daily_value,
  
  CASE 
    WHEN customer_lifespan_days > 0 
    THEN frequency_count * 1.0 / customer_lifespan_days * 30 
    ELSE NULL 
  END AS monthly_purchase_frequency,
  
  -- Risk indicators
  CASE
    WHEN recency_days > 90 AND customer_segment NOT IN ('hibernating') THEN TRUE
    ELSE FALSE
  END AS at_risk_flag,
  
  CASE
    WHEN recency_days > 180 THEN TRUE
    ELSE FALSE
  END AS dormant_flag
  
FROM customer_segments