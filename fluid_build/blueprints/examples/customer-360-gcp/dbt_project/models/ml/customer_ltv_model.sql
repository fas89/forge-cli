{{
  config(
    materialized='table',
    description='BigQuery ML model for customer lifetime value prediction'
  )
}}

-- This model creates and trains a BigQuery ML linear regression model for CLV prediction

CREATE OR REPLACE MODEL `{{ target.database }}.{{ target.schema }}_ml.customer_ltv_model`
OPTIONS(
  model_type='LINEAR_REG',
  input_label_cols=['customer_ltv'],
  data_split_method='AUTO_SPLIT',
  data_split_eval_fraction=0.2,
  data_split_test_fraction=0.1,
  max_iterations=50,
  learn_rate=0.1,
  l1_reg=0.01,
  l2_reg=0.01
) AS

WITH ltv_training_data AS (
  SELECT
    c.customer_id,
    
    -- Features for LTV prediction
    rfm.recency_score,
    rfm.frequency_score,
    rfm.monetary_score,
    
    -- Customer age and tenure
    c.customer_tenure_days / 365.0 AS customer_tenure_years,
    COALESCE(c.age, 35) AS customer_age,
    
    -- Transactional features
    rfm.frequency_count,
    rfm.avg_order_value,
    rfm.monetary_value AS historical_value,
    
    -- Behavioral patterns
    COALESCE(bp.categories_purchased, 1) AS categories_purchased,
    COALESCE(bp.holiday_purchase_ratio, 0) AS holiday_purchase_ratio,
    COALESCE(bp.promotion_usage_rate, 0) AS promotion_usage_rate,
    
    -- Seasonality and timing
    EXTRACT(MONTH FROM c.customer_since) AS acquisition_month,
    
    -- Geographic and demographic encoding
    CASE c.geographic_segment
      WHEN 'West' THEN 1
      WHEN 'East' THEN 2
      WHEN 'South' THEN 3
      WHEN 'Midwest' THEN 4
      ELSE 0
    END AS geographic_segment_encoded,
    
    -- Target variable: Customer LTV (calculated as total value over customer lifetime)
    CASE 
      WHEN c.customer_tenure_days >= 365 THEN rfm.monetary_value
      ELSE NULL  -- Only include customers with at least 1 year of data
    END AS customer_ltv
    
  FROM {{ ref('stg_customers') }} c
  INNER JOIN {{ ref('int_customer_rfm') }} rfm 
    ON c.customer_id = rfm.customer_id
  LEFT JOIN (
    SELECT 
      customer_id,
      COUNT(DISTINCT product_category) AS categories_purchased,
      AVG(CASE WHEN transaction_month IN (11, 12) THEN 1.0 ELSE 0.0 END) AS holiday_purchase_ratio,
      AVG(CASE WHEN has_promotion THEN 1.0 ELSE 0.0 END) AS promotion_usage_rate
    FROM {{ ref('stg_transactions') }}
    WHERE transaction_status = 'Completed'
    GROUP BY customer_id
  ) bp ON c.customer_id = bp.customer_id
  
  WHERE c.customer_status = 'Active'
    AND c.customer_tenure_days >= 365  -- At least 1 year of data
    AND rfm.frequency_count >= 3  -- Multiple purchases for reliable LTV
)

SELECT *
FROM ltv_training_data
WHERE customer_ltv IS NOT NULL
  AND customer_ltv > 0
  AND customer_ltv < 10000  -- Remove outliers
  AND RAND() < 0.1  -- Sample 10% for faster training in demo