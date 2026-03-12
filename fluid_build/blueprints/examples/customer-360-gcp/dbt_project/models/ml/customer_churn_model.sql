{{
  config(
    materialized='table',
    description='BigQuery ML model for customer churn prediction'
  )
}}

-- This model creates and trains a BigQuery ML logistic regression model for churn prediction
-- The model is automatically retrained when this dbt model runs

CREATE OR REPLACE MODEL `{{ target.database }}.{{ target.schema }}_ml.customer_churn_model`
OPTIONS(
  model_type='LOGISTIC_REG',
  input_label_cols=['churn_label'],
  auto_class_weights=TRUE,
  data_split_method='AUTO_SPLIT',
  data_split_eval_fraction=0.2,
  data_split_test_fraction=0.1,
  max_iterations=50,
  learn_rate=0.1,
  l1_reg=0.01,
  l2_reg=0.01
) AS

WITH training_data AS (
  SELECT
    -- Features for churn prediction
    c.customer_id,
    
    -- RFM features (normalized)
    rfm.recency_days / 365.0 AS recency_years,
    LOG(rfm.frequency_count + 1) AS log_frequency,
    LOG(rfm.monetary_value + 1) AS log_monetary,
    rfm.recency_score,
    rfm.frequency_score,
    rfm.monetary_score,
    
    -- Customer characteristics
    c.customer_tenure_days / 365.0 AS customer_tenure_years,
    CASE c.age_group
      WHEN '18-24' THEN 1
      WHEN '25-34' THEN 2  
      WHEN '35-44' THEN 3
      WHEN '45-54' THEN 4
      WHEN '55-64' THEN 5
      WHEN '65+' THEN 6
      ELSE 0
    END AS age_group_encoded,
    
    CASE c.gender
      WHEN 'Male' THEN 1
      WHEN 'Female' THEN 2
      WHEN 'Non-Binary' THEN 3
      ELSE 0
    END AS gender_encoded,
    
    -- Behavioral features
    bp.categories_purchased,
    bp.holiday_purchase_ratio,
    bp.weekend_purchase_ratio,
    bp.promotion_usage_rate,
    
    CASE bp.preferred_channel
      WHEN 'Online' THEN 1
      WHEN 'Mobile App' THEN 2
      WHEN 'In-Store' THEN 3
      WHEN 'Phone' THEN 4
      ELSE 0
    END AS preferred_channel_encoded,
    
    -- Engagement metrics
    rfm.avg_order_value,
    CASE 
      WHEN rfm.customer_lifespan_days > 0 
      THEN rfm.frequency_count * 30.0 / rfm.customer_lifespan_days 
      ELSE 0 
    END AS monthly_transaction_rate,
    
    -- Target variable (churn label)
    CASE 
      WHEN rfm.recency_days > {{ var('churn_prediction_days') }} 
        AND c.customer_status = 'Active'
      THEN 1  -- Churned
      WHEN rfm.recency_days <= 30 
        AND c.customer_status = 'Active'
      THEN 0  -- Active
      ELSE NULL  -- Exclude uncertain cases
    END AS churn_label
    
  FROM {{ ref('stg_customers') }} c
  INNER JOIN {{ ref('int_customer_rfm') }} rfm 
    ON c.customer_id = rfm.customer_id
  LEFT JOIN (
    SELECT 
      customer_id,
      COUNT(DISTINCT product_category) AS categories_purchased,
      AVG(CASE WHEN transaction_month IN (11, 12) THEN 1.0 ELSE 0.0 END) AS holiday_purchase_ratio,
      AVG(CASE WHEN day_type = 'Weekend' THEN 1.0 ELSE 0.0 END) AS weekend_purchase_ratio,
      AVG(CASE WHEN has_promotion THEN 1.0 ELSE 0.0 END) AS promotion_usage_rate,
      MODE(channel) AS preferred_channel
    FROM {{ ref('stg_transactions') }}
    WHERE transaction_status = 'Completed'
    GROUP BY customer_id
  ) bp ON c.customer_id = bp.customer_id
  
  WHERE c.customer_since <= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('churn_prediction_days') }} DAY)
    AND rfm.frequency_count >= 2  -- Exclude one-time buyers
)

SELECT *
FROM training_data  
WHERE churn_label IS NOT NULL
  AND RAND() < 0.1  -- Sample 10% for faster training in demo