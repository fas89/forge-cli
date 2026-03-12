{{
  config(
    materialized='table',
    partition_by={
      'field': 'last_updated',
      'data_type': 'date'
    },
    cluster_by=['customer_segment', 'churn_risk_tier'],
    description='Enterprise customer profiles with advanced ML predictions and segmentation'
  )
}}

WITH customer_base AS (
  SELECT * FROM {{ ref('stg_customers') }}
),

customer_rfm AS (
  SELECT * FROM {{ ref('int_customer_rfm') }}
),

-- Get latest ML predictions (this would be populated by BigQuery ML models)
ml_predictions AS (
  SELECT
    customer_id,
    
    -- Placeholder for ML predictions - these would come from BigQuery ML models
    -- For now, we'll create rule-based proxies
    CASE 
      WHEN rfm.recency_days > 90 AND rfm.frequency_count <= 2 THEN 0.8
      WHEN rfm.recency_days > 60 AND rfm.frequency_count <= 3 THEN 0.6
      WHEN rfm.recency_days > 30 AND rfm.value_tier = 'low_value' THEN 0.4
      WHEN rfm.recency_days > 14 AND rfm.engagement_level = 'low_engagement' THEN 0.3
      ELSE 0.1
    END AS churn_probability,
    
    -- CLV prediction based on RFM and historical data
    CASE 
      WHEN rfm.customer_segment = 'champions' THEN rfm.monetary_value * 3.5
      WHEN rfm.customer_segment = 'loyal_customers' THEN rfm.monetary_value * 2.8
      WHEN rfm.customer_segment = 'potential_loyalists' THEN rfm.monetary_value * 2.2
      WHEN rfm.customer_segment = 'new_customers' THEN rfm.avg_order_value * 4.5
      WHEN rfm.customer_segment = 'promising' THEN rfm.monetary_value * 1.8
      WHEN rfm.customer_segment = 'need_attention' THEN rfm.monetary_value * 1.5
      WHEN rfm.customer_segment = 'about_to_sleep' THEN rfm.monetary_value * 1.2
      WHEN rfm.customer_segment = 'at_risk' THEN rfm.monetary_value * 1.1
      WHEN rfm.customer_segment = 'cannot_lose' THEN rfm.monetary_value * 2.0
      ELSE rfm.monetary_value * 0.8
    END AS clv_prediction,
    
    CURRENT_TIMESTAMP() AS model_last_updated
    
  FROM customer_rfm rfm
),

behavioral_patterns AS (
  SELECT 
    t.customer_id,
    
    -- Purchase patterns
    COUNT(DISTINCT t.product_category) AS categories_purchased,
    MODE(t.channel) AS preferred_channel,
    MODE(t.payment_method_group) AS preferred_payment_method,
    
    -- Seasonality
    COUNT(DISTINCT CASE WHEN t.transaction_month IN (11, 12) THEN t.transaction_id END) / 
    NULLIF(COUNT(DISTINCT t.transaction_id), 0) AS holiday_purchase_ratio,
    
    -- Time preferences
    MODE(t.time_of_day) AS preferred_time_of_day,
    COUNT(DISTINCT CASE WHEN t.day_type = 'Weekend' THEN t.transaction_id END) /
    NULLIF(COUNT(DISTINCT t.transaction_id), 0) AS weekend_purchase_ratio,
    
    -- Promotion sensitivity
    COUNT(DISTINCT CASE WHEN t.has_promotion THEN t.transaction_id END) /
    NULLIF(COUNT(DISTINCT t.transaction_id), 0) AS promotion_usage_rate
    
  FROM {{ ref('stg_transactions') }} t
  WHERE t.transaction_status = 'Completed'
    AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
  GROUP BY t.customer_id
),

next_best_actions AS (
  SELECT
    customer_id,
    churn_probability,
    customer_segment,
    
    -- Rule-based next best action recommendations
    CASE
      WHEN churn_probability > 0.7 THEN 'urgent_retention_offer'
      WHEN churn_probability > 0.5 THEN 'retention_campaign'
      WHEN customer_segment = 'champions' THEN 'vip_program_invite'
      WHEN customer_segment = 'loyal_customers' THEN 'loyalty_rewards'
      WHEN customer_segment = 'potential_loyalists' THEN 'engagement_campaign'
      WHEN customer_segment = 'new_customers' THEN 'onboarding_sequence'
      WHEN customer_segment = 'promising' THEN 'product_recommendations'
      WHEN customer_segment = 'need_attention' THEN 'win_back_offer'
      WHEN customer_segment = 'about_to_sleep' THEN 'reactivation_campaign'
      WHEN customer_segment = 'at_risk' THEN 'personalized_offer'
      WHEN customer_segment = 'cannot_lose' THEN 'premium_support'
      ELSE 'standard_marketing'
    END AS next_best_action,
    
    -- Propensity scores (simplified)
    TO_JSON_STRING(STRUCT(
      CASE WHEN customer_segment IN ('champions', 'loyal_customers') THEN 0.8 ELSE 0.3 END AS upsell_propensity,
      CASE WHEN customer_segment IN ('new_customers', 'promising') THEN 0.7 ELSE 0.4 END AS cross_sell_propensity,
      CASE WHEN churn_probability < 0.3 THEN 0.9 ELSE 0.2 END AS retention_propensity
    )) AS propensity_scores
    
  FROM ml_predictions ml
  LEFT JOIN customer_rfm rfm ON ml.customer_id = rfm.customer_id
)

SELECT
  -- Customer identifiers and basic info
  cb.customer_id,
  cb.email_hash,
  cb.full_name,
  cb.phone_cleaned,
  
  -- Demographics
  cb.age,
  cb.age_group,
  cb.gender,
  cb.geographic_segment,
  cb.customer_tenure_days,
  cb.customer_tenure_segment,
  cb.customer_status,
  cb.marketing_opt_in,
  
  -- RFM Analysis
  rfm.recency_days,
  rfm.frequency_count,
  rfm.monetary_value,
  rfm.avg_order_value,
  rfm.recency_score,
  rfm.frequency_score,
  rfm.monetary_score,
  rfm.rfm_score,
  rfm.customer_segment,
  rfm.value_tier,
  rfm.engagement_level,
  rfm.purchase_behavior,
  
  -- ML Predictions
  ml.churn_probability,
  CASE 
    WHEN ml.churn_probability >= 0.7 THEN 'critical'
    WHEN ml.churn_probability >= 0.5 THEN 'high'
    WHEN ml.churn_probability >= 0.3 THEN 'medium'
    ELSE 'low'
  END AS churn_risk_tier,
  
  ml.clv_prediction,
  
  -- Behavioral Patterns
  bp.categories_purchased,
  bp.preferred_channel,
  bp.preferred_payment_method,
  bp.holiday_purchase_ratio,
  bp.preferred_time_of_day,
  bp.weekend_purchase_ratio,
  bp.promotion_usage_rate,
  
  -- Next Best Actions
  nba.next_best_action,
  nba.propensity_scores,
  
  -- Create behavioral patterns JSON
  TO_JSON_STRING(STRUCT(
    bp.categories_purchased,
    bp.preferred_channel,
    bp.preferred_payment_method,
    ROUND(bp.holiday_purchase_ratio, 3) AS holiday_purchase_ratio,
    bp.preferred_time_of_day,
    ROUND(bp.weekend_purchase_ratio, 3) AS weekend_purchase_ratio,
    ROUND(bp.promotion_usage_rate, 3) AS promotion_usage_rate
  )) AS behavioral_patterns,
  
  -- Demographic clustering (simplified)
  CONCAT(cb.age_group, '-', cb.gender, '-', cb.geographic_segment) AS demographic_cluster,
  
  -- Risk indicators
  rfm.at_risk_flag,
  rfm.dormant_flag,
  
  -- Metadata
  ml.model_last_updated AS last_model_update,
  CURRENT_DATE() AS last_updated,
  CURRENT_TIMESTAMP() AS dbt_loaded_at

FROM customer_base cb
LEFT JOIN customer_rfm rfm ON cb.customer_id = rfm.customer_id
LEFT JOIN ml_predictions ml ON cb.customer_id = ml.customer_id
LEFT JOIN behavioral_patterns bp ON cb.customer_id = bp.customer_id
LEFT JOIN next_best_actions nba ON cb.customer_id = nba.customer_id
WHERE cb.customer_status = 'Active'  -- Focus on active customers