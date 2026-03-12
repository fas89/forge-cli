-- models/v2/customer_intelligence_v2.sql
-- Enhanced customer intelligence with comprehensive metrics
{{
  config(
    materialized='table',
    tags=['v2', 'customer_intelligence', 'current']
  )
}}

WITH customers AS (
  SELECT * FROM {{ ref('dim_customer') }}
  WHERE is_current = TRUE
),

transactions AS (
  SELECT 
    customer_key,
    COUNT(*) as total_orders,
    SUM(amount) as total_revenue,
    AVG(amount) as average_order_value,
    MIN(transaction_date) as first_order_date,
    MAX(transaction_date) as last_order_date,
    DATEDIFF(day, MAX(transaction_date), CURRENT_DATE()) as days_since_last_order
  FROM {{ ref('fact_transactions') }}
  GROUP BY customer_key
),

interactions AS (
  SELECT 
    customer_key,
    COUNT(*) as total_interactions,
    SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) as page_views,
    SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) as add_to_carts,
    MAX(interaction_date) as last_interaction_date
  FROM {{ ref('fact_customer_interactions') }}
  GROUP BY customer_key
),

-- Calculate RFM scores
rfm AS (
  SELECT 
    customer_key,
    NTILE(5) OVER (ORDER BY days_since_last_order DESC) as recency_score,
    NTILE(5) OVER (ORDER BY total_orders) as frequency_score,
    NTILE(5) OVER (ORDER BY total_revenue) as monetary_score
  FROM transactions
),

-- Calculate lifetime value (predictive)
ltv_calc AS (
  SELECT 
    customer_key,
    total_revenue as historical_revenue,
    average_order_value * 
      (365.0 / NULLIF(DATEDIFF(day, first_order_date, last_order_date), 0)) * 
      {{ var('ltv_window_days') }} * 
      0.7 as predicted_future_value,  -- 70% retention assumption
    total_revenue + 
      (average_order_value * 
       (365.0 / NULLIF(DATEDIFF(day, first_order_date, last_order_date), 0)) * 
       {{ var('ltv_window_days') }} * 0.7) as lifetime_value
  FROM transactions
),

-- Calculate churn risk
churn_calc AS (
  SELECT 
    customer_key,
    CASE 
      WHEN days_since_last_order > {{ var('churn_threshold_days') }} THEN 0.9
      WHEN days_since_last_order > ({{ var('churn_threshold_days') }} * 0.75) THEN 0.7
      WHEN days_since_last_order > ({{ var('churn_threshold_days') }} * 0.5) THEN 0.4
      WHEN days_since_last_order > ({{ var('churn_threshold_days') }} * 0.25) THEN 0.2
      ELSE 0.1
    END as churn_risk_score
  FROM transactions
),

-- Product affinity (simplified)
product_affinity AS (
  SELECT 
    t.customer_key,
    OBJECT_AGG(
      p.category,
      COUNT(*)::NUMBER
    ) as product_affinity_scores,
    FIRST_VALUE(p.category) OVER (
      PARTITION BY t.customer_key 
      ORDER BY COUNT(*) DESC
    ) as top_product_category
  FROM {{ ref('fact_transactions') }} t
  JOIN {{ ref('dim_product') }} p ON t.product_key = p.product_key
  GROUP BY t.customer_key
),

final AS (
  SELECT 
    -- Identity
    c.customer_key,
    c.customer_id,
    
    -- Demographics
    c.email,
    c.first_name,
    c.last_name,
    c.phone,
    c.customer_segment,
    c.region,
    c.country,
    c.state,
    c.city,
    
    -- Calculated Metrics
    COALESCE(l.lifetime_value, 0) as lifetime_value,
    COALESCE(ch.churn_risk_score, 0.5) as churn_risk_score,
    COALESCE(r.recency_score * 100 + r.frequency_score * 10 + r.monetary_score, 0) as rfm_score,
    COALESCE(t.days_since_last_order, 999) as recency_days,
    COALESCE(t.total_orders, 0) as frequency,
    COALESCE(t.total_revenue, 0) as monetary_value,
    
    -- Aggregations
    COALESCE(t.total_orders, 0) as total_orders,
    COALESCE(t.total_revenue, 0) as total_revenue,
    COALESCE(t.average_order_value, 0) as average_order_value,
    t.first_order_date,
    t.last_order_date,
    COALESCE(t.days_since_last_order, 999) as days_since_last_order,
    
    -- Interactions
    COALESCE(i.total_interactions, 0) as total_interactions,
    COALESCE(i.page_views, 0) as page_views,
    COALESCE(i.add_to_carts, 0) as add_to_carts,
    i.last_interaction_date,
    
    -- Product Affinity
    COALESCE(pa.top_product_category, 'Unknown') as top_product_category,
    COALESCE(pa.product_affinity_scores, OBJECT_CONSTRUCT()) as product_affinity_scores,
    
    -- SCD Tracking
    c.valid_from,
    c.valid_to,
    c.is_current,
    
    -- Audit
    c.dw_created_at,
    CURRENT_TIMESTAMP() as dw_updated_at
  FROM customers c
  LEFT JOIN transactions t ON c.customer_key = t.customer_key
  LEFT JOIN interactions i ON c.customer_key = i.customer_key
  LEFT JOIN rfm r ON c.customer_key = r.customer_key
  LEFT JOIN ltv_calc l ON c.customer_key = l.customer_key
  LEFT JOIN churn_calc ch ON c.customer_key = ch.customer_key
  LEFT JOIN product_affinity pa ON c.customer_key = pa.customer_key
)

SELECT * FROM final
