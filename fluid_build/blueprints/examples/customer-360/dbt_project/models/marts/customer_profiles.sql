-- Customer 360 - Customer Profiles Mart
-- Creates unified customer profiles with segmentation and key metrics

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['customer_id'], 'unique': true},
        {'columns': ['customer_segment']},
        {'columns': ['registration_date']}
    ]
) }}

with customer_base as (
    select * from {{ ref('stg_customers') }}
),

order_metrics as (
    select * from {{ ref('int_customer_order_metrics') }}
),

engagement_metrics as (
    select * from {{ ref('int_customer_engagement') }}
),

segmentation as (
    select * from {{ ref('int_customer_segmentation') }}
)

select
    -- Customer Identity
    c.customer_id,
    c.email,
    c.first_name,
    c.last_name,
    c.registration_date,
    
    -- Segmentation
    seg.customer_segment,
    seg.lifetime_value,
    seg.churn_probability,
    
    -- Order Metrics
    coalesce(om.total_orders, 0) as total_orders,
    coalesce(om.total_spent, 0) as total_spent,
    coalesce(om.avg_order_value, 0) as avg_order_value,
    om.first_order_date,
    om.last_order_date,
    om.days_since_last_order,
    
    -- Engagement Metrics  
    coalesce(em.engagement_score, 0) as engagement_score,
    em.preferred_channel,
    
    -- Metadata
    current_timestamp as last_updated

from customer_base c
left join order_metrics om on c.customer_id = om.customer_id
left join engagement_metrics em on c.customer_id = em.customer_id
left join segmentation seg on c.customer_id = seg.customer_id