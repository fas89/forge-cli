-- Customer 360 - Customer Segments Analysis
-- Segment-level analytics and metrics

{{ config(materialized='table') }}

with segment_metrics as (
    select
        customer_segment as segment_name,
        
        -- Segment Description
        case customer_segment
            when 'high_value' then 'High-value customers with LTV > $5,000'
            when 'regular' then 'Regular customers with moderate engagement'
            when 'at_risk' then 'Customers at risk of churning'
            when 'dormant' then 'Inactive customers requiring re-engagement'
            else 'Unknown segment'
        end as segment_description,
        
        -- Counts and Shares
        count(*) as customer_count,
        count(*) * 100.0 / sum(count(*)) over () as customer_share_pct,
        
        -- Financial Metrics
        avg(lifetime_value) as avg_lifetime_value,
        sum(total_spent) as total_segment_revenue,
        sum(total_spent) * 100.0 / sum(sum(total_spent)) over () as segment_revenue_share,
        avg(avg_order_value) as avg_order_value,
        
        -- Behavioral Metrics
        avg(total_orders) as avg_order_count,
        avg(case when total_orders > 0 
                then total_orders * 30.0 / greatest(days_since_last_order + 30, 30)
                else 0 end) as avg_order_frequency,
        avg(engagement_score) as avg_engagement_score,
        
        -- Risk Metrics
        avg(churn_probability) as churn_rate,
        sum(case when churn_probability > 0.5 then 1 else 0 end) as high_churn_risk_count,
        
        -- Temporal
        current_timestamp as last_updated
        
    from {{ ref('customer_profiles') }}
    group by customer_segment
)

select * from segment_metrics
order by avg_lifetime_value desc