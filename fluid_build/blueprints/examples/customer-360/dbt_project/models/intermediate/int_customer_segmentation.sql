-- Intermediate: Customer segmentation logic

{{ config(materialized='view') }}

with customer_metrics as (
    select
        c.customer_id,
        coalesce(om.total_spent, 0) as total_spent,
        coalesce(om.total_orders, 0) as total_orders,
        coalesce(om.days_since_last_order, 9999) as days_since_last_order,
        {{ dbt.datediff('c.registration_date', 'current_date', 'day') }} as customer_age_days
        
    from {{ ref('stg_customers') }} c
    left join {{ ref('int_customer_order_metrics') }} om on c.customer_id = om.customer_id
),

segmentation as (
    select
        customer_id,
        
        -- Calculate lifetime value (simplified)
        case 
            when total_orders = 0 then 0
            else total_spent * 1.2  -- Simple LTV multiplier
        end as lifetime_value,
        
        -- Determine customer segment
        case
            when total_spent >= {{ var('high_value_ltv_threshold') }} then 'high_value'
            when total_spent >= {{ var('regular_value_ltv_threshold') }} then 'regular'
            when days_since_last_order > {{ var('churn_risk_days_threshold') }} and total_orders > 0 then 'at_risk'
            when total_orders = 0 and customer_age_days > 30 then 'dormant'
            else 'regular'
        end as customer_segment,
        
        -- Calculate churn probability (simplified)
        case
            when total_orders = 0 then 0.8
            when days_since_last_order > 180 then 0.7
            when days_since_last_order > 90 then 0.4
            when days_since_last_order > 30 then 0.2
            else 0.1
        end as churn_probability
        
    from customer_metrics
)

select * from segmentation