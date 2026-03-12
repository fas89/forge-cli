-- Intermediate: Customer engagement metrics

{{ config(materialized='view') }}

with engagement_base as (
    select
        customer_id,
        count(*) as total_interactions,
        max(order_date) as last_interaction_date,
        
        -- Channel preference
        mode() within group (order by channel) as preferred_channel,
        
        -- Recency, frequency components for engagement score
        {{ dbt.datediff('max(order_date)', 'current_date', 'day') }} as days_since_last_interaction,
        count(*) * 1.0 / nullif({{ dbt.datediff('min(order_date)', 'max(order_date)', 'day') }}, 0) as interaction_frequency
        
    from {{ ref('stg_orders') }}
    group by customer_id
),

engagement_scores as (
    select
        customer_id,
        preferred_channel,
        
        -- Calculate engagement score (0-100)
        least(100, greatest(0,
            -- Recency component (40% weight)
            case 
                when days_since_last_interaction <= 7 then 40
                when days_since_last_interaction <= 30 then 30
                when days_since_last_interaction <= 90 then 20
                when days_since_last_interaction <= 180 then 10
                else 0
            end +
            -- Frequency component (40% weight) 
            least(40, coalesce(total_interactions * 2, 0)) +
            -- Consistency component (20% weight)
            case
                when interaction_frequency >= 0.1 then 20  -- Regular activity
                when interaction_frequency >= 0.05 then 15
                when interaction_frequency >= 0.01 then 10
                else 5
            end
        )) as engagement_score
        
    from engagement_base
)

select * from engagement_scores