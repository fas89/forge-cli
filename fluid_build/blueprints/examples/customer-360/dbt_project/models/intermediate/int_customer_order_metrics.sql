-- Intermediate: Calculate customer order metrics

{{ config(materialized='view') }}

with order_stats as (
    select
        customer_id,
        count(*) as total_orders,
        sum(order_amount) as total_spent,
        avg(order_amount) as avg_order_value,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        {{ dbt.datediff('max(order_date)', 'current_date', 'day') }} as days_since_last_order
        
    from {{ ref('stg_orders') }}
    group by customer_id
)

select * from order_stats