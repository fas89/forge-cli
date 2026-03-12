-- Intermediate: Transaction context enrichment

{{ config(materialized='view') }}

with transaction_sequence as (
    select
        transaction_id,
        customer_id,
        order_date,
        row_number() over (partition by customer_id order by order_date) as order_sequence,
        lag(order_date) over (partition by customer_id order by order_date) as prev_order_date
        
    from {{ ref('stg_orders') }}
),

customer_context as (
    select
        c.customer_id,
        c.registration_date,
        {{ dbt.datediff('c.registration_date', 'current_date', 'day') }} as customer_tenure_days
        
    from {{ ref('stg_customers') }} c
),

enriched_context as (
    select
        ts.transaction_id,
        ts.customer_id,
        
        -- First purchase flag
        case when ts.order_sequence = 1 then true else false end as is_first_purchase,
        
        -- Days since previous order
        case 
            when ts.prev_order_date is null then null
            else {{ dbt.datediff('ts.prev_order_date', 'ts.order_date', 'day') }}
        end as days_since_prev_order,
        
        -- Customer tenure at time of order
        {{ dbt.datediff('cc.registration_date', 'ts.order_date', 'day') }} as customer_tenure_days
        
    from transaction_sequence ts
    left join customer_context cc on ts.customer_id = cc.customer_id
)

select * from enriched_context