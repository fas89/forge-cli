-- Customer 360 - Customer Transactions Mart
-- Enriched transaction history with customer context

{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    on_schema_change='fail'
) }}

with transactions_base as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
        where order_date >= (select max(order_date) from {{ this }})
    {% endif %}
),

customer_context as (
    select * from {{ ref('int_customer_transaction_context') }}
),

enriched_transactions as (
    select
        -- Transaction Identity
        t.transaction_id,
        t.customer_id,
        t.order_date,
        t.order_amount,
        t.product_category,
        t.channel,
        t.payment_method,
        t.discount_amount,
        
        -- Customer Context
        cc.is_first_purchase,
        cc.days_since_prev_order,
        cc.customer_tenure_days,
        
        -- Metadata
        current_timestamp as last_updated
        
    from transactions_base t
    left join customer_context cc on t.transaction_id = cc.transaction_id
)

select * from enriched_transactions