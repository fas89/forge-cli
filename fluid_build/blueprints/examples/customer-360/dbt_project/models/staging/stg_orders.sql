-- Staging: Clean and standardize order data

{{ config(materialized='view') }}

select
    order_id as transaction_id,
    customer_id,
    order_date::date as order_date,
    total_amount as order_amount,
    coalesce(product_category, 'unknown') as product_category,
    coalesce(channel, 'unknown') as channel,
    coalesce(payment_method, 'unknown') as payment_method,
    coalesce(discount_amount, 0) as discount_amount,
    created_at,
    updated_at

from {{ source('raw', 'orders') }}
where order_id is not null
  and customer_id is not null
  and total_amount >= 0