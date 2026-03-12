-- Staging: Clean and standardize raw customer data

{{ config(materialized='view') }}

select
    customer_id,
    lower(trim(email)) as email,
    trim(first_name) as first_name,
    trim(last_name) as last_name,
    registration_date::date as registration_date,
    created_at,
    updated_at

from {{ source('raw', 'customers') }}
where customer_id is not null
  and email is not null
  and email != ''