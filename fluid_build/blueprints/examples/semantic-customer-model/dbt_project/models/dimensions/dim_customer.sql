-- models/dimensions/dim_customer.sql
-- Slowly Changing Dimension Type 2 for customer profiles
{{
  config(
    materialized='incremental',
    unique_key='customer_key',
    merge_update_columns=['valid_to', 'is_current'],
    tags=['dimensions', 'scd2', 'customer']
  )
}}

WITH source_data AS (
  SELECT 
    customer_id,
    email,
    first_name,
    last_name,
    phone,
    customer_segment,
    region,
    country,
    state,
    city,
    created_at,
    updated_at
  FROM {{ source('bronze_crm', 'customer_raw') }}
  {% if is_incremental() %}
  WHERE updated_at > (SELECT MAX(valid_from) FROM {{ this }})
  {% endif %}
),

-- Get current records from target
{% if is_incremental() %}
current_records AS (
  SELECT 
    *
  FROM {{ this }}
  WHERE is_current = TRUE
),

-- Detect changes
changes AS (
  SELECT 
    s.customer_id,
    s.email,
    s.first_name,
    s.last_name,
    s.phone,
    s.customer_segment,
    s.region,
    s.country,
    s.state,
    s.city,
    s.updated_at,
    c.customer_key as existing_key,
    c.valid_from as existing_valid_from,
    CASE 
      WHEN c.customer_key IS NULL THEN 'INSERT'
      WHEN s.email != c.email 
        OR s.customer_segment != c.customer_segment 
        OR s.region != c.region 
        OR s.phone != c.phone THEN 'UPDATE'
      ELSE 'NO_CHANGE'
    END as change_type
  FROM source_data s
  LEFT JOIN current_records c
    ON s.customer_id = c.customer_id
),

-- Close out old records
closing_records AS (
  SELECT 
    existing_key as customer_key,
    customer_id,
    email,
    first_name,
    last_name,
    phone,
    customer_segment,
    region,
    country,
    state,
    city,
    existing_valid_from as valid_from,
    updated_at as valid_to,
    FALSE as is_current,
    existing_valid_from as dw_created_at,
    CURRENT_TIMESTAMP() as dw_updated_at
  FROM changes
  WHERE change_type = 'UPDATE'
),

-- Insert new records
new_records AS (
  SELECT 
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'updated_at']) }} as customer_key,
    customer_id,
    email,
    first_name,
    last_name,
    phone,
    customer_segment,
    region,
    country,
    state,
    city,
    updated_at as valid_from,
    '9999-12-31'::TIMESTAMP as valid_to,
    TRUE as is_current,
    CURRENT_TIMESTAMP() as dw_created_at,
    CURRENT_TIMESTAMP() as dw_updated_at
  FROM changes
  WHERE change_type IN ('INSERT', 'UPDATE')
),

final AS (
  SELECT * FROM closing_records
  UNION ALL
  SELECT * FROM new_records
)

SELECT * FROM final

{% else %}

-- Initial load
SELECT 
  {{ dbt_utils.generate_surrogate_key(['customer_id', 'created_at']) }} as customer_key,
  customer_id,
  email,
  first_name,
  last_name,
  phone,
  customer_segment,
  region,
  country,
  state,
  city,
  created_at as valid_from,
  '9999-12-31'::TIMESTAMP as valid_to,
  TRUE as is_current,
  CURRENT_TIMESTAMP() as dw_created_at,
  CURRENT_TIMESTAMP() as dw_updated_at
FROM source_data

{% endif %}
