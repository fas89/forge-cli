{{
  config(
    materialized='view',
    description='Staged customer data from CRM with data quality improvements'
  )
}}

WITH source_data AS (
  SELECT 
    customer_id,
    email,
    first_name,
    last_name,
    phone,
    address_line_1,
    address_line_2,
    city,
    state,
    zip_code,
    country,
    date_of_birth,
    gender,
    customer_since,
    customer_status,
    marketing_opt_in,
    created_at,
    updated_at,
    -- Add row number for deduplication
    ROW_NUMBER() OVER (
      PARTITION BY customer_id 
      ORDER BY updated_at DESC
    ) AS row_num
    
  FROM {{ source('raw', 'crm_customers') }}
  WHERE customer_id IS NOT NULL
),

cleaned_data AS (
  SELECT
    customer_id,
    
    -- Email handling and hashing for privacy
    CASE 
      WHEN email IS NOT NULL AND REGEXP_CONTAINS(email, r'^[^@]+@[^@]+\.[^@]+$')
      THEN TO_BASE64(SHA256(LOWER(TRIM(email))))
      ELSE NULL 
    END AS email_hash,
    
    -- Name standardization
    INITCAP(TRIM(first_name)) AS first_name,
    INITCAP(TRIM(last_name)) AS last_name,
    CONCAT(
      INITCAP(TRIM(first_name)), 
      ' ', 
      INITCAP(TRIM(last_name))
    ) AS full_name,
    
    -- Phone number cleaning
    REGEXP_REPLACE(
      REGEXP_REPLACE(phone, r'[^\d]', ''),
      r'^1?(\d{10})$',
      r'\1'
    ) AS phone_cleaned,
    
    -- Address standardization
    INITCAP(TRIM(address_line_1)) AS address_line_1,
    INITCAP(TRIM(address_line_2)) AS address_line_2,
    INITCAP(TRIM(city)) AS city,
    UPPER(TRIM(state)) AS state,
    REGEXP_REPLACE(zip_code, r'[^\d-]', '') AS zip_code,
    UPPER(TRIM(country)) AS country,
    
    -- Demographics
    CASE 
      WHEN date_of_birth IS NOT NULL 
        AND date_of_birth > '1900-01-01' 
        AND date_of_birth < CURRENT_DATE()
      THEN date_of_birth
      ELSE NULL
    END AS date_of_birth,
    
    -- Calculate age
    CASE 
      WHEN date_of_birth IS NOT NULL 
        AND date_of_birth > '1900-01-01' 
        AND date_of_birth < CURRENT_DATE()
      THEN DATE_DIFF(CURRENT_DATE(), date_of_birth, YEAR)
      ELSE NULL
    END AS age,
    
    -- Age grouping
    CASE 
      WHEN DATE_DIFF(CURRENT_DATE(), date_of_birth, YEAR) < 25 THEN '18-24'
      WHEN DATE_DIFF(CURRENT_DATE(), date_of_birth, YEAR) < 35 THEN '25-34'
      WHEN DATE_DIFF(CURRENT_DATE(), date_of_birth, YEAR) < 45 THEN '35-44'
      WHEN DATE_DIFF(CURRENT_DATE(), date_of_birth, YEAR) < 55 THEN '45-54'
      WHEN DATE_DIFF(CURRENT_DATE(), date_of_birth, YEAR) < 65 THEN '55-64'
      WHEN DATE_DIFF(CURRENT_DATE(), date_of_birth, YEAR) >= 65 THEN '65+'
      ELSE 'Unknown'
    END AS age_group,
    
    -- Gender standardization
    CASE 
      WHEN UPPER(gender) IN ('M', 'MALE') THEN 'Male'
      WHEN UPPER(gender) IN ('F', 'FEMALE') THEN 'Female'
      WHEN UPPER(gender) IN ('NB', 'NON-BINARY', 'NON_BINARY') THEN 'Non-Binary'
      WHEN UPPER(gender) IN ('O', 'OTHER') THEN 'Other'
      ELSE 'Not Specified'
    END AS gender,
    
    -- Customer lifecycle
    customer_since,
    DATE_DIFF(CURRENT_DATE(), customer_since, DAY) AS customer_tenure_days,
    
    CASE 
      WHEN DATE_DIFF(CURRENT_DATE(), customer_since, DAY) <= 30 THEN 'New (0-30 days)'
      WHEN DATE_DIFF(CURRENT_DATE(), customer_since, DAY) <= 90 THEN 'Recent (31-90 days)'
      WHEN DATE_DIFF(CURRENT_DATE(), customer_since, DAY) <= 365 THEN 'Established (3-12 months)'
      WHEN DATE_DIFF(CURRENT_DATE(), customer_since, DAY) <= 1095 THEN 'Long-term (1-3 years)'
      ELSE 'Veteran (3+ years)'
    END AS customer_tenure_segment,
    
    -- Status and preferences
    CASE 
      WHEN UPPER(customer_status) = 'ACTIVE' THEN 'Active'
      WHEN UPPER(customer_status) = 'INACTIVE' THEN 'Inactive'
      WHEN UPPER(customer_status) = 'SUSPENDED' THEN 'Suspended'
      WHEN UPPER(customer_status) = 'CHURNED' THEN 'Churned'
      ELSE 'Unknown'
    END AS customer_status,
    
    COALESCE(marketing_opt_in, FALSE) AS marketing_opt_in,
    
    -- Geographic segment
    CASE 
      WHEN UPPER(country) = 'US' THEN
        CASE 
          WHEN state IN ('CA', 'WA', 'OR', 'NV', 'AZ', 'UT', 'ID', 'MT', 'WY', 'CO', 'NM', 'AK', 'HI') 
            THEN 'West'
          WHEN state IN ('TX', 'OK', 'AR', 'LA', 'MS', 'AL', 'TN', 'KY', 'WV', 'VA', 'NC', 'SC', 'GA', 'FL', 'DE', 'MD', 'DC') 
            THEN 'South'
          WHEN state IN ('ND', 'SD', 'NE', 'KS', 'MN', 'IA', 'MO', 'WI', 'IL', 'IN', 'MI', 'OH') 
            THEN 'Midwest'
          WHEN state IN ('ME', 'NH', 'VT', 'MA', 'RI', 'CT', 'NY', 'NJ', 'PA') 
            THEN 'Northeast'
          ELSE 'Other US'
        END
      WHEN UPPER(country) = 'CA' THEN 'Canada'
      WHEN UPPER(country) IN ('GB', 'FR', 'DE', 'IT', 'ES', 'NL', 'BE', 'AT', 'CH', 'IE', 'SE', 'NO', 'DK', 'FI') 
        THEN 'Europe'
      ELSE 'International'
    END AS geographic_segment,
    
    -- Metadata
    created_at,
    updated_at,
    CURRENT_TIMESTAMP() AS dbt_loaded_at
    
  FROM source_data
  WHERE row_num = 1  -- Deduplicate
)

SELECT * FROM cleaned_data