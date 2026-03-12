{{
  config(
    materialized='view',
    description='Staged customer transaction data with enrichment and data quality improvements'
  )
}}

WITH source_data AS (
  SELECT 
    transaction_id,
    customer_id,
    order_id,
    transaction_date,
    transaction_timestamp,
    amount,
    currency,
    payment_method,
    transaction_type,
    status,
    product_category,
    product_subcategory,
    product_id,
    quantity,
    unit_price,
    discount_amount,
    tax_amount,
    shipping_amount,
    channel,
    promotion_code,
    created_at,
    updated_at,
    -- Add row number for deduplication
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id 
      ORDER BY updated_at DESC
    ) AS row_num
    
  FROM {{ source('raw', 'transactions') }}
  WHERE transaction_id IS NOT NULL
    AND customer_id IS NOT NULL
),

cleaned_data AS (
  SELECT
    transaction_id,
    customer_id,
    order_id,
    
    -- Date and time handling
    CASE 
      WHEN transaction_date IS NOT NULL 
        AND transaction_date >= '2010-01-01' 
        AND transaction_date <= CURRENT_DATE()
      THEN transaction_date
      WHEN transaction_timestamp IS NOT NULL
        AND DATE(transaction_timestamp) >= '2010-01-01'
        AND DATE(transaction_timestamp) <= CURRENT_DATE()
      THEN DATE(transaction_timestamp)
      ELSE NULL
    END AS transaction_date,
    
    CASE 
      WHEN transaction_timestamp IS NOT NULL
        AND transaction_timestamp >= '2010-01-01'
        AND transaction_timestamp <= CURRENT_TIMESTAMP()
      THEN transaction_timestamp
      ELSE NULL
    END AS transaction_timestamp,
    
    -- Amount validation and conversion
    CASE 
      WHEN amount > 0 AND amount < 1000000  -- Reasonable bounds
      THEN amount
      ELSE NULL
    END AS amount,
    
    -- Currency standardization
    CASE 
      WHEN UPPER(currency) IN ('USD', 'CAD', 'EUR', 'GBP', 'JPY', 'AUD')
      THEN UPPER(currency)
      ELSE 'USD'  -- Default to USD
    END AS currency,
    
    -- Convert to USD for analysis (simplified - in reality would use exchange rates)
    CASE 
      WHEN UPPER(currency) = 'USD' THEN amount
      WHEN UPPER(currency) = 'CAD' THEN amount * 0.75  -- Approximate
      WHEN UPPER(currency) = 'EUR' THEN amount * 1.10  -- Approximate  
      WHEN UPPER(currency) = 'GBP' THEN amount * 1.25  -- Approximate
      WHEN UPPER(currency) = 'JPY' THEN amount * 0.0067 -- Approximate
      WHEN UPPER(currency) = 'AUD' THEN amount * 0.65  -- Approximate
      ELSE amount  -- Default to original amount
    END AS amount_usd,
    
    -- Payment method standardization
    CASE 
      WHEN UPPER(payment_method) LIKE '%CREDIT%' OR UPPER(payment_method) LIKE '%VISA%' 
        OR UPPER(payment_method) LIKE '%MASTERCARD%' OR UPPER(payment_method) LIKE '%AMEX%'
      THEN 'Credit Card'
      WHEN UPPER(payment_method) LIKE '%DEBIT%' THEN 'Debit Card'
      WHEN UPPER(payment_method) LIKE '%PAYPAL%' THEN 'PayPal'
      WHEN UPPER(payment_method) LIKE '%APPLE%' THEN 'Apple Pay'
      WHEN UPPER(payment_method) LIKE '%GOOGLE%' THEN 'Google Pay'
      WHEN UPPER(payment_method) LIKE '%BANK%' OR UPPER(payment_method) LIKE '%ACH%' 
      THEN 'Bank Transfer'
      WHEN UPPER(payment_method) LIKE '%CASH%' THEN 'Cash'
      WHEN UPPER(payment_method) LIKE '%CHECK%' THEN 'Check'
      ELSE 'Other'
    END AS payment_method_group,
    
    -- Transaction type standardization
    CASE 
      WHEN UPPER(transaction_type) IN ('PURCHASE', 'SALE', 'ORDER') THEN 'Purchase'
      WHEN UPPER(transaction_type) IN ('REFUND', 'RETURN') THEN 'Refund'
      WHEN UPPER(transaction_type) IN ('ADJUSTMENT', 'CREDIT') THEN 'Adjustment'
      WHEN UPPER(transaction_type) IN ('FEE', 'CHARGE') THEN 'Fee'
      ELSE 'Other'
    END AS transaction_type,
    
    -- Status standardization
    CASE 
      WHEN UPPER(status) IN ('COMPLETED', 'SUCCESS', 'PAID') THEN 'Completed'
      WHEN UPPER(status) IN ('PENDING', 'PROCESSING') THEN 'Pending'
      WHEN UPPER(status) IN ('FAILED', 'DECLINED', 'ERROR') THEN 'Failed'
      WHEN UPPER(status) IN ('CANCELLED', 'CANCELED', 'VOID') THEN 'Cancelled'
      WHEN UPPER(status) IN ('REFUNDED') THEN 'Refunded'
      ELSE 'Unknown'
    END AS transaction_status,
    
    -- Product categorization
    COALESCE(INITCAP(TRIM(product_category)), 'Uncategorized') AS product_category,
    COALESCE(INITCAP(TRIM(product_subcategory)), 'Uncategorized') AS product_subcategory,
    
    product_id,
    
    -- Quantity validation
    CASE 
      WHEN quantity > 0 AND quantity <= 1000  -- Reasonable bounds
      THEN quantity
      ELSE 1  -- Default to 1
    END AS quantity,
    
    -- Unit price calculation
    CASE 
      WHEN unit_price IS NOT NULL AND unit_price > 0
      THEN unit_price
      WHEN quantity > 0 AND amount > 0
      THEN amount / quantity
      ELSE NULL
    END AS unit_price,
    
    -- Amount components
    COALESCE(discount_amount, 0) AS discount_amount,
    COALESCE(tax_amount, 0) AS tax_amount,
    COALESCE(shipping_amount, 0) AS shipping_amount,
    
    -- Net amount calculation
    amount - COALESCE(discount_amount, 0) AS net_amount,
    
    -- Channel standardization
    CASE 
      WHEN UPPER(channel) LIKE '%ONLINE%' OR UPPER(channel) LIKE '%WEB%' 
        OR UPPER(channel) LIKE '%WEBSITE%'
      THEN 'Online'
      WHEN UPPER(channel) LIKE '%MOBILE%' OR UPPER(channel) LIKE '%APP%'
      THEN 'Mobile App'
      WHEN UPPER(channel) LIKE '%STORE%' OR UPPER(channel) LIKE '%RETAIL%' 
        OR UPPER(channel) LIKE '%POS%'
      THEN 'In-Store'
      WHEN UPPER(channel) LIKE '%PHONE%' OR UPPER(channel) LIKE '%CALL%'
      THEN 'Phone'
      WHEN UPPER(channel) LIKE '%EMAIL%' OR UPPER(channel) LIKE '%MAIL%'
      THEN 'Email'
      ELSE 'Other'
    END AS channel,
    
    -- Promotion handling
    CASE 
      WHEN promotion_code IS NOT NULL AND TRIM(promotion_code) != ''
      THEN UPPER(TRIM(promotion_code))
      ELSE NULL
    END AS promotion_code,
    
    CASE 
      WHEN promotion_code IS NOT NULL AND TRIM(promotion_code) != ''
      THEN TRUE
      ELSE FALSE
    END AS has_promotion,
    
    -- Time-based attributes
    EXTRACT(YEAR FROM transaction_date) AS transaction_year,
    EXTRACT(QUARTER FROM transaction_date) AS transaction_quarter,
    EXTRACT(MONTH FROM transaction_date) AS transaction_month,
    EXTRACT(DAYOFWEEK FROM transaction_date) AS transaction_dayofweek,
    EXTRACT(HOUR FROM transaction_timestamp) AS transaction_hour,
    
    -- Day of week name
    CASE EXTRACT(DAYOFWEEK FROM transaction_date)
      WHEN 1 THEN 'Sunday'
      WHEN 2 THEN 'Monday'
      WHEN 3 THEN 'Tuesday'
      WHEN 4 THEN 'Wednesday'
      WHEN 5 THEN 'Thursday'
      WHEN 6 THEN 'Friday'
      WHEN 7 THEN 'Saturday'
    END AS transaction_day_name,
    
    -- Time period classifications
    CASE 
      WHEN EXTRACT(HOUR FROM transaction_timestamp) BETWEEN 6 AND 11 THEN 'Morning'
      WHEN EXTRACT(HOUR FROM transaction_timestamp) BETWEEN 12 AND 17 THEN 'Afternoon'
      WHEN EXTRACT(HOUR FROM transaction_timestamp) BETWEEN 18 AND 21 THEN 'Evening'
      ELSE 'Night'
    END AS time_of_day,
    
    CASE 
      WHEN EXTRACT(DAYOFWEEK FROM transaction_date) IN (1, 7) THEN 'Weekend'
      ELSE 'Weekday'
    END AS day_type,
    
    -- Metadata
    created_at,
    updated_at,
    CURRENT_TIMESTAMP() AS dbt_loaded_at
    
  FROM source_data
  WHERE row_num = 1  -- Deduplicate
    AND UPPER(status) IN ('COMPLETED', 'SUCCESS', 'PAID')  -- Only successful transactions
)

SELECT * FROM cleaned_data