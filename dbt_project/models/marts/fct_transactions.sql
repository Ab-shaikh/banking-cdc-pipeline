{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge'
) }}

SELECT
    transaction_id,
    account_id,
    transaction_type,
    transaction_amount,
    reference_number,
    transaction_time,
    cdc_operation,
    CASE
        WHEN transaction_amount IS NULL THEN 'CORRUPT_LEGACY_RECORD'
        ELSE 'VALID'
    END AS data_quality_flag
FROM {{ ref('stg_transactions') }}

{% if is_incremental() %}
    
  WHERE transaction_time > (SELECT MAX(transaction_time) FROM {{ this }})
{% endif %}

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY transaction_id 
    ORDER BY transaction_time DESC
) = 1
