{{ config(materialized='table') }}

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
