{{ config(materialized='view') }}

WITH raw_source AS (
    SELECT raw_data FROM {{ source('banking_raw', 'raw_transactions') }}
    WHERE raw_data:payload:after IS NOT NULL
)

SELECT
    raw_data:payload:after:txn_id::INT                          AS transaction_id,
    raw_data:payload:after:account_id::INT                      AS account_id,
    raw_data:payload:after:txn_type::VARCHAR(10)                AS transaction_type,
    TRY_CAST(raw_data:payload:after:amount::VARCHAR AS FLOAT)   AS transaction_amount,
    raw_data:payload:after:reference_number::VARCHAR(50)        AS reference_number,
    TO_TIMESTAMP(raw_data:payload:after:txn_timestamp::NUMBER / 1000000) AS transaction_time,
    raw_data:payload:op::VARCHAR(5)                             AS cdc_operation
FROM raw_source
