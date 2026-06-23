{{ config(materialized='view') }}

WITH raw_source AS (
    SELECT raw_data FROM {{ source('banking_raw', 'raw_accounts') }}
    WHERE raw_data:payload:after IS NOT NULL
)

SELECT
    raw_data:payload:after:account_id::INT                       AS account_id,
    raw_data:payload:after:customer_id::INT                      AS customer_id,
    raw_data:payload:after:branch_id::INT                        AS branch_id,
    raw_data:payload:after:account_type::VARCHAR(20)             AS account_type,
    TRY_CAST(raw_data:payload:after:balance::VARCHAR AS FLOAT)   AS balance,
    TO_TIMESTAMP(raw_data:payload:after:created_at::NUMBER / 1000000) AS created_at,
    raw_data:payload:op::VARCHAR(5)                              AS cdc_operation
FROM raw_source
