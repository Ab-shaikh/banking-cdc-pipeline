{{ config(materialized='view') }}

WITH raw_source AS (
    SELECT raw_data FROM {{ source('banking_raw', 'raw_customers') }}
    WHERE raw_data:payload:after IS NOT NULL
)

SELECT
    raw_data:payload:after:customer_id::INT          AS customer_id,
    raw_data:payload:after:first_name::VARCHAR(50)   AS first_name,
    raw_data:payload:after:last_name::VARCHAR(50)    AS last_name,
    raw_data:payload:after:pan_number::VARCHAR(15)   AS pan_number,
    raw_data:payload:after:kyc_status::VARCHAR(20)   AS kyc_status,
    raw_data:payload:op::VARCHAR(5)                  AS cdc_operation
FROM raw_source
