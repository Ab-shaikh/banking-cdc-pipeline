{{ config(materialized='view') }}

WITH raw_source AS (
    SELECT raw_data FROM {{ source('banking_raw', 'raw_branches') }}
    WHERE raw_data:payload:after IS NOT NULL
)

SELECT
    raw_data:payload:after:branch_id::INT           AS branch_id,
    raw_data:payload:after:branch_code::VARCHAR(20) AS branch_code,
    raw_data:payload:after:city::VARCHAR(50)        AS city,
    raw_data:payload:op::VARCHAR(5)                 AS cdc_operation
FROM raw_source
