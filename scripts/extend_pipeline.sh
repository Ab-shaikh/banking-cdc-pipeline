#!/bin/bash
#
# ============================================================================
# Extends the banking CDC pipeline to cover accounts, branches, customers
# (in addition to transactions which was already wired up).
#
# Creates:
#   - dbt staging models: stg_accounts, stg_branches, stg_customers
#   - dbt marts models:   dim_accounts, dim_branches, dim_customers
#   - updates src_banking.yml with the 3 new sources
#   - overwrites the Airflow DAG so it loads all 4 RAW tables and runs
#     dbt for staging + marts in one go
#
# Run this ON THE EC2 INSTANCE from anywhere (paths are absolute).
# ============================================================================
set -euo pipefail

DBT_PROJECT_DIR="$HOME/cdc_lab/airflow_setup/dbt_project"
STAGING_DIR="$DBT_PROJECT_DIR/models/staging"
MARTS_DIR="$DBT_PROJECT_DIR/models/marts"
DAGS_DIR="$HOME/cdc_lab/airflow_setup/dags"

mkdir -p "$STAGING_DIR" "$MARTS_DIR" "$DAGS_DIR"

echo "[1/5] Writing updated src_banking.yml (adds raw_accounts/raw_branches/raw_customers)..."
cat > "$STAGING_DIR"/src_banking.yml << 'EOF'
version: 2

sources:
  - name: banking_raw
    database: BANKING_DWH
    schema: RAW
    tables:
      - name: raw_transactions
      - name: raw_accounts
      - name: raw_branches
      - name: raw_customers
EOF

echo "[2/5] Writing staging models (stg_accounts, stg_branches, stg_customers)..."

cat > "$STAGING_DIR"/stg_accounts.sql << 'EOF'
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
EOF

cat > "$STAGING_DIR"/stg_branches.sql << 'EOF'
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
EOF

cat > "$STAGING_DIR"/stg_customers.sql << 'EOF'
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
EOF

echo "[3/5] Writing marts models (dim_accounts, dim_branches, dim_customers)..."

cat > "$MARTS_DIR"/dim_accounts.sql << 'EOF'
{{ config(materialized='table') }}
SELECT * FROM {{ ref('stg_accounts') }}
EOF

cat > "$MARTS_DIR"/dim_branches.sql << 'EOF'
{{ config(materialized='table') }}
SELECT * FROM {{ ref('stg_branches') }}
EOF

cat > "$MARTS_DIR"/dim_customers.sql << 'EOF'
{{ config(materialized='table') }}
SELECT * FROM {{ ref('stg_customers') }}
EOF

echo "[4/5] Creating RAW tables in Snowflake (idempotent, safe to re-run)..."
if command -v snowsql &> /dev/null; then
    if [ -f "$HOME/.snowsql/config" ] || [ -n "${SNOWSQL_ACCOUNT:-}" ]; then
        echo "  (skipping auto-create: run the CREATE TABLE statements manually via snowsql if not already done)"
    fi
fi
echo "  NOTE: if raw_accounts / raw_branches / raw_customers don't exist yet in Snowflake,"
echo "  run this once via snowsql:"
echo "    CREATE TABLE IF NOT EXISTS BANKING_DWH.RAW.raw_accounts  (raw_data VARIANT, _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP());"
echo "    CREATE TABLE IF NOT EXISTS BANKING_DWH.RAW.raw_branches  (raw_data VARIANT, _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP());"
echo "    CREATE TABLE IF NOT EXISTS BANKING_DWH.RAW.raw_customers (raw_data VARIANT, _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP());"

echo "[5/5] Overwriting Airflow DAG to load + transform all 4 tables..."
cat > "$DAGS_DIR"/banking_pipeline.py << 'EOF'
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
}

with DAG(
    'banking_s3_to_marts',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['banking', 'cdc'],
) as dag:

    load_raw_transactions = SnowflakeOperator(
        task_id='copy_into_raw_transactions',
        snowflake_conn_id='snowflake_conn',
        sql="""
        COPY INTO BANKING_DWH.RAW.raw_transactions (raw_data)
        FROM @BANKING_DWH.RAW.raw_stage
        PATTERN = '.*banking\\\\.public\\\\.transactions/.*\\\\.json'
        FILE_FORMAT = (TYPE = JSON)
        ON_ERROR = 'CONTINUE';
        """
    )

    load_raw_accounts = SnowflakeOperator(
        task_id='copy_into_raw_accounts',
        snowflake_conn_id='snowflake_conn',
        sql="""
        COPY INTO BANKING_DWH.RAW.raw_accounts (raw_data)
        FROM @BANKING_DWH.RAW.raw_stage
        PATTERN = '.*banking\\\\.public\\\\.accounts/.*\\\\.json'
        FILE_FORMAT = (TYPE = JSON)
        ON_ERROR = 'CONTINUE';
        """
    )

    load_raw_branches = SnowflakeOperator(
        task_id='copy_into_raw_branches',
        snowflake_conn_id='snowflake_conn',
        sql="""
        COPY INTO BANKING_DWH.RAW.raw_branches (raw_data)
        FROM @BANKING_DWH.RAW.raw_stage
        PATTERN = '.*banking\\\\.public\\\\.branches/.*\\\\.json'
        FILE_FORMAT = (TYPE = JSON)
        ON_ERROR = 'CONTINUE';
        """
    )

    load_raw_customers = SnowflakeOperator(
        task_id='copy_into_raw_customers',
        snowflake_conn_id='snowflake_conn',
        sql="""
        COPY INTO BANKING_DWH.RAW.raw_customers (raw_data)
        FROM @BANKING_DWH.RAW.raw_stage
        PATTERN = '.*banking\\\\.public\\\\.customers/.*\\\\.json'
        FILE_FORMAT = (TYPE = JSON)
        ON_ERROR = 'CONTINUE';
        """
    )

    dbt_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=(
            'cd /opt/airflow/dbt_project && '
            'dbt run --select staging '
            '--profiles-dir /opt/airflow/dbt_profiles'
        )
    )

    dbt_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command=(
            'cd /opt/airflow/dbt_project && '
            'dbt run --select marts '
            '--profiles-dir /opt/airflow/dbt_profiles'
        )
    )

    # All 4 RAW loads run in parallel, then staging, then marts
    [load_raw_transactions, load_raw_accounts, load_raw_branches, load_raw_customers] >> dbt_staging >> dbt_marts
EOF

echo ""
echo "=================================================="
echo " DONE"
echo "=================================================="
echo "Files written:"
echo "  - $STAGING_DIR/src_banking.yml"
echo "  - $STAGING_DIR/stg_accounts.sql"
echo "  - $STAGING_DIR/stg_branches.sql"
echo "  - $STAGING_DIR/stg_customers.sql"
echo "  - $MARTS_DIR/dim_accounts.sql"
echo "  - $MARTS_DIR/dim_branches.sql"
echo "  - $MARTS_DIR/dim_customers.sql"
echo "  - $DAGS_DIR/banking_pipeline.py (overwritten with all 4 tables)"
echo ""
echo "If raw_accounts/raw_branches/raw_customers tables don't exist in Snowflake yet,"
echo "create them first (see Step 4 output above), then trigger the DAG:"
echo "  docker exec -it airflow_setup-airflow-scheduler-1 airflow dags trigger banking_s3_to_marts"
