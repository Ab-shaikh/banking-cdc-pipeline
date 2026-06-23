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
