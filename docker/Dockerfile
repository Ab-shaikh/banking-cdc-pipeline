FROM apache/airflow:2.9.3

RUN pip install --no-cache-dir dbt-snowflake==1.8.0 && \
    pip install --no-cache-dir --no-deps apache-airflow-providers-snowflake==5.4.0 && \
    pip install --no-cache-dir snowflake-sqlalchemy==1.6.1
