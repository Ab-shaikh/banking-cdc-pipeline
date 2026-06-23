#!/bin/bash
#
# ============================================================================
# Banking CDC Pipeline - Full Automated Setup
# Postgres -> Debezium -> Kafka -> S3 Sink -> S3 -> Snowflake -> dbt -> Airflow
# ============================================================================
#
# Run this on a fresh Ubuntu 24.04 EC2 instance (e.g. m7i-flex.large, 8GB+ RAM)
# Usage: bash setup_cdc_lab.sh
#
set -euo pipefail

echo "=================================================="
echo " Banking CDC Pipeline - Automated Setup Starting"
echo "=================================================="

# ----------------------------------------------------------------------------
# CONFIG - edit these if needed
# ----------------------------------------------------------------------------
S3_BUCKET="cdc-lab-banking-20-2026"
AWS_REGION="ap-south-1"
PROJECT_DIR="$HOME/cdc_lab"

# ----------------------------------------------------------------------------
# STEP 0: Base packages (skip if already run by Terraform user_data)
# ----------------------------------------------------------------------------
echo ""
echo "[1/10] Installing base packages..."
sudo apt-get update -y
sudo apt-get install -y docker.io docker-compose-v2 postgresql-client unzip curl

sudo usermod -aG docker "$USER" || true

if ! command -v aws &> /dev/null; then
    echo "Installing AWS CLI v2..."
    curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip"
    (cd /tmp && unzip -q awscliv2.zip && sudo ./aws/install)
    rm -rf /tmp/awscliv2.zip /tmp/aws
fi

# ----------------------------------------------------------------------------
# STEP 1: Project structure
# ----------------------------------------------------------------------------
echo ""
echo "[2/10] Creating project structure..."
mkdir -p "$PROJECT_DIR"/postgres
mkdir -p "$PROJECT_DIR"/connect-plugins/s3-sink
mkdir -p "$PROJECT_DIR"/airflow_setup/dags
mkdir -p "$PROJECT_DIR"/airflow_setup/logs
mkdir -p "$PROJECT_DIR"/airflow_setup/plugins
mkdir -p "$PROJECT_DIR"/airflow_setup/dbt_project/models/staging
mkdir -p "$PROJECT_DIR"/airflow_setup/dbt_project/models/marts
mkdir -p "$PROJECT_DIR"/airflow_setup/dbt_project/macros
mkdir -p "$PROJECT_DIR"/airflow_setup/dbt_profiles

# ----------------------------------------------------------------------------
# STEP 2: Postgres init.sql (3NF banking schema)
# ----------------------------------------------------------------------------
echo ""
echo "[3/10] Writing Postgres init script..."
cat > "$PROJECT_DIR"/postgres/init.sql << 'EOF'
CREATE TABLE branches (
    branch_id SERIAL PRIMARY KEY,
    branch_code VARCHAR(20) UNIQUE,
    city VARCHAR(50)
);

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    pan_number VARCHAR(15) UNIQUE,
    kyc_status VARCHAR(20)
);

CREATE TABLE accounts (
    account_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    branch_id INT REFERENCES branches(branch_id),
    account_type VARCHAR(20),
    balance DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE transactions (
    txn_id SERIAL PRIMARY KEY,
    account_id INT REFERENCES accounts(account_id),
    txn_type VARCHAR(10),
    amount DECIMAL(15,2),
    reference_number VARCHAR(50) UNIQUE,
    txn_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE branches REPLICA IDENTITY FULL;
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE accounts REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;

INSERT INTO branches (branch_code, city) VALUES ('MUM001', 'Mumbai'), ('DEL001', 'Delhi');
INSERT INTO customers (first_name, last_name, pan_number, kyc_status) VALUES
  ('Rahul', 'Sharma', 'ABCDE1234F', 'VERIFIED'),
  ('Priya', 'Singh', 'VWXYZ9876Q', 'PENDING');
INSERT INTO accounts (customer_id, branch_id, account_type, balance) VALUES
  (1, 1, 'SAVINGS', 50000.00),
  (2, 2, 'CURRENT', 15000.00);
INSERT INTO transactions (account_id, txn_type, amount, reference_number) VALUES
  (1, 'CR', 10000.00, 'REF10001'),
  (1, 'DR', 500.00, 'REF10002'),
  (2, 'CR', 25000.00, 'REF10003');
EOF

# ----------------------------------------------------------------------------
# STEP 3: Main docker-compose.yml (Postgres + Kafka + Kafka Connect)
# Uses apache/kafka (NOT bitnami - bitnami images were pulled from public Hub)
# ----------------------------------------------------------------------------
echo ""
echo "[4/10] Writing main docker-compose.yml (Postgres/Kafka/Connect)..."
cat > "$PROJECT_DIR"/docker-compose.yml << 'EOF'
services:
  postgres:
    image: debezium/postgres:16
    container_name: postgres
    environment:
      POSTGRES_DB: banking_db
      POSTGRES_USER: data_eng
      POSTGRES_PASSWORD: mypassword
    volumes:
      - ./postgres/init.sql:/docker-entrypoint-initdb.d/init.sql
    command: ["postgres", "-c", "wal_level=logical"]
    networks:
      - cdc_net

  kafka:
    image: apache/kafka:3.7.1
    container_name: kafka
    ports:
      - "9092:9092"
      - "9093:9093"
    environment:
      - KAFKA_NODE_ID=1
      - KAFKA_PROCESS_ROLES=broker,controller
      - KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092
      - KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      - KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093
      - KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
      - KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1
      - KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1
    networks:
      - cdc_net

  kafka-connect:
    image: debezium/connect:2.6
    container_name: kafka-connect
    depends_on:
      - kafka
      - postgres
    ports:
      - "8083:8083"
    environment:
      - BOOTSTRAP_SERVERS=kafka:9092
      - GROUP_ID=banking_connect_cluster
      - CONFIG_STORAGE_TOPIC=connect_configs
      - OFFSET_STORAGE_TOPIC=connect_offsets
      - STATUS_STORAGE_TOPIC=connect_statuses
      - CONFIG_STORAGE_REPLICATION_FACTOR=1
      - OFFSET_STORAGE_REPLICATION_FACTOR=1
      - STATUS_STORAGE_REPLICATION_FACTOR=1
      - CONNECT_PLUGIN_PATH=/kafka/connect
    volumes:
      - ./connect-plugins/s3-sink:/kafka/connect/s3-sink
    networks:
      - cdc_net

networks:
  cdc_net:
    driver: bridge
EOF

# ----------------------------------------------------------------------------
# STEP 4: Stop any native postgres that would conflict on port 5432
# ----------------------------------------------------------------------------
echo ""
echo "[5/10] Disabling native postgresql service if present (port 5432 conflict)..."
sudo systemctl stop postgresql 2>/dev/null || true
sudo systemctl disable postgresql 2>/dev/null || true

# ----------------------------------------------------------------------------
# STEP 5: Download S3 Sink connector plugin
# ----------------------------------------------------------------------------
echo ""
echo "[6/10] Downloading Confluent S3 Sink connector plugin..."
sudo chown -R "$USER":"$USER" "$PROJECT_DIR"/connect-plugins
cd "$PROJECT_DIR"/connect-plugins/s3-sink

if [ ! -f kafka-connect-s3-installed.marker ]; then
    curl -sL -o s3-sink.zip \
      "https://api.hub.confluent.io/api/plugins/confluentinc/kafka-connect-s3/versions/10.5.7/archive"
    unzip -q s3-sink.zip
    mv confluentinc-kafka-connect-s3-*/lib/* .
    rm -rf confluentinc-kafka-connect-s3-* s3-sink.zip
    touch kafka-connect-s3-installed.marker
fi

# ----------------------------------------------------------------------------
# STEP 6: Start the Kafka/Postgres/Connect stack
# ----------------------------------------------------------------------------
echo ""
echo "[7/10] Starting Postgres + Kafka + Kafka Connect containers..."
cd "$PROJECT_DIR"

# Use sg docker to run with the new group membership in this same session
sg docker -c "docker compose up -d"

echo "Waiting for Kafka Connect REST API to become available..."
for i in $(seq 1 30); do
    if curl -s localhost:8083/ > /dev/null 2>&1; then
        echo "Kafka Connect is up."
        break
    fi
    sleep 5
done
sleep 10  # extra buffer for plugin scan

# ----------------------------------------------------------------------------
# STEP 7: Register Debezium source connector
#   FIX APPLIED: decimal.handling.mode=double (avoids base64-encoded amounts)
# ----------------------------------------------------------------------------
echo ""
echo "[8/10] Registering Debezium PostgreSQL source connector..."
cat > "$PROJECT_DIR"/debezium-source.json << 'EOF'
{
  "name": "banking-postgres-source",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "data_eng",
    "database.password": "mypassword",
    "database.dbname": "banking_db",
    "topic.prefix": "banking",
    "plugin.name": "pgoutput",
    "table.include.list": "public.branches,public.customers,public.accounts,public.transactions",
    "publication.autocreate.mode": "filtered",
    "tombstones.on.delete": "false",
    "decimal.handling.mode": "double"
  }
}
EOF

curl -s -X POST -H "Content-Type: application/json" \
  --data @"$PROJECT_DIR"/debezium-source.json \
  http://localhost:8083/connectors > /dev/null

sleep 5

# ----------------------------------------------------------------------------
# STEP 8: Register S3 Sink connector
#   FIX APPLIED: flush.size=2 + rotate.interval.ms=10000 so files land in S3
#   quickly during testing instead of waiting for large batches.
# ----------------------------------------------------------------------------
echo ""
echo "[9/10] Registering S3 Sink connector..."
cat > "$PROJECT_DIR"/s3-sink.json << EOF
{
  "name": "banking-s3-sink",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "1",
    "topics.regex": "banking\\\\.public\\\\..*",
    "s3.bucket.name": "${S3_BUCKET}",
    "s3.region": "${AWS_REGION}",
    "topics.dir": "raw_banking_data",
    "flush.size": "2",
    "rotate.interval.ms": "10000",
    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
    "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
    "schema.compatibility": "NONE",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter"
  }
}
EOF

curl -s -X POST -H "Content-Type: application/json" \
  --data @"$PROJECT_DIR"/s3-sink.json \
  http://localhost:8083/connectors > /dev/null

sleep 5

echo "Connector status:"
curl -s localhost:8083/connectors/banking-postgres-source/status | python3 -m json.tool || true
curl -s localhost:8083/connectors/banking-s3-sink/status | python3 -m json.tool || true

# ----------------------------------------------------------------------------
# STEP 9: Airflow setup
#   FIXES APPLIED:
#   - LocalExecutor (no Redis/Worker/Triggerer/Flower -> much lighter on RAM)
#   - Custom Dockerfile with PINNED versions and --no-deps on the airflow
#     provider (avoids pip's catastrophic dependency backtracking)
#   - dbt schema macro so marts models land in the MARTS schema, not STAGING
# ----------------------------------------------------------------------------
echo ""
echo "[10/10] Setting up Airflow (LocalExecutor, custom image)..."
cd "$PROJECT_DIR"/airflow_setup

if [ ! -f docker-compose.yaml ]; then
    curl -sLfO 'https://airflow.apache.org/docs/apache-airflow/2.9.3/docker-compose.yaml'
fi

echo "AIRFLOW_UID=$(id -u)" > .env

# --- Custom Dockerfile: pinned versions avoid pip backtracking hell ---
cat > Dockerfile << 'EOF'
FROM apache/airflow:2.9.3

RUN pip install --no-cache-dir dbt-snowflake==1.8.0 && \
    pip install --no-cache-dir --no-deps apache-airflow-providers-snowflake==5.4.0 && \
    pip install --no-cache-dir snowflake-sqlalchemy==1.6.1
EOF

# --- Patch docker-compose.yaml: switch to LocalExecutor, build instead of pull ---
python3 << 'PYEOF'
import re

with open("docker-compose.yaml") as f:
    content = f.read()

# Use the locally built image instead of the public one
content = content.replace(
    "  image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.9.3}",
    "  # image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.9.3}\n  build: ."
)

# Switch executor
content = content.replace(
    "AIRFLOW__CORE__EXECUTOR: CeleryExecutor",
    "AIRFLOW__CORE__EXECUTOR: LocalExecutor"
)

# Comment out Celery-only settings (not needed/valid under LocalExecutor)
content = re.sub(
    r"^(\s*AIRFLOW__CELERY__RESULT_BACKEND:.*)$",
    r"# \1",
    content,
    flags=re.MULTILINE,
)
content = re.sub(
    r"^(\s*AIRFLOW__CELERY__BROKER_URL:.*)$",
    r"# \1",
    content,
    flags=re.MULTILINE,
)

# Remove redis dependency from the common depends_on anchor block
content = content.replace(
    "  depends_on:\n    &airflow-common-depends-on\n    redis:\n      condition: service_healthy\n    postgres:\n      condition: service_healthy",
    "  depends_on:\n    &airflow-common-depends-on\n    postgres:\n      condition: service_healthy"
)

# Drop the redis, airflow-worker, airflow-triggerer, flower service blocks entirely
service_block_pattern = re.compile(
    r"\n  (redis|airflow-worker|airflow-triggerer|flower):\n(?:    .*\n|\n)*?(?=\n  [a-z][a-zA-Z0-9_-]*:|\Z)"
)
content = service_block_pattern.sub("\n", content)

with open("docker-compose.yaml", "w") as f:
    f.write(content)

print("docker-compose.yaml patched for LocalExecutor.")
PYEOF

# Sanity-check the YAML before doing anything else
if ! docker compose config > /dev/null 2>&1; then
    echo "ERROR: docker-compose.yaml is invalid after patching. Inspect manually:"
    docker compose config
    exit 1
fi
echo "docker-compose.yaml validated OK."

# --- dbt project files ---
cat > dbt_project/dbt_project.yml << 'EOF'
name: 'banking_dbt'
version: '1.0.0'
config-version: 2

profile: 'banking_dbt'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  banking_dbt:
    staging:
      +materialized: view
    marts:
      +materialized: table
      +schema: marts
EOF

# FIX APPLIED: custom schema macro so +schema: marts means the MARTS schema
# exactly, not a STAGING_marts concatenation (dbt's default behavior).
cat > dbt_project/macros/generate_schema_name.sql << 'EOF'
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
EOF

mkdir -p dbt_project/models/staging dbt_project/models/marts

cat > dbt_project/models/staging/src_banking.yml << 'EOF'
version: 2

sources:
  - name: banking_raw
    database: BANKING_DWH
    schema: RAW
    tables:
      - name: raw_transactions
EOF

# FIXES APPLIED:
# - path goes through payload.after (S3 JSON keeps the full Debezium envelope
#   because the source-side converter still writes schema+payload)
# - TRY_CAST on amount (legacy pre-fix records may be base64-encoded decimals)
# - txn_timestamp divided by 1,000,000 (Debezium MicroTimestamp = microseconds,
#   not milliseconds)
cat > dbt_project/models/staging/stg_transactions.sql << 'EOF'
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
EOF

cat > dbt_project/models/marts/fct_transactions.sql << 'EOF'
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
EOF

# --- profiles.yml: PLACEHOLDER credentials, fill in manually ---
if [ ! -f dbt_profiles/profiles.yml ]; then
cat > dbt_profiles/profiles.yml << 'EOF'
banking_dbt:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "<YOUR_SNOWFLAKE_ACCOUNT_LOCATOR>"
      user: "<YOUR_SNOWFLAKE_USERNAME>"
      password: "<YOUR_SNOWFLAKE_PASSWORD>"
      role: ACCOUNTADMIN
      database: BANKING_DWH
      warehouse: COMPUTE_WH
      schema: STAGING
      threads: 4
      client_session_keep_alive: False
EOF
    echo ""
    echo "  >>> IMPORTANT: edit $PROJECT_DIR/airflow_setup/dbt_profiles/profiles.yml"
    echo "  >>> and fill in your real Snowflake account / user / password before"
    echo "  >>> running any dbt-related Airflow tasks."
    echo ""
fi

# --- Add dbt volume mounts to docker-compose.yaml if not already present ---
if ! grep -q "dbt_project:/opt/airflow/dbt_project" docker-compose.yaml; then
    python3 << 'PYEOF'
with open("docker-compose.yaml") as f:
    content = f.read()

old = "    - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins\n"
new = old + (
    "    - ${AIRFLOW_PROJ_DIR:-.}/dbt_project:/opt/airflow/dbt_project\n"
    "    - ${AIRFLOW_PROJ_DIR:-.}/dbt_profiles:/opt/airflow/dbt_profiles\n"
)
content = content.replace(old, new, 1)

with open("docker-compose.yaml", "w") as f:
    f.write(content)
PYEOF
fi

# --- Airflow DAG: COPY INTO (with PATTERN fix) -> dbt staging -> dbt marts ---
cat > dags/banking_pipeline.py << 'EOF'
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

    # FIX APPLIED: PATTERN clause so only transactions/*.json files load into
    # this RAW table (without it, ALL tables' files get pulled into one table
    # if the stage URL is the top-level prefix).
    load_raw = SnowflakeOperator(
        task_id='copy_into_raw',
        snowflake_conn_id='snowflake_conn',
        sql="""
        COPY INTO BANKING_DWH.RAW.raw_transactions (raw_data)
        FROM @BANKING_DWH.RAW.raw_stage
        PATTERN = '.*banking\\\\.public\\\\.transactions/.*\\\\.json'
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

    load_raw >> dbt_staging >> dbt_marts
EOF

# --- Build and start Airflow ---
echo "Building custom Airflow image (pinned deps, avoids pip backtracking)..."
sg docker -c "docker compose build"

echo "Running airflow-init (one-time DB migration + admin user)..."
sg docker -c "docker compose up airflow-init"

echo "Starting Airflow webserver + scheduler..."
sg docker -c "docker compose up -d"

echo ""
echo "=================================================="
echo " SETUP COMPLETE"
echo "=================================================="
echo ""
echo "Next manual steps required (cannot be automated safely):"
echo ""
echo "1. SNOWFLAKE SIDE (run once in a Snowflake worksheet):"
echo "   - CREATE DATABASE/SCHEMA BANKING_DWH.RAW/STAGING/MARTS"
echo "   - CREATE TABLE BANKING_DWH.RAW.raw_transactions (raw_data VARIANT, _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())"
echo "   - CREATE STORAGE INTEGRATION + external stage 'raw_stage' pointing at"
echo "     s3://${S3_BUCKET}/raw_banking_data/"
echo "   - Update the IAM role trust policy with the integration's"
echo "     STORAGE_AWS_IAM_USER_ARN / STORAGE_AWS_EXTERNAL_ID (DESC STORAGE INTEGRATION)"
echo "   - NOTE: if your Snowflake account is in a newer AWS region (e.g."
echo "     ap-southeast-7), make sure that region is ENABLED in your AWS"
echo "     account (aws account enable-region --region-name <region>) or"
echo "     cross-region AssumeRole calls will fail with AccessDenied."
echo ""
echo "2. Edit dbt_profiles/profiles.yml with your real Snowflake credentials:"
echo "   $PROJECT_DIR/airflow_setup/dbt_profiles/profiles.yml"
echo ""
echo "3. In the Airflow UI (http://<EC2_PUBLIC_IP>:8080, login airflow/airflow):"
echo "   Admin -> Connections -> add 'snowflake_conn' (Snowflake type) with"
echo "   your account/user/password/warehouse/database/role."
echo ""
echo "4. Trigger the 'banking_s3_to_marts' DAG manually once everything above"
echo "   is in place."
echo ""
echo "Containers running:"
docker compose -f "$PROJECT_DIR"/docker-compose.yml ps 2>/dev/null || true
docker compose -f "$PROJECT_DIR"/airflow_setup/docker-compose.yaml ps 2>/dev/null || true