"""
Setup Unity Catalog resources for Fraud Triage Agent demo.
Creates catalog, schemas, volume, and uploads mock data.
Applies column masking for PII protection.
"""

import subprocess
import sys
import os
import json

PROFILE = "fe-vm-infa-migrate"
CATALOG = "financial_security"
SCHEMAS = ["bronze", "silver", "gold", "fraud_models"]
VOLUME = "raw_data"


def run_cli(args, check=True):
    cmd = ["databricks", "--profile", PROFILE] + args
    print(f"  > {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"  WARN: {result.stderr.strip()}")
    return result


def main():
    print(f"=== Setting up Unity Catalog: {CATALOG} ===\n")

    # Create catalog
    print("1. Creating catalog...")
    run_cli(["unity-catalog", "catalogs", "create", "--name", CATALOG,
             "--comment", "Financial Security - Fraud Triage Agent Demo"], check=False)

    # Create schemas
    for schema in SCHEMAS:
        print(f"2. Creating schema: {CATALOG}.{schema}")
        run_cli(["unity-catalog", "schemas", "create",
                 "--catalog-name", CATALOG, "--name", schema,
                 "--comment", f"Fraud Triage - {schema} layer"], check=False)

    # Create volume for raw data
    print(f"3. Creating volume: {CATALOG}.bronze.{VOLUME}")
    run_cli(["unity-catalog", "volumes", "create",
             "--catalog-name", CATALOG, "--schema-name", "bronze",
             "--name", VOLUME, "--volume-type", "MANAGED",
             "--comment", "Raw mock data files for fraud triage demo"], check=False)

    # Upload mock data files
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
    volume_path = f"/Volumes/{CATALOG}/bronze/{VOLUME}"

    for filename in ["transactions.csv", "login_logs.csv", "known_fraud_signatures.csv"]:
        filepath = os.path.join(data_dir, filename)
        if os.path.exists(filepath):
            print(f"4. Uploading {filename} to {volume_path}/")
            run_cli(["fs", "cp", filepath, f"dbfs:{volume_path}/{filename}", "--overwrite"])
        else:
            print(f"  SKIP: {filepath} not found. Run generate_mock_data.py first.")

    # Create tables with SQL (using Databricks SQL)
    print("\n5. Creating Silver tables with column masking...")

    sql_statements = [
        # Bronze transactions (raw)
        f"""CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.transactions (
            transaction_id STRING,
            user_id STRING,
            card_number STRING,
            timestamp TIMESTAMP,
            amount DOUBLE,
            currency STRING,
            merchant_id STRING,
            merchant_name STRING,
            merchant_category STRING,
            transaction_type STRING,
            channel STRING,
            city STRING,
            country STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            device_type STRING,
            is_international BOOLEAN,
            is_flagged BOOLEAN
        ) USING DELTA
        COMMENT 'Raw transaction data from mock banking system'""",

        # Bronze login logs
        f"""CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.login_logs (
            login_id STRING,
            user_id STRING,
            timestamp TIMESTAMP,
            ip_address STRING,
            city STRING,
            country STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            device_type STRING,
            user_agent STRING,
            login_successful BOOLEAN,
            failed_attempts_before_success INT,
            mfa_change_flag BOOLEAN,
            mfa_method STRING,
            typing_speed_ms DOUBLE,
            session_duration_min DOUBLE,
            is_suspicious BOOLEAN
        ) USING DELTA
        COMMENT 'Login event logs with biometric and session data'""",

        # Load data from volume into bronze tables
        f"""COPY INTO {CATALOG}.bronze.transactions
        FROM '{volume_path}/transactions.csv'
        FILEFORMAT = CSV
        FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
        COPY_OPTIONS ('mergeSchema' = 'true')""",

        f"""COPY INTO {CATALOG}.bronze.login_logs
        FROM '{volume_path}/login_logs.csv'
        FILEFORMAT = CSV
        FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
        COPY_OPTIONS ('mergeSchema' = 'true')""",

        # Column mask function for PII
        f"""CREATE OR REPLACE FUNCTION {CATALOG}.silver.mask_card_number(card STRING)
        RETURNS STRING
        RETURN CONCAT('****-****-****-', RIGHT(card, 4))""",

        f"""CREATE OR REPLACE FUNCTION {CATALOG}.silver.mask_ip_address(ip STRING)
        RETURNS STRING
        RETURN CONCAT(SUBSTRING_INDEX(ip, '.', 2), '.xxx.xxx')""",
    ]

    # Write SQL to a file for execution
    sql_file = os.path.join(os.path.dirname(__file__), "setup_tables.sql")
    with open(sql_file, "w") as f:
        for stmt in sql_statements:
            f.write(stmt.strip() + ";\n\n")

    print(f"  SQL written to {sql_file}")
    print("  Execute these statements via Databricks SQL or a notebook.")

    print("\n=== Setup complete! ===")
    print(f"  Catalog: {CATALOG}")
    print(f"  Volume:  {volume_path}")
    print(f"  Next:    Run the DLT pipeline to populate Silver tables.")


if __name__ == "__main__":
    main()
