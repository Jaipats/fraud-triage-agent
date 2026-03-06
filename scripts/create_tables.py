"""Create bronze tables from Volume CSVs using CREATE TABLE AS SELECT."""
import subprocess
import json

PROFILE = "fe-vm-infa-migrate"
WH = "137319fcf822f914"
CATALOG = "infa_migrate_catalog"
VOL = f"/Volumes/{CATALOG}/fraud_bronze/raw_data"

def run_sql(statement):
    payload = json.dumps({
        "statement": statement,
        "warehouse_id": WH,
        "format": "JSON_ARRAY",
        "wait_timeout": "50s"
    })
    result = subprocess.run(
        ["databricks", "--profile", PROFILE, "api", "post", "/api/2.0/sql/statements/", "--json", payload],
        capture_output=True, text=True, timeout=120
    )
    resp = json.loads(result.stdout)
    state = resp["status"]["state"]
    if state == "FAILED":
        err = resp["status"].get("error", {}).get("message", "Unknown error")
        print(f"  FAILED: {err[:200]}")
        return False
    print(f"  {state}")
    if state == "SUCCEEDED" and "result" in resp:
        data = resp["result"].get("data_array", [])
        if data:
            for row in data[:3]:
                print(f"    {row}")
    return state == "SUCCEEDED"

statements = [
    # Drop existing empty tables
    f"DROP TABLE IF EXISTS {CATALOG}.fraud_bronze.transactions",
    f"DROP TABLE IF EXISTS {CATALOG}.fraud_bronze.login_logs",
    f"DROP TABLE IF EXISTS {CATALOG}.fraud_bronze.known_fraud_signatures",

    # Create tables directly from CSV using read_files
    f"""CREATE TABLE {CATALOG}.fraud_bronze.transactions AS
    SELECT * FROM read_files('{VOL}/transactions.csv',
        format => 'csv', header => true, inferSchema => true)""",

    f"""CREATE TABLE {CATALOG}.fraud_bronze.login_logs AS
    SELECT * FROM read_files('{VOL}/login_logs.csv',
        format => 'csv', header => true, inferSchema => true)""",

    f"""CREATE TABLE {CATALOG}.fraud_bronze.known_fraud_signatures AS
    SELECT * FROM read_files('{VOL}/known_fraud_signatures.csv',
        format => 'csv', header => true, inferSchema => true)""",

    # Verify
    f"SELECT 'transactions' as tbl, COUNT(*) as cnt FROM {CATALOG}.fraud_bronze.transactions",
    f"SELECT 'login_logs' as tbl, COUNT(*) as cnt FROM {CATALOG}.fraud_bronze.login_logs",
    f"SELECT 'fraud_sigs' as tbl, COUNT(*) as cnt FROM {CATALOG}.fraud_bronze.known_fraud_signatures",
]

for i, stmt in enumerate(statements):
    label = stmt.strip().split('\n')[0][:80]
    print(f"\n[{i+1}/{len(statements)}] {label}")
    run_sql(stmt)
