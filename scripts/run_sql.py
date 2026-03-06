"""Helper to run SQL statements against Databricks."""
import subprocess
import json
import sys

PROFILE = "fe-vm-infa-migrate"
WH = "137319fcf822f914"
CATALOG = "infa_migrate_catalog"

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
        print(f"  FAILED: {err[:150]}")
        return None
    if state == "SUCCEEDED":
        return resp.get("result", {}).get("data_array", [])
    print(f"  State: {state}")
    return None

def main():
    statements = sys.argv[1:] if len(sys.argv) > 1 else []
    if not statements:
        # Run the full setup
        setup_statements = [
            f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.fraud_bronze",
            f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.fraud_silver",
            f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.fraud_gold",
            f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.fraud_models",
            f"CREATE VOLUME IF NOT EXISTS {CATALOG}.fraud_bronze.raw_data",
        ]
        for stmt in setup_statements:
            print(f"Running: {stmt[:80]}...")
            run_sql(stmt)
            print("  OK")
    else:
        for stmt in statements:
            print(f"Running: {stmt[:80]}...")
            result = run_sql(stmt)
            if result:
                for row in result[:10]:
                    print(f"  {row}")

if __name__ == "__main__":
    main()
