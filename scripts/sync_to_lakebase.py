"""Sync flagged transactions from Gold layer to Lakebase."""
import subprocess
import json

PROFILE = "fe-vm-infa-migrate"
WH = "137319fcf822f914"
CATALOG = "infa_migrate_catalog"
LAKEBASE_HOST = "ep-wild-bar-d2yqbtiy.database.us-east-1.cloud.databricks.com"
LAKEBASE_DB = "databricks_postgres"
PSQL = "/opt/homebrew/opt/postgresql@16/bin/psql"


def get_lakebase_creds():
    result = subprocess.run([
        "databricks", "--profile", PROFILE, "database", "generate-database-credential",
        "--json", '{"instance_names": ["fraud-triage"]}', "-o", "json"
    ], capture_output=True, text=True)
    token = json.loads(result.stdout)["token"]

    result = subprocess.run([
        "databricks", "--profile", PROFILE, "current-user", "me", "-o", "json"
    ], capture_output=True, text=True)
    email = json.loads(result.stdout)["userName"]
    return token, email


def run_sql(statement):
    payload = json.dumps({
        "statement": statement, "warehouse_id": WH,
        "format": "JSON_ARRAY", "wait_timeout": "50s"
    })
    result = subprocess.run([
        "databricks", "--profile", PROFILE, "api", "post",
        "/api/2.0/sql/statements/", "--json", payload
    ], capture_output=True, text=True, timeout=120)
    resp = json.loads(result.stdout)
    if resp["status"]["state"] != "SUCCEEDED":
        print(f"  SQL FAILED: {resp['status'].get('error', {}).get('message', '')[:150]}")
        return None, None
    columns = [c["name"] for c in resp["manifest"]["schema"]["columns"]]
    rows = resp["result"].get("data_array", [])
    return columns, rows


def psql_exec(token, email, sql):
    import os
    env = os.environ.copy()
    env["PGPASSWORD"] = token
    result = subprocess.run([
        PSQL, f"host={LAKEBASE_HOST} port=5432 dbname={LAKEBASE_DB} user={email} sslmode=require",
        "-c", sql
    ], capture_output=True, text=True, env=env, timeout=30)
    if result.returncode != 0:
        print(f"  psql error: {result.stderr[:200]}")
    return result.stdout


def main():
    print("1. Fetching flagged transactions from Gold layer...")
    cols, rows = run_sql(f"""
        SELECT
            transaction_id, user_id, CAST(amount AS DOUBLE) as amount, 'USD' as currency,
            transaction_type, merchant_category, tx_channel,
            CAST(risk_score AS INT) as risk_score, risk_level, automated_action,
            CASE WHEN geo_mismatch THEN '{{"geo_mismatch": true}}' ELSE '{{}}' END as triggered_factors,
            '' as explanation,
            tx_city, tx_country,
            COALESCE(login_city, '') as login_city, COALESCE(login_country, '') as login_country,
            CAST(COALESCE(geo_mismatch, false) AS BOOLEAN) as geo_mismatch,
            CAST(COALESCE(mfa_change_flag, false) AS BOOLEAN) as mfa_changed,
            CAST(COALESCE(is_bot_typing, false) AS BOOLEAN) as bot_detected
        FROM {CATALOG}.fraud_gold.gold_user_session_risk
        WHERE automated_action IN ('YELLOW_FLAG', 'BLOCK')
    """)

    if not rows:
        print("  No flagged transactions found.")
        return

    print(f"  Found {len(rows)} flagged transactions.")

    print("\n2. Getting Lakebase credentials...")
    token, email = get_lakebase_creds()

    print("\n3. Inserting into Lakebase...")
    # Build batch insert
    batch_size = 50
    inserted = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        values = []
        for r in batch:
            # Escape single quotes
            vals = [v.replace("'", "''") if v else '' for v in r]
            geo = 'true' if vals[16] == 'true' else 'false'
            mfa = 'true' if vals[17] == 'true' else 'false'
            bot = 'true' if vals[18] == 'true' else 'false'
            values.append(
                f"('{vals[0]}','{vals[1]}',{vals[2]},'{vals[3]}','{vals[4]}','{vals[5]}','{vals[6]}',"
                f"{vals[7]},'{vals[8]}','{vals[9]}','{vals[10]}','{vals[11]}',"
                f"'{vals[12]}','{vals[13]}','{vals[14]}','{vals[15]}',"
                f"{geo},{mfa},{bot})"
            )

        sql = f"""
        INSERT INTO real_time_fraud_triage (
            transaction_id, user_id, amount, currency, transaction_type,
            merchant_name, channel, risk_score, risk_level, automated_action,
            triggered_factors, explanation, tx_city, tx_country, login_city, login_country,
            geo_mismatch, mfa_changed, bot_detected
        ) VALUES {','.join(values)}
        ON CONFLICT (transaction_id) DO UPDATE SET
            risk_score = EXCLUDED.risk_score,
            risk_level = EXCLUDED.risk_level,
            automated_action = EXCLUDED.automated_action,
            updated_at = NOW();
        """

        result = psql_exec(token, email, sql)
        inserted += len(batch)
        print(f"  Batch {i//batch_size + 1}: inserted {len(batch)} rows (total: {inserted})")

    print(f"\n4. Verifying Lakebase data...")
    result = psql_exec(token, email,
        "SELECT automated_action, COUNT(*) as cnt, ROUND(AVG(risk_score)) as avg_score "
        "FROM real_time_fraud_triage GROUP BY 1 ORDER BY 1;")
    print(result)

    result = psql_exec(token, email, "SELECT COUNT(*) FROM fraud_queue;")
    print(f"  Fraud queue size: {result.strip().split(chr(10))[-2].strip()}")


if __name__ == "__main__":
    main()
