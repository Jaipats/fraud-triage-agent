"""Generate AI reasoning explanations for all flagged transactions in Lakebase."""
import subprocess
import json
import os
from datetime import datetime

PROFILE = "fe-vm-infa-migrate"
WH = "137319fcf822f914"
CATALOG = "infa_migrate_catalog"
LAKEBASE_HOST = "ep-wild-bar-d2yqbtiy.database.us-east-1.cloud.databricks.com"
LAKEBASE_DB = "databricks_postgres"
PSQL = "/opt/homebrew/opt/postgresql@16/bin/psql"

RISK_FACTORS = {
    "high_amount": {
        "condition": lambda r: float(r.get("amount", 0)) > 10000,
        "weight": 25,
        "explanation": lambda r: f"Transaction amount (${float(r['amount']):,.2f}) exceeds the $10,000 high-value threshold"
    },
    "high_risk_merchant": {
        "condition": lambda r: r.get("merchant_category", "") in ("wire", "crypto", "gambling", "p2p"),
        "weight": 15,
        "explanation": lambda r: f"Merchant category '{r['merchant_category']}' is classified as high-risk"
    },
    "geo_mismatch": {
        "condition": lambda r: r.get("geo_mismatch", "false") == "true",
        "weight": 20,
        "explanation": lambda r: f"Transaction location ({r.get('tx_city','?')}, {r.get('tx_country','?')}) differs from login location ({r.get('login_city','?')}, {r.get('login_country','?')})"
    },
    "mfa_change_then_wire": {
        "condition": lambda r: r.get("mfa_change_flag", "false") == "true" and r.get("transaction_type", "") == "wire_transfer",
        "weight": 30,
        "explanation": lambda r: "MFA settings were modified prior to initiating a wire transfer - common account takeover pattern"
    },
    "bot_typing_detected": {
        "condition": lambda r: r.get("typing_speed_ms") and float(r.get("typing_speed_ms", 999)) < 50,
        "weight": 25,
        "explanation": lambda r: f"Typing cadence ({float(r['typing_speed_ms']):.0f}ms avg) suggests automated/bot input rather than human interaction"
    },
    "rapid_login_failures": {
        "condition": lambda r: r.get("failed_attempts") and int(r.get("failed_attempts", 0)) >= 3,
        "weight": 10,
        "explanation": lambda r: f"{r['failed_attempts']} failed login attempts detected before successful authentication"
    },
    "off_hours_activity": {
        "condition": lambda r: r.get("hour_of_day") and (int(r.get("hour_of_day", 12)) < 6 or int(r.get("hour_of_day", 12)) > 22),
        "weight": 5,
        "explanation": lambda r: f"Transaction occurred during off-hours (hour {r['hour_of_day']}), outside normal banking patterns"
    },
    "international_wire": {
        "condition": lambda r: r.get("is_international", "false") == "true" and r.get("transaction_type", "") == "wire_transfer",
        "weight": 15,
        "explanation": lambda r: f"International wire transfer to {r.get('tx_country', '?')} flagged for enhanced due diligence"
    },
}


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
        print(f"  SQL FAILED: {resp['status'].get('error', {}).get('message', '')[:200]}")
        return None, None
    columns = [c["name"] for c in resp["manifest"]["schema"]["columns"]]
    rows = resp["result"].get("data_array", [])
    return columns, rows


def get_lakebase_creds():
    result = subprocess.run([
        "databricks", "--profile", PROFILE, "database", "generate-database-credential",
        "--json", '{"instance_names": ["fraud-triage"]}', "-o", "json"
    ], capture_output=True, text=True)
    token = json.loads(result.stdout)["token"]
    return token


def psql_exec(token, sql):
    env = os.environ.copy()
    env["PGPASSWORD"] = token
    result = subprocess.run([
        PSQL, f"host={LAKEBASE_HOST} port=5432 dbname={LAKEBASE_DB} user=jaideep.patel@databricks.com sslmode=require",
        "-c", sql
    ], capture_output=True, text=True, env=env, timeout=30)
    if result.returncode != 0:
        print(f"  psql error: {result.stderr[:200]}")
    return result.stdout


def generate_explanation(record):
    tx_id = record.get("transaction_id", "N/A")
    user_id = record.get("user_id", "N/A")
    amount = float(record.get("amount", 0))
    tx_type = record.get("transaction_type", "N/A")
    risk_score = int(record.get("risk_score", 0))
    risk_level = record.get("risk_level", "N/A").upper()
    action = record.get("automated_action", "N/A")

    triggered = []
    explanations = []
    for name, factor in RISK_FACTORS.items():
        try:
            if factor["condition"](record):
                triggered.append(name)
                explanations.append(factor["explanation"](record))
        except (KeyError, TypeError, ValueError):
            continue

    lines = [
        f"FRAUD RISK ASSESSMENT - {tx_id}",
        f"{'='*55}",
        f"User: {user_id} | Type: {tx_type} | Amount: ${amount:,.2f}",
        f"Risk Score: {risk_score}/100 | Level: {risk_level} | Action: {action}",
        f"",
        f"RISK FACTORS DETECTED ({len(triggered)}):",
    ]

    for i, exp in enumerate(explanations, 1):
        lines.append(f"  {i}. {exp}")

    if not explanations:
        lines.append("  No specific risk factors matched - flagged by aggregate scoring.")

    # Add contextual analysis
    lines.append("")
    lines.append("ANALYSIS:")
    if "geo_mismatch" in triggered and "mfa_change_then_wire" in triggered:
        lines.append("  CRITICAL: Geographic anomaly combined with MFA modification")
        lines.append("  strongly suggests an Account Takeover (ATO) attack.")
        lines.append(f"  The user appears to have logged in from {record.get('login_city', '?')}")
        lines.append(f"  but initiated a wire transfer from {record.get('tx_city', '?')}.")
    elif "bot_typing_detected" in triggered:
        lines.append("  WARNING: Automated input patterns detected. This may indicate")
        lines.append("  credential stuffing or bot-driven fraud using stolen credentials.")
    elif "high_amount" in triggered and "high_risk_merchant" in triggered:
        lines.append(f"  High-value transaction (${amount:,.2f}) through a high-risk")
        lines.append(f"  merchant channel requires enhanced scrutiny per BSA/AML policy.")
    else:
        lines.append("  Multiple low-to-medium risk indicators combined to exceed")
        lines.append("  the flagging threshold. Individual factors may be benign but")
        lines.append("  the combination warrants review.")

    lines.extend([
        "",
        "RECOMMENDED ACTIONS:",
    ])
    if action == "BLOCK":
        lines.append("  1. Verify customer identity via out-of-band contact")
        lines.append("  2. Freeze account pending investigation")
        lines.append("  3. File SAR if confirmed fraudulent")
    else:
        lines.append("  1. Review transaction details and customer history")
        lines.append("  2. Contact customer if patterns persist")
        lines.append("  3. Release if verified legitimate")

    lines.extend([
        "",
        "REGULATORY COMPLIANCE:",
        "This assessment was generated by an automated fraud detection system.",
        "Under GDPR Article 22 and CCPA Section 1798.185, the customer has the",
        "right to request human review of this automated decision.",
        "",
        f"Analyzed at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
    ])

    return "\n".join(lines), json.dumps(triggered)


def main():
    print("1. Fetching transaction metadata from Gold layer...")
    cols, rows = run_sql(f"""
        SELECT
            transaction_id, user_id, CAST(amount AS DOUBLE) as amount,
            transaction_type, merchant_category, tx_channel,
            CAST(risk_score AS INT) as risk_score, risk_level, automated_action,
            tx_city, tx_country,
            COALESCE(login_city, '') as login_city, COALESCE(login_country, '') as login_country,
            CAST(COALESCE(geo_mismatch, false) AS STRING) as geo_mismatch,
            CAST(COALESCE(mfa_change_flag, false) AS STRING) as mfa_change_flag,
            CAST(COALESCE(is_bot_typing, false) AS STRING) as bot_typing,
            COALESCE(CAST(typing_speed_ms AS STRING), '') as typing_speed_ms,
            CAST(COALESCE(is_rapid_fail, false) AS STRING) as failed_attempts,
            CAST(COALESCE(is_off_hours, false) AS STRING) as is_off_hours,
            CAST(COALESCE(mfa_then_wire, false) AS STRING) as mfa_then_wire,
            CAST(COALESCE(is_high_risk_merchant, false) AS STRING) as is_high_risk_merchant,
            CAST(tx_country != COALESCE(login_country, tx_country) AS STRING) as is_international
        FROM {CATALOG}.fraud_gold.gold_user_session_risk
        WHERE automated_action IN ('YELLOW_FLAG', 'BLOCK')
    """)

    if not rows:
        print("  No data found.")
        return

    print(f"  Found {len(rows)} transactions. Generating explanations...")

    # Build dict records
    records = [dict(zip(cols, row)) for row in rows]

    print("2. Getting Lakebase credentials...")
    token = get_lakebase_creds()

    print("3. Updating explanations in Lakebase...")
    batch_size = 25
    updated = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        updates = []
        for rec in batch:
            explanation, factors = generate_explanation(rec)
            # Escape single quotes for SQL
            explanation_escaped = explanation.replace("'", "''")
            factors_escaped = factors.replace("'", "''")
            updates.append(
                f"UPDATE real_time_fraud_triage SET "
                f"explanation = '{explanation_escaped}', "
                f"triggered_factors = '{factors_escaped}' "
                f"WHERE transaction_id = '{rec['transaction_id']}';"
            )

        sql = "\n".join(updates)
        psql_exec(token, sql)
        updated += len(batch)
        print(f"  Batch {i//batch_size + 1}: updated {len(batch)} (total: {updated})")

    print(f"\n4. Verifying...")
    result = psql_exec(token,
        "SELECT transaction_id, LEFT(explanation, 60) as explanation_preview "
        "FROM real_time_fraud_triage WHERE explanation != '' LIMIT 3;")
    print(result)

    result = psql_exec(token,
        "SELECT COUNT(*) as with_explanation FROM real_time_fraud_triage WHERE explanation != '' AND explanation IS NOT NULL;")
    print(f"  Transactions with explanations: {result.strip().split(chr(10))[-2].strip()}")


if __name__ == "__main__":
    main()
