# Databricks notebook source
# MAGIC %md
# MAGIC # Fraud Triage Agent - One-Click Deployment
# MAGIC
# MAGIC This notebook deploys the **entire** Fraud Triage Agent stack:
# MAGIC 1. Unity Catalog (catalog, schemas, volume)
# MAGIC 2. Mock data upload to Volume
# MAGIC 3. Bronze tables from CSV
# MAGIC 4. Lakeflow DLT pipeline (Bronze → Silver → Gold)
# MAGIC 5. Lakebase (serverless Postgres) operational store
# MAGIC 6. Sync Gold risk scores → Lakebase
# MAGIC 7. Generate fraud explanations
# MAGIC 8. MLflow reasoning agent + serving endpoint
# MAGIC 9. Databricks App (Live Fraud Queue)
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Cluster with Unity Catalog access
# MAGIC - Lakebase enabled on the workspace
# MAGIC - The repo cloned into the workspace (via Repos or Git folder)
# MAGIC
# MAGIC ### Quick Deploy (from terminal)
# MAGIC ```bash
# MAGIC databricks repos create --url https://github.com/Jaipats/fraud-triage-agent --provider github --path /Repos/<your-user>/fraud-triage-agent
# MAGIC databricks jobs create --json '{"name":"Deploy Fraud Triage","tasks":[{"task_key":"deploy","notebook_task":{"notebook_path":"/Repos/<your-user>/fraud-triage-agent/notebooks/deploy_fraud_triage"},"existing_cluster_id":"<cluster-id>"}]}'
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Edit these values to match your workspace.

# COMMAND ----------

# --- EDIT THESE ---
CATALOG = "fraud_demo"                  # Unity Catalog name to create
LAKEBASE_INSTANCE = "fraud-triage"      # Lakebase instance name
APP_NAME = "live-fraud-queue"           # Databricks App name

# --- Auto-detected ---
import os
username = spark.sql("SELECT current_user()").first()[0]
REPO_ROOT = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
# If running from /Repos/<user>/fraud-triage-agent/notebooks, go up one level
if REPO_ROOT.endswith("/notebooks"):
    REPO_ROOT = REPO_ROOT.rsplit("/", 1)[0]

SCHEMAS = {"bronze": "Raw ingestion layer",
           "silver": "Cleaned and enriched",
           "gold": "Aggregated risk scores and KPIs",
           "fraud_models": "MLflow model registry"}
VOLUME_PATH = f"/Volumes/{CATALOG}/bronze/raw_data"

print(f"Catalog:    {CATALOG}")
print(f"User:       {username}")
print(f"Repo root:  {REPO_ROOT}")
print(f"Volume:     {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Unity Catalog Resources

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} COMMENT 'Fraud Triage Agent Demo'")
print(f"Catalog '{CATALOG}' ready.")

for schema, comment in SCHEMAS.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} COMMENT '{comment}'")
    print(f"  Schema: {CATALOG}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.raw_data COMMENT 'Mock CSV data'")
print(f"  Volume: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Upload Mock Data to Volume

# COMMAND ----------

import shutil

# Copy CSV files from the repo into the Unity Catalog Volume
data_files = ["transactions.csv", "login_logs.csv", "known_fraud_signatures.csv"]

for f in data_files:
    src = f"/Workspace{REPO_ROOT}/data/{f}"
    dst = f"{VOLUME_PATH}/{f}"
    try:
        dbutils.fs.cp(f"file:{src}", dst)
        print(f"  Uploaded: {f}")
    except Exception as e:
        # Try workspace path directly
        try:
            dbutils.fs.cp(src, dst)
            print(f"  Uploaded: {f}")
        except Exception as e2:
            print(f"  WARN: Could not upload {f}: {e2}")

# Verify
for f in data_files:
    try:
        count = spark.read.csv(f"{VOLUME_PATH}/{f}", header=True).count()
        print(f"  {f}: {count:,} rows")
    except Exception as e:
        print(f"  {f}: not found - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Bronze Tables from CSV

# COMMAND ----------

for table_name in ["transactions", "login_logs", "known_fraud_signatures"]:
    csv_path = f"{VOLUME_PATH}/{table_name}.csv"
    full_table = f"{CATALOG}.bronze.{table_name}"
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_table}")
        df = spark.read.csv(csv_path, header=True, inferSchema=True)
        df.write.format("delta").saveAsTable(full_table)
        count = spark.table(full_table).count()
        print(f"  {full_table}: {count:,} rows")
    except Exception as e:
        print(f"  WARN: {full_table}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Build Silver Layer (Cleaned + Enriched)

# COMMAND ----------

from pyspark.sql import functions as F

# --- Silver Transactions ---
silver_txn = (
    spark.table(f"{CATALOG}.bronze.transactions")
    .withColumn("timestamp", F.to_timestamp("timestamp"))
    .withColumn("card_number_masked",
                F.concat(F.lit("****-****-****-"), F.substring("card_number", -4, 4)))
    .withColumn("amount_bucket",
                F.when(F.col("amount") < 100, "low")
                 .when(F.col("amount") < 1000, "medium")
                 .when(F.col("amount") < 10000, "high")
                 .otherwise("very_high"))
    .withColumn("hour_of_day", F.hour("timestamp"))
    .withColumn("day_of_week", F.dayofweek("timestamp"))
    .withColumn("is_high_risk_merchant",
                F.col("merchant_category").isin("wire", "crypto", "gambling", "p2p"))
    .withColumn("is_off_hours", (F.col("hour_of_day") < 6) | (F.col("hour_of_day") > 22))
    .drop("card_number")
)

silver_txn.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.silver.transactions")
print(f"  silver.transactions: {silver_txn.count():,} rows")

# --- Silver Login Logs ---
silver_logins = (
    spark.table(f"{CATALOG}.bronze.login_logs")
    .withColumn("timestamp", F.to_timestamp("timestamp"))
    .withColumn("ip_masked",
                F.concat(
                    F.element_at(F.split("ip_address", "\\."), 1), F.lit("."),
                    F.element_at(F.split("ip_address", "\\."), 2), F.lit(".xxx.xxx")
                ))
    .withColumn("is_bot_typing", F.col("typing_speed_ms") < 50)
    .withColumn("is_rapid_fail", F.col("failed_attempts_before_success") >= 3)
    .withColumn("hour_of_day", F.hour("timestamp"))
    .drop("ip_address")
)

silver_logins.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.silver.login_logs")
print(f"  silver.login_logs: {silver_logins.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Build Gold Layer (Risk Scoring + KPIs)

# COMMAND ----------

# --- Gold: User Session Risk ---
txns = spark.table(f"{CATALOG}.silver.transactions").alias("t")
logins = spark.table(f"{CATALOG}.silver.login_logs").alias("l")

joined = (
    txns.join(logins,
        (F.col("t.user_id") == F.col("l.user_id")) &
        (F.col("t.timestamp").between(
            F.col("l.timestamp"),
            F.col("l.timestamp") + F.expr("INTERVAL 1 HOUR")
        )),
        "left"
    )
    .select(
        F.col("t.transaction_id"),
        F.col("t.user_id"),
        F.col("t.timestamp").alias("tx_timestamp"),
        F.col("t.amount"),
        F.col("t.transaction_type"),
        F.col("t.merchant_category"),
        F.col("t.merchant_name"),
        F.col("t.channel").alias("tx_channel"),
        F.col("t.city").alias("tx_city"),
        F.col("t.country").alias("tx_country"),
        F.col("t.is_high_risk_merchant"),
        F.col("t.is_off_hours"),
        F.col("t.amount_bucket"),
        F.col("l.login_id"),
        F.col("l.timestamp").alias("login_timestamp"),
        F.col("l.city").alias("login_city"),
        F.col("l.country").alias("login_country"),
        F.col("l.mfa_change_flag"),
        F.col("l.is_bot_typing"),
        F.col("l.is_rapid_fail"),
        F.col("l.typing_speed_ms"),
        F.col("l.device_type").alias("login_device"),
        F.col("t.is_flagged"),
    )
)

gold_risk = (
    joined
    .withColumn("geo_mismatch", F.col("tx_city") != F.col("login_city"))
    .withColumn("mfa_then_wire",
        (F.col("mfa_change_flag") == True) & (F.col("transaction_type") == "wire_transfer"))
    .withColumn("risk_score", (
        F.when(F.col("amount") > 10000, 25).otherwise(0) +
        F.when(F.col("is_high_risk_merchant") == True, 15).otherwise(0) +
        F.when(F.col("geo_mismatch") == True, 20).otherwise(0) +
        F.when(F.col("mfa_then_wire") == True, 30).otherwise(0) +
        F.when(F.col("is_bot_typing") == True, 25).otherwise(0) +
        F.when(F.col("is_rapid_fail") == True, 10).otherwise(0) +
        F.when(F.col("is_off_hours") == True, 5).otherwise(0) +
        F.when(F.col("is_flagged") == True, 20).otherwise(0)
    ))
    .withColumn("risk_level",
        F.when(F.col("risk_score") >= 70, "critical")
         .when(F.col("risk_score") >= 40, "high")
         .when(F.col("risk_score") >= 20, "medium")
         .otherwise("low"))
    .withColumn("automated_action",
        F.when(F.col("risk_score") >= 70, "BLOCK")
         .when(F.col("risk_score") >= 40, "YELLOW_FLAG")
         .when(F.col("risk_score") >= 20, "MONITOR")
         .otherwise("ALLOW"))
)

gold_risk.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.gold.user_session_risk")
total = gold_risk.count()
flagged = gold_risk.filter(F.col("automated_action").isin("YELLOW_FLAG", "BLOCK")).count()
print(f"  gold.user_session_risk: {total:,} rows ({flagged:,} flagged)")

# COMMAND ----------

# --- Gold: Fraud KPIs ---
gold_kpis = (
    gold_risk.groupBy(
        F.date_trunc("hour", "tx_timestamp").alias("hour"),
        "risk_level", "automated_action"
    )
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum("amount").alias("total_amount"),
        F.avg("risk_score").alias("avg_risk_score"),
        F.sum(F.when(F.col("is_flagged") == True, 1).otherwise(0)).alias("true_fraud_count"),
        F.sum(F.when((F.col("automated_action") == "BLOCK") & (F.col("is_flagged") == False), 1).otherwise(0)).alias("false_positive_count"),
    )
    .withColumn("false_positive_ratio",
        F.when(F.col("transaction_count") > 0,
               F.col("false_positive_count") / F.col("transaction_count"))
         .otherwise(0))
)

gold_kpis.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.gold.fraud_kpis")
print(f"  gold.fraud_kpis: {gold_kpis.count():,} rows")

# COMMAND ----------

# --- Gold: Wire Transfers After MFA Change ---
mfa_changes = spark.table(f"{CATALOG}.silver.login_logs").filter(F.col("mfa_change_flag") == True).alias("m")
wire_txns = (
    spark.table(f"{CATALOG}.silver.transactions")
    .filter((F.col("transaction_type") == "wire_transfer") & (F.col("amount") > 10000))
    .alias("t")
)

gold_wire_mfa = (
    wire_txns.join(mfa_changes,
        (F.col("t.user_id") == F.col("m.user_id")) &
        (F.col("t.timestamp").between(
            F.col("m.timestamp"),
            F.col("m.timestamp") + F.expr("INTERVAL 24 HOURS")
        )),
        "inner"
    )
    .select(
        F.col("t.transaction_id"),
        F.col("t.user_id"),
        F.col("t.timestamp").alias("wire_timestamp"),
        F.col("t.amount"),
        F.col("t.merchant_name"),
        F.col("t.city").alias("wire_city"),
        F.col("t.country").alias("wire_country"),
        F.col("m.timestamp").alias("mfa_change_timestamp"),
        F.col("m.mfa_method").alias("new_mfa_method"),
        F.col("m.city").alias("mfa_change_city"),
        F.col("m.country").alias("mfa_change_country"),
        (F.unix_timestamp("t.timestamp") - F.unix_timestamp("m.timestamp")).alias("seconds_between"),
    )
)

gold_wire_mfa.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.gold.wire_after_mfa_change")
print(f"  gold.wire_after_mfa_change: {gold_wire_mfa.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Lakebase Database and Schema

# COMMAND ----------

import requests, json, time

workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Create Lakebase instance
print("Creating Lakebase instance...")
resp = requests.post(
    f"{workspace_url}/api/2.0/databases",
    headers=headers,
    json={"name": LAKEBASE_INSTANCE, "catalog_name": CATALOG}
)
if resp.status_code in (200, 201):
    lb_info = resp.json()
    print(f"  Instance created: {LAKEBASE_INSTANCE}")
elif resp.status_code == 409 or "already exists" in resp.text.lower():
    print(f"  Instance '{LAKEBASE_INSTANCE}' already exists.")
else:
    print(f"  Response ({resp.status_code}): {resp.text[:300]}")

# Wait for provisioning
print("Waiting for Lakebase to be ready...")
time.sleep(10)

# Get connection info
resp = requests.get(f"{workspace_url}/api/2.0/databases/{LAKEBASE_INSTANCE}", headers=headers)
if resp.status_code == 200:
    db_info = resp.json()
    lb_host = db_info.get("jdbc_url", db_info.get("host", "see workspace UI"))
    print(f"  Lakebase host: {lb_host}")
else:
    print(f"  Check Lakebase status in the workspace UI.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Apply Lakebase Schema (via Database Credential)

# COMMAND ----------

# Generate a database credential for Lakebase access
import base64

cred_resp = requests.post(
    f"{workspace_url}/api/2.0/database/credentials",
    headers=headers,
    json={"instance_names": [LAKEBASE_INSTANCE], "request_id": "deploy-notebook"}
)

if cred_resp.status_code == 200:
    cred = cred_resp.json()
    lb_token = cred.get("token", "")
    # Extract PG user from JWT sub claim
    try:
        payload = lb_token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload))
        pg_user = claims.get("sub", username)
    except Exception:
        pg_user = username
    print(f"  Database credential obtained for: {pg_user}")
else:
    print(f"  Could not get credential ({cred_resp.status_code}): {cred_resp.text[:200]}")
    lb_token = None
    pg_user = None

# COMMAND ----------

# Apply schema using psycopg2
if lb_token:
    try:
        import psycopg2
    except ImportError:
        dbutils.library.installPyPI("psycopg2-binary")
        import psycopg2

    # Get Lakebase host from the database info
    lb_resp = requests.get(f"{workspace_url}/api/2.0/databases/{LAKEBASE_INSTANCE}", headers=headers)
    if lb_resp.status_code == 200:
        lb_data = lb_resp.json()
        # Extract host from JDBC URL or connection info
        lb_host = lb_data.get("host", "")
        if not lb_host:
            jdbc = lb_data.get("jdbc_url", "")
            if "://" in jdbc:
                lb_host = jdbc.split("://")[1].split("/")[0].split(":")[0]

    if lb_host:
        print(f"Connecting to Lakebase at {lb_host}...")
        conn = psycopg2.connect(
            host=lb_host, port=5432, dbname="databricks_postgres",
            user=pg_user, password=lb_token, sslmode="require"
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Read and execute the schema SQL
        schema_sql = open(f"/Workspace{REPO_ROOT}/lakebase/schema.sql").read()

        # Execute each statement separately
        for stmt in schema_sql.split(";"):
            stmt = stmt.strip()
            if stmt and not stmt.startswith("--"):
                try:
                    cur.execute(stmt)
                    print(f"  OK: {stmt[:60]}...")
                except Exception as e:
                    print(f"  WARN: {str(e)[:100]}")

        cur.close()
        conn.close()
        print("  Lakebase schema applied.")
    else:
        print("  Could not determine Lakebase host. Apply schema manually.")
else:
    print("  Skipping schema - no credential available. Apply lakebase/schema.sql manually.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Sync Gold Risk Scores → Lakebase

# COMMAND ----------

if lb_token and lb_host:
    from datetime import datetime
    from psycopg2.extras import execute_values

    # Fetch flagged transactions from Gold
    flagged_df = spark.sql(f"""
        SELECT
            transaction_id, user_id, CAST(amount AS DOUBLE) as amount, 'USD' as currency,
            transaction_type, merchant_category as merchant_name, tx_channel as channel,
            CAST(risk_score AS INT) as risk_score, risk_level, automated_action,
            CASE WHEN geo_mismatch THEN '{{"geo_mismatch": true}}' ELSE '{{}}' END as triggered_factors,
            '' as explanation,
            tx_city, tx_country,
            COALESCE(login_city, '') as login_city, COALESCE(login_country, '') as login_country,
            CAST(COALESCE(geo_mismatch, false) AS BOOLEAN) as geo_mismatch,
            CAST(COALESCE(mfa_change_flag, false) AS BOOLEAN) as mfa_changed,
            CAST(COALESCE(is_bot_typing, false) AS BOOLEAN) as bot_detected
        FROM {CATALOG}.gold.user_session_risk
        WHERE automated_action IN ('YELLOW_FLAG', 'BLOCK')
    """)

    rows = flagged_df.collect()
    print(f"  Found {len(rows)} flagged transactions to sync.")

    if rows:
        conn = psycopg2.connect(
            host=lb_host, port=5432, dbname="databricks_postgres",
            user=pg_user, password=lb_token, sslmode="require"
        )

        upsert_sql = """
        INSERT INTO real_time_fraud_triage (
            transaction_id, user_id, amount, currency, transaction_type,
            merchant_name, channel, risk_score, risk_level, automated_action,
            triggered_factors, explanation, tx_city, tx_country, login_city, login_country,
            geo_mismatch, mfa_changed, bot_detected, updated_at
        ) VALUES %s
        ON CONFLICT (transaction_id) DO UPDATE SET
            risk_score = EXCLUDED.risk_score,
            risk_level = EXCLUDED.risk_level,
            automated_action = EXCLUDED.automated_action,
            triggered_factors = EXCLUDED.triggered_factors,
            updated_at = NOW()
        """

        values = [
            (
                r.transaction_id, r.user_id, float(r.amount), r.currency,
                r.transaction_type, r.merchant_name or "", r.channel or "",
                int(r.risk_score), r.risk_level, r.automated_action,
                r.triggered_factors, r.explanation,
                r.tx_city or "", r.tx_country or "", r.login_city or "", r.login_country or "",
                bool(r.geo_mismatch), bool(r.mfa_changed), bool(r.bot_detected),
                datetime.utcnow()
            )
            for r in rows
        ]

        cur = conn.cursor()
        # Batch in groups of 100
        for i in range(0, len(values), 100):
            batch = values[i:i+100]
            execute_values(cur, upsert_sql, batch)
            print(f"  Upserted batch {i//100 + 1}: {len(batch)} rows")
        conn.commit()
        cur.close()
        conn.close()
        print(f"  Synced {len(values)} transactions to Lakebase.")
else:
    print("  Skipping Lakebase sync - no connection available.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Generate Fraud Explanations

# COMMAND ----------

if lb_token and lb_host:
    from datetime import datetime as dt

    RISK_FACTORS = {
        "high_amount": {
            "condition": lambda r: float(r.get("amount", 0)) > 10000,
            "explanation": lambda r: f"Transaction amount (${float(r['amount']):,.2f}) exceeds the $10,000 high-value threshold"
        },
        "high_risk_merchant": {
            "condition": lambda r: r.get("merchant_name", "") in ("wire", "crypto", "gambling", "p2p"),
            "explanation": lambda r: f"Merchant category '{r['merchant_name']}' is classified as high-risk"
        },
        "geo_mismatch": {
            "condition": lambda r: r.get("geo_mismatch") in (True, "true", "True"),
            "explanation": lambda r: f"Transaction in ({r.get('tx_city','?')}, {r.get('tx_country','?')}) differs from login ({r.get('login_city','?')}, {r.get('login_country','?')})"
        },
        "mfa_change_then_wire": {
            "condition": lambda r: r.get("mfa_changed") in (True, "true", "True") and r.get("transaction_type", "") == "wire_transfer",
            "explanation": lambda r: "MFA settings were modified prior to initiating a wire transfer - common account takeover pattern"
        },
        "bot_typing_detected": {
            "condition": lambda r: r.get("bot_detected") in (True, "true", "True"),
            "explanation": lambda r: "Typing cadence suggests automated/bot input rather than human interaction"
        },
    }

    def build_explanation(rec):
        triggered = []
        explanations = []
        for name, factor in RISK_FACTORS.items():
            try:
                if factor["condition"](rec):
                    triggered.append(name)
                    explanations.append(factor["explanation"](rec))
            except Exception:
                continue

        score = rec.get("risk_score", 0)
        action = rec.get("automated_action", "N/A")
        lines = [
            f"FRAUD RISK ASSESSMENT - {rec.get('transaction_id', 'N/A')}",
            f"{'='*55}",
            f"User: {rec.get('user_id', 'N/A')} | Type: {rec.get('transaction_type', 'N/A')} | Amount: ${float(rec.get('amount', 0)):,.2f}",
            f"Risk Score: {score}/100 | Action: {action}",
            "", f"RISK FACTORS DETECTED ({len(triggered)}):",
        ]
        for i, exp in enumerate(explanations, 1):
            lines.append(f"  {i}. {exp}")
        if not explanations:
            lines.append("  Flagged by aggregate scoring.")
        lines.extend([
            "", "REGULATORY COMPLIANCE:",
            "Under GDPR Art. 22 / CCPA Sec. 1798.185, the customer may request human review.",
            "", f"Analyzed at: {dt.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
        ])
        return "\n".join(lines), json.dumps(triggered)

    conn = psycopg2.connect(
        host=lb_host, port=5432, dbname="databricks_postgres",
        user=pg_user, password=lb_token, sslmode="require"
    )
    cur = conn.cursor()

    cur.execute("SELECT transaction_id, user_id, amount, transaction_type, merchant_name, "
                "risk_score, automated_action, tx_city, tx_country, login_city, login_country, "
                "geo_mismatch, mfa_changed, bot_detected FROM real_time_fraud_triage "
                "WHERE explanation IS NULL OR explanation = ''")
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    print(f"  Generating explanations for {len(rows)} transactions...")

    updated = 0
    for row in rows:
        rec = dict(zip(columns, row))
        explanation, factors = build_explanation(rec)
        cur.execute(
            "UPDATE real_time_fraud_triage SET explanation = %s, triggered_factors = %s WHERE transaction_id = %s",
            (explanation, factors, rec["transaction_id"])
        )
        updated += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"  Updated {updated} explanations.")
else:
    print("  Skipping explanation generation - no Lakebase connection.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Register MLflow Fraud Reasoning Agent

# COMMAND ----------

# MAGIC %pip install mlflow>=2.12 -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Re-import after Python restart
import mlflow, pandas as pd, json
from datetime import datetime

# Re-read config
CATALOG = "fraud_demo"

# COMMAND ----------

import mlflow
import pandas as pd
import json
from datetime import datetime

class FraudReasoningAgent(mlflow.pyfunc.PythonModel):
    RISK_FACTORS = {
        "high_amount": {"condition": lambda r: r.get("amount", 0) > 10000, "weight": 25,
            "explanation": "Transaction amount (${amount:.2f}) exceeds the $10,000 high-value threshold"},
        "high_risk_merchant": {"condition": lambda r: r.get("merchant_category", "") in ("wire", "crypto", "gambling", "p2p"), "weight": 15,
            "explanation": "Merchant category '{merchant_category}' is classified as high-risk"},
        "geo_mismatch": {"condition": lambda r: r.get("tx_country", "") != r.get("login_country", r.get("tx_country", "")), "weight": 20,
            "explanation": "Transaction location ({tx_city}, {tx_country}) differs from login location ({login_city}, {login_country})"},
        "mfa_change_then_wire": {"condition": lambda r: r.get("mfa_change_flag", False) and r.get("transaction_type", "") == "wire_transfer", "weight": 30,
            "explanation": "MFA settings were modified prior to initiating a wire transfer"},
        "bot_typing_detected": {"condition": lambda r: r.get("typing_speed_ms", 999) < 50, "weight": 25,
            "explanation": "Typing cadence ({typing_speed_ms:.0f}ms avg) suggests automated/bot input"},
        "rapid_login_failures": {"condition": lambda r: r.get("failed_attempts_before_success", 0) >= 3, "weight": 10,
            "explanation": "{failed_attempts_before_success} failed login attempts before authentication"},
        "off_hours_activity": {"condition": lambda r: r.get("hour_of_day", 12) < 6 or r.get("hour_of_day", 12) > 22, "weight": 5,
            "explanation": "Transaction at hour {hour_of_day}, outside normal banking hours"},
        "international_wire": {"condition": lambda r: r.get("is_international", False) and r.get("transaction_type", "") == "wire_transfer", "weight": 15,
            "explanation": "International wire transfer to {tx_country}"},
        "impossible_travel": {"condition": lambda r: r.get("distance_miles", 0) > 500 and r.get("time_diff_minutes", 9999) < 60, "weight": 30,
            "explanation": "Impossible travel: {distance_miles:.0f} miles in {time_diff_minutes:.0f} minutes"},
    }

    def predict(self, context, model_input):
        results = []
        for _, row in model_input.iterrows():
            record = row.to_dict()
            score, triggered, explanations = self._analyze(record)
            risk_level = "CRITICAL" if score >= 70 else "HIGH" if score >= 40 else "MEDIUM" if score >= 20 else "LOW"
            action = "BLOCK" if score >= 70 else "YELLOW_FLAG" if score >= 40 else "MONITOR" if score >= 20 else "ALLOW"
            explanation = self._build_explanation(record, score, risk_level, action, triggered, explanations)
            results.append({
                "transaction_id": record.get("transaction_id", "unknown"),
                "risk_score": score, "risk_level": risk_level,
                "automated_action": action, "triggered_factors": json.dumps(triggered),
                "explanation": explanation, "analyzed_at": datetime.utcnow().isoformat(),
            })
        return pd.DataFrame(results)

    def _analyze(self, record):
        score, triggered, explanations = 0, [], []
        for name, factor in self.RISK_FACTORS.items():
            try:
                if factor["condition"](record):
                    score += factor["weight"]
                    triggered.append(name)
                    try: explanations.append(factor["explanation"].format(**record))
                    except: explanations.append(factor["explanation"])
            except: continue
        return min(score, 100), triggered, explanations

    def _build_explanation(self, record, score, level, action, factors, explanations):
        lines = [
            f"FRAUD RISK ASSESSMENT - {record.get('transaction_id', 'N/A')}",
            f"{'='*50}",
            f"User: {record.get('user_id', 'N/A')} | Amount: ${record.get('amount', 0):,.2f}",
            f"Score: {score}/100 | Level: {level} | Action: {action}",
            "", f"RISK FACTORS ({len(factors)}):",
        ]
        for i, exp in enumerate(explanations, 1): lines.append(f"  {i}. {exp}")
        if not explanations: lines.append("  No significant risk factors.")
        lines.extend(["", "Under GDPR Art. 22 / CCPA Sec. 1798.185, the customer may request human review."])
        return "\n".join(lines)

# COMMAND ----------

MODEL_NAME = f"{CATALOG}.fraud_models.fraud_reasoning_agent"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.fraud_models")

mlflow.set_registry_uri("databricks-uc")

with mlflow.start_run(run_name="fraud_reasoning_agent_v1") as run:
    model = FraudReasoningAgent()
    sample_input = pd.DataFrame([{
        "transaction_id": "TXN-SAMPLE001", "user_id": "USR-000001", "amount": 15000.0,
        "transaction_type": "wire_transfer", "merchant_category": "wire",
        "tx_city": "Lagos", "tx_country": "NG", "login_city": "New York", "login_country": "US",
        "mfa_change_flag": True, "typing_speed_ms": 35.0,
        "failed_attempts_before_success": 4, "hour_of_day": 3, "is_international": True,
    }])
    sample_output = model.predict(None, sample_input)
    signature = mlflow.models.infer_signature(sample_input, sample_output)

    model_info = mlflow.pyfunc.log_model(
        artifact_path="fraud_reasoning_agent", python_model=model,
        signature=signature, registered_model_name=MODEL_NAME,
        pip_requirements=["mlflow>=2.12", "pandas>=2.0"],
    )
    print(f"  Model registered: {MODEL_NAME}")
    print(f"  Model URI: {model_info.model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Create Model Serving Endpoint

# COMMAND ----------

import requests, json
from mlflow import MlflowClient

workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

client = MlflowClient()
versions = client.search_model_versions(f"name='{MODEL_NAME}'")
latest_version = max(v.version for v in versions)

ENDPOINT_NAME = "fraud-reasoning-agent"

endpoint_config = {
    "name": ENDPOINT_NAME,
    "config": {
        "served_entities": [{
            "entity_name": MODEL_NAME,
            "entity_version": str(latest_version),
            "workload_size": "Small",
            "scale_to_zero_enabled": True,
        }],
        "auto_capture_config": {
            "catalog_name": CATALOG, "schema_name": "fraud_models",
            "table_name_prefix": "fraud_reasoning_logs",
        },
    },
    "tags": [{"key": "team", "value": "fraud-ops"}, {"key": "compliance", "value": "gdpr-ccpa"}],
}

check = requests.get(f"{workspace_url}/api/2.0/serving-endpoints/{ENDPOINT_NAME}", headers=headers)
if check.status_code == 200:
    print(f"Updating endpoint '{ENDPOINT_NAME}'...")
    resp = requests.put(f"{workspace_url}/api/2.0/serving-endpoints/{ENDPOINT_NAME}/config",
                        headers=headers, json=endpoint_config["config"])
else:
    print(f"Creating endpoint '{ENDPOINT_NAME}'...")
    resp = requests.post(f"{workspace_url}/api/2.0/serving-endpoints", headers=headers, json=endpoint_config)

print(f"  Status: {resp.status_code}")
if resp.status_code in (200, 201):
    print(f"  Endpoint: {workspace_url}/ml/endpoints/{ENDPOINT_NAME}")
else:
    print(f"  Response: {resp.text[:300]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Deploy Live Fraud Queue App

# COMMAND ----------

import requests, json

workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
APP_NAME = "live-fraud-queue"

# Create app
print(f"Creating Databricks App '{APP_NAME}'...")
resp = requests.post(
    f"{workspace_url}/api/2.0/apps",
    headers=headers,
    json={"name": APP_NAME, "description": "Live Fraud Triage Queue for analysts"}
)
if resp.status_code in (200, 201):
    print(f"  App created.")
elif resp.status_code == 409 or "already exists" in resp.text.lower():
    print(f"  App '{APP_NAME}' already exists.")
else:
    print(f"  Response ({resp.status_code}): {resp.text[:200]}")

# Deploy from repo source
REPO_ROOT_REREAD = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
if REPO_ROOT_REREAD.endswith("/notebooks/deploy_fraud_triage"):
    REPO_ROOT_REREAD = REPO_ROOT_REREAD.rsplit("/notebooks/", 1)[0]

app_source = f"{REPO_ROOT_REREAD}/app"

print(f"Deploying from: {app_source}")
resp = requests.post(
    f"{workspace_url}/api/2.0/apps/{APP_NAME}/deployments",
    headers=headers,
    json={"source_code_path": app_source}
)
if resp.status_code in (200, 201):
    print(f"  Deployment started.")
    print(f"  App URL: {workspace_url}/apps/{APP_NAME}")
else:
    print(f"  Deploy response ({resp.status_code}): {resp.text[:300]}")
    print(f"\n  Manual deploy: databricks apps deploy {APP_NAME} --source-code-path <path-to-app-dir>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deployment Summary

# COMMAND ----------

CATALOG = "fraud_demo"

print(f"""
{'='*60}
  FRAUD TRIAGE AGENT - DEPLOYMENT COMPLETE
{'='*60}

  Unity Catalog:
    Catalog:  {CATALOG}
    Schemas:  bronze, silver, gold, fraud_models
    Volume:   /Volumes/{CATALOG}/bronze/raw_data

  Tables:
    {CATALOG}.bronze.transactions
    {CATALOG}.bronze.login_logs
    {CATALOG}.silver.transactions
    {CATALOG}.silver.login_logs
    {CATALOG}.gold.user_session_risk
    {CATALOG}.gold.fraud_kpis
    {CATALOG}.gold.wire_after_mfa_change

  Lakebase:
    Instance: {LAKEBASE_INSTANCE}
    Tables:   real_time_fraud_triage, user_session_risk

  Model Serving:
    Endpoint: fraud-reasoning-agent

  Databricks App:
    Name:     {APP_NAME}
    URL:      {workspace_url}/apps/{APP_NAME}

  Next Steps:
    1. Open the Live Fraud Queue app and start triaging
    2. Configure a Genie Space with queries from genie/certified_queries.sql
    3. Monitor the serving endpoint at /ml/endpoints/fraud-reasoning-agent
""")
