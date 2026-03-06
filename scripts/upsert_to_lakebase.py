"""
Lakebase Upsert Service - Streams risk scores from the Gold layer
into Lakebase (Serverless Postgres) for sub-second blocking decisions.

This service reads from the gold_user_session_risk table and upserts
YELLOW_FLAG and BLOCK transactions into the Lakebase triage store.
"""

import os
import json
import psycopg2
from psycopg2.extras import execute_values
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


CATALOG = "infa_migrate_catalog"
GOLD_TABLE = f"{CATALOG}.fraud_gold.gold_user_session_risk"


def get_lakebase_connection(db_name="fraud_triage"):
    """Connect to Lakebase instance using Databricks SDK."""
    w = WorkspaceClient(profile="fe-vm-infa-migrate")

    # Get Lakebase connection details from the workspace
    # The connection string is available via the Lakebase API
    databases = w.api_client.do("GET", "/api/2.0/lakebase/databases")
    db = next((d for d in databases.get("databases", []) if d["name"] == db_name), None)

    if not db:
        raise ValueError(f"Lakebase database '{db_name}' not found. Create it first.")

    conn_info = db["connection_info"]
    return psycopg2.connect(
        host=conn_info["host"],
        port=conn_info["port"],
        dbname=db_name,
        user=conn_info["user"],
        password=conn_info["token"],
        sslmode="require"
    )


def fetch_flagged_transactions(w, warehouse_id, batch_size=500):
    """Fetch YELLOW_FLAG and BLOCK transactions from the Gold table."""
    query = f"""
    SELECT
        transaction_id, user_id, amount, 'USD' as currency,
        transaction_type, merchant_category as merchant_name, tx_channel as channel,
        risk_score, risk_level, automated_action,
        CASE WHEN mfa_then_wire OR geo_mismatch OR is_bot_typing
             THEN to_json(named_struct(
                'geo_mismatch', geo_mismatch,
                'mfa_then_wire', mfa_then_wire,
                'bot_detected', is_bot_typing,
                'off_hours', is_off_hours
             ))
             ELSE '{{}}'
        END as triggered_factors,
        tx_city, tx_country, login_city, login_country,
        geo_mismatch, COALESCE(mfa_change_flag, false) as mfa_changed,
        COALESCE(is_bot_typing, false) as bot_detected
    FROM {GOLD_TABLE}
    WHERE automated_action IN ('YELLOW_FLAG', 'BLOCK')
    LIMIT {batch_size}
    """

    stmt = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        catalog=CATALOG,
        schema="gold",
        statement=query,
        wait_timeout="30s"
    )

    if stmt.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"Query failed: {stmt.status.error}")

    columns = [col.name for col in stmt.manifest.schema.columns]
    return [dict(zip(columns, row)) for row in stmt.result.data_array]


def upsert_to_lakebase(conn, transactions):
    """Upsert flagged transactions into the Lakebase triage table."""
    if not transactions:
        print("No transactions to upsert.")
        return 0

    upsert_sql = """
    INSERT INTO real_time_fraud_triage (
        transaction_id, user_id, amount, currency, transaction_type,
        merchant_name, channel, risk_score, risk_level, automated_action,
        triggered_factors, tx_city, tx_country, login_city, login_country,
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
            t["transaction_id"], t["user_id"], float(t["amount"]), t.get("currency", "USD"),
            t["transaction_type"], t.get("merchant_name", ""), t.get("channel", ""),
            int(t["risk_score"]), t["risk_level"], t["automated_action"],
            t.get("triggered_factors", "{}"),
            t.get("tx_city", ""), t.get("tx_country", ""),
            t.get("login_city", ""), t.get("login_country", ""),
            t.get("geo_mismatch", False), t.get("mfa_changed", False),
            t.get("bot_detected", False), "NOW()"
        )
        for t in transactions
    ]

    with conn.cursor() as cur:
        execute_values(cur, upsert_sql, values)
    conn.commit()

    print(f"Upserted {len(values)} transactions to Lakebase.")
    return len(values)


def run_sync(warehouse_id):
    """Main sync loop: fetch from Gold -> upsert to Lakebase."""
    w = WorkspaceClient(profile="fe-vm-infa-migrate")

    print("Fetching flagged transactions from Gold layer...")
    transactions = fetch_flagged_transactions(w, warehouse_id)
    print(f"Found {len(transactions)} flagged transactions.")

    if transactions:
        print("Connecting to Lakebase...")
        conn = get_lakebase_connection()
        try:
            count = upsert_to_lakebase(conn, transactions)
            print(f"Sync complete. {count} records upserted.")
        finally:
            conn.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python upsert_to_lakebase.py <warehouse_id>")
        sys.exit(1)
    run_sync(sys.argv[1])
