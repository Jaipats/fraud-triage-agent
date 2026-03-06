"""
Live Fraud Queue - Databricks App
FastAPI backend serving the fraud triage queue from Lakebase.
Analysts can review, release, or escalate yellow-flagged transactions.
"""

import os
import json
import logging
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
from databricks.sdk import WorkspaceClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Live Fraud Queue", version="1.0.0")

# --- Databricks SDK singleton ---
_workspace_client: Optional[WorkspaceClient] = None

def _get_workspace_client() -> WorkspaceClient:
    global _workspace_client
    if _workspace_client is None:
        _workspace_client = WorkspaceClient()
    return _workspace_client


def _get_lakebase_credential() -> dict:
    """Generate a Lakebase database credential via the Databricks API.

    Calls POST /api/2.0/database/credentials which:
      1. Provisions the caller's PG role on the Lakebase instance (if not
         already created).
      2. Returns a short-lived JWT token that Lakebase accepts as the
         PG password.

    The ``sub`` claim in the returned JWT is the PG role name (email for
    users, client-id for service principals).

    Returns dict with keys: token, expiration_time
    """
    import base64
    w = _get_workspace_client()
    instance_name = os.environ.get("LAKEBASE_INSTANCE", "fraud-triage")

    resp = w.api_client.do(
        "POST",
        "/api/2.0/database/credentials",
        body={"instance_names": [instance_name], "request_id": "app-cred"},
    )

    token = resp.get("token", "")

    # Extract the PG role from the JWT ``sub`` claim
    try:
        payload = token.split(".")[1]
        # Fix base64 padding
        payload += "=" * (-len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload))
        pg_user = claims.get("sub", "")
    except Exception:
        pg_user = ""

    return {"token": token, "pg_user": pg_user}


# --- Database connection ---

def get_db():
    """Get Lakebase connection using a database credential JWT.

    Calls the Databricks database-credentials API to obtain a short-lived
    JWT.  The JWT ``sub`` claim is used as the PG username and the JWT
    itself is used as the password.  This works for both service principals
    (inside a Databricks App) and interactive users (local development).
    """
    host = os.environ.get("LAKEBASE_HOST",
        "instance-ed179467-4f2b-463a-b627-95e6d85fc72a.database.cloud.databricks.com")
    db_name = os.environ.get("LAKEBASE_DB", "fraud_triage")

    cred = _get_lakebase_credential()
    pg_user = cred["pg_user"]
    token = cred["token"]

    if not pg_user:
        raise RuntimeError("Could not determine PG role from database credential JWT")

    logger.info("Connecting to Lakebase at %s, db=%s, user=%s", host, db_name, pg_user)

    return psycopg2.connect(
        host=host, port=5432, dbname=db_name,
        user=pg_user, password=token, sslmode="require",
        cursor_factory=RealDictCursor
    )


# --- Models ---

class AnalystDecision(BaseModel):
    transaction_id: str
    analyst_id: str
    decision: str  # APPROVE, RELEASE, ESCALATE, BLOCK_CONFIRMED
    notes: Optional[str] = None


class QueueStats(BaseModel):
    total_pending: int
    yellow_flagged: int
    blocked: int
    avg_risk_score: float
    oldest_age_minutes: float


# --- API Routes ---

@app.get("/api/health")
def health_check():
    """Health check endpoint."""
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "degraded", "database": "error", "detail": str(e)}


@app.get("/api/queue")
def get_fraud_queue(
    action: Optional[str] = Query(None, description="Filter: YELLOW_FLAG or BLOCK"),
    risk_level: Optional[str] = Query(None),
    limit: int = Query(50, le=200),
    offset: int = Query(0)
):
    """Get the active fraud triage queue."""
    conn = get_db()
    try:
        with conn.cursor() as cur:
            where_clauses = [
                "analyst_decision IS NULL",
                "expires_at > NOW()",
                "automated_action IN ('YELLOW_FLAG', 'BLOCK')"
            ]
            params = []

            if action:
                where_clauses.append("automated_action = %s")
                params.append(action)
            if risk_level:
                where_clauses.append("risk_level = %s")
                params.append(risk_level.upper())

            where = " AND ".join(where_clauses)
            cur.execute(f"""
                SELECT
                    transaction_id, user_id, amount, currency,
                    transaction_type, merchant_name, channel,
                    risk_score, risk_level, automated_action,
                    triggered_factors, explanation,
                    tx_city, tx_country, login_city, login_country,
                    geo_mismatch, mfa_changed, bot_detected,
                    created_at,
                    EXTRACT(EPOCH FROM (NOW() - created_at)) AS age_seconds
                FROM real_time_fraud_triage
                WHERE {where}
                ORDER BY risk_score DESC, created_at ASC
                LIMIT %s OFFSET %s
            """, params + [limit, offset])

            rows = cur.fetchall()
            for row in rows:
                if row.get("triggered_factors"):
                    if isinstance(row["triggered_factors"], str):
                        row["triggered_factors"] = json.loads(row["triggered_factors"])
                if row.get("created_at"):
                    row["created_at"] = row["created_at"].isoformat()
                row["age_minutes"] = round(row.pop("age_seconds", 0) / 60, 1)

            return {"transactions": rows, "count": len(rows)}
    finally:
        conn.close()


@app.get("/api/queue/stats")
def get_queue_stats():
    """Get summary statistics for the fraud queue."""
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS total_pending,
                    COUNT(*) FILTER (WHERE automated_action = 'YELLOW_FLAG') AS yellow_flagged,
                    COUNT(*) FILTER (WHERE automated_action = 'BLOCK') AS blocked,
                    COALESCE(AVG(risk_score), 0) AS avg_risk_score,
                    COALESCE(MAX(EXTRACT(EPOCH FROM (NOW() - created_at))) / 60, 0) AS oldest_age_minutes
                FROM real_time_fraud_triage
                WHERE analyst_decision IS NULL
                  AND automated_action IN ('YELLOW_FLAG', 'BLOCK')
                  AND expires_at > NOW()
            """)
            stats = cur.fetchone()
            return QueueStats(**stats)
    finally:
        conn.close()


@app.post("/api/queue/decide")
def submit_decision(decision: AnalystDecision):
    """Submit an analyst decision for a flagged transaction."""
    if decision.decision not in ("APPROVE", "RELEASE", "ESCALATE", "BLOCK_CONFIRMED"):
        raise HTTPException(400, "Invalid decision. Must be APPROVE, RELEASE, ESCALATE, or BLOCK_CONFIRMED")

    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE real_time_fraud_triage
                SET analyst_id = %s,
                    analyst_decision = %s,
                    analyst_notes = %s,
                    reviewed_at = NOW(),
                    updated_at = NOW()
                WHERE transaction_id = %s
                  AND analyst_decision IS NULL
                RETURNING transaction_id, automated_action, analyst_decision
            """, (decision.analyst_id, decision.decision, decision.notes, decision.transaction_id))

            result = cur.fetchone()
            if not result:
                raise HTTPException(404, "Transaction not found or already reviewed")

            conn.commit()
            return {
                "status": "success",
                "transaction_id": result["transaction_id"],
                "previous_action": result["automated_action"],
                "analyst_decision": result["analyst_decision"]
            }
    finally:
        conn.close()


@app.get("/api/transaction/{transaction_id}")
def get_transaction_detail(transaction_id: str):
    """Get full details for a specific transaction including explanation."""
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM real_time_fraud_triage
                WHERE transaction_id = %s
            """, (transaction_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "Transaction not found")
            for key in ("created_at", "updated_at", "reviewed_at", "expires_at"):
                if row.get(key):
                    row[key] = row[key].isoformat()
            if isinstance(row.get("triggered_factors"), str):
                row["triggered_factors"] = json.loads(row["triggered_factors"])
            return row
    finally:
        conn.close()


@app.get("/api/queue/reviewed")
def get_reviewed_transactions(
    decision: Optional[str] = Query(None),
    limit: int = Query(50, le=200),
    offset: int = Query(0)
):
    """Get transactions that have been reviewed by analysts."""
    conn = get_db()
    try:
        with conn.cursor() as cur:
            where_clauses = ["analyst_decision IS NOT NULL"]
            params = []
            if decision:
                where_clauses.append("analyst_decision = %s")
                params.append(decision)
            where = " AND ".join(where_clauses)
            cur.execute(f"""
                SELECT
                    transaction_id, user_id, amount, currency,
                    transaction_type, merchant_name, channel,
                    risk_score, risk_level, automated_action,
                    triggered_factors, explanation,
                    tx_city, tx_country, login_city, login_country,
                    geo_mismatch, mfa_changed, bot_detected,
                    analyst_id, analyst_decision, analyst_notes,
                    created_at, reviewed_at,
                    EXTRACT(EPOCH FROM (reviewed_at - created_at)) AS review_time_seconds
                FROM real_time_fraud_triage
                WHERE {where}
                ORDER BY reviewed_at DESC
                LIMIT %s OFFSET %s
            """, params + [limit, offset])
            rows = cur.fetchall()
            for row in rows:
                if row.get("triggered_factors"):
                    if isinstance(row["triggered_factors"], str):
                        try:
                            row["triggered_factors"] = json.loads(row["triggered_factors"])
                        except json.JSONDecodeError:
                            pass
                for key in ("created_at", "reviewed_at"):
                    if row.get(key):
                        row[key] = row[key].isoformat()
                row["review_time_minutes"] = round(row.pop("review_time_seconds", 0) / 60, 1) if row.get("review_time_seconds") is not None else 0

            # Also get summary counts
            cur.execute("""
                SELECT
                    COUNT(*) AS total_reviewed,
                    COUNT(*) FILTER (WHERE analyst_decision = 'RELEASE') AS released,
                    COUNT(*) FILTER (WHERE analyst_decision = 'BLOCK_CONFIRMED') AS confirmed,
                    COUNT(*) FILTER (WHERE analyst_decision = 'ESCALATE') AS escalated
                FROM real_time_fraud_triage
                WHERE analyst_decision IS NOT NULL
            """)
            summary = cur.fetchone()
            return {"transactions": rows, "count": len(rows), "summary": summary}
    finally:
        conn.close()


@app.get("/api/analyst/metrics")
def get_analyst_metrics():
    """Get performance metrics for all analysts."""
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM analyst_metrics")
            return {"analysts": cur.fetchall()}
    finally:
        conn.close()


# --- Serve frontend ---

@app.get("/", response_class=HTMLResponse)
def serve_frontend():
    return FRONTEND_HTML


FRONTEND_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Live Fraud Queue</title>
<style>
  :root {
    --bg: #0f1117; --surface: #1a1d27; --border: #2a2d3a;
    --text: #e4e4e7; --muted: #71717a; --accent: #3b82f6;
    --red: #ef4444; --yellow: #eab308; --green: #22c55e; --orange: #f97316;
  }
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: var(--bg); color: var(--text); }
  .header { background: var(--surface); border-bottom: 1px solid var(--border);
            padding: 16px 24px; display: flex; align-items: center; justify-content: space-between; }
  .header h1 { font-size: 20px; font-weight: 600; }
  .header h1 span { color: var(--red); }
  .stats-bar { display: flex; gap: 16px; padding: 16px 24px; background: var(--surface);
               border-bottom: 1px solid var(--border); }
  .stat { background: var(--bg); border: 1px solid var(--border); border-radius: 8px;
          padding: 12px 20px; min-width: 140px; }
  .stat .label { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; }
  .stat .value { font-size: 24px; font-weight: 700; margin-top: 4px; }
  .stat .value.red { color: var(--red); }
  .stat .value.yellow { color: var(--yellow); }
  .stat .value.blue { color: var(--accent); }
  .filters { padding: 12px 24px; display: flex; gap: 8px; }
  .filters button { padding: 6px 14px; border-radius: 6px; border: 1px solid var(--border);
                    background: var(--surface); color: var(--text); cursor: pointer; font-size: 13px; }
  .filters button.active { background: var(--accent); border-color: var(--accent); color: white; }
  .queue { padding: 0 24px 24px; }
  .tx-card { background: var(--surface); border: 1px solid var(--border); border-radius: 10px;
            margin-top: 12px; overflow: hidden; transition: border-color 0.2s; }
  .tx-card:hover { border-color: var(--accent); }
  .tx-card.critical { border-left: 4px solid var(--red); }
  .tx-card.high { border-left: 4px solid var(--orange); }
  .tx-card.medium { border-left: 4px solid var(--yellow); }
  .tx-header { display: flex; justify-content: space-between; align-items: center;
              padding: 14px 18px; cursor: pointer; }
  .tx-header .left { display: flex; align-items: center; gap: 14px; }
  .risk-badge { padding: 3px 10px; border-radius: 12px; font-size: 11px; font-weight: 700;
               text-transform: uppercase; }
  .risk-badge.BLOCK { background: rgba(239,68,68,0.15); color: var(--red); }
  .risk-badge.YELLOW_FLAG { background: rgba(234,179,8,0.15); color: var(--yellow); }
  .amount { font-size: 18px; font-weight: 600; }
  .meta { font-size: 12px; color: var(--muted); }
  .score { font-size: 28px; font-weight: 800; }
  .score.high { color: var(--red); }
  .score.med { color: var(--orange); }
  .tx-details { display: none; padding: 0 18px 18px; border-top: 1px solid var(--border); }
  .tx-details.open { display: block; padding-top: 14px; }
  .detail-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; }
  .detail-item { font-size: 13px; }
  .detail-item .dl { color: var(--muted); font-size: 11px; }
  .factors { margin: 12px 0; }
  .factor-tag { display: inline-block; padding: 3px 8px; margin: 2px; border-radius: 4px;
               background: rgba(239,68,68,0.1); color: var(--red); font-size: 11px; }
  .explanation { background: var(--bg); border-radius: 6px; padding: 14px; margin: 12px 0;
                font-family: monospace; font-size: 12px; line-height: 1.6; white-space: pre-wrap;
                max-height: 300px; overflow-y: auto; }
  .actions { display: flex; gap: 8px; margin-top: 14px; }
  .actions button { padding: 8px 18px; border-radius: 6px; border: none; cursor: pointer;
                   font-weight: 600; font-size: 13px; }
  .btn-release { background: var(--green); color: white; }
  .btn-block { background: var(--red); color: white; }
  .btn-escalate { background: var(--orange); color: white; }
  .empty { text-align: center; padding: 60px; color: var(--muted); }
  .refresh-btn { background: none; border: 1px solid var(--border); color: var(--accent);
                padding: 6px 14px; border-radius: 6px; cursor: pointer; font-size: 13px; }
  .live-dot { width: 8px; height: 8px; background: var(--green); border-radius: 50%;
             display: inline-block; margin-right: 6px; animation: pulse 2s infinite; }
  @keyframes pulse { 0%,100% { opacity:1; } 50% { opacity:0.4; } }
  .tabs { display: flex; border-bottom: 2px solid var(--border); margin: 0 24px; }
  .tab { padding: 12px 24px; cursor: pointer; font-size: 14px; font-weight: 600;
         color: var(--muted); border-bottom: 2px solid transparent; margin-bottom: -2px;
         transition: all 0.2s; }
  .tab:hover { color: var(--text); }
  .tab.active { color: var(--accent); border-bottom-color: var(--accent); }
  .tab .badge { display: inline-block; padding: 1px 8px; border-radius: 10px;
               font-size: 11px; margin-left: 6px; font-weight: 700; }
  .tab .badge.pending { background: rgba(239,68,68,0.15); color: var(--red); }
  .tab .badge.done { background: rgba(34,197,94,0.15); color: var(--green); }
  .reviewed-card { background: var(--surface); border: 1px solid var(--border); border-radius: 10px;
                  margin-top: 12px; overflow: hidden; }
  .reviewed-card .rc-header { display: flex; justify-content: space-between; align-items: center;
                             padding: 14px 18px; cursor: pointer; }
  .reviewed-card .rc-header .left { display: flex; align-items: center; gap: 14px; }
  .decision-badge { padding: 3px 10px; border-radius: 12px; font-size: 11px; font-weight: 700;
                   text-transform: uppercase; }
  .decision-badge.RELEASE { background: rgba(34,197,94,0.15); color: var(--green); }
  .decision-badge.BLOCK_CONFIRMED { background: rgba(239,68,68,0.15); color: var(--red); }
  .decision-badge.ESCALATE { background: rgba(249,115,22,0.15); color: var(--orange); }
  .reviewed-summary { display: flex; gap: 16px; padding: 16px 24px; }
  .reviewed-summary .rs-item { background: var(--bg); border: 1px solid var(--border);
                               border-radius: 8px; padding: 10px 16px; min-width: 120px; }
  .reviewed-summary .rs-label { font-size: 11px; color: var(--muted); text-transform: uppercase; }
  .reviewed-summary .rs-value { font-size: 20px; font-weight: 700; margin-top: 2px; }
  .rs-value.green { color: var(--green); }
  .rs-value.red { color: var(--red); }
  .rs-value.orange { color: var(--orange); }
  .analyst-note { background: rgba(59,130,246,0.08); border-left: 3px solid var(--accent);
                 padding: 8px 12px; margin-top: 8px; border-radius: 0 6px 6px 0;
                 font-size: 12px; color: var(--text); }
  .review-filters { padding: 12px 24px; display: flex; gap: 8px; }
  .review-filters button { padding: 6px 14px; border-radius: 6px; border: 1px solid var(--border);
                          background: var(--surface); color: var(--text); cursor: pointer; font-size: 13px; }
  .review-filters button.active { background: var(--accent); border-color: var(--accent); color: white; }
  .balloon { position: fixed; bottom: -80px; width: 30px; height: 40px; border-radius: 50%;
            z-index: 9999; pointer-events: none; opacity: 0.9; }
  .balloon::after { content: ''; position: absolute; bottom: -12px; left: 50%;
                   transform: translateX(-50%); width: 1px; height: 14px; background: rgba(255,255,255,0.4); }
  @keyframes floatUp {
    0% { transform: translateY(0) rotate(0deg); opacity: 0.9; }
    20% { opacity: 1; }
    100% { transform: translateY(-110vh) rotate(20deg); opacity: 0; }
  }
</style>
</head>
<body>
<div class="header">
  <h1><span>&#9679;</span> Live Fraud Queue</h1>
  <div>
    <span class="live-dot"></span>
    <span style="font-size:12px;color:var(--muted)">Auto-refresh: 10s</span>
    <button class="refresh-btn" onclick="loadQueue()" style="margin-left:12px">Refresh Now</button>
  </div>
</div>
<div class="stats-bar" id="stats"></div>
<div class="tabs">
  <div class="tab active" id="tab-pending" onclick="switchTab('pending')">
    Pending Queue <span class="badge pending" id="badge-pending">0</span>
  </div>
  <div class="tab" id="tab-reviewed" onclick="switchTab('reviewed')">
    Reviewed <span class="badge done" id="badge-reviewed">0</span>
  </div>
</div>

<div id="pending-section">
  <div class="filters">
    <button class="active" onclick="setFilter(null, this)">All</button>
    <button onclick="setFilter('YELLOW_FLAG', this)">Yellow Flag</button>
    <button onclick="setFilter('BLOCK', this)">Blocked</button>
  </div>
  <div class="queue" id="queue"></div>
</div>

<div id="reviewed-section" style="display:none">
  <div class="reviewed-summary" id="reviewed-summary"></div>
  <div class="review-filters">
    <button class="active" onclick="setReviewFilter(null, this)">All</button>
    <button onclick="setReviewFilter('RELEASE', this)">Released</button>
    <button onclick="setReviewFilter('BLOCK_CONFIRMED', this)">Confirmed Blocks</button>
    <button onclick="setReviewFilter('ESCALATE', this)">Escalated</button>
  </div>
  <div class="queue" id="reviewed-queue"></div>
</div>

<script>
let currentFilter = null;
let currentReviewFilter = null;
let currentTab = 'pending';
const ANALYST_ID = 'analyst_' + Math.random().toString(36).substr(2,6);

function switchTab(tab) {
  currentTab = tab;
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById('tab-'+tab).classList.add('active');
  document.getElementById('pending-section').style.display = tab === 'pending' ? '' : 'none';
  document.getElementById('reviewed-section').style.display = tab === 'reviewed' ? '' : 'none';
  if (tab === 'reviewed') loadReviewed();
}

async function loadStats() {
  try {
    const r = await fetch('/api/queue/stats');
    const s = await r.json();
    document.getElementById('stats').innerHTML = `
      <div class="stat"><div class="label">Pending Review</div><div class="value blue">${s.total_pending}</div></div>
      <div class="stat"><div class="label">Yellow Flagged</div><div class="value yellow">${s.yellow_flagged}</div></div>
      <div class="stat"><div class="label">Blocked</div><div class="value red">${s.blocked}</div></div>
      <div class="stat"><div class="label">Avg Risk Score</div><div class="value">${Math.round(s.avg_risk_score)}</div></div>
      <div class="stat"><div class="label">Oldest (min)</div><div class="value">${Math.round(s.oldest_age_minutes)}</div></div>
    `;
    document.getElementById('badge-pending').textContent = s.total_pending;
  } catch(e) { console.error('Stats error:', e); }
}

function renderTxCard(tx, showActions) {
  const factors = tx.triggered_factors
    ? (typeof tx.triggered_factors === 'string' ? (() => { try { return JSON.parse(tx.triggered_factors); } catch(e) { return {}; } })() : tx.triggered_factors)
    : {};
  const factorTags = Array.isArray(factors)
    ? factors.map(f => `<span class="factor-tag">${f}</span>`).join('')
    : Object.entries(factors).filter(([k,v]) => v).map(([k]) => `<span class="factor-tag">${k}</span>`).join('');

  let decisionHtml = '';
  if (tx.analyst_decision) {
    decisionHtml = `
      <div style="margin-top:12px;padding:10px 14px;background:var(--bg);border-radius:6px;border:1px solid var(--border)">
        <div style="display:flex;justify-content:space-between;align-items:center">
          <div>
            <span class="decision-badge ${tx.analyst_decision}">${tx.analyst_decision.replace('_',' ')}</span>
            <span style="font-size:12px;color:var(--muted);margin-left:8px">by ${tx.analyst_id || 'unknown'}</span>
          </div>
          <div style="font-size:12px;color:var(--muted)">${tx.reviewed_at ? new Date(tx.reviewed_at).toLocaleString() : ''}</div>
        </div>
        ${tx.analyst_notes ? `<div class="analyst-note">${tx.analyst_notes}</div>` : ''}
      </div>`;
  }

  return `
    <div class="tx-card ${tx.risk_level}" style="${tx.analyst_decision === 'RELEASE' ? 'border-left:4px solid var(--green)' : tx.analyst_decision === 'BLOCK_CONFIRMED' ? 'border-left:4px solid var(--red)' : tx.analyst_decision === 'ESCALATE' ? 'border-left:4px solid var(--orange)' : ''}">
      <div class="tx-header" onclick="toggle('${tx.transaction_id}')">
        <div class="left">
          <span class="risk-badge ${tx.automated_action}">${tx.automated_action.replace('_',' ')}</span>
          ${tx.analyst_decision ? `<span class="decision-badge ${tx.analyst_decision}" style="margin-left:-6px">${tx.analyst_decision.replace('_',' ')}</span>` : ''}
          <div>
            <div class="amount">$${Number(tx.amount).toLocaleString('en-US', {minimumFractionDigits:2})}</div>
            <div class="meta">${tx.transaction_type} &bull; ${tx.merchant_name || 'N/A'} &bull; ${tx.user_id}</div>
          </div>
        </div>
        <div style="text-align:right">
          <div class="score ${tx.risk_score>=70?'high':'med'}">${tx.risk_score}</div>
          <div class="meta">${tx.age_minutes ? tx.age_minutes+'m ago' : tx.review_time_minutes ? 'reviewed in '+tx.review_time_minutes+'m' : ''}</div>
        </div>
      </div>
      <div class="tx-details" id="det-${tx.transaction_id}">
        <div class="detail-grid">
          <div class="detail-item"><div class="dl">Location</div>${tx.tx_city}, ${tx.tx_country}</div>
          <div class="detail-item"><div class="dl">Login From</div>${tx.login_city||'N/A'}, ${tx.login_country||'N/A'}</div>
          <div class="detail-item"><div class="dl">Channel</div>${tx.channel||'N/A'}</div>
          <div class="detail-item"><div class="dl">Geo Mismatch</div>${tx.geo_mismatch ? '&#10060; YES' : '&#9989; No'}</div>
          <div class="detail-item"><div class="dl">MFA Changed</div>${tx.mfa_changed ? '&#10060; YES' : '&#9989; No'}</div>
          <div class="detail-item"><div class="dl">Bot Detected</div>${tx.bot_detected ? '&#10060; YES' : '&#9989; No'}</div>
        </div>
        ${factorTags ? `<div class="factors">${factorTags}</div>` : ''}
        ${tx.explanation ? `<div class="explanation">${tx.explanation}</div>` : ''}
        ${decisionHtml}
        ${showActions ? `<div class="actions">
          <button class="btn-release" onclick="event.stopPropagation();decide('${tx.transaction_id}','RELEASE')">Release</button>
          <button class="btn-block" onclick="event.stopPropagation();decide('${tx.transaction_id}','BLOCK_CONFIRMED')">Confirm Block</button>
          <button class="btn-escalate" onclick="event.stopPropagation();decide('${tx.transaction_id}','ESCALATE')">Escalate</button>
        </div>` : ''}
      </div>
    </div>`;
}

async function loadQueue() {
  try {
    let url = '/api/queue?limit=50';
    if (currentFilter) url += `&action=${currentFilter}`;
    const r = await fetch(url);
    const data = await r.json();
    const el = document.getElementById('queue');

    if (!data.transactions.length) {
      el.innerHTML = '<div class="empty">No pending transactions in queue</div>';
      return;
    }
    el.innerHTML = data.transactions.map(tx => renderTxCard(tx, true)).join('');
  } catch(e) {
    document.getElementById('queue').innerHTML = '<div class="empty">Error loading queue. Check Lakebase connection.</div>';
  }
}

async function loadReviewed() {
  try {
    let url = '/api/queue/reviewed?limit=50';
    if (currentReviewFilter) url += `&decision=${currentReviewFilter}`;
    const r = await fetch(url);
    const data = await r.json();
    const el = document.getElementById('reviewed-queue');
    const sum = data.summary || {};

    document.getElementById('badge-reviewed').textContent = sum.total_reviewed || 0;
    document.getElementById('reviewed-summary').innerHTML = `
      <div class="rs-item"><div class="rs-label">Total Reviewed</div><div class="rs-value">${sum.total_reviewed||0}</div></div>
      <div class="rs-item"><div class="rs-label">Released</div><div class="rs-value green">${sum.released||0}</div></div>
      <div class="rs-item"><div class="rs-label">Confirmed Blocks</div><div class="rs-value red">${sum.confirmed||0}</div></div>
      <div class="rs-item"><div class="rs-label">Escalated</div><div class="rs-value orange">${sum.escalated||0}</div></div>
    `;

    if (!data.transactions.length) {
      el.innerHTML = '<div class="empty">No reviewed transactions yet</div>';
      return;
    }
    el.innerHTML = data.transactions.map(tx => renderTxCard(tx, false)).join('');
  } catch(e) {
    document.getElementById('reviewed-queue').innerHTML = '<div class="empty">Error loading reviewed transactions.</div>';
  }
}

function toggle(id) {
  document.getElementById('det-'+id).classList.toggle('open');
}

function setFilter(f, btn) {
  currentFilter = f;
  document.querySelectorAll('.filters button').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  loadQueue();
}

function setReviewFilter(f, btn) {
  currentReviewFilter = f;
  document.querySelectorAll('.review-filters button').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  loadReviewed();
}

function launchBalloons(decision) {
  const colors = {
    RELEASE: ['#22c55e','#4ade80','#86efac','#16a34a','#a3e635'],
    BLOCK_CONFIRMED: ['#ef4444','#f87171','#fca5a5','#dc2626','#fb923c'],
    ESCALATE: ['#f97316','#fb923c','#fdba74','#ea580c','#facc15']
  };
  const palette = colors[decision] || colors.RELEASE;
  for (let i = 0; i < 15; i++) {
    const b = document.createElement('div');
    b.className = 'balloon';
    const size = 20 + Math.random() * 25;
    b.style.width = size + 'px';
    b.style.height = (size * 1.3) + 'px';
    b.style.left = (5 + Math.random() * 90) + 'vw';
    b.style.bottom = '-80px';
    b.style.background = palette[Math.floor(Math.random() * palette.length)];
    b.style.animation = `floatUp ${2 + Math.random() * 2}s ease-out ${Math.random() * 0.5}s forwards`;
    document.body.appendChild(b);
    setTimeout(() => b.remove(), 5000);
  }
}

async function decide(txId, decision) {
  const notes = decision === 'ESCALATE' ? prompt('Escalation notes:') : null;
  try {
    const r = await fetch('/api/queue/decide', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({transaction_id: txId, analyst_id: ANALYST_ID, decision, notes})
    });
    if (r.ok) { launchBalloons(decision); loadQueue(); loadStats(); loadReviewed(); }
    else { const e = await r.json(); alert(e.detail || 'Error'); }
  } catch(e) { alert('Network error'); }
}

// Auto-refresh
loadStats(); loadQueue(); loadReviewed();
setInterval(() => { loadStats(); if (currentTab === 'pending') loadQueue(); else loadReviewed(); }, 10000);
</script>
</body>
</html>"""
