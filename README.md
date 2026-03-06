# Fraud Triage Agent

![Fraud Triage Agent Architecture](images/architecture.png)

An end-to-end fraud detection and analyst triage system built on Databricks. It combines a Lakeflow (DLT) pipeline for risk scoring, an MLflow-based reasoning agent for plain-English explanations, a Lakebase (serverless Postgres) operational store for sub-second decisions, and a live Databricks App where analysts review, release, or escalate flagged transactions.

## Architecture

```
CSV Data (Volume)
    │
    ▼
┌──────────────────────────────┐
│  Lakeflow DLT Pipeline       │
│  Bronze → Silver → Gold      │
│  (risk scoring, joins,       │
│   PII masking, KPIs)         │
└──────────┬───────────────────┘
           │
     ┌─────┴──────┐
     ▼            ▼
┌─────────┐  ┌──────────────────┐
│ Genie   │  │ Lakebase         │
│ Space   │  │ (Postgres)       │
│ (BI/NL  │  │ real-time triage │
│ queries)│  │ store            │
└─────────┘  └───────┬──────────┘
                     │
                     ▼
            ┌─────────────────┐
            │ Databricks App  │
            │ (FastAPI +      │
            │  Live Fraud     │
            │  Queue UI)      │
            └─────────────────┘

┌──────────────────────────────┐
│ Fraud Reasoning Agent        │
│ (MLflow pyfunc endpoint)     │
│ Generates risk explanations  │
└──────────────────────────────┘
```

## Components

| Directory | Description |
|-----------|-------------|
| `pipeline/` | Lakeflow Declarative (DLT) pipeline — ingests transactions and login logs through Bronze/Silver/Gold layers with risk scoring, geo-mismatch detection, and bot-typing analysis |
| `agent/` | MLflow pyfunc `FraudReasoningAgent` — computes risk scores from 9 weighted factors and generates regulatory-compliant (GDPR/CCPA) explanations |
| `app/` | Databricks App (FastAPI) — live fraud queue UI where analysts review YELLOW_FLAG and BLOCK transactions in real time |
| `lakebase/` | Lakebase (serverless Postgres) schema — operational triage table, session risk tracking, analyst metrics views |
| `genie/` | Certified SQL queries for a Databricks Genie Space — false positive ratio, account takeover rate, impossible travel, bot detection, and more |
| `data/` | Mock CSV datasets (50k transactions, 20k login logs, 200 fraud signatures) |
| `scripts/` | Deployment and utility scripts (catalog setup, data generation, Lakebase sync, full deploy orchestration) |

## Risk Scoring

The agent evaluates 9 risk factors, each with a weighted score (max 100):

| Factor | Weight | Trigger |
|--------|--------|---------|
| Impossible travel | 30 | 500+ miles between events in < 60 min |
| MFA change + wire | 30 | MFA modified before wire transfer |
| High amount | 25 | Transaction > $10,000 |
| Bot typing | 25 | Typing cadence < 50ms avg |
| Geo mismatch | 20 | Transaction and login in different countries |
| High-risk merchant | 15 | Wire, crypto, gambling, or P2P category |
| International wire | 15 | International wire transfer |
| Rapid login failures | 10 | 3+ failed attempts before success |
| Off-hours activity | 5 | Transaction outside 6am–10pm |

**Automated actions:** BLOCK (score >= 70), YELLOW_FLAG (>= 40), MONITOR (>= 20), ALLOW (< 20)

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks CLI configured with a profile (default: `fe-vm-infa-migrate`)
- Lakebase (serverless Postgres) enabled on the workspace
- Python 3.10+

## Quick Start

### 1. Generate mock data

```bash
python scripts/generate_mock_data.py
```

### 2. Deploy everything

The master deployment script handles catalog creation, data upload, Lakebase setup, DLT pipeline, and app deployment:

```bash
python scripts/deploy_all.py
```

### 3. Post-deployment steps

1. Verify the DLT pipeline completes in the Databricks UI
2. Sync scored data to Lakebase:
   ```bash
   python scripts/upsert_to_lakebase.py <warehouse_id>
   ```
3. Configure a Genie Space with queries from `genie/certified_queries.sql`
4. Register the reasoning agent:
   ```bash
   python agent/reasoning_agent.py
   ```
5. Open the Live Fraud Queue app and start triaging

## App UI

The Live Fraud Queue app provides:

- **Stats bar** — pending count, yellow-flagged, blocked, avg risk score, oldest item age
- **Pending queue** — sortable by risk score, expandable cards with full explanation, geo/MFA/bot indicators
- **Analyst actions** — Release, Confirm Block, or Escalate with notes
- **Reviewed tab** — audit trail of all analyst decisions with performance metrics
- **Auto-refresh** — 10-second polling for new flagged transactions

## Genie Space Queries

Pre-built certified queries for natural language BI:

- False positive ratio by hour
- Account takeover detection rate
- Wire transfers after MFA change (> $10k)
- Risk distribution dashboard
- Top risky users
- Bot detection analysis
- Cross-channel fraud patterns
- Impossible travel candidates

## Tech Stack

- **Databricks Lakeflow (DLT)** — medallion architecture pipeline
- **Unity Catalog** — governance, lineage, model registry
- **Lakebase** — serverless Postgres for operational serving
- **MLflow** — model packaging and serving
- **Databricks Apps** — FastAPI hosting with SDK auth
- **Genie Space** — natural language BI queries
