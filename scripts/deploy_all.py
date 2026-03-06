"""
Master deployment script for the Fraud Triage Agent demo.
Orchestrates: Catalog setup -> Data upload -> Lakebase -> DLT Pipeline -> App deploy.
"""

import subprocess
import sys
import os
import json
import time

PROFILE = "fe-vm-infa-migrate"
CATALOG = "financial_security"
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def cli(args, check=True):
    cmd = ["databricks", "--profile", PROFILE] + args
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0 and check:
        print(f"  ERROR: {result.stderr.strip()}")
    if result.stdout.strip():
        print(f"  {result.stdout.strip()[:200]}")
    return result


def step(n, msg):
    print(f"\n{'='*60}")
    print(f"  STEP {n}: {msg}")
    print(f"{'='*60}\n")


def main():
    print("\n" + "="*60)
    print("  FRAUD TRIAGE AGENT - FULL DEPLOYMENT")
    print("="*60)

    # --- Step 1: Create Unity Catalog resources ---
    step(1, "Create Unity Catalog (catalog, schemas, volume)")

    cli(["unity-catalog", "catalogs", "create", "--name", CATALOG,
         "--comment", "Financial Security - Fraud Triage Agent"], check=False)

    for schema in ["bronze", "silver", "gold", "fraud_models"]:
        cli(["unity-catalog", "schemas", "create",
             "--catalog-name", CATALOG, "--name", schema], check=False)

    cli(["unity-catalog", "volumes", "create",
         "--catalog-name", CATALOG, "--schema-name", "bronze",
         "--name", "raw_data", "--volume-type", "MANAGED"], check=False)

    # --- Step 2: Upload mock data ---
    step(2, "Upload mock data to Unity Catalog Volume")

    volume_path = f"/Volumes/{CATALOG}/bronze/raw_data"
    data_dir = os.path.join(PROJECT_ROOT, "data")
    for f in ["transactions.csv", "login_logs.csv", "known_fraud_signatures.csv"]:
        cli(["fs", "cp", os.path.join(data_dir, f), f"dbfs:{volume_path}/{f}", "--overwrite"])

    # --- Step 3: Create Lakebase database ---
    step(3, "Create Lakebase database and apply schema")

    result = cli(["api", "get", "/api/2.0/lakebase/databases"], check=False)
    if result.returncode == 0:
        dbs = json.loads(result.stdout).get("databases", [])
        if not any(d["name"] == "fraud_triage" for d in dbs):
            cli(["api", "post", "/api/2.0/lakebase/databases",
                 "--json", json.dumps({"name": "fraud_triage", "comment": "Fraud Triage operational store"})])
            print("  Waiting for Lakebase provisioning...")
            time.sleep(15)
    else:
        print("  WARN: Could not check Lakebase databases. Create manually if needed.")

    # --- Step 4: Upload DLT pipeline notebook ---
    step(4, "Upload DLT pipeline to workspace")

    workspace_path = f"/Workspace/Users/{get_username()}/fraud_triage_agent"
    cli(["workspace", "mkdirs", workspace_path], check=False)

    pipeline_src = os.path.join(PROJECT_ROOT, "pipeline", "fraud_pipeline.py")
    cli(["workspace", "import", pipeline_src,
         f"{workspace_path}/fraud_pipeline", "--language", "PYTHON", "--overwrite"])

    # --- Step 5: Create DLT Pipeline ---
    step(5, "Create Lakeflow Declarative Pipeline")

    pipeline_config = {
        "name": "fraud_triage_pipeline",
        "catalog": CATALOG,
        "target": "gold",
        "development": True,
        "libraries": [{"notebook": {"path": f"{workspace_path}/fraud_pipeline"}}],
        "configuration": {"pipelines.channel": "CURRENT"},
        "clusters": [{"label": "default", "num_workers": 1}]
    }

    result = cli(["pipelines", "create", "--json", json.dumps(pipeline_config)], check=False)
    if result.returncode == 0:
        pipeline_id = json.loads(result.stdout).get("pipeline_id", "")
        print(f"  Pipeline created: {pipeline_id}")
        cli(["pipelines", "start-update", "--pipeline-id", pipeline_id, "--full-refresh"])
        print("  Pipeline update started.")
    else:
        print("  Pipeline may already exist. Check workspace.")

    # --- Step 6: Deploy Databricks App ---
    step(6, "Deploy Live Fraud Queue App")

    app_dir = os.path.join(PROJECT_ROOT, "app")
    cli(["apps", "create", "--name", "live-fraud-queue",
         "--description", "Live Fraud Triage Queue for analysts"], check=False)
    cli(["apps", "deploy", "live-fraud-queue", "--source-code-path", app_dir])

    print("\n" + "="*60)
    print("  DEPLOYMENT COMPLETE!")
    print("="*60)
    print(f"""
  Resources Created:
    - Catalog:   {CATALOG} (bronze/silver/gold/fraud_models)
    - Volume:    /Volumes/{CATALOG}/bronze/raw_data
    - Lakebase:  fraud_triage (Serverless Postgres)
    - Pipeline:  fraud_triage_pipeline (DLT)
    - App:       live-fraud-queue

  Next Steps:
    1. Verify pipeline completes in the Databricks UI
    2. Run: python3 scripts/upsert_to_lakebase.py <warehouse_id>
    3. Configure Genie Space with queries from genie/certified_queries.sql
    4. Register the reasoning agent: python3 agent/reasoning_agent.py
    5. Open the Live Fraud Queue app and start triaging!
""")


def get_username():
    result = cli(["current-user", "me"], check=False)
    if result.returncode == 0:
        return json.loads(result.stdout).get("userName", "unknown")
    return "unknown"


if __name__ == "__main__":
    main()
