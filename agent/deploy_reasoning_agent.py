# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Fraud Reasoning Agent
# MAGIC Registers the FraudReasoningAgent as an MLflow pyfunc model in Unity Catalog
# MAGIC and creates a Model Serving endpoint.

# COMMAND ----------

# MAGIC %pip install mlflow>=2.12
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import pandas as pd
import json
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Define the FraudReasoningAgent model class

# COMMAND ----------

class FraudReasoningAgent(mlflow.pyfunc.PythonModel):
    """An AI agent that takes transaction + session metadata, computes a risk score,
    and produces a human-readable regulatory-compliant explanation."""

    RISK_FACTORS = {
        "high_amount": {
            "condition": lambda r: r.get("amount", 0) > 10000,
            "weight": 25,
            "explanation": "Transaction amount (${amount:.2f}) exceeds the $10,000 high-value threshold"
        },
        "high_risk_merchant": {
            "condition": lambda r: r.get("merchant_category", "") in ("wire", "crypto", "gambling", "p2p"),
            "weight": 15,
            "explanation": "Merchant category '{merchant_category}' is classified as high-risk"
        },
        "geo_mismatch": {
            "condition": lambda r: r.get("tx_country", "") != r.get("login_country", r.get("tx_country", "")),
            "weight": 20,
            "explanation": "Transaction location ({tx_city}, {tx_country}) differs from login location ({login_city}, {login_country})"
        },
        "mfa_change_then_wire": {
            "condition": lambda r: r.get("mfa_change_flag", False) and r.get("transaction_type", "") == "wire_transfer",
            "weight": 30,
            "explanation": "MFA settings were modified prior to initiating a wire transfer - common account takeover pattern"
        },
        "bot_typing_detected": {
            "condition": lambda r: r.get("typing_speed_ms", 999) < 50,
            "weight": 25,
            "explanation": "Typing cadence ({typing_speed_ms:.0f}ms avg) suggests automated/bot input rather than human interaction"
        },
        "rapid_login_failures": {
            "condition": lambda r: r.get("failed_attempts_before_success", 0) >= 3,
            "weight": 10,
            "explanation": "{failed_attempts_before_success} failed login attempts detected before successful authentication"
        },
        "off_hours_activity": {
            "condition": lambda r: r.get("hour_of_day", 12) < 6 or r.get("hour_of_day", 12) > 22,
            "weight": 5,
            "explanation": "Transaction occurred during off-hours (hour {hour_of_day}), outside normal banking patterns"
        },
        "international_wire": {
            "condition": lambda r: r.get("is_international", False) and r.get("transaction_type", "") == "wire_transfer",
            "weight": 15,
            "explanation": "International wire transfer to {tx_country} flagged for enhanced due diligence"
        },
        "impossible_travel": {
            "condition": lambda r: r.get("distance_miles", 0) > 500 and r.get("time_diff_minutes", 9999) < 60,
            "weight": 30,
            "explanation": "Impossible travel detected: {distance_miles:.0f} miles between events in {time_diff_minutes:.0f} minutes"
        },
    }

    def predict(self, context, model_input):
        """Score transactions and generate explanations.

        Input: DataFrame with transaction + session metadata columns
        Output: DataFrame with risk_score, risk_level, automated_action, explanation
        """
        results = []
        for _, row in model_input.iterrows():
            record = row.to_dict()
            score, triggered_factors, explanations = self._analyze(record)

            risk_level = (
                "CRITICAL" if score >= 70
                else "HIGH" if score >= 40
                else "MEDIUM" if score >= 20
                else "LOW"
            )
            action = (
                "BLOCK" if score >= 70
                else "YELLOW_FLAG" if score >= 40
                else "MONITOR" if score >= 20
                else "ALLOW"
            )

            explanation = self._build_explanation(
                record, score, risk_level, action, triggered_factors, explanations
            )

            results.append({
                "transaction_id": record.get("transaction_id", "unknown"),
                "risk_score": score,
                "risk_level": risk_level,
                "automated_action": action,
                "triggered_factors": json.dumps(triggered_factors),
                "explanation": explanation,
                "analyzed_at": datetime.utcnow().isoformat(),
            })

        return pd.DataFrame(results)

    def _analyze(self, record):
        score = 0
        triggered = []
        explanations = []

        for factor_name, factor in self.RISK_FACTORS.items():
            try:
                if factor["condition"](record):
                    score += factor["weight"]
                    triggered.append(factor_name)
                    try:
                        exp = factor["explanation"].format(**record)
                    except (KeyError, ValueError):
                        exp = factor["explanation"]
                    explanations.append(exp)
            except (KeyError, TypeError):
                continue

        return min(score, 100), triggered, explanations

    def _build_explanation(self, record, score, level, action, factors, explanations):
        tx_id = record.get("transaction_id", "N/A")
        user_id = record.get("user_id", "N/A")
        amount = record.get("amount", 0)
        tx_type = record.get("transaction_type", "N/A")

        lines = [
            f"FRAUD RISK ASSESSMENT - {tx_id}",
            f"{'='*50}",
            f"User: {user_id} | Type: {tx_type} | Amount: ${amount:,.2f}",
            f"Risk Score: {score}/100 | Level: {level} | Action: {action}",
            f"",
            f"RISK FACTORS DETECTED ({len(factors)}):",
        ]

        for i, exp in enumerate(explanations, 1):
            lines.append(f"  {i}. {exp}")

        if not explanations:
            lines.append("  No significant risk factors detected.")

        lines.extend([
            f"",
            f"REGULATORY COMPLIANCE NOTE:",
            f"This assessment was generated by an automated fraud detection system.",
            f"Under GDPR Article 22 and CCPA Section 1798.185, the customer has the",
            f"right to request human review of this automated decision.",
            f"",
            f"Analyzed at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
        ])

        return "\n".join(lines)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Register the model in Unity Catalog

# COMMAND ----------

CATALOG = "infa_migrate_catalog"
SCHEMA = "fraud_models"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.fraud_reasoning_agent"

# Ensure schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

with mlflow.start_run(run_name="fraud_reasoning_agent_v1") as run:
    model = FraudReasoningAgent()

    # Sample input for signature inference
    sample_input = pd.DataFrame([{
        "transaction_id": "TXN-SAMPLE001",
        "user_id": "USR-000001",
        "amount": 15000.0,
        "transaction_type": "wire_transfer",
        "merchant_category": "wire",
        "tx_city": "Lagos",
        "tx_country": "NG",
        "login_city": "New York",
        "login_country": "US",
        "mfa_change_flag": True,
        "typing_speed_ms": 35.0,
        "failed_attempts_before_success": 4,
        "hour_of_day": 3,
        "is_international": True,
        "distance_miles": 0.0,
        "time_diff_minutes": 9999.0,
    }])

    sample_output = model.predict(None, sample_input)
    signature = mlflow.models.infer_signature(sample_input, sample_output)

    model_info = mlflow.pyfunc.log_model(
        artifact_path="fraud_reasoning_agent",
        python_model=model,
        signature=signature,
        registered_model_name=MODEL_NAME,
        pip_requirements=["mlflow>=2.12", "pandas>=2.0"],
    )

    print(f"Model URI: {model_info.model_uri}")
    print(f"Run ID:    {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Get latest model version

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()
versions = client.search_model_versions(f"name='{MODEL_NAME}'")
latest_version = max(v.version for v in versions)
print(f"Latest version: {latest_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create the Model Serving endpoint

# COMMAND ----------

import requests
import time

# Get workspace host and token from notebook context
workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}

ENDPOINT_NAME = "fraud-reasoning-agent"

# Check if endpoint already exists
check_resp = requests.get(
    f"{workspace_url}/api/2.0/serving-endpoints/{ENDPOINT_NAME}",
    headers=headers,
)

endpoint_config = {
    "name": ENDPOINT_NAME,
    "config": {
        "served_entities": [
            {
                "entity_name": MODEL_NAME,
                "entity_version": str(latest_version),
                "workload_size": "Small",
                "scale_to_zero_enabled": True,
            }
        ],
        "auto_capture_config": {
            "catalog_name": CATALOG,
            "schema_name": SCHEMA,
            "table_name_prefix": "fraud_reasoning_agent_logs",
        },
    },
    "tags": [
        {"key": "team", "value": "fraud-ops"},
        {"key": "compliance", "value": "gdpr-ccpa"},
    ],
}

if check_resp.status_code == 200:
    # Update existing endpoint
    print(f"Endpoint '{ENDPOINT_NAME}' exists. Updating config...")
    resp = requests.put(
        f"{workspace_url}/api/2.0/serving-endpoints/{ENDPOINT_NAME}/config",
        headers=headers,
        json=endpoint_config["config"],
    )
else:
    # Create new endpoint
    print(f"Creating endpoint '{ENDPOINT_NAME}'...")
    resp = requests.post(
        f"{workspace_url}/api/2.0/serving-endpoints",
        headers=headers,
        json=endpoint_config,
    )

resp.raise_for_status()
print(f"Endpoint response: {resp.status_code}")
print(json.dumps(resp.json(), indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Wait for endpoint to be ready (optional - can take several minutes)

# COMMAND ----------

print(f"Endpoint '{ENDPOINT_NAME}' is being provisioned.")
print(f"Monitor status at: {workspace_url}/ml/endpoints/{ENDPOINT_NAME}")
print()
print("To test once ready, run:")
print(f"""
import requests, json

payload = {{
    "dataframe_records": [{{
        "transaction_id": "TXN-TEST001",
        "user_id": "USR-999",
        "amount": 25000.0,
        "transaction_type": "wire_transfer",
        "merchant_category": "wire",
        "tx_city": "Lagos",
        "tx_country": "NG",
        "login_city": "New York",
        "login_country": "US",
        "mfa_change_flag": True,
        "typing_speed_ms": 30.0,
        "failed_attempts_before_success": 5,
        "hour_of_day": 2,
        "is_international": True,
        "distance_miles": 5500.0,
        "time_diff_minutes": 15.0,
    }}]
}}

resp = requests.post(
    f"{workspace_url}/serving-endpoints/{ENDPOINT_NAME}/invocations",
    headers={{"Authorization": f"Bearer {{token}}", "Content-Type": "application/json"}},
    json=payload,
)
print(json.dumps(resp.json(), indent=2))
""")
