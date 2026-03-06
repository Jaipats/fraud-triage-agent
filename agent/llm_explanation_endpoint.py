# Databricks notebook source
# MAGIC %md
# MAGIC # LLM-Powered Fraud Explanation Endpoint (Alternative Approach)
# MAGIC
# MAGIC Uses the Foundation Model API (`databricks-claude-sonnet-4-5`) to generate
# MAGIC rich, natural-language explanations for fraud decisions.
# MAGIC
# MAGIC **Architecture:** Rule-based scoring (fast, deterministic) + LLM explanation
# MAGIC (rich, human-readable for GDPR/CCPA Article 22 compliance).

# COMMAND ----------

# MAGIC %pip install mlflow>=2.12 openai>=1.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import pandas as pd
import json
from datetime import datetime
from openai import OpenAI

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Define the LLM-enhanced Fraud Reasoning Agent

# COMMAND ----------

class FraudReasoningAgentLLM(mlflow.pyfunc.PythonModel):
    """Fraud reasoning agent that combines deterministic rule scoring with
    LLM-generated natural-language explanations via Foundation Model API.

    The rule engine produces a score and list of triggered factors.
    The LLM (databricks-claude-sonnet-4-5) then synthesizes these into a
    compliance-ready narrative explanation.
    """

    RISK_FACTORS = {
        "high_amount": {
            "condition": lambda r: r.get("amount", 0) > 10000,
            "weight": 25,
            "label": "High-value transaction",
            "detail": lambda r: f"Amount ${r.get('amount', 0):,.2f} exceeds $10,000 threshold",
        },
        "high_risk_merchant": {
            "condition": lambda r: r.get("merchant_category", "") in ("wire", "crypto", "gambling", "p2p"),
            "weight": 15,
            "label": "High-risk merchant category",
            "detail": lambda r: f"Category: {r.get('merchant_category', 'unknown')}",
        },
        "geo_mismatch": {
            "condition": lambda r: r.get("tx_country", "") != r.get("login_country", r.get("tx_country", "")),
            "weight": 20,
            "label": "Geographic mismatch",
            "detail": lambda r: f"Transaction in {r.get('tx_city', '?')}, {r.get('tx_country', '?')} but login from {r.get('login_city', '?')}, {r.get('login_country', '?')}",
        },
        "mfa_change_then_wire": {
            "condition": lambda r: r.get("mfa_change_flag", False) and r.get("transaction_type", "") == "wire_transfer",
            "weight": 30,
            "label": "MFA change before wire transfer",
            "detail": lambda r: "MFA settings modified prior to wire transfer initiation",
        },
        "bot_typing_detected": {
            "condition": lambda r: r.get("typing_speed_ms", 999) < 50,
            "weight": 25,
            "label": "Bot-like typing pattern",
            "detail": lambda r: f"Typing speed: {r.get('typing_speed_ms', 0):.0f}ms avg (human baseline >80ms)",
        },
        "rapid_login_failures": {
            "condition": lambda r: r.get("failed_attempts_before_success", 0) >= 3,
            "weight": 10,
            "label": "Multiple failed login attempts",
            "detail": lambda r: f"{r.get('failed_attempts_before_success', 0)} failed attempts before success",
        },
        "off_hours_activity": {
            "condition": lambda r: r.get("hour_of_day", 12) < 6 or r.get("hour_of_day", 12) > 22,
            "weight": 5,
            "label": "Off-hours activity",
            "detail": lambda r: f"Transaction at hour {r.get('hour_of_day', 0)} (outside 06:00-22:00)",
        },
        "international_wire": {
            "condition": lambda r: r.get("is_international", False) and r.get("transaction_type", "") == "wire_transfer",
            "weight": 15,
            "label": "International wire transfer",
            "detail": lambda r: f"Wire to {r.get('tx_country', 'unknown')}",
        },
        "impossible_travel": {
            "condition": lambda r: r.get("distance_miles", 0) > 500 and r.get("time_diff_minutes", 9999) < 60,
            "weight": 30,
            "label": "Impossible travel",
            "detail": lambda r: f"{r.get('distance_miles', 0):.0f} miles in {r.get('time_diff_minutes', 0):.0f} minutes",
        },
    }

    SYSTEM_PROMPT = """You are a fraud analyst AI. Given a transaction's metadata and the risk factors
that were triggered by the rule engine, write a clear, professional explanation of WHY
this transaction was flagged and what action was taken.

Your explanation must:
1. Be written in plain English that a customer or regulator can understand
2. Reference specific data points from the transaction
3. Explain the reasoning chain - how the factors combine to indicate risk
4. Note the automated action taken and why
5. Include a GDPR/CCPA compliance statement about the right to human review
6. Be 150-300 words

Format the response as a structured assessment with sections:
- SUMMARY (one sentence)
- RISK FACTORS ANALYSIS (numbered list with reasoning)
- DECISION (action taken and justification)
- CUSTOMER RIGHTS (compliance notice)"""

    def load_context(self, context):
        """Initialize the Foundation Model API client."""
        import os
        # On Databricks Model Serving, the token is available as an env var
        self.llm_client = OpenAI(
            api_key=os.environ.get("DATABRICKS_TOKEN", ""),
            base_url=os.environ.get("DATABRICKS_HOST", "") + "/serving-endpoints",
        )
        self.llm_model = "databricks-claude-sonnet-4-5"

    def predict(self, context, model_input):
        results = []
        for _, row in model_input.iterrows():
            record = row.to_dict()
            score, triggered, factor_details = self._score(record)

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

            # Generate LLM explanation
            explanation = self._generate_explanation(
                record, score, risk_level, action, triggered, factor_details
            )

            results.append({
                "transaction_id": record.get("transaction_id", "unknown"),
                "risk_score": score,
                "risk_level": risk_level,
                "automated_action": action,
                "triggered_factors": json.dumps(triggered),
                "explanation": explanation,
                "analyzed_at": datetime.utcnow().isoformat(),
            })

        return pd.DataFrame(results)

    def _score(self, record):
        score = 0
        triggered = []
        details = []

        for name, factor in self.RISK_FACTORS.items():
            try:
                if factor["condition"](record):
                    score += factor["weight"]
                    triggered.append(name)
                    try:
                        detail = factor["detail"](record)
                    except Exception:
                        detail = factor["label"]
                    details.append(f"{factor['label']}: {detail} (weight: {factor['weight']})")
            except (KeyError, TypeError):
                continue

        return min(score, 100), triggered, details

    def _generate_explanation(self, record, score, level, action, triggered, details):
        """Call the Foundation Model API to generate a natural-language explanation."""
        user_msg = f"""Transaction to analyze:
- Transaction ID: {record.get('transaction_id', 'N/A')}
- User ID: {record.get('user_id', 'N/A')}
- Amount: ${record.get('amount', 0):,.2f}
- Type: {record.get('transaction_type', 'N/A')}
- Merchant Category: {record.get('merchant_category', 'N/A')}
- Transaction Location: {record.get('tx_city', 'N/A')}, {record.get('tx_country', 'N/A')}
- Login Location: {record.get('login_city', 'N/A')}, {record.get('login_country', 'N/A')}
- MFA Changed: {record.get('mfa_change_flag', False)}
- Typing Speed: {record.get('typing_speed_ms', 'N/A')}ms
- Failed Login Attempts: {record.get('failed_attempts_before_success', 0)}
- Hour of Day: {record.get('hour_of_day', 'N/A')}
- International: {record.get('is_international', False)}

Rule Engine Results:
- Risk Score: {score}/100
- Risk Level: {level}
- Automated Action: {action}
- Triggered Factors ({len(triggered)}):
"""
        for d in details:
            user_msg += f"  - {d}\n"

        if not details:
            user_msg += "  (none)\n"

        try:
            response = self.llm_client.chat.completions.create(
                model=self.llm_model,
                messages=[
                    {"role": "system", "content": self.SYSTEM_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
                max_tokens=800,
                temperature=0.2,
            )
            return response.choices[0].message.content
        except Exception as e:
            # Fallback to template-based explanation if LLM is unavailable
            return self._fallback_explanation(record, score, level, action, details)

    def _fallback_explanation(self, record, score, level, action, details):
        """Deterministic fallback if the LLM call fails."""
        lines = [
            f"FRAUD RISK ASSESSMENT - {record.get('transaction_id', 'N/A')}",
            f"{'='*50}",
            f"User: {record.get('user_id', 'N/A')} | Amount: ${record.get('amount', 0):,.2f}",
            f"Score: {score}/100 | Level: {level} | Action: {action}",
            "",
            "RISK FACTORS:",
        ]
        for i, d in enumerate(details, 1):
            lines.append(f"  {i}. {d}")
        if not details:
            lines.append("  No risk factors detected.")
        lines.extend([
            "",
            "NOTE: LLM-generated explanation unavailable; showing rule-based summary.",
            "Under GDPR Art. 22 / CCPA Sec. 1798.185, you may request human review.",
        ])
        return "\n".join(lines)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Register LLM-enhanced model in Unity Catalog

# COMMAND ----------

CATALOG = "infa_migrate_catalog"
SCHEMA = "fraud_models"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.fraud_reasoning_agent_llm"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

with mlflow.start_run(run_name="fraud_reasoning_agent_llm_v1") as run:
    model = FraudReasoningAgentLLM()

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

    # Use a mock output for signature since we can't call the LLM at registration time
    sample_output = pd.DataFrame([{
        "transaction_id": "TXN-SAMPLE001",
        "risk_score": 100,
        "risk_level": "CRITICAL",
        "automated_action": "BLOCK",
        "triggered_factors": json.dumps(["high_amount", "high_risk_merchant"]),
        "explanation": "Sample explanation placeholder",
        "analyzed_at": datetime.utcnow().isoformat(),
    }])

    signature = mlflow.models.infer_signature(sample_input, sample_output)

    model_info = mlflow.pyfunc.log_model(
        artifact_path="fraud_reasoning_agent_llm",
        python_model=model,
        signature=signature,
        registered_model_name=MODEL_NAME,
        pip_requirements=["mlflow>=2.12", "pandas>=2.0", "openai>=1.0"],
    )

    print(f"Model URI: {model_info.model_uri}")
    print(f"Run ID:    {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Serving Endpoint with Foundation Model API dependency

# COMMAND ----------

import requests

workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}

# Get latest version
from mlflow import MlflowClient
client = MlflowClient()
versions = client.search_model_versions(f"name='{MODEL_NAME}'")
latest_version = max(v.version for v in versions)
print(f"Deploying version: {latest_version}")

# COMMAND ----------

ENDPOINT_NAME = "fraud-reasoning-agent-llm"

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
                "environment_vars": {
                    "DATABRICKS_HOST": "{{secrets/fraud-triage/databricks-host}}",
                    "DATABRICKS_TOKEN": "{{secrets/fraud-triage/databricks-token}}",
                },
            }
        ],
        "auto_capture_config": {
            "catalog_name": CATALOG,
            "schema_name": SCHEMA,
            "table_name_prefix": "fraud_reasoning_llm_logs",
        },
    },
    "tags": [
        {"key": "team", "value": "fraud-ops"},
        {"key": "compliance", "value": "gdpr-ccpa"},
        {"key": "llm-backend", "value": "databricks-claude-sonnet-4-5"},
    ],
}

if check_resp.status_code == 200:
    print(f"Updating existing endpoint '{ENDPOINT_NAME}'...")
    resp = requests.put(
        f"{workspace_url}/api/2.0/serving-endpoints/{ENDPOINT_NAME}/config",
        headers=headers,
        json=endpoint_config["config"],
    )
else:
    print(f"Creating endpoint '{ENDPOINT_NAME}'...")
    resp = requests.post(
        f"{workspace_url}/api/2.0/serving-endpoints",
        headers=headers,
        json=endpoint_config,
    )

resp.raise_for_status()
print(f"Status: {resp.status_code}")
print(json.dumps(resp.json(), indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Setup instructions
# MAGIC
# MAGIC **Before running this notebook**, create the required Databricks secrets:
# MAGIC ```bash
# MAGIC databricks secrets create-scope fraud-triage
# MAGIC databricks secrets put-secret fraud-triage databricks-host --string-value "https://<workspace-url>"
# MAGIC databricks secrets put-secret fraud-triage databricks-token --string-value "<your-pat>"
# MAGIC ```
# MAGIC
# MAGIC **Alternative**: If your workspace supports AI Gateway / automatic auth passthrough
# MAGIC for Foundation Model API endpoints, you can remove the `environment_vars` block
# MAGIC from the endpoint config and the model will authenticate automatically.

# COMMAND ----------

print(f"""
DEPLOYMENT SUMMARY
==================
Two fraud reasoning endpoints are now available:

1. fraud-reasoning-agent (rule-based, deterministic)
   - Fast, no LLM dependency
   - Template-based explanations
   - Model: {CATALOG}.{SCHEMA}.fraud_reasoning_agent

2. fraud-reasoning-agent-llm (rule-based + LLM explanations)
   - Uses databricks-claude-sonnet-4-5 for rich narratives
   - Falls back to templates if LLM unavailable
   - Model: {MODEL_NAME}

Both endpoints produce GDPR/CCPA-compliant explanations.
Monitor at: {workspace_url}/ml/endpoints
""")
