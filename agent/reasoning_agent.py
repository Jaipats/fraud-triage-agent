"""
Fraud Reasoning Agent - Analyzes transaction metadata and provides
plain-English explanations for risk scores.

Designed to be deployed as a Databricks Model Serving endpoint using
the MLflow pyfunc interface with Foundation Model API.
"""

import mlflow
import pandas as pd
import json
from datetime import datetime


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


# --- Registration script ---
def register_model():
    """Register the FraudReasoningAgent as an MLflow model."""
    import mlflow

    mlflow.set_registry_uri("databricks-uc")

    with mlflow.start_run(run_name="fraud_reasoning_agent_v1"):
        model = FraudReasoningAgent()

        # Sample input for signature
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
        }])

        signature = mlflow.models.infer_signature(
            sample_input,
            model.predict(None, sample_input)
        )

        mlflow.pyfunc.log_model(
            artifact_path="fraud_reasoning_agent",
            python_model=model,
            signature=signature,
            registered_model_name="infa_migrate_catalog.fraud_models.fraud_reasoning_agent",
        )

    print("Model registered successfully!")


if __name__ == "__main__":
    register_model()
