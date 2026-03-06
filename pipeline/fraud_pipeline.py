"""
Lakeflow Spark Declarative Pipeline (DLT) for Fraud Triage Agent.
Ingests mock transaction and login data from Volume into Silver tables,
joining session biometrics with transaction value for fraud detection.

Deploy as a DLT pipeline in Databricks.
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "infa_migrate_catalog"
VOLUME_PATH = f"/Volumes/{CATALOG}/fraud_bronze/raw_data"

# ============================================================
# BRONZE LAYER - Raw Ingestion
# ============================================================

@dlt.table(
    name="bronze_transactions",
    comment="Raw transaction data ingested from CSV volume",
    table_properties={"quality": "bronze"}
)
def bronze_transactions():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{VOLUME_PATH}/transactions.csv")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


@dlt.table(
    name="bronze_login_logs",
    comment="Raw login log data ingested from CSV volume",
    table_properties={"quality": "bronze"}
)
def bronze_login_logs():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{VOLUME_PATH}/login_logs.csv")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


# ============================================================
# SILVER LAYER - Cleaned + Enriched
# ============================================================

@dlt.table(
    name="silver_transactions",
    comment="Cleaned transactions with masked PII and derived features",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_amount", "amount > 0")
@dlt.expect_or_drop("valid_user", "user_id IS NOT NULL")
def silver_transactions():
    return (
        dlt.read("bronze_transactions")
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("card_number_masked", F.concat(F.lit("****-****-****-"), F.substring("card_number", -4, 4)))
        .withColumn("amount_bucket", F.when(F.col("amount") < 100, "low")
                     .when(F.col("amount") < 1000, "medium")
                     .when(F.col("amount") < 10000, "high")
                     .otherwise("very_high"))
        .withColumn("hour_of_day", F.hour("timestamp"))
        .withColumn("day_of_week", F.dayofweek("timestamp"))
        .withColumn("is_high_risk_merchant", F.col("merchant_category").isin("wire", "crypto", "gambling", "p2p"))
        .withColumn("is_off_hours", (F.col("hour_of_day") < 6) | (F.col("hour_of_day") > 22))
        .drop("card_number")  # Remove raw PII
    )


@dlt.table(
    name="silver_login_logs",
    comment="Cleaned login logs with masked IP and anomaly features",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_login_id", "login_id IS NOT NULL")
def silver_login_logs():
    return (
        dlt.read("bronze_login_logs")
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("ip_masked", F.concat(
            F.element_at(F.split("ip_address", "\\."), 1), F.lit("."),
            F.element_at(F.split("ip_address", "\\."), 2), F.lit(".xxx.xxx")
        ))
        .withColumn("is_bot_typing", F.col("typing_speed_ms") < 50)
        .withColumn("is_rapid_fail", F.col("failed_attempts_before_success") >= 3)
        .withColumn("hour_of_day", F.hour("timestamp"))
        .drop("ip_address")  # Remove raw PII
    )


# ============================================================
# GOLD LAYER - Joined + Fraud Signals
# ============================================================

@dlt.table(
    name="gold_user_session_risk",
    comment="User sessions joined with transactions, biometrics scored for fraud risk",
    table_properties={"quality": "gold"}
)
def gold_user_session_risk():
    """Join login sessions with transactions within a time window to detect
    cross-channel fraud patterns (e.g., login from one location, wire from another)."""

    logins = dlt.read("silver_login_logs").alias("l")
    txns = dlt.read("silver_transactions").alias("t")

    # Join: transactions within 1 hour after login
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

    # Compute risk signals
    return (
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


# ============================================================
# GOLD - Aggregated KPIs
# ============================================================

@dlt.table(
    name="gold_fraud_kpis",
    comment="Aggregated fraud detection KPIs for Genie Space",
    table_properties={"quality": "gold"}
)
def gold_fraud_kpis():
    risk = dlt.read("gold_user_session_risk")
    return (
        risk.groupBy(
            F.date_trunc("hour", "tx_timestamp").alias("hour"),
            "risk_level",
            "automated_action"
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


@dlt.table(
    name="gold_wire_after_mfa_change",
    comment="Wire transfers >$10k where user changed MFA in last 24 hours - key Genie query",
    table_properties={"quality": "gold"}
)
def gold_wire_after_mfa_change():
    """Specifically addresses the Genie query: 'Show me all wire transfers over $10k
    where the user changed their MFA settings in the last 24 hours.'"""

    logins = dlt.read("silver_login_logs").alias("l")
    txns = dlt.read("silver_transactions").alias("t")

    mfa_changes = logins.filter(F.col("mfa_change_flag") == True)

    return (
        txns.filter(
            (F.col("transaction_type") == "wire_transfer") &
            (F.col("amount") > 10000)
        ).alias("t")
        .join(
            mfa_changes.alias("m"),
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
