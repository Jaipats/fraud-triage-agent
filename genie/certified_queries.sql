-- Certified SQL Queries for Databricks Genie Space
-- Banking KPIs: False Positive Ratio, Account Takeover Rate, Fraud Detection Metrics
-- Use these when configuring the Genie Space for the Financial_Security catalog.

-- ============================================================
-- 1. FALSE POSITIVE RATIO (FPR) - Key Banking KPI
-- "What is our false positive ratio by hour?"
-- ============================================================
-- [Title: False Positive Ratio by Hour]
-- [Description: Shows the ratio of legitimate transactions incorrectly flagged as fraud. Target: <10%]
SELECT
    date_trunc('hour', tx_timestamp) AS hour,
    COUNT(*) AS total_flagged,
    SUM(CASE WHEN automated_action IN ('BLOCK', 'YELLOW_FLAG') AND is_flagged = false THEN 1 ELSE 0 END) AS false_positives,
    SUM(CASE WHEN automated_action IN ('BLOCK', 'YELLOW_FLAG') AND is_flagged = true THEN 1 ELSE 0 END) AS true_positives,
    ROUND(
        SUM(CASE WHEN automated_action IN ('BLOCK', 'YELLOW_FLAG') AND is_flagged = false THEN 1 ELSE 0 END) * 100.0 /
        NULLIF(SUM(CASE WHEN automated_action IN ('BLOCK', 'YELLOW_FLAG') THEN 1 ELSE 0 END), 0),
    2) AS false_positive_rate_pct
FROM infa_migrate_catalog.fraud_gold.gold_user_session_risk
WHERE automated_action IN ('BLOCK', 'YELLOW_FLAG')
GROUP BY 1
ORDER BY 1 DESC;


-- ============================================================
-- 2. ACCOUNT TAKEOVER RATE
-- "What is our account takeover detection rate?"
-- ============================================================
-- [Title: Account Takeover Rate]
-- [Description: Percentage of logins showing account takeover indicators (MFA change + geo mismatch)]
SELECT
    date_trunc('day', tx_timestamp) AS day,
    COUNT(DISTINCT user_id) AS total_users,
    COUNT(DISTINCT CASE WHEN mfa_change_flag = true AND geo_mismatch = true THEN user_id END) AS ato_suspected_users,
    ROUND(
        COUNT(DISTINCT CASE WHEN mfa_change_flag = true AND geo_mismatch = true THEN user_id END) * 100.0 /
        NULLIF(COUNT(DISTINCT user_id), 0),
    2) AS ato_rate_pct
FROM infa_migrate_catalog.fraud_gold.gold_user_session_risk
GROUP BY 1
ORDER BY 1 DESC;


-- ============================================================
-- 3. WIRE TRANSFERS AFTER MFA CHANGE (Key Investigator Query)
-- "Show me all wire transfers over $10k where the user changed MFA in last 24 hours"
-- ============================================================
-- [Title: High-Value Wire Transfers After MFA Change]
-- [Description: Critical alert - wire transfers >$10k following MFA modification within 24 hours]
SELECT
    transaction_id,
    user_id,
    wire_timestamp,
    amount,
    merchant_name,
    wire_city,
    wire_country,
    mfa_change_timestamp,
    new_mfa_method,
    mfa_change_city,
    mfa_change_country,
    ROUND(seconds_between / 60.0, 1) AS minutes_between_events
FROM infa_migrate_catalog.fraud_gold.gold_wire_after_mfa_change
ORDER BY wire_timestamp DESC;


-- ============================================================
-- 4. RISK DISTRIBUTION DASHBOARD
-- "How are our transactions distributed across risk levels?"
-- ============================================================
-- [Title: Transaction Risk Distribution]
-- [Description: Breakdown of transactions by risk level and automated action]
SELECT
    risk_level,
    automated_action,
    COUNT(*) AS transaction_count,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(risk_score), 1) AS avg_risk_score,
    ROUND(AVG(amount), 2) AS avg_transaction_amount,
    MIN(tx_timestamp) AS earliest,
    MAX(tx_timestamp) AS latest
FROM infa_migrate_catalog.fraud_gold.gold_user_session_risk
GROUP BY risk_level, automated_action
ORDER BY
    CASE risk_level WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END;


-- ============================================================
-- 5. TOP RISKY USERS
-- "Who are our highest risk users?"
-- ============================================================
-- [Title: Top 20 Highest Risk Users]
-- [Description: Users ranked by cumulative risk score and suspicious activity indicators]
SELECT
    user_id,
    COUNT(*) AS total_transactions,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(risk_score), 1) AS avg_risk_score,
    MAX(risk_score) AS max_risk_score,
    SUM(CASE WHEN geo_mismatch THEN 1 ELSE 0 END) AS geo_mismatches,
    SUM(CASE WHEN mfa_change_flag THEN 1 ELSE 0 END) AS mfa_changes,
    SUM(CASE WHEN is_bot_typing THEN 1 ELSE 0 END) AS bot_typing_events,
    SUM(CASE WHEN automated_action = 'BLOCK' THEN 1 ELSE 0 END) AS blocked_count
FROM infa_migrate_catalog.fraud_gold.gold_user_session_risk
GROUP BY user_id
HAVING AVG(risk_score) > 20
ORDER BY avg_risk_score DESC
LIMIT 20;


-- ============================================================
-- 6. BOT DETECTION ANALYSIS
-- "How many transactions show bot-like typing patterns?"
-- ============================================================
-- [Title: Bot Detection - Typing Cadence Analysis]
-- [Description: Transactions where typing speed suggests automated input (<50ms)]
SELECT
    date_trunc('day', tx_timestamp) AS day,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_bot_typing THEN 1 ELSE 0 END) AS bot_detected_count,
    ROUND(AVG(CASE WHEN is_bot_typing THEN typing_speed_ms END), 1) AS avg_bot_typing_ms,
    ROUND(AVG(CASE WHEN NOT is_bot_typing THEN typing_speed_ms END), 1) AS avg_human_typing_ms,
    ROUND(
        SUM(CASE WHEN is_bot_typing THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
    2) AS bot_rate_pct
FROM infa_migrate_catalog.fraud_gold.gold_user_session_risk
WHERE typing_speed_ms IS NOT NULL
GROUP BY 1
ORDER BY 1 DESC;


-- ============================================================
-- 7. CROSS-CHANNEL FRAUD PATTERNS
-- "Show cross-channel fraud: mobile login then web wire transfer"
-- ============================================================
-- [Title: Cross-Channel Fraud Patterns]
-- [Description: Cases where login channel differs from transaction channel within 1 hour]
SELECT
    transaction_id,
    user_id,
    login_timestamp,
    login_device,
    login_city,
    tx_timestamp,
    tx_channel,
    tx_city,
    amount,
    transaction_type,
    risk_score,
    automated_action
FROM infa_migrate_catalog.fraud_gold.gold_user_session_risk
WHERE login_device IS NOT NULL
  AND tx_channel != login_device
  AND amount > 5000
ORDER BY risk_score DESC
LIMIT 50;


-- ============================================================
-- 8. IMPOSSIBLE TRAVEL CANDIDATES
-- "Find users who logged in from locations far apart in a short time"
-- ============================================================
-- [Title: Impossible Travel Detection]
-- [Description: Users with login/transaction location mismatches suggesting impossible travel]
SELECT
    user_id,
    login_city,
    login_country,
    tx_city,
    tx_country,
    login_timestamp,
    tx_timestamp,
    ROUND(EXTRACT(EPOCH FROM (tx_timestamp - login_timestamp)) / 60, 1) AS minutes_between,
    amount,
    risk_score,
    automated_action
FROM infa_migrate_catalog.fraud_gold.gold_user_session_risk
WHERE geo_mismatch = true
  AND login_country != tx_country
  AND tx_timestamp > login_timestamp
ORDER BY risk_score DESC
LIMIT 50;
