-- Lakebase (Serverless Postgres) Schema for Real-Time Fraud Triage
-- This is the "Operational Triage Store" for sub-second blocking decisions.

-- Main triage table: stores active risk assessments for high-concurrency access
CREATE TABLE IF NOT EXISTS real_time_fraud_triage (
    transaction_id   VARCHAR(50) PRIMARY KEY,
    user_id          VARCHAR(20) NOT NULL,
    amount           DECIMAL(15,2) NOT NULL,
    currency         VARCHAR(3) DEFAULT 'USD',
    transaction_type VARCHAR(30),
    merchant_name    VARCHAR(100),
    channel          VARCHAR(20),
    risk_score       INTEGER NOT NULL CHECK (risk_score BETWEEN 0 AND 100),
    risk_level       VARCHAR(10) NOT NULL CHECK (risk_level IN ('LOW','MEDIUM','HIGH','CRITICAL')),
    automated_action VARCHAR(15) NOT NULL CHECK (automated_action IN ('ALLOW','MONITOR','YELLOW_FLAG','BLOCK')),
    triggered_factors JSONB DEFAULT '[]',
    explanation      TEXT,
    tx_city          VARCHAR(50),
    tx_country       VARCHAR(5),
    login_city       VARCHAR(50),
    login_country    VARCHAR(5),
    geo_mismatch     BOOLEAN DEFAULT FALSE,
    mfa_changed      BOOLEAN DEFAULT FALSE,
    bot_detected     BOOLEAN DEFAULT FALSE,
    analyst_id       VARCHAR(50),
    analyst_decision VARCHAR(15) CHECK (analyst_decision IN ('APPROVE','RELEASE','ESCALATE','BLOCK_CONFIRMED')),
    analyst_notes    TEXT,
    reviewed_at      TIMESTAMP,
    created_at       TIMESTAMP DEFAULT NOW(),
    updated_at       TIMESTAMP DEFAULT NOW(),
    expires_at       TIMESTAMP DEFAULT (NOW() + INTERVAL '24 hours')
);

-- Index for fast lookups by action status (the fraud queue)
CREATE INDEX IF NOT EXISTS idx_triage_action ON real_time_fraud_triage(automated_action)
    WHERE automated_action IN ('YELLOW_FLAG', 'BLOCK');

-- Index for user-level queries
CREATE INDEX IF NOT EXISTS idx_triage_user ON real_time_fraud_triage(user_id);

-- Index for time-based expiry cleanup
CREATE INDEX IF NOT EXISTS idx_triage_expires ON real_time_fraud_triage(expires_at);

-- Index for analyst review queue
CREATE INDEX IF NOT EXISTS idx_triage_unreviewed ON real_time_fraud_triage(automated_action, analyst_decision)
    WHERE analyst_decision IS NULL AND automated_action = 'YELLOW_FLAG';

-- Session risk tracking for impossible travel detection
CREATE TABLE IF NOT EXISTS user_session_risk (
    session_id       VARCHAR(50) PRIMARY KEY,
    user_id          VARCHAR(20) NOT NULL,
    login_timestamp  TIMESTAMP NOT NULL,
    ip_address_hash  VARCHAR(64),
    city             VARCHAR(50),
    country          VARCHAR(5),
    latitude         DECIMAL(8,4),
    longitude        DECIMAL(8,4),
    device_type      VARCHAR(30),
    typing_speed_ms  DECIMAL(6,1),
    mfa_change_flag  BOOLEAN DEFAULT FALSE,
    is_bot_typing    BOOLEAN DEFAULT FALSE,
    risk_score       INTEGER DEFAULT 0,
    created_at       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_session_user ON user_session_risk(user_id, login_timestamp DESC);

-- View: Active fraud queue for the Databricks App
CREATE OR REPLACE VIEW fraud_queue AS
SELECT
    transaction_id,
    user_id,
    amount,
    transaction_type,
    merchant_name,
    risk_score,
    risk_level,
    automated_action,
    triggered_factors,
    explanation,
    tx_city,
    tx_country,
    geo_mismatch,
    mfa_changed,
    bot_detected,
    created_at,
    EXTRACT(EPOCH FROM (NOW() - created_at)) AS age_seconds
FROM real_time_fraud_triage
WHERE automated_action IN ('YELLOW_FLAG', 'BLOCK')
  AND analyst_decision IS NULL
  AND expires_at > NOW()
ORDER BY risk_score DESC, created_at ASC;

-- View: Analyst performance metrics
CREATE OR REPLACE VIEW analyst_metrics AS
SELECT
    analyst_id,
    COUNT(*) AS total_reviewed,
    COUNT(*) FILTER (WHERE analyst_decision = 'RELEASE') AS released,
    COUNT(*) FILTER (WHERE analyst_decision = 'BLOCK_CONFIRMED') AS confirmed_blocks,
    COUNT(*) FILTER (WHERE analyst_decision = 'ESCALATE') AS escalated,
    AVG(EXTRACT(EPOCH FROM (reviewed_at - created_at))) AS avg_review_time_seconds
FROM real_time_fraud_triage
WHERE analyst_decision IS NOT NULL
GROUP BY analyst_id;
