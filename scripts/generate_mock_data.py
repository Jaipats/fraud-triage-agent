"""
Generate mock banking datasets for the Fraud Triage Agent demo.
Creates:
  - transactions.csv: High-velocity transaction data
  - login_logs.csv: Login events with IP, geolocation, MFA changes
  - known_fraud_signatures.csv: Known fraud patterns for vector search
"""

import csv
import random
import uuid
import hashlib
from datetime import datetime, timedelta

random.seed(42)

NUM_USERS = 500
NUM_TRANSACTIONS = 50_000
NUM_LOGINS = 20_000
NUM_FRAUD_SIGNATURES = 200

# --- Helpers ---
MERCHANTS = [
    ("MCH-001", "Amazon", "online_retail"), ("MCH-002", "Walmart", "retail"),
    ("MCH-003", "Shell Gas", "fuel"), ("MCH-004", "Starbucks", "food"),
    ("MCH-005", "Wire Transfer Intl", "wire"), ("MCH-006", "Zelle P2P", "p2p"),
    ("MCH-007", "Venmo", "p2p"), ("MCH-008", "ACH Payroll", "ach"),
    ("MCH-009", "Luxury Watches Co", "luxury"), ("MCH-010", "Crypto Exchange X", "crypto"),
    ("MCH-011", "Foreign ATM", "atm"), ("MCH-012", "Online Casino", "gambling"),
    ("MCH-013", "Travel Agency", "travel"), ("MCH-014", "Electronics Depot", "retail"),
    ("MCH-015", "Offshore Holdings LLC", "wire"),
]

CITIES = [
    ("New York", "US", 40.71, -74.01), ("Los Angeles", "US", 34.05, -118.24),
    ("Chicago", "US", 41.88, -87.63), ("Houston", "US", 29.76, -95.37),
    ("Miami", "US", 25.76, -80.19), ("London", "UK", 51.51, -0.13),
    ("Lagos", "NG", 6.52, 3.38), ("Moscow", "RU", 55.76, 37.62),
    ("Beijing", "CN", 39.90, 116.41), ("Sao Paulo", "BR", -23.55, -46.63),
    ("Mumbai", "IN", 19.08, 72.88), ("Dubai", "AE", 25.20, 55.27),
]

DEVICE_TYPES = ["mobile_ios", "mobile_android", "desktop_chrome", "desktop_firefox", "tablet", "unknown"]
CHANNELS = ["mobile_app", "web", "atm", "branch", "api"]
TX_TYPES = ["purchase", "wire_transfer", "p2p", "ach", "atm_withdrawal", "refund"]

USER_IDS = [f"USR-{str(i).zfill(6)}" for i in range(1, NUM_USERS + 1)]

# Assign each user a "home" city
USER_HOME = {uid: random.choice(CITIES[:5]) for uid in USER_IDS}  # US-based users

# Mark ~5% of users as "fraud actors"
FRAUD_USERS = set(random.sample(USER_IDS, int(NUM_USERS * 0.05)))


def gen_ip(city_idx):
    base = (hash(CITIES[city_idx][0]) % 200) + 10
    return f"{base}.{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}"


def mask_card(user_id):
    h = hashlib.md5(user_id.encode()).hexdigest()[:12]
    return f"****-****-****-{h[:4].upper()}"


# --- Generate Transactions ---
def generate_transactions(filepath):
    now = datetime.now()
    rows = []
    for _ in range(NUM_TRANSACTIONS):
        uid = random.choice(USER_IDS)
        is_fraud = uid in FRAUD_USERS and random.random() < 0.3
        ts = now - timedelta(hours=random.uniform(0, 720))  # last 30 days

        if is_fraud:
            merchant = random.choice(MERCHANTS[4:])  # high-risk merchants
            amount = round(random.uniform(5000, 75000), 2)
            tx_type = random.choice(["wire_transfer", "p2p", "atm_withdrawal"])
            channel = random.choice(["api", "mobile_app"])
        else:
            merchant = random.choice(MERCHANTS[:8])
            amount = round(random.lognormvariate(4.5, 1.2), 2)
            amount = min(amount, 15000)
            tx_type = random.choice(TX_TYPES)
            channel = random.choice(CHANNELS)

        city = random.choice(CITIES) if is_fraud else USER_HOME[uid]

        rows.append({
            "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
            "user_id": uid,
            "card_number": mask_card(uid),
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "amount": amount,
            "currency": "USD",
            "merchant_id": merchant[0],
            "merchant_name": merchant[1],
            "merchant_category": merchant[2],
            "transaction_type": tx_type,
            "channel": channel,
            "city": city[0],
            "country": city[1],
            "latitude": round(city[2] + random.gauss(0, 0.05), 4),
            "longitude": round(city[3] + random.gauss(0, 0.05), 4),
            "device_type": random.choice(DEVICE_TYPES),
            "is_international": city[1] != "US",
            "is_flagged": is_fraud,
        })

    rows.sort(key=lambda r: r["timestamp"])

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"Generated {len(rows)} transactions -> {filepath}")


# --- Generate Login Logs ---
def generate_login_logs(filepath):
    now = datetime.now()
    rows = []
    for _ in range(NUM_LOGINS):
        uid = random.choice(USER_IDS)
        is_fraud = uid in FRAUD_USERS and random.random() < 0.4
        ts = now - timedelta(hours=random.uniform(0, 720))

        home = USER_HOME[uid]
        if is_fraud:
            city = random.choice(CITIES[5:])  # foreign city
            mfa_changed = random.random() < 0.5
            login_success = random.random() < 0.7
            failed_attempts = random.randint(2, 10) if not login_success else random.randint(0, 3)
        else:
            city = home
            mfa_changed = random.random() < 0.02
            login_success = random.random() < 0.97
            failed_attempts = 0 if login_success else random.randint(1, 2)

        ip = gen_ip(CITIES.index(city))
        typing_speed_ms = random.gauss(120, 30) if not is_fraud else random.gauss(45, 15)
        typing_speed_ms = max(20, typing_speed_ms)

        rows.append({
            "login_id": f"LOG-{uuid.uuid4().hex[:12].upper()}",
            "user_id": uid,
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "ip_address": ip,
            "city": city[0],
            "country": city[1],
            "latitude": round(city[2] + random.gauss(0, 0.05), 4),
            "longitude": round(city[3] + random.gauss(0, 0.05), 4),
            "device_type": random.choice(DEVICE_TYPES),
            "user_agent": f"Mozilla/5.0 ({random.choice(['Windows NT 10.0', 'Macintosh', 'Linux', 'iPhone'])})",
            "login_successful": login_success,
            "failed_attempts_before_success": failed_attempts,
            "mfa_change_flag": mfa_changed,
            "mfa_method": random.choice(["sms", "authenticator_app", "email", "hardware_key"]),
            "typing_speed_ms": round(typing_speed_ms, 1),
            "session_duration_min": round(random.uniform(1, 120), 1) if login_success else 0,
            "is_suspicious": is_fraud,
        })

    rows.sort(key=lambda r: r["timestamp"])

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"Generated {len(rows)} login logs -> {filepath}")


# --- Generate Known Fraud Signatures ---
def generate_fraud_signatures(filepath):
    patterns = [
        ("rapid_geo_hop", "User location changes >500 miles within 10 minutes", "high"),
        ("velocity_spike", "Transaction frequency exceeds 5x normal rate in 1 hour", "high"),
        ("mfa_then_wire", "MFA settings changed followed by wire transfer within 24h", "critical"),
        ("new_device_large_tx", "Large transaction from never-seen-before device", "medium"),
        ("midnight_atm", "ATM withdrawal between 1-4 AM in foreign country", "high"),
        ("round_amount_wire", "Wire transfer of exact round amount >$10k to new recipient", "high"),
        ("bot_typing", "Typing cadence <50ms average suggesting automated input", "critical"),
        ("multiple_failed_mfa", "3+ failed MFA attempts followed by successful login", "high"),
        ("ip_tor_exit", "Login from known Tor exit node IP range", "critical"),
        ("account_drain", "Multiple transactions draining >80% of account balance in 1h", "critical"),
        ("sim_swap_pattern", "MFA method changed to SMS after phone number update", "high"),
        ("cross_channel_rapid", "Mobile login + web wire transfer within 5 minutes", "high"),
        ("dormant_reactivation", "Account dormant >90 days suddenly initiates wire transfer", "medium"),
        ("credential_stuffing", "Same IP attempts login on 10+ accounts within 1 hour", "critical"),
        ("card_testing", "Multiple small transactions (<$1) in rapid succession", "medium"),
    ]
    rows = []
    for i in range(NUM_FRAUD_SIGNATURES):
        base = patterns[i % len(patterns)]
        rows.append({
            "signature_id": f"SIG-{str(i+1).zfill(4)}",
            "pattern_name": base[0],
            "description": base[1],
            "severity": base[2],
            "detection_rule": f"WHEN {base[0]} THEN flag(severity='{base[2]}')",
            "false_positive_rate": round(random.uniform(0.01, 0.15), 3),
            "created_date": (datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d"),
            "last_triggered": (datetime.now() - timedelta(hours=random.randint(1, 720))).strftime("%Y-%m-%d %H:%M:%S"),
            "trigger_count_30d": random.randint(5, 500),
            "embedding_text": f"{base[0]}: {base[1]}. Severity: {base[2]}. Common in {random.choice(['online banking', 'mobile banking', 'wire transfers', 'P2P payments'])} fraud.",
        })

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"Generated {len(rows)} fraud signatures -> {filepath}")


if __name__ == "__main__":
    import os
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
    os.makedirs(data_dir, exist_ok=True)
    generate_transactions(os.path.join(data_dir, "transactions.csv"))
    generate_login_logs(os.path.join(data_dir, "login_logs.csv"))
    generate_fraud_signatures(os.path.join(data_dir, "known_fraud_signatures.csv"))
