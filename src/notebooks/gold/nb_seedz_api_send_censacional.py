# Notebook:  nb_seedz_api_send_censacional
# Domain:    ventas
# Layer:     gold (API integration)
# Action:    send
# Entity:    censacional (single /v1/censacional endpoint)
# Source:    lh_ventas_gold.gold_seedz_cens_payload
# Target:    Seedz REST API /v1/censacional
# Run after: nb_ventas_gold_build_cens_payload
#
# Sends Censacional data (billing enriched with equipment/VIN) to Seedz.
# Same batch + retry logic as Superación but single endpoint.
#
# SAP BW equivalent:
#   Process chain: Open Hub → REST API call

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
import json
import time
import requests
from pyspark.sql import functions as F

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_GOLD = "lh_ventas_gold"
SOURCE_TABLE   = "gold_seedz_cens_payload"
ENDPOINT       = "/v1/censacional"

# API settings — TODO: move to Key Vault / Fabric secrets
API_BASE_URL = "https://api-sandbox.seedz.ag"
API_EMAIL    = ""  # TODO: set from Fabric secrets
API_PASSWORD = ""  # TODO: set from Fabric secrets

BATCH_SIZE       = 500
MAX_RETRIES      = 5
INITIAL_BACKOFF  = 1
MAX_BACKOFF      = 60


# ── Cell 3: Authentication ───────────────────────────────────────────────────

def authenticate(base_url: str, email: str, password: str) -> str:
    """Authenticate with Seedz API and return Bearer token."""
    if not email or not password:
        raise ValueError(
            "API credentials not configured. "
            "Set API_EMAIL and API_PASSWORD from Fabric secrets."
        )

    resp = requests.post(
        f"{base_url}/v1/auth/login",
        json={"email": email, "password": password},
        headers={"Content-Type": "application/json"},
        timeout=30,
    )

    if resp.status_code != 200:
        raise RuntimeError(
            f"Authentication failed: {resp.status_code} — {resp.text}"
        )

    token = resp.json().get("token")
    if not token:
        raise RuntimeError("Authentication response missing token field.")

    print("[api:cens] Authenticated successfully.")
    return token


# ── Cell 4: Batch sender ────────────────────────────────────────────────────

def send_batch(
    base_url: str,
    token: str,
    records: list,
) -> dict:
    """
    Send a batch of Censacional records.
    Exponential backoff on 429 (rate limit) and 5xx (server errors).
    """
    url = f"{base_url}{ENDPOINT}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(
                url,
                json=records,
                headers=headers,
                timeout=60,
            )

            if resp.status_code == 200:
                return {"status": "ok", "sent": len(records), "errors": 0}

            if resp.status_code == 401:
                return {"status": "auth_expired", "sent": 0, "errors": len(records)}

            if resp.status_code == 429:
                print(
                    f"  [429] Rate limited. "
                    f"Retry {attempt}/{MAX_RETRIES} in {backoff}s..."
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
                continue

            if resp.status_code == 400:
                print(
                    f"  [400] Bad request: {resp.text[:200]}. "
                    f"Skipping batch of {len(records)} records."
                )
                return {"status": "bad_request", "sent": 0, "errors": len(records)}

            if resp.status_code >= 500:
                print(
                    f"  [{resp.status_code}] Server error. "
                    f"Retry {attempt}/{MAX_RETRIES} in {backoff}s..."
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
                continue

            print(f"  [{resp.status_code}] Unexpected: {resp.text[:200]}")
            return {"status": f"http_{resp.status_code}", "sent": 0, "errors": len(records)}

        except requests.exceptions.Timeout:
            print(
                f"  [timeout] Retry {attempt}/{MAX_RETRIES} in {backoff}s..."
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)
            continue

        except requests.exceptions.ConnectionError as e:
            print(f"  [connection_error] {e}")
            return {"status": "connection_error", "sent": 0, "errors": len(records)}

    return {"status": "max_retries", "sent": 0, "errors": len(records)}


# ── Cell 5: Main ────────────────────────────────────────────────────────────

def run(spark) -> None:
    """Send Censacional payload to Seedz API."""
    print("[api:cens] Starting Censacional API send...")

    full_table = f"{LAKEHOUSE_GOLD}.{SOURCE_TABLE}"

    try:
        df = spark.table(full_table)
    except Exception:
        print(f"[api:cens] Table {full_table} not found — aborting.")
        return

    total_rows = df.count()
    if total_rows == 0:
        print(f"[api:cens] Table {full_table} is empty — nothing to send.")
        return

    print(f"[api:cens] Records to send: {total_rows:,}")

    # Report machinery vs parts breakdown
    machinery = df.filter(F.col("isMachinery") == "true").count()
    print(
        f"[api:cens] Machinery: {machinery:,} | "
        f"Parts: {total_rows - machinery:,}"
    )

    token = authenticate(API_BASE_URL, API_EMAIL, API_PASSWORD)

    all_records = [row.asDict() for row in df.collect()]

    total_sent = 0
    total_errors = 0
    batch_count = 0

    for i in range(0, len(all_records), BATCH_SIZE):
        batch = all_records[i:i + BATCH_SIZE]
        batch_count += 1

        result = send_batch(API_BASE_URL, token, batch)
        total_sent += result["sent"]
        total_errors += result["errors"]

        if result["status"] == "auth_expired":
            print("[api:cens] Token expired — re-authenticating...")
            token = authenticate(API_BASE_URL, API_EMAIL, API_PASSWORD)
            # Retry this batch
            result = send_batch(API_BASE_URL, token, batch)
            total_sent += result["sent"]
            total_errors += result["errors"]

        if batch_count % 10 == 0:
            print(
                f"  [progress] {total_sent:,} / {total_rows:,} "
                f"({100 * total_sent / total_rows:.0f}%)"
            )

    # Summary
    print("\n" + "=" * 60)
    print("[api:cens] CENSACIONAL SEND SUMMARY")
    print("=" * 60)
    print(f"  Total records : {total_rows:,}")
    print(f"  Sent OK       : {total_sent:,}")
    print(f"  Errors        : {total_errors:,}")
    print(f"  Batches       : {batch_count}")
    print("=" * 60)

    if total_errors > 0:
        print(
            f"\n[api:cens] WARNING: {total_errors:,} records "
            "failed to send. Check logs above."
        )
    else:
        print("[api:cens] All records sent successfully.")


run(spark)
