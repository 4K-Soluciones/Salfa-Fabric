# Notebook:  nb_seedz_api_send_superacion
# Domain:    ventas
# Layer:     gold (API integration)
# Action:    send
# Entity:    superacion (all 8 Superación entities)
# Source:    lh_ventas_gold.gold_seedz_sup_* tables
# Target:    Seedz REST API (api-prod.seedz.ag)
# Run after: All Gold Superación build notebooks
#
# Sends data to 8 Seedz endpoints in order:
#   1. /v1/customer      (master data first)
#   2. /v1/property
#   3. /v1/item
#   4. /v1/seller         (skipped if table empty / not yet built)
#   5. /v1/order
#   6. /v1/orderItem
#   7. /v1/invoice
#   8. /v1/invoiceItem
#
# Rate limits: 100 req/min, batch size 500, exponential backoff on 429
#
# SAP BW equivalent:
#   Process chain: Open Hub → REST API call via ABAP program

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
import json
import time
import requests
from pyspark.sql import functions as F

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_GOLD = "lh_ventas_gold"

# API settings — TODO: move to Key Vault / Fabric secrets
API_BASE_URL = "https://api-sandbox.seedz.ag"  # Switch to api-prod for production
API_EMAIL    = ""  # TODO: set from Fabric secrets
API_PASSWORD = ""  # TODO: set from Fabric secrets

BATCH_SIZE       = 500
MAX_RETRIES      = 5
INITIAL_BACKOFF  = 1  # seconds
MAX_BACKOFF      = 60

# Superación entity send order (master data first, then transactional)
ENTITIES = [
    {"table": "gold_seedz_sup_customer",      "endpoint": "/v1/customer"},
    {"table": "gold_seedz_sup_property",      "endpoint": "/v1/property"},
    {"table": "gold_seedz_sup_item",          "endpoint": "/v1/item"},
    # {"table": "gold_seedz_sup_seller",      "endpoint": "/v1/seller"},  # Blocked (D19)
    {"table": "gold_seedz_sup_order",         "endpoint": "/v1/order"},
    {"table": "gold_seedz_sup_order_item",    "endpoint": "/v1/orderItem"},
    {"table": "gold_seedz_sup_invoice",       "endpoint": "/v1/invoice"},
    {"table": "gold_seedz_sup_invoice_item",  "endpoint": "/v1/invoiceItem"},
    {"table": "gold_seedz_sup_acct_recv",     "endpoint": "/v1/accountReceivable"},
]


# ── Cell 3: Authentication ───────────────────────────────────────────────────

def authenticate(base_url: str, email: str, password: str) -> str:
    """
    Authenticate with Seedz API and return Bearer token.

    Raises:
        ValueError: If credentials are not configured.
        RuntimeError: If authentication fails.
    """
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

    print("[api:auth] Authenticated successfully.")
    return token


# ── Cell 4: Batch sender ────────────────────────────────────────────────────

def send_batch(
    base_url: str,
    endpoint: str,
    token: str,
    records: list,
) -> dict:
    """
    Send a batch of records to a Seedz endpoint.
    Implements exponential backoff on 429 (rate limit).

    Args:
        base_url: API base URL.
        endpoint: API endpoint path (e.g. '/v1/customer').
        token: Bearer token.
        records: List of dicts to send.

    Returns:
        dict with 'status', 'sent', 'errors' counts.
    """
    url = f"{base_url}{endpoint}"
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
                # Token expired — caller should re-authenticate
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

            # Unexpected status
            print(
                f"  [{resp.status_code}] Unexpected: {resp.text[:200]}"
            )
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

    # Exhausted retries
    return {"status": "max_retries", "sent": 0, "errors": len(records)}


# ── Cell 5: Entity sender ───────────────────────────────────────────────────

def send_entity(
    spark,
    base_url: str,
    token: str,
    table_name: str,
    endpoint: str,
) -> tuple:
    """
    Read a Gold table and send all records to the Seedz endpoint in batches.

    Returns:
        tuple of (dict with total_sent/total_errors/batch_count, current_token).
    """
    full_table = f"{LAKEHOUSE_GOLD}.{table_name}"

    try:
        df = spark.table(full_table)
    except Exception:
        print(f"  [skip] Table {full_table} not found — skipping.")
        return {"total_sent": 0, "total_errors": 0, "batch_count": 0}, token

    total_rows = df.count()
    if total_rows == 0:
        print(f"  [skip] Table {full_table} is empty — skipping.")
        return {"total_sent": 0, "total_errors": 0, "batch_count": 0}, token

    # Collect to driver as list of dicts
    # For large tables, this could be done in partitions
    all_records = [row.asDict() for row in df.collect()]

    total_sent = 0
    total_errors = 0
    batch_count = 0

    for i in range(0, len(all_records), BATCH_SIZE):
        batch = all_records[i:i + BATCH_SIZE]
        batch_count += 1

        result = send_batch(base_url, endpoint, token, batch)
        total_sent += result["sent"]
        total_errors += result["errors"]

        if result["status"] == "auth_expired":
            # Re-authenticate and retry the failed batch once
            print("  [auth] Token expired — re-authenticating...")
            token = authenticate(base_url, API_EMAIL, API_PASSWORD)
            result = send_batch(base_url, endpoint, token, batch)
            total_sent += result["sent"]
            total_errors += result["errors"]
            if result["status"] != "ok":
                print("  [auth] Re-auth failed — aborting entity.")
                total_errors += len(all_records) - i - len(batch)
                break

        if batch_count % 10 == 0:
            print(
                f"  [progress] {total_sent:,} sent / "
                f"{total_rows:,} total ({endpoint})"
            )

    return {
        "total_sent": total_sent,
        "total_errors": total_errors,
        "batch_count": batch_count,
    }, token


# ── Cell 6: Main ────────────────────────────────────────────────────────────

def run(spark) -> None:
    """Send all Superación entities to Seedz API."""
    print("[api:superacion] Starting API send...")

    token = authenticate(API_BASE_URL, API_EMAIL, API_PASSWORD)

    results = {}
    for entity in ENTITIES:
        table = entity["table"]
        endpoint = entity["endpoint"]
        entity_name = table.replace("gold_seedz_sup_", "")

        print(f"\n[api:{entity_name}] Sending → {endpoint}")

        result, token = send_entity(spark, API_BASE_URL, token, table, endpoint)
        results[entity_name] = result

        print(
            f"[api:{entity_name}] Done: "
            f"sent={result['total_sent']:,}, "
            f"errors={result['total_errors']:,}, "
            f"batches={result['batch_count']}"
        )

    # Summary
    print("\n" + "=" * 60)
    print("[api:superacion] SEND SUMMARY")
    print("=" * 60)
    total_sent = 0
    total_errors = 0
    for name, r in results.items():
        status = "OK" if r["total_errors"] == 0 else "ERRORS"
        print(
            f"  {name:20s} → sent: {r['total_sent']:>8,}  "
            f"errors: {r['total_errors']:>6,}  [{status}]"
        )
        total_sent += r["total_sent"]
        total_errors += r["total_errors"]

    print(f"\n  TOTAL: sent={total_sent:,}, errors={total_errors:,}")
    print("=" * 60)

    if total_errors > 0:
        print(
            f"\n[api:superacion] WARNING: {total_errors:,} records "
            "failed to send. Check logs above."
        )


run(spark)
