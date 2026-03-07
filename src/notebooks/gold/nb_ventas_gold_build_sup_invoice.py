# Notebook:  nb_ventas_gold_build_sup_invoice
# Domain:    ventas
# Layer:     gold
# Action:    build
# Entity:    sup_invoice (Superación → /v1/invoice)
# Source:    lh_ventas_silver.billing_document + lh_ventas_silver.customer
# Target:    lh_ventas_gold.gold_seedz_sup_invoice
# Load:      Full overwrite
# Run after: nb_ventas_silver_transform_billing, nb_ventas_silver_transform_customer
#
# Key decisions:
#   D11 — All STRING, no NULLs
#   D12 — identification = {company_rut}_{plant_code}
#
# Note: Invoice is at HEADER level — one row per billing_id.
#       Deduplicate from billing_document (which is at item level).

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_SILVER = "lh_ventas_silver"
LAKEHOUSE_GOLD   = "lh_ventas_gold"
TARGET_TABLE     = "gold_seedz_sup_invoice"

COMPANY_RUT   = "96541530-9"
DEFAULT_PLANT = "1000"

# ── Cell 3: Helpers ──────────────────────────────────────────────────────────

def _to_gold_date(col_name: str) -> F.Column:
    return (
        F.when(
            F.col(col_name).isNull() | (F.col(col_name) == ""),
            F.lit("")
        )
        .otherwise(
            F.date_format(
                F.to_date(F.col(col_name), "yyyy-MM-dd"),
                "dd-MM-yyyy 00:00:00"
            )
        )
    )


def _to_gold_date_short(col_name: str) -> F.Column:
    """Date without time (for issuedAt which uses short format in examples)."""
    return (
        F.when(
            F.col(col_name).isNull() | (F.col(col_name) == ""),
            F.lit("")
        )
        .otherwise(
            F.date_format(
                F.to_date(F.col(col_name), "yyyy-MM-dd"),
                "dd-MM-yyyy"
            )
        )
    )


def _e(col_name: str) -> F.Column:
    """Empty if null (D11)."""
    return F.coalesce(F.col(col_name).cast("string"), F.lit(""))


# Invoice status text mapping
STATUS_TEXT = {
    "0": "Cancelada",
    "1": "Pendente",
    "2": "Concluída",
}

# ── Cell 4: Transform ───────────────────────────────────────────────────────

def transform(spark) -> "DataFrame":
    """
    Build Gold invoice payload for Seedz /v1/invoice.
    One row per billing_id (header level).
    JOIN customer for clientDocument (RUT).
    """
    df_bill = spark.table(f"{LAKEHOUSE_SILVER}.billing_document")
    df_cust = spark.table(f"{LAKEHOUSE_SILVER}.customer")

    # Deduplicate to header level — take first item per billing_id
    w = Window.partitionBy("billing_id").orderBy("billing_item")
    df_header = (
        df_bill
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # JOIN customer for clientDocument (RUT)
    df_cust_slim = (
        df_cust
        .select(
            F.col("customer_id").alias("_cust_id"),
            F.col("rut").alias("_cust_rut"),
        )
    )

    df = df_header.join(
        df_cust_slim,
        df_header["customer_id"] == df_cust_slim["_cust_id"],
        "left"
    )

    return (
        df
        .withColumn("identification",
            F.lit(f"{COMPANY_RUT}_{DEFAULT_PLANT}")
        )
        .withColumn("id", _e("billing_id"))
        .withColumn("clientId", _e("customer_id"))
        .withColumn("clientDocument",
            F.coalesce(F.col("_cust_rut").cast("string"), F.lit(""))
        )
        .withColumn("number", _e("billing_id"))
        .withColumn("serie", F.lit("1"))
        .withColumn("issuedAt", _to_gold_date_short("billing_date"))
        .withColumn("type", _e("invoice_type"))
        .withColumn("status", _e("invoice_status"))
        .withColumn("statusId", _e("invoice_status"))
        .withColumn("orderId", _e("sales_order_ref"))
        .withColumn("createdAt", _to_gold_date("created_date"))
        .withColumn("updatedAt",
            F.date_format(F.col("_silver_ts"), "dd-MM-yyyy HH:mm:ss")
        )

        .select(
            "identification", "id", "clientId", "clientDocument",
            "number", "serie", "issuedAt", "type", "status", "statusId",
            "orderId", "createdAt", "updatedAt",
        )
    )


# ── Cell 5: Validate ────────────────────────────────────────────────────────

def validate(df: "DataFrame") -> None:
    total = df.count()
    if total == 0:
        raise ValueError("[gold:sup_invoice] Transform returned 0 rows.")

    nulls = sum(
        df.filter(F.col(c).isNull()).count()
        for c in df.columns
    )
    if nulls > 0:
        raise ValueError(
            f"[gold:sup_invoice] {nulls} NULL values found (D11)."
        )

    print("[gold:sup_invoice] Status distribution:")
    df.groupBy("status").count().show(truncate=False)
    print(f"[gold:sup_invoice] Validation passed: {total:,} rows")


# ── Cell 6: Write ───────────────────────────────────────────────────────────

def run(spark) -> None:
    print("[gold:sup_invoice] Starting Gold build...")

    df_gold = transform(spark)
    validate(df_gold)

    (
        df_gold.write
               .format("delta")
               .mode("overwrite")
               .option("overwriteSchema", "true")
               .saveAsTable(f"{LAKEHOUSE_GOLD}.{TARGET_TABLE}")
    )

    print(f"[gold:sup_invoice] Complete → {LAKEHOUSE_GOLD}.{TARGET_TABLE}")


run(spark)
