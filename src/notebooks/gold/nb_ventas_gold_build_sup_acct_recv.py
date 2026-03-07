# Notebook:  nb_ventas_gold_build_sup_acct_recv
# Domain:    ventas
# Layer:     gold
# Action:    build
# Entity:    sup_acct_recv (Superación → /v1/accountReceivable)
# Source:    lh_ventas_silver.account_receivable + lh_ventas_silver.customer
# Target:    lh_ventas_gold.gold_seedz_sup_acct_recv
# Load:      Full overwrite
# Run after: nb_ventas_silver_transform_acct_recv, customer

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
from pyspark.sql import functions as F

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_SILVER = "lh_ventas_silver"
LAKEHOUSE_GOLD   = "lh_ventas_gold"
TARGET_TABLE     = "gold_seedz_sup_acct_recv"

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
    return F.coalesce(F.col(col_name).cast("string"), F.lit(""))


def _decimal_to_str(col_name: str) -> F.Column:
    return (
        F.when(F.col(col_name).isNull(), F.lit("0.00"))
         .otherwise(F.format_number(F.col(col_name), 2))
    )


# ── Cell 4: Transform ───────────────────────────────────────────────────────

def transform(spark) -> "DataFrame":
    """
    Build Gold AR payload for Seedz /v1/accountReceivable.
    JOIN customer for supplierName (customer name).
    """
    df_ar = spark.table(f"{LAKEHOUSE_SILVER}.account_receivable")
    df_cust = spark.table(f"{LAKEHOUSE_SILVER}.customer")

    df_cust_slim = df_cust.select(
        F.col("customer_id").alias("_cust_id"),
        F.col("name").alias("_cust_name"),
    )

    df = df_ar.join(
        df_cust_slim,
        df_ar["customer_id"] == df_cust_slim["_cust_id"],
        "left"
    )

    return (
        df
        .withColumn("identification",
            F.lit(f"{COMPANY_RUT}_{DEFAULT_PLANT}")
        )
        .withColumn("id",
            F.concat_ws("_",
                _e("doc_number"), _e("fiscal_year"), _e("line_item")
            )
        )
        .withColumn("titlenumber", _e("doc_number"))
        .withColumn("clientId", _e("customer_id"))
        .withColumn("supplierName",
            F.coalesce(F.col("_cust_name").cast("string"), F.lit(""))
        )
        .withColumn("issuedAt", _to_gold_date_short("document_date"))
        .withColumn("number", _e("doc_number"))
        .withColumn("serie", _e("fiscal_year"))
        .withColumn("originalDueAt", _to_gold_date_short("due_date"))
        .withColumn("extendedDueAt", _to_gold_date_short("due_date"))
        .withColumn("netValue", _decimal_to_str("amount"))
        .withColumn("status", _e("status"))
        .withColumn("paidAt",
            F.when(
                F.col("is_cleared") == True,
                _to_gold_date_short("clearing_date")
            ).otherwise(F.lit(""))
        )
        .withColumn("sellers", F.lit(""))
        .withColumn("createdAt", _to_gold_date("posting_date"))
        .withColumn("updatedAt",
            F.date_format(F.col("_silver_ts"), "dd-MM-yyyy HH:mm:ss")
        )

        .select(
            "identification", "id", "titlenumber", "clientId",
            "supplierName", "issuedAt", "number", "serie",
            "originalDueAt", "extendedDueAt", "netValue",
            "status", "paidAt", "sellers",
            "createdAt", "updatedAt",
        )
    )


# ── Cell 5: Validate ────────────────────────────────────────────────────────

def validate(df: "DataFrame") -> None:
    total = df.count()
    if total == 0:
        raise ValueError("[gold:sup_acct_recv] Transform returned 0 rows.")

    nulls = sum(
        df.filter(F.col(c).isNull()).count()
        for c in df.columns
    )
    if nulls > 0:
        raise ValueError(
            f"[gold:sup_acct_recv] {nulls} NULL values found (D11)."
        )

    print("[gold:sup_acct_recv] Status distribution:")
    df.groupBy("status").count().show(truncate=False)
    print(f"[gold:sup_acct_recv] Validation passed: {total:,} rows")


# ── Cell 6: Write ───────────────────────────────────────────────────────────

def run(spark) -> None:
    print("[gold:sup_acct_recv] Starting Gold build...")

    df_gold = transform(spark)
    validate(df_gold)

    (
        df_gold.write
               .format("delta")
               .mode("overwrite")
               .option("overwriteSchema", "true")
               .saveAsTable(f"{LAKEHOUSE_GOLD}.{TARGET_TABLE}")
    )

    print(
        f"[gold:sup_acct_recv] Complete → "
        f"{LAKEHOUSE_GOLD}.{TARGET_TABLE}"
    )


run(spark)
