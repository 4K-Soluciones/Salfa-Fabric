# Notebook:  nb_ventas_gold_build_sup_order
# Domain:    ventas
# Layer:     gold
# Action:    build
# Entity:    sup_order (Superación → /v1/order)
# Source:    lh_ventas_silver.sales_order + lh_ventas_silver.customer
# Target:    lh_ventas_gold.gold_seedz_sup_order
# Load:      Full overwrite
# Run after: nb_ventas_silver_transform_sales_order, customer
#
# Note: Order is at HEADER level — deduplicate from sales_order items.

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_SILVER = "lh_ventas_silver"
LAKEHOUSE_GOLD   = "lh_ventas_gold"
TARGET_TABLE     = "gold_seedz_sup_order"

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


# ── Cell 4: Transform ───────────────────────────────────────────────────────

def transform(spark) -> "DataFrame":
    """
    Build Gold order payload for Seedz /v1/order.
    Header level — one row per order_id.
    JOIN customer for clientIdentification (RUT).
    """
    df_ord = spark.table(f"{LAKEHOUSE_SILVER}.sales_order")
    df_cust = spark.table(f"{LAKEHOUSE_SILVER}.customer")

    # Deduplicate to header level
    w = Window.partitionBy("order_id").orderBy("order_item")
    df_header = (
        df_ord
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # JOIN customer for clientIdentification
    df_cust_slim = df_cust.select(
        F.col("customer_id").alias("_cust_id"),
        F.col("rut").alias("_cust_rut"),
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
        .withColumn("id", _e("order_id"))
        .withColumn("createdAt", _to_gold_date("created_date"))
        .withColumn("updatedAt",
            F.date_format(F.col("_silver_ts"), "dd-MM-yyyy HH:mm:ss")
        )
        .withColumn("issuedAt", _to_gold_date_short("order_date"))
        .withColumn("status", _e("status"))
        .withColumn("clientId", _e("customer_id"))
        .withColumn("clientIdentification",
            F.coalesce(F.col("_cust_rut").cast("string"), F.lit(""))
        )
        .withColumn("companyIdentification", F.lit(COMPANY_RUT))
        .withColumn("sellers", _e("seller_id"))

        .select(
            "identification", "id", "createdAt", "updatedAt",
            "issuedAt", "status", "clientId", "clientIdentification",
            "companyIdentification", "sellers",
        )
    )


# ── Cell 5: Validate ────────────────────────────────────────────────────────

def validate(df: "DataFrame") -> None:
    total = df.count()
    if total == 0:
        raise ValueError("[gold:sup_order] Transform returned 0 rows.")

    nulls = sum(
        df.filter(F.col(c).isNull()).count()
        for c in df.columns
    )
    if nulls > 0:
        raise ValueError(
            f"[gold:sup_order] {nulls} NULL values found (D11)."
        )

    print("[gold:sup_order] Status distribution:")
    df.groupBy("status").count().show(truncate=False)
    print(f"[gold:sup_order] Validation passed: {total:,} rows")


# ── Cell 6: Write ───────────────────────────────────────────────────────────

def run(spark) -> None:
    print("[gold:sup_order] Starting Gold build...")

    df_gold = transform(spark)
    validate(df_gold)

    (
        df_gold.write
               .format("delta")
               .mode("overwrite")
               .option("overwriteSchema", "true")
               .saveAsTable(f"{LAKEHOUSE_GOLD}.{TARGET_TABLE}")
    )

    print(f"[gold:sup_order] Complete → {LAKEHOUSE_GOLD}.{TARGET_TABLE}")


run(spark)
