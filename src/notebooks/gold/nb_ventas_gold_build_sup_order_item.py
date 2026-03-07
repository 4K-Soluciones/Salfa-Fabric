# Notebook:  nb_ventas_gold_build_sup_order_item
# Domain:    ventas
# Layer:     gold
# Action:    build
# Entity:    sup_order_item (Superación → /v1/orderItem)
# Source:    lh_ventas_silver.sales_order + lh_ventas_silver.customer
# Target:    lh_ventas_gold.gold_seedz_sup_order_item
# Load:      Full overwrite
# Run after: nb_ventas_silver_transform_sales_order, customer

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
from pyspark.sql import functions as F

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_SILVER = "lh_ventas_silver"
LAKEHOUSE_GOLD   = "lh_ventas_gold"
TARGET_TABLE     = "gold_seedz_sup_order_item"

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
    Build Gold order item payload for Seedz /v1/orderItem.
    One row per order_id + order_item (item level).
    JOIN customer for clientIdentification (RUT).
    """
    df_ord = spark.table(f"{LAKEHOUSE_SILVER}.sales_order")
    df_cust = spark.table(f"{LAKEHOUSE_SILVER}.customer")

    df_cust_slim = df_cust.select(
        F.col("customer_id").alias("_cust_id"),
        F.col("rut").alias("_cust_rut"),
    )

    df = df_ord.join(
        df_cust_slim,
        df_ord["customer_id"] == df_cust_slim["_cust_id"],
        "left"
    )

    # Unit value = net_value / quantity
    unit_value = (
        F.when(
            F.col("quantity").isNull() | (F.col("quantity") == 0),
            F.col("net_value")
        )
        .otherwise(F.col("net_value") / F.col("quantity"))
    )

    return (
        df
        .withColumn("identification",
            F.lit(f"{COMPANY_RUT}_{DEFAULT_PLANT}")
        )
        .withColumn("id",
            F.concat_ws("_", _e("order_id"), _e("order_item"))
        )
        .withColumn("orderId", _e("order_id"))
        .withColumn("issuedAt", _to_gold_date_short("order_date"))
        .withColumn("status", _e("status"))
        .withColumn("clientIdentification",
            F.coalesce(F.col("_cust_rut").cast("string"), F.lit(""))
        )
        .withColumn("idItem", _e("material_id"))
        .withColumn("itemSku", _e("material_id"))
        .withColumn("itemDescription", _e("description"))
        .withColumn("itemMeasureUnit", _e("sales_unit"))
        .withColumn("quantity", _decimal_to_str("quantity"))
        .withColumn("netUnitValue",
            F.format_number(F.coalesce(unit_value, F.lit(0)), 2)
        )
        .withColumn("totalValue", _decimal_to_str("net_value"))
        .withColumn("createdAt", _to_gold_date("created_date"))
        .withColumn("updatedAt",
            F.date_format(F.col("_silver_ts"), "dd-MM-yyyy HH:mm:ss")
        )

        .select(
            "identification", "id", "orderId", "issuedAt", "status",
            "clientIdentification",
            "idItem", "itemSku", "itemDescription", "itemMeasureUnit",
            "quantity", "netUnitValue", "totalValue",
            "createdAt", "updatedAt",
        )
    )


# ── Cell 5: Validate ────────────────────────────────────────────────────────

def validate(df: "DataFrame") -> None:
    total = df.count()
    if total == 0:
        raise ValueError("[gold:sup_order_item] Transform returned 0 rows.")

    nulls = sum(
        df.filter(F.col(c).isNull()).count()
        for c in df.columns
    )
    if nulls > 0:
        raise ValueError(
            f"[gold:sup_order_item] {nulls} NULL values found (D11)."
        )

    print(f"[gold:sup_order_item] Validation passed: {total:,} rows")


# ── Cell 6: Write ───────────────────────────────────────────────────────────

def run(spark) -> None:
    print("[gold:sup_order_item] Starting Gold build...")

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
        f"[gold:sup_order_item] Complete → "
        f"{LAKEHOUSE_GOLD}.{TARGET_TABLE}"
    )


run(spark)
