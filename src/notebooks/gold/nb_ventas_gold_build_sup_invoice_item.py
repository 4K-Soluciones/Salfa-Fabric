# Notebook:  nb_ventas_gold_build_sup_invoice_item
# Domain:    ventas
# Layer:     gold
# Action:    build
# Entity:    sup_invoice_item (Superación → /v1/invoiceItem)
# Source:    lh_ventas_silver.billing_document
#            + lh_ventas_silver.customer (for clientDocument)
#            + lh_ventas_silver.material (for category)
# Target:    lh_ventas_gold.gold_seedz_sup_invoice_item
# Load:      Full overwrite
# Run after: nb_ventas_silver_transform_billing, customer, material
#
# Key decisions:
#   D11 — All STRING, no NULLs
#   D12 — identification = {company_rut}_{plant_code}
#
# This is the MOST COMPLEX Superación entity (3 JOINs).

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
from pyspark.sql import functions as F

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_SILVER = "lh_ventas_silver"
LAKEHOUSE_GOLD   = "lh_ventas_gold"
TARGET_TABLE     = "gold_seedz_sup_invoice_item"

COMPANY_RUT   = "96541530-9"
DEFAULT_PLANT = "1000"

# Invoice status text (for status field in payload)
INVOICE_STATUS_TEXT = {
    "0": "Cancelada",
    "1": "Pendente",
    "2": "Concluída",
}

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
    """Decimal → formatted string with 2 decimals, empty if null."""
    return (
        F.when(
            F.col(col_name).isNull(), F.lit("0.00")
        )
        .otherwise(
            F.format_number(F.col(col_name), 2)
        )
    )


# ── Cell 4: Transform ───────────────────────────────────────────────────────

def transform(spark) -> "DataFrame":
    """
    Build Gold invoice item payload for Seedz /v1/invoiceItem.
    One row per billing_id + billing_item.
    JOINs: customer (clientDocument), material (category).
    """
    df_bill = spark.table(f"{LAKEHOUSE_SILVER}.billing_document")
    df_cust = spark.table(f"{LAKEHOUSE_SILVER}.customer")
    df_mat  = spark.table(f"{LAKEHOUSE_SILVER}.material")

    # Slim customer for JOIN
    df_cust_slim = df_cust.select(
        F.col("customer_id").alias("_cust_id"),
        F.col("rut").alias("_cust_rut"),
    )

    # Slim material for category JOIN
    df_mat_slim = df_mat.select(
        F.col("material_id").alias("_mat_id"),
        F.col("category").alias("_mat_category"),
    ).dropDuplicates(["_mat_id"])

    # JOIN customer
    df = df_bill.join(
        df_cust_slim,
        df_bill["customer_id"] == df_cust_slim["_cust_id"],
        "left"
    )

    # JOIN material
    df = df.join(
        df_mat_slim,
        df["material_id"] == df_mat_slim["_mat_id"],
        "left"
    )

    # Invoice status text mapping
    status_expr = (
        F.when(F.col("invoice_status") == "0", F.lit("Cancelada"))
         .when(F.col("invoice_status") == "1", F.lit("Pendente"))
         .when(F.col("invoice_status") == "2", F.lit("Concluída"))
         .otherwise(F.lit("Pendente"))
    )

    # Unit value = net_value_item / billed_qty (avoid division by zero)
    unit_value = (
        F.when(
            F.col("billed_qty").isNull() |
            (F.col("billed_qty") == 0),
            F.col("net_value_item")
        )
        .otherwise(F.col("net_value_item") / F.col("billed_qty"))
    )

    return (
        df
        .withColumn("identification",
            F.lit(f"{COMPANY_RUT}_{DEFAULT_PLANT}")
        )
        .withColumn("id",
            F.concat_ws("_", _e("billing_id"), _e("billing_item"))
        )
        .withColumn("number", _e("billing_id"))
        .withColumn("serie", F.lit("1"))
        .withColumn("issuedAt", _to_gold_date_short("billing_date"))
        .withColumn("orderId", _e("sales_order_ref"))
        .withColumn("type", _e("invoice_type"))
        .withColumn("clientId", _e("customer_id"))
        .withColumn("clientDocument",
            F.coalesce(F.col("_cust_rut").cast("string"), F.lit(""))
        )
        .withColumn("parentInvoiceId", _e("cancelled_billing"))
        .withColumn("parentInvoice", _e("cancelled_billing"))
        .withColumn("itemId", _e("material_id"))
        .withColumn("itemDescription", _e("item_description"))
        .withColumn("itemSku", _e("material_id"))
        .withColumn("itemMeasureUnit", _e("sales_unit"))
        .withColumn("itemQuantity", _decimal_to_str("billed_qty"))
        .withColumn("itemUnitValue",
            F.format_number(
                F.coalesce(unit_value, F.lit(0)),
                2
            )
        )
        .withColumn("itemTotalValue", _decimal_to_str("net_value_item"))
        .withColumn("itemCfop", F.lit(""))  # Not applicable for Chile
        .withColumn("sellerIdentification", _e("seller_id"))
        .withColumn("category",
            F.coalesce(F.col("_mat_category").cast("string"), F.lit(""))
        )
        .withColumn("status", status_expr)
        .withColumn("statusId", _e("invoice_status"))
        .withColumn("createdAt", _to_gold_date("created_date"))
        .withColumn("updatedAt",
            F.date_format(F.col("_silver_ts"), "dd-MM-yyyy HH:mm:ss")
        )

        .select(
            "identification", "id", "number", "serie", "issuedAt",
            "orderId", "type", "clientId", "clientDocument",
            "parentInvoiceId", "parentInvoice",
            "itemId", "itemDescription", "itemSku", "itemMeasureUnit",
            "itemQuantity", "itemUnitValue", "itemTotalValue", "itemCfop",
            "sellerIdentification", "category",
            "status", "statusId", "createdAt", "updatedAt",
        )
    )


# ── Cell 5: Validate ────────────────────────────────────────────────────────

def validate(df: "DataFrame") -> None:
    total = df.count()
    if total == 0:
        raise ValueError("[gold:sup_invoice_item] Transform returned 0 rows.")

    nulls = sum(
        df.filter(F.col(c).isNull()).count()
        for c in df.columns
    )
    if nulls > 0:
        raise ValueError(
            f"[gold:sup_invoice_item] {nulls} NULL values found (D11)."
        )

    print("[gold:sup_invoice_item] Category distribution:")
    df.groupBy("category").count().show(truncate=False)
    print(f"[gold:sup_invoice_item] Validation passed: {total:,} rows")


# ── Cell 6: Write ───────────────────────────────────────────────────────────

def run(spark) -> None:
    print("[gold:sup_invoice_item] Starting Gold build...")

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
        f"[gold:sup_invoice_item] Complete → "
        f"{LAKEHOUSE_GOLD}.{TARGET_TABLE}"
    )


run(spark)
