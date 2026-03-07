# Notebook:  nb_ventas_silver_transform_sales_order
# Domain:    ventas
# Layer:     silver
# Action:    transform
# Entity:    sales_order
# Source:    lh_ventas_bronze.brz_sap_sd_sales_order_raw
# Target:    lh_ventas_silver.sales_order
# Load:      MERGE upsert on order_id + order_item
# Run after: nb_ventas_bronze_ingest_sales_order
# Run before: nb_ventas_gold_build_sup_order,
#             nb_ventas_gold_build_sup_order_item
#
# Key decisions:
#   D10 — Seller via PARNR / SELLER_ID (raw partner number)
#   D15 — Silver is interface-agnostic
#   D18 — Trailing minus handling for amounts
#
# Business rules:
#   status: '0' = pending, '1' = billed, '2' = cancelled
#   abgru != '' → '2' (cancelled)
#   else → '0' (pending) — billing cross-reference deferred to Gold
#
# SAP BW equivalent:
#   Source : PSA / Bronze DSO (2LIS_11_VAITM)
#   Target : Core DSO key = order_id + order_item
# SALFA naming: nb_<dominio>_<capa>_<accion>_<entidad> (Documento_Fabric.docx)

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DecimalType

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_BRONZE  = "lh_ventas_bronze"
LAKEHOUSE_SILVER  = "lh_ventas_silver"
SOURCE_TABLE      = "brz_sap_sd_sales_order_raw"
TARGET_TABLE      = "sales_order"
MERGE_KEYS        = ["order_id", "order_item"]

# ── Cell 3: Transform ───────────────────────────────────────────────────────

def _safe_cast_decimal(col_name: str, scale: int = 2) -> F.Column:
    """Cast amount string to Decimal, handling trailing minus (D18)."""
    return (
        F.when(
            F.col(col_name).isNull() | (F.trim(F.col(col_name)) == ""),
            F.lit(None).cast(DecimalType(15, scale))
        )
        .when(
            F.col(col_name).endswith("-"),
            F.concat(F.lit("-"), F.regexp_replace(col_name, "-$", ""))
             .cast(DecimalType(15, scale))
        )
        .otherwise(F.col(col_name).cast(DecimalType(15, scale)))
    )


def _strip_leading_zeros(col_name: str) -> F.Column:
    """Strip leading zeros from purely numeric strings."""
    return (
        F.when(
            F.col(col_name).isNull(), F.lit(None).cast(StringType())
        )
        .when(
            F.col(col_name).rlike(r"^\d+$"),
            F.regexp_replace(col_name, r"^0+", "")
        )
        .otherwise(F.col(col_name))
    )


def _sap_date_to_iso(col_name: str) -> F.Column:
    """YYYYMMDD → yyyy-MM-dd, with NULL for invalid/empty dates."""
    return (
        F.when(
            F.col(col_name).isin("00000000", "0", "", " ") |
            F.col(col_name).isNull(),
            F.lit(None)
        )
        .otherwise(
            F.to_date(F.col(col_name), "yyyyMMdd").cast(StringType())
        )
    )


def _null_if_blank(col_name: str) -> F.Column:
    """Return None if value is empty/whitespace."""
    return (
        F.when(
            F.col(col_name).isNull() | (F.trim(F.col(col_name)) == ""),
            F.lit(None).cast(StringType())
        )
        .otherwise(F.trim(F.col(col_name)))
    )


def normalize_columns(df: "DataFrame") -> "DataFrame":
    """
    Handle column name variations between OData and fixture.
    Seller: PARNR (OData) vs SELLER_ID (fixture).
    Net value: NETWR (header) vs ITEM_NETWR (fixture item-level).
    """
    existing = set(df.columns)

    # Seller column normalization
    if "PARNR" not in existing and "SELLER_ID" in existing:
        df = df.withColumnRenamed("SELLER_ID", "PARNR")
    elif "PARNR" not in existing and "SellerPartner" in existing:
        df = df.withColumnRenamed("SellerPartner", "PARNR")
    if "PARNR" not in set(df.columns):
        df = df.withColumn("PARNR", F.lit(None).cast(StringType()))

    # Item net value: ITEM_NETWR in fixture vs NETWR_I in OData
    if "ITEM_NETWR" in existing and "NETWR_I" not in existing:
        df = df.withColumnRenamed("ITEM_NETWR", "NETWR_I")
    elif "NETWR_I" not in set(df.columns):
        # Fallback: use NETWR as item net value if only one exists
        if "NETWR" in existing:
            df = df.withColumnRenamed("NETWR", "NETWR_I")
        else:
            df = df.withColumn("NETWR_I", F.lit(None).cast(StringType()))

    # Ensure optional columns exist
    optional = {
        "VTWEG": StringType(), "VKORG": StringType(), "SPART": StringType(),
        "WAERK": StringType(), "ARKTX": StringType(), "KWMENG": StringType(),
        "VRKME": StringType(), "WERKS": StringType(), "MATKL": StringType(),
        "PSTYV": StringType(), "ABGRU": StringType(), "AUART": StringType(),
        "LGORT": StringType(),
    }
    current_cols = set(df.columns)
    for col_name, col_type in optional.items():
        if col_name not in current_cols:
            df = df.withColumn(col_name, F.lit(None).cast(col_type))

    return df


def transform(df: "DataFrame") -> "DataFrame":
    """
    Apply Silver transformation rules to Bronze sales order data.
    Derives order status from rejection reason.
    """
    df = normalize_columns(df)

    return (
        df

        # ── order_id / order_item ────────────────────────────────────────
        .withColumn("order_id",   _strip_leading_zeros("VBELN"))
        .withColumn("order_item", _strip_leading_zeros("POSNR"))

        # ── header fields ────────────────────────────────────────────────
        .withColumn("order_type",    _null_if_blank("AUART"))
        .withColumn("sales_org",     _null_if_blank("VKORG"))
        .withColumn("dist_channel",  _null_if_blank("VTWEG"))
        .withColumn("division",      _null_if_blank("SPART"))
        .withColumn("customer_id",   _strip_leading_zeros("KUNNR"))

        # ── item fields ──────────────────────────────────────────────────
        .withColumn("material_id",   _strip_leading_zeros("MATNR"))
        .withColumn("description",   _null_if_blank("ARKTX"))
        .withColumn("quantity",      _safe_cast_decimal("KWMENG", scale=3))
        .withColumn("net_value",     _safe_cast_decimal("NETWR_I"))
        .withColumn("currency",      _null_if_blank("WAERK"))
        .withColumn("plant",         _null_if_blank("WERKS"))
        .withColumn("material_group", _null_if_blank("MATKL"))
        .withColumn("item_category", _null_if_blank("PSTYV"))
        .withColumn("sales_unit",    _null_if_blank("VRKME"))
        .withColumn("seller_id",     _strip_leading_zeros("PARNR"))

        # ── rejection reason ─────────────────────────────────────────────
        .withColumn("rejection_reason", _null_if_blank("ABGRU"))

        # ── order status derivation ──────────────────────────────────────
        # abgru != '' → '2' (cancelled)
        # billing cross-reference not available in Silver → '0' (pending)
        # Gold layer can upgrade to '1' (billed) by checking billing_document
        .withColumn("status",
            F.when(
                F.col("rejection_reason").isNotNull() &
                (F.col("rejection_reason") != ""),
                F.lit("2")
            ).otherwise(F.lit("0"))
        )

        # ── dates + audit ────────────────────────────────────────────────
        .withColumn("order_date",   _sap_date_to_iso("ERDAT"))
        .withColumn("created_date", _sap_date_to_iso("ERDAT"))
        .withColumn("_silver_ts",   F.current_timestamp())

        # ── final Silver columns ─────────────────────────────────────────
        .select(
            "order_id", "order_item", "order_type",
            "sales_org", "dist_channel", "division",
            "customer_id", "material_id", "description",
            "quantity", "net_value", "currency",
            "order_date", "rejection_reason", "plant",
            "material_group", "item_category", "sales_unit",
            "seller_id", "status", "created_date", "_silver_ts",
        )
    )


# ── Cell 4: Validate ────────────────────────────────────────────────────────

def validate(df: "DataFrame") -> None:
    """Quality checks on Silver sales order output."""
    total = df.count()
    if total == 0:
        raise ValueError("[sales_order] Silver transform returned 0 rows.")

    dupes = (
        df.groupBy("order_id", "order_item")
          .count()
          .filter(F.col("count") > 1)
          .count()
    )
    if dupes > 0:
        raise ValueError(
            f"[sales_order] {dupes} duplicate order_id+order_item "
            "combinations detected."
        )

    # Report status distribution
    print("[sales_order] Status distribution:")
    df.groupBy("status").count().show(truncate=False)

    # Report rejection reason count
    cancelled = df.filter(F.col("status") == "2").count()
    print(
        f"[sales_order] Cancelled (ABGRU): {cancelled:,} / {total:,}"
    )

    print(f"[sales_order] Silver validation passed: {total:,} rows")


# ── Cell 5: Upsert ──────────────────────────────────────────────────────────

def upsert(spark, df: "DataFrame") -> None:
    """
    MERGE into Silver sales_order table. Creates on first run.
    SAP BW equivalent: DSO activation.
    """
    from delta.tables import DeltaTable

    target = f"{LAKEHOUSE_SILVER}.{TARGET_TABLE}"
    merge_cond = " AND ".join(
        [f"t.{k} = s.{k}" for k in MERGE_KEYS]
    )

    if DeltaTable.isDeltaTable(spark, target):
        (
            DeltaTable.forName(spark, target)
                      .alias("t")
                      .merge(df.alias("s"), merge_cond)
                      .whenMatchedUpdateAll()
                      .whenNotMatchedInsertAll()
                      .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(target)

    print(f"[sales_order] Upsert complete → {target}")


# ── Cell 6: Main ────────────────────────────────────────────────────────────

def run(spark) -> None:
    print("[sales_order] Starting Silver transform...")

    df_bronze = spark.table(f"{LAKEHOUSE_BRONZE}.{SOURCE_TABLE}")
    df_silver = transform(df_bronze)

    validate(df_silver)
    upsert(spark, df_silver)

    print("[sales_order] Silver transform complete.")


run(spark)
