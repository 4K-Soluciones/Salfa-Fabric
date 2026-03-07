# Notebook:  nb_ventas_silver_transform_billing
# Domain:    ventas
# Layer:     silver
# Action:    transform
# Entity:    billing_document
# Source:    lh_ventas_bronze.brz_sap_sd_billing_raw
# Target:    lh_ventas_silver.billing_document
# Load:      MERGE upsert on billing_id + billing_item (D14)
# Run after: nb_ventas_bronze_ingest_billing
# Run before: nb_ventas_gold_build_sup_invoice,
#             nb_ventas_gold_build_sup_invoice_item,
#             nb_ventas_gold_build_cens_payload
#
# Key decisions:
#   D5  — CHARG (batch) preserved as JOIN key to equipment
#   D8  — Invoice type from VBTYP: M/P → '1' (debit), O/N → '0' (credit)
#   D9  — Invoice status priority chain (fksto → sfakn → rfbsk)
#   D10 — Seller via PARNR / SELLER_ID (raw partner number)
#   D15 — Silver is interface-agnostic
#   D18 — Trailing minus handling for amounts
#
# SAP BW equivalent:
#   Source : PSA / Bronze DSO (2LIS_13_VDITM billing items)
#   Target : Core DSO key = billing_id + billing_item
#   Start routine: invoice type + status derivation
# SALFA naming: nb_<dominio>_<capa>_<accion>_<entidad> (Documento_Fabric.docx)

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DecimalType, BooleanType

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_BRONZE  = "lh_ventas_bronze"
LAKEHOUSE_SILVER  = "lh_ventas_silver"
SOURCE_TABLE      = "brz_sap_sd_billing_raw"
TARGET_TABLE      = "billing_document"
MERGE_KEYS        = ["billing_id", "billing_item"]

# ── Cell 3: Column normalization ─────────────────────────────────────────────

def normalize_columns(df: "DataFrame") -> "DataFrame":
    """
    Normalize column names between OData (CDS aliases) and fixture (SAP names).

    OData returns CDS aliases (BillingDocument, NetValueHeader, etc.)
    while fixtures return SAP technical names (VBELN, NETWR, etc.).
    Bronze already ingests as-is, so we must handle both.

    Also handles seller column: PARNR (OData) vs SELLER_ID (fixture).
    Also handles fixture NETWR (item-level only) vs OData NETWR_H + NETWR_I.
    """
    existing = set(df.columns)

    # Seller: PARNR (from OData) or SELLER_ID (from fixture)
    if "PARNR" not in existing and "SELLER_ID" in existing:
        df = df.withColumnRenamed("SELLER_ID", "PARNR")
    elif "PARNR" not in existing and "SellerPartner" in existing:
        df = df.withColumnRenamed("SellerPartner", "PARNR")

    # Ensure PARNR exists even if missing
    if "PARNR" not in set(df.columns):
        df = df.withColumn("PARNR", F.lit(None).cast(StringType()))

    # Item net value: NETWR in fixtures is item-level
    if "NETWR" in existing and "NETWR_I" not in existing:
        df = df.withColumnRenamed("NETWR", "NETWR_I")

    # Ensure optional columns exist with NULL defaults
    optional_cols = {
        "VKORG": StringType(), "VTWEG": StringType(), "SPART": StringType(),
        "KUNAG": StringType(), "KUNRG": StringType(), "NETWR_H": StringType(),
        "WAERK": StringType(), "BUKRS": StringType(), "ARKTX": StringType(),
        "FKIMG": StringType(), "VRKME": StringType(), "NETWR_I": StringType(),
        "MWSBP": StringType(), "PSTYV": StringType(), "AUBEL": StringType(),
        "CHARG": StringType(), "SFAKN": StringType(), "FKSTO": StringType(),
        "RFBSK": StringType(),
    }
    current_cols = set(df.columns)
    for col_name, col_type in optional_cols.items():
        if col_name not in current_cols:
            df = df.withColumn(col_name, F.lit(None).cast(col_type))

    return df


# ── Cell 4: Transform ───────────────────────────────────────────────────────

def _safe_cast_decimal(col_name: str) -> F.Column:
    """Cast amount string to Decimal, handling trailing minus (D18)."""
    return (
        F.when(
            F.col(col_name).isNull() | (F.trim(F.col(col_name)) == ""),
            F.lit(None).cast(DecimalType(15, 2))
        )
        .when(
            F.col(col_name).endswith("-"),
            F.concat(F.lit("-"), F.regexp_replace(col_name, "-$", ""))
             .cast(DecimalType(15, 2))
        )
        .otherwise(F.col(col_name).cast(DecimalType(15, 2)))
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


def transform(df: "DataFrame") -> "DataFrame":
    """
    Apply Silver transformation rules to Bronze billing data.
    Derives invoice_type (D8) and invoice_status (D9).
    """
    df = normalize_columns(df)

    return (
        df

        # ── billing_id / billing_item ────────────────────────────────────
        .withColumn("billing_id",   _strip_leading_zeros("VBELN"))
        .withColumn("billing_item", _strip_leading_zeros("POSNR"))

        # ── header fields ────────────────────────────────────────────────
        .withColumn("billing_type",   _null_if_blank("FKART"))
        .withColumn("billing_date",   _sap_date_to_iso("FKDAT"))
        .withColumn("sales_org",      _null_if_blank("VKORG"))
        .withColumn("dist_channel",   _null_if_blank("VTWEG"))
        .withColumn("division",       _null_if_blank("SPART"))
        .withColumn("customer_id",    _strip_leading_zeros("KUNAG"))
        .withColumn("payer_id",       _strip_leading_zeros("KUNRG"))
        .withColumn("net_value_header", _safe_cast_decimal("NETWR_H"))
        .withColumn("currency",       _null_if_blank("WAERK"))
        .withColumn("company_code",   _null_if_blank("BUKRS"))

        # ── document category + status flags ─────────────────────────────
        .withColumn("sd_doc_category", _null_if_blank("VBTYP"))
        .withColumn("billing_status",  _null_if_blank("RFBSK"))
        .withColumn("is_cancelled",
            F.when(F.trim(F.col("FKSTO")) == "X", F.lit(True))
             .otherwise(F.lit(False))
        )
        .withColumn("cancelled_billing", _null_if_blank("SFAKN"))

        # ── item fields ──────────────────────────────────────────────────
        .withColumn("material_id",       _strip_leading_zeros("MATNR"))
        .withColumn("item_description",  _null_if_blank("ARKTX"))
        .withColumn("billed_qty",        _safe_cast_decimal("FKIMG"))
        .withColumn("sales_unit",        _null_if_blank("VRKME"))
        .withColumn("net_value_item",    _safe_cast_decimal("NETWR_I"))
        .withColumn("tax_amount",        _safe_cast_decimal("MWSBP"))
        .withColumn("plant",             _null_if_blank("WERKS"))
        .withColumn("material_group",    _null_if_blank("MATKL"))
        .withColumn("item_category",     _null_if_blank("PSTYV"))
        .withColumn("sales_order_ref",   _strip_leading_zeros("AUBEL"))
        .withColumn("batch",             _null_if_blank("CHARG"))
        .withColumn("seller_id",         _strip_leading_zeros("PARNR"))

        # ── invoice_type (D8): VBTYP → '0' (credit) or '1' (debit) ──────
        .withColumn("invoice_type",
            F.when(
                F.col("sd_doc_category").isin("O", "N"), F.lit("0")
            ).otherwise(F.lit("1"))
        )

        # ── invoice_status (D9): priority chain ─────────────────────────
        .withColumn("invoice_status",
            F.when(F.col("is_cancelled") == True, F.lit("0"))
             .when(
                 F.col("cancelled_billing").isNotNull() &
                 (F.col("cancelled_billing") != ""),
                 F.lit("0")
             )
             .when(F.col("billing_status") == "C", F.lit("2"))
             .when(F.col("billing_status").isin("A", "B"), F.lit("1"))
             .when(
                 F.col("billing_status").isNull() |
                 (F.col("billing_status") == ""),
                 F.lit("2")
             )
             .otherwise(F.lit("1"))
        )

        # ── dates + audit ────────────────────────────────────────────────
        .withColumn("created_date", _sap_date_to_iso("ERDAT"))
        .withColumn("_silver_ts",   F.current_timestamp())

        # ── final Silver columns ─────────────────────────────────────────
        .select(
            "billing_id", "billing_item", "billing_type", "billing_date",
            "sales_org", "dist_channel", "division",
            "customer_id", "payer_id", "net_value_header", "currency",
            "company_code", "sd_doc_category", "billing_status",
            "is_cancelled", "cancelled_billing",
            "material_id", "item_description", "billed_qty", "sales_unit",
            "net_value_item", "tax_amount", "plant", "material_group",
            "item_category", "sales_order_ref", "batch", "seller_id",
            "invoice_type", "invoice_status",
            "created_date", "_silver_ts",
        )
    )


# ── Cell 5: Validate ────────────────────────────────────────────────────────

def validate(df: "DataFrame") -> None:
    """Quality checks on Silver billing output."""
    total = df.count()
    if total == 0:
        raise ValueError("[billing] Silver transform returned 0 rows.")

    dupes = (
        df.groupBy("billing_id", "billing_item")
          .count()
          .filter(F.col("count") > 1)
          .count()
    )
    if dupes > 0:
        raise ValueError(
            f"[billing] {dupes} duplicate billing_id+billing_item "
            "combinations detected."
        )

    # Report invoice_type distribution (D8)
    print("[billing] Invoice type distribution:")
    df.groupBy("invoice_type").count().show(truncate=False)

    # Report invoice_status distribution (D9)
    print("[billing] Invoice status distribution:")
    df.groupBy("invoice_status").count().show(truncate=False)

    # Report batch coverage (D5 — equipment JOIN key)
    with_batch = df.filter(
        F.col("batch").isNotNull() & (F.col("batch") != "")
    ).count()
    print(
        f"[billing] With batch (equipment JOIN key): "
        f"{with_batch:,} / {total:,} ({100 * with_batch / total:.1f}%)"
    )

    print(f"[billing] Silver validation passed: {total:,} rows")


# ── Cell 6: Upsert ──────────────────────────────────────────────────────────

def upsert(spark, df: "DataFrame") -> None:
    """
    MERGE into Silver billing_document table. Creates on first run.
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

    print(f"[billing] Upsert complete → {target}")


# ── Cell 7: Main ────────────────────────────────────────────────────────────

def run(spark) -> None:
    print("[billing] Starting Silver transform...")

    df_bronze = spark.table(f"{LAKEHOUSE_BRONZE}.{SOURCE_TABLE}")
    df_silver = transform(df_bronze)

    validate(df_silver)
    upsert(spark, df_silver)

    print("[billing] Silver transform complete.")


run(spark)
