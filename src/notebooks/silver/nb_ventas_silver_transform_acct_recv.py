# Notebook:  nb_ventas_silver_transform_acct_recv
# Domain:    ventas
# Layer:     silver
# Action:    transform
# Entity:    account_receivable
# Source:    lh_ventas_bronze.brz_sap_fi_acct_recv_raw
# Target:    lh_ventas_silver.account_receivable
# Load:      MERGE upsert on company_code + doc_number + fiscal_year + line_item
# Run after: nb_ventas_bronze_ingest_acct_recv
# Run before: nb_ventas_gold_build_sup_acct_recv
#
# Key decisions:
#   D15 — Silver is interface-agnostic
#   D18 — Trailing minus handling for amounts
#
# Business rules:
#   is_cleared = augbl != ''
#   is_overdue = due_date < today AND NOT is_cleared
#   status: Liquidado / Atrasado / Aberto
#
# SAP BW equivalent:
#   Source : PSA / Bronze DSO (0FI_AR_4)
#   Target : Core DSO key = company_code + doc_number + fiscal_year + line_item
# SALFA naming: nb_<dominio>_<capa>_<accion>_<entidad> (Documento_Fabric.docx)

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DecimalType

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_BRONZE  = "lh_ventas_bronze"
LAKEHOUSE_SILVER  = "lh_ventas_silver"
SOURCE_TABLE      = "brz_sap_fi_acct_recv_raw"
TARGET_TABLE      = "account_receivable"
MERGE_KEYS        = ["company_code", "doc_number", "fiscal_year", "line_item"]

# ── Cell 3: Transform ───────────────────────────────────────────────────────

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


def normalize_columns(df: "DataFrame") -> "DataFrame":
    """Ensure optional columns exist with NULL defaults."""
    existing = set(df.columns)
    optional = {"BUZEI": StringType(), "AUGBL": StringType(),
                "AUGDT": StringType(), "ZTERM": StringType(),
                "XBLNR": StringType()}
    for col_name, col_type in optional.items():
        if col_name not in existing:
            df = df.withColumn(col_name, F.lit(None).cast(col_type))
    return df


def transform(df: "DataFrame") -> "DataFrame":
    """
    Apply Silver transformation rules to Bronze AR data.
    Derives is_cleared, is_overdue, and status (Liquidado/Atrasado/Aberto).
    """
    df = normalize_columns(df)

    return (
        df

        # ── key fields ───────────────────────────────────────────────────
        .withColumn("company_code", F.trim(F.col("BUKRS")))
        .withColumn("customer_id",  _strip_leading_zeros("KUNNR"))
        .withColumn("doc_number",   F.trim(F.col("BELNR")))
        .withColumn("fiscal_year",  F.trim(F.col("GJAHR")))
        .withColumn("line_item",    F.trim(F.col("BUZEI")))

        # ── dates ────────────────────────────────────────────────────────
        .withColumn("posting_date",  _sap_date_to_iso("BUDAT"))
        .withColumn("document_date", _sap_date_to_iso("BLDAT"))
        .withColumn("due_date",      _sap_date_to_iso("ZFBDT"))

        # ── amount ───────────────────────────────────────────────────────
        .withColumn("amount",   _safe_cast_decimal("DMBTR"))
        .withColumn("currency", _null_if_blank("WAERS"))

        # ── clearing fields ──────────────────────────────────────────────
        .withColumn("clearing_doc",  _null_if_blank("AUGBL"))
        .withColumn("clearing_date", _sap_date_to_iso("AUGDT"))

        # ── is_cleared: has a clearing document ──────────────────────────
        .withColumn("is_cleared",
            F.when(
                F.col("clearing_doc").isNotNull() &
                (F.col("clearing_doc") != ""),
                F.lit(True)
            ).otherwise(F.lit(False))
        )

        # ── is_overdue: past due and not cleared ─────────────────────────
        .withColumn("is_overdue",
            F.when(
                (F.col("is_cleared") == False) &
                F.col("due_date").isNotNull() &
                (F.to_date(F.col("due_date")) < F.current_date()),
                F.lit(True)
            ).otherwise(F.lit(False))
        )

        # ── status: Liquidado / Atrasado / Aberto ───────────────────────
        .withColumn("status",
            F.when(F.col("is_cleared") == True, F.lit("Liquidado"))
             .when(F.col("is_overdue") == True, F.lit("Atrasado"))
             .otherwise(F.lit("Aberto"))
        )

        # ── audit ────────────────────────────────────────────────────────
        .withColumn("_silver_ts", F.current_timestamp())

        # ── final Silver columns ─────────────────────────────────────────
        .select(
            "company_code", "customer_id", "doc_number", "fiscal_year",
            "line_item", "posting_date", "document_date", "due_date",
            "amount", "currency", "clearing_doc", "clearing_date",
            "is_cleared", "is_overdue", "status", "_silver_ts",
        )
    )


# ── Cell 4: Validate ────────────────────────────────────────────────────────

def validate(df: "DataFrame") -> None:
    """Quality checks on Silver AR output."""
    total = df.count()
    if total == 0:
        raise ValueError("[acct_recv] Silver transform returned 0 rows.")

    dupes = (
        df.groupBy("company_code", "doc_number", "fiscal_year", "line_item")
          .count()
          .filter(F.col("count") > 1)
          .count()
    )
    if dupes > 0:
        raise ValueError(
            f"[acct_recv] {dupes} duplicate key combinations detected."
        )

    # Report status distribution
    print("[acct_recv] Status distribution:")
    df.groupBy("status").count().show(truncate=False)

    # Report overdue count
    overdue = df.filter(F.col("is_overdue") == True).count()
    cleared = df.filter(F.col("is_cleared") == True).count()
    print(
        f"[acct_recv] Cleared: {cleared:,} | Overdue: {overdue:,} | "
        f"Open: {total - cleared - overdue:,}"
    )

    print(f"[acct_recv] Silver validation passed: {total:,} rows")


# ── Cell 5: Upsert ──────────────────────────────────────────────────────────

def upsert(spark, df: "DataFrame") -> None:
    """
    MERGE into Silver account_receivable table. Creates on first run.
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

    print(f"[acct_recv] Upsert complete → {target}")


# ── Cell 6: Main ────────────────────────────────────────────────────────────

def run(spark) -> None:
    print("[acct_recv] Starting Silver transform...")

    df_bronze = spark.table(f"{LAKEHOUSE_BRONZE}.{SOURCE_TABLE}")
    df_silver = transform(df_bronze)

    validate(df_silver)
    upsert(spark, df_silver)

    print("[acct_recv] Silver transform complete.")


run(spark)
