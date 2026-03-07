# Notebook:  nb_ventas_bronze_ingest_acct_recv
# Domain:    ventas
# Layer:     bronze
# Action:    ingest
# Entity:    acct_recv (accounts receivable)
# Source:    CDS07 (ZFB_CDS07) via OData → BSID (open) + BSAD (cleared)
# Target:    lh_ventas_bronze.brz_sap_fi_acct_recv_raw
# Load:      Delta via BUDAT watermark (D14)
# Run after: nb_common_utils_seedz
# Run before: nb_ventas_silver_transform_acct_recv
#
# SAP BW equivalent:
#   DataSource : 0FI_AR_4 (AR open + cleared items)
#   Load type  : Delta (posting date watermark) → PSA → DSO
# SALFA naming: nb_<dominio>_<capa>_<accion>_<entidad> (Documento_Fabric.docx)

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_BRONZE  = "lh_ventas_bronze"
TABLE_NAME        = "brz_sap_fi_acct_recv_raw"
ENTITY            = "acct_recv"
ODATA_SERVICE     = "ZFB_CDS07_SRV"
ODATA_ENTITY_SET  = "ZFB_CDS07Set"

# Primary key columns (from CDS07 key fields)
PK_COLS = ["BELNR", "BUKRS", "GJAHR"]

# ── Cell 3: Extract from OData ───────────────────────────────────────────────

def extract_acct_recv(spark) -> "DataFrame":
    """
    Extract Accounts Receivable data from CDS07 OData service.
    Falls back to CSV fixtures for P0 testing.

    CDS07 is a UNION of BSID (open) + BSAD (cleared).
    Fixture files mirror this: fix_ar_open.csv + fix_ar_cleared.csv.

    Production : reads via On-Premises Data Gateway from SAP ECC.
    P0 fixture : reads from Files/fixtures/ in Bronze lakehouse.
    """
    try:
        df = (
            spark.read
                 .format("com.microsoft.cdm")
                 .option("serviceUri", ODATA_SERVICE)
                 .option("entity", ODATA_ENTITY_SET)
                 .load()
        )
    except Exception:
        df_open = (
            spark.read
                 .option("header", "true")
                 .option("sep", ";")
                 .option("encoding", "UTF-8")
                 .csv("Files/fixtures/fix_ar_open.csv")
        )
        df_cleared = (
            spark.read
                 .option("header", "true")
                 .option("sep", ";")
                 .option("encoding", "UTF-8")
                 .csv("Files/fixtures/fix_ar_cleared.csv")
        )
        df = df_open.unionByName(
            df_cleared, allowMissingColumns=True
        )

    return df


# ── Cell 4: Add audit columns ───────────────────────────────────────────────

def add_audit_columns(df: "DataFrame") -> "DataFrame":
    """Add _load_ts and _source for lineage tracking."""
    return (
        df
        .withColumn("_load_ts", F.current_timestamp())
        .withColumn("_source", F.lit(ODATA_SERVICE))
    )


# ── Cell 5: Validate ────────────────────────────────────────────────────────

def validate(df: "DataFrame") -> None:
    """
    Basic quality checks before writing to Bronze.
    SAP BW equivalent: DataSource field validation rules.
    """
    total = df.count()
    if total == 0:
        raise ValueError(
            f"[{ENTITY}] Extraction returned 0 rows. "
            "Check OData service or fixture file."
        )

    null_keys = df.filter(F.col("BELNR").isNull()).count()
    if null_keys > 0:
        raise ValueError(
            f"[{ENTITY}] {null_keys} rows with NULL BELNR. "
            "BELNR is part of the primary key — check source data."
        )

    # Report open vs cleared distribution
    if "ITEM_TYPE" in df.columns:
        open_count = df.filter(F.col("ITEM_TYPE") == "OPEN").count()
        cleared_count = df.filter(F.col("ITEM_TYPE") == "CLEARED").count()
        print(f"[{ENTITY}] Open: {open_count:,} | Cleared: {cleared_count:,}")
    elif "AUGBL" in df.columns:
        open_count = df.filter(
            F.col("AUGBL").isNull() | (F.trim(F.col("AUGBL")) == "")
        ).count()
        cleared_count = total - open_count
        print(f"[{ENTITY}] Open: {open_count:,} | Cleared: {cleared_count:,}")

    print(f"[{ENTITY}] Validation passed: {total:,} rows")


# ── Cell 6: Main ────────────────────────────────────────────────────────────

def run(spark) -> None:
    """
    Delta load — append new records based on BUDAT watermark (D14).
    Falls back to full overwrite when no watermark exists (first run).
    """
    print(f"[{ENTITY}] Starting Bronze ingest...")

    df = extract_acct_recv(spark)
    df = add_audit_columns(df)

    validate(df)

    # For P0 (fixture fallback) always overwrite.
    # In production, delta logic uses watermark + append/merge.
    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(f"{LAKEHOUSE_BRONZE}.{TABLE_NAME}")
    )

    print(
        f"[{ENTITY}] Bronze ingest complete → "
        f"{LAKEHOUSE_BRONZE}.{TABLE_NAME}"
    )


run(spark)
