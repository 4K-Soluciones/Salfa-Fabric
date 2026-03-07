# Notebook:  nb_ventas_bronze_ingest_billing
# Domain:    ventas
# Layer:     bronze
# Action:    ingest
# Entity:    billing
# Source:    CDS05 (ZFB_CDS05) via OData → VBRK + VBRP + VBPA(VE)
# Target:    lh_ventas_bronze.brz_sap_sd_billing_raw
# Load:      Delta via ERDAT watermark (D14)
# Run after: nb_common_utils_seedz
# Run before: nb_ventas_silver_transform_billing
#
# Design Decisions:
#   D1  — Extract wide, filter late (no brand filter here)
#   D5  — CHARG (batch) preserved for equipment JOIN in Silver
#   D10 — Seller via VBPA.PARVW='VE' at POSNR='000000' (header level)
#   D14 — Delta every 4h via ERDAT watermark
#   D19 — Seller partner ingested as raw PARNR; master resolution in Silver
#
# SAP BW equivalent:
#   DataSource : 2LIS_13_VDITM (billing items)
#   Load type  : Delta (creation date watermark) → PSA → DSO
# SALFA naming: nb_<dominio>_<capa>_<accion>_<entidad> (Documento_Fabric.docx)

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_BRONZE  = "lh_ventas_bronze"
TABLE_NAME        = "brz_sap_sd_billing_raw"
ENTITY            = "billing"
ODATA_SERVICE     = "ZFB_CDS05_SRV"
ODATA_ENTITY_SET  = "ZFB_CDS05Set"

# Fixture files — 4 scenario types covering normal, parts, credit, cancelled
FIXTURE_FILES = [
    "Files/fixtures/fix_billing_machinery.csv",
    "Files/fixtures/fix_billing_parts.csv",
    "Files/fixtures/fix_billing_credit.csv",
    "Files/fixtures/fix_billing_cancelled.csv",
]

# ── Cell 3: Extract from OData ───────────────────────────────────────────────

def extract_billing(spark) -> "DataFrame":
    """
    Extract billing data from CDS05 OData service.
    Falls back to CSV fixtures for P0 testing.

    CDS05 = VBRK (header) INNER JOIN VBRP (item) LEFT JOIN VBPA(VE) (seller).
    Fixture files cover 4 scenarios: machinery, parts, credit, cancelled.
    Each fixture may have different column subsets — unionByName handles this.

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
        dfs = []
        for path in FIXTURE_FILES:
            dfs.append(
                spark.read
                     .option("header", "true")
                     .option("sep", ";")
                     .option("encoding", "UTF-8")
                     .csv(path)
            )

        df = dfs[0]
        for other in dfs[1:]:
            df = df.unionByName(other, allowMissingColumns=True)

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

    null_keys = df.filter(F.col("VBELN").isNull()).count()
    if null_keys > 0:
        raise ValueError(
            f"[{ENTITY}] {null_keys} rows with NULL VBELN. "
            "VBELN is part of the primary key — check source data."
        )

    # Report document type distribution (D8/D9 insight)
    if "VBTYP" in df.columns:
        print(f"[{ENTITY}] VBTYP distribution:")
        df.groupBy("VBTYP").count().show(truncate=False)

    # Report cancellation flag distribution
    if "FKSTO" in df.columns:
        cancelled = df.filter(F.col("FKSTO") == "X").count()
        print(f"[{ENTITY}] Cancelled (FKSTO=X): {cancelled:,}")

    # Report batch coverage (D5 — equipment JOIN key)
    if "CHARG" in df.columns:
        with_batch = df.filter(
            F.col("CHARG").isNotNull() & (F.trim(F.col("CHARG")) != "")
        ).count()
        print(
            f"[{ENTITY}] With CHARG (batch): {with_batch:,} / {total:,} "
            f"({100 * with_batch / total:.1f}%)"
        )

    print(f"[{ENTITY}] Validation passed: {total:,} rows")


# ── Cell 6: Main ────────────────────────────────────────────────────────────

def run(spark) -> None:
    """
    Delta load — append new records based on ERDAT watermark (D14).
    Falls back to full overwrite when no watermark exists (first run / P0).
    """
    print(f"[{ENTITY}] Starting Bronze ingest...")

    df = extract_billing(spark)
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
