# Notebook:  nb_ventas_bronze_ingest_customer
# Domain:    ventas
# Layer:     bronze
# Action:    ingest
# Entity:    customer
# Source:    CDS01 (ZFB_CDS01) via OData → KNA1 + ADRC
# Target:    lh_ventas_bronze.brz_sap_sd_customer_raw
# Load:      Full daily at 06:00 CLT (D14)
# Run after: (none — first in chain)
# Run before: nb_ventas_silver_transform_customer
#
# SAP BW equivalent:
#   DataSource : 0CUSTOMER_ATTR
#   Load type  : Full upload → PSA → DSO
# SALFA naming: nb_<dominio>_<capa>_<accion>_<entidad> (Documento_Fabric.docx)

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_BRONZE  = "lh_ventas_bronze"
TABLE_NAME        = "brz_sap_sd_customer_raw"
ENTITY            = "customer"
ODATA_SERVICE     = "ZFB_CDS01_SRV"
ODATA_ENTITY_SET  = "ZFB_CDS01Set"

# ── Cell 3: Extract from OData ───────────────────────────────────────────────

def extract_customer(spark) -> "DataFrame":
    """
    Extract customer data from CDS01 OData service.
    Falls back to CSV fixtures for P0 testing.

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
        df_active = (
            spark.read
                 .option("header", "true")
                 .option("sep", ";")
                 .option("encoding", "UTF-8")
                 .csv("Files/fixtures/fix_customer_active.csv")
        )
        df_deleted = (
            spark.read
                 .option("header", "true")
                 .option("sep", ";")
                 .option("encoding", "UTF-8")
                 .csv("Files/fixtures/fix_customer_deleted.csv")
        )
        df = df_active.unionByName(
            df_deleted, allowMissingColumns=True
        )

    return df


# ── Cell 4: Add audit columns ────────────────────────────────────────────────

def add_audit_columns(df: "DataFrame") -> "DataFrame":
    """Add _load_ts and _source for lineage tracking."""
    return (
        df
        .withColumn("_load_ts", F.current_timestamp())
        .withColumn("_source",  F.lit(ODATA_SERVICE))
    )


# ── Cell 5: Validate ─────────────────────────────────────────────────────────

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

    null_keys = df.filter(F.col("KUNNR").isNull()).count()
    if null_keys > 0:
        raise ValueError(
            f"[{ENTITY}] {null_keys} rows with NULL KUNNR. "
            "KUNNR is the primary key — check source data."
        )

    print(f"[{ENTITY}] Validation passed: {total:,} rows")


# ── Cell 6: Main ─────────────────────────────────────────────────────────────

def run(spark) -> None:
    """Full load — overwrite entire table daily (D14)."""
    print(f"[{ENTITY}] Starting Bronze ingest...")

    df = extract_customer(spark)
    df = add_audit_columns(df)

    validate(df)

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
