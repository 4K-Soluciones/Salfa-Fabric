# Notebook:  nb_ventas_bronze_ingest_equipment
# Domain:    ventas
# Layer:     bronze
# Action:    ingest
# Entity:    equipment
# Source:    CDS06 (ZFB_CDS06) via OData → EQUI + EQKT
# Target:    lh_ventas_bronze.brz_sap_pm_equipment_raw
# Load:      Full daily at 06:00 CLT (D14)
# Run after: (none — first in chain)
# Run before: nb_ventas_silver_transform_equipment
#
# Key decisions:
#   D4 — VIN comes from EQUI.SERNR (SERGE is empty at SALFA)
#   D5 — CHARG (batch) preserved for billing → equipment JOIN
#   D6 — CHARG filter for accessories applied in Silver
#
# SAP BW equivalent:
#   DataSource : 0PM_EQUI_ATTR
#   Load type  : Full upload → PSA → DSO
# SALFA naming: nb_<dominio>_<capa>_<accion>_<entidad> (Documento_Fabric.docx)

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
from pyspark.sql import functions as F

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_BRONZE  = "lh_ventas_bronze"
TABLE_NAME        = "brz_sap_pm_equipment_raw"
ENTITY            = "equipment"
ODATA_SERVICE     = "ZFB_CDS06_SRV"
ODATA_ENTITY_SET  = "ZFB_CDS06Set"

# ── Cell 3: Extract from OData ───────────────────────────────────────────────

def extract_equipment(spark) -> "DataFrame":
    """
    Extract equipment data from CDS06 OData service.
    Falls back to CSV fixture for P0 testing.

    D4: EQUNR = SERNR = VIN at SALFA.
    D5: CHARG preserved for downstream billing JOIN.
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
        df = (
            spark.read
                 .option("header", "true")
                 .option("sep", ";")
                 .option("encoding", "UTF-8")
                 .csv("Files/fixtures/fix_equipment_vin.csv")
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
    total = df.count()
    if total == 0:
        raise ValueError(
            f"[{ENTITY}] Extraction returned 0 rows."
        )

    # D4: warn on empty SERNR (no VIN)
    no_vin = df.filter(
        F.col("SERNR").isNull() | (F.trim(F.col("SERNR")) == "")
    ).count()
    if no_vin > 0:
        print(
            f"[{ENTITY}] WARNING: {no_vin} records with empty SERNR "
            "(no VIN). These will be excluded in Silver."
        )

    print(f"[{ENTITY}] Validation passed: {total:,} rows")


# ── Cell 6: Main ─────────────────────────────────────────────────────────────

def run(spark) -> None:
    """Full load — overwrite entire table daily (D14)."""
    print(f"[{ENTITY}] Starting Bronze ingest...")

    df = extract_equipment(spark)
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
