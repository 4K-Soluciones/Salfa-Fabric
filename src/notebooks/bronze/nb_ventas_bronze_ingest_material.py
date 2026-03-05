# Notebook:  nb_ventas_bronze_ingest_material
# Domain:    ventas
# Layer:     bronze
# Action:    ingest
# Entity:    material
# Source:    CDS04 (ZFB_CDS04) via OData → MARA + MAKT + MARC
# Target:    lh_ventas_bronze.brz_sap_mm_material_raw
# Load:      Full daily at 06:00 CLT (D14)
# Run after: (none — first in chain)
# Run before: nb_ventas_silver_transform_material
#
# SAP BW equivalent:
#   DataSource : 0MATERIAL_ATTR
#   Load type  : Full upload → PSA → DSO
# SALFA naming: nb_<dominio>_<capa>_<accion>_<entidad> (Documento_Fabric.docx)

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
from pyspark.sql import functions as F

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_BRONZE  = "lh_ventas_bronze"
TABLE_NAME        = "brz_sap_mm_material_raw"
ENTITY            = "material"
ODATA_SERVICE     = "ZFB_CDS04_SRV"
ODATA_ENTITY_SET  = "ZFB_CDS04Set"

# ── Cell 3: Extract from OData ───────────────────────────────────────────────

def extract_material(spark) -> "DataFrame":
    """
    Extract material data from CDS04 OData service.
    Falls back to CSV fixtures for P0 testing.
    Combines parts and machines into one raw table (D1: extract wide).
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
        df_parts = (
            spark.read
                 .option("header", "true")
                 .option("sep", ";")
                 .option("encoding", "UTF-8")
                 .csv("Files/fixtures/fix_material_part.csv")
        )
        df_machines = (
            spark.read
                 .option("header", "true")
                 .option("sep", ";")
                 .option("encoding", "UTF-8")
                 .csv("Files/fixtures/fix_material_machine.csv")
        )
        df = df_parts.unionByName(df_machines, allowMissingColumns=True)

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

    null_keys = df.filter(F.col("MATNR").isNull()).count()
    if null_keys > 0:
        raise ValueError(
            f"[{ENTITY}] {null_keys} rows with NULL MATNR."
        )

    print(f"[{ENTITY}] Validation passed: {total:,} rows")


# ── Cell 6: Main ─────────────────────────────────────────────────────────────

def run(spark) -> None:
    """Full load — overwrite entire table daily (D14)."""
    print(f"[{ENTITY}] Starting Bronze ingest...")

    df = extract_material(spark)
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
