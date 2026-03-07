# Notebook:  nb_ventas_bronze_load_config
# Domain:    ventas
# Layer:     bronze
# Action:    load
# Entity:    config (brand_rules, vehicle_state)
# Source:    config/config_brand_rules.csv, config/config_vehicle_state.csv
# Target:    lh_ventas_bronze.brz_config_brand_rules
#            lh_ventas_bronze.brz_config_vehicle_state
# Load:      Full overwrite on each run
# Run after: (none — independent)
# Run before: nb_ventas_silver_transform_material (uses brand rules)
#             nb_ventas_silver_transform_equipment (uses vehicle_state)
#
# SAP BW equivalent:
#   InfoObject attributes loaded from flat file → PSA → DSO
# SALFA naming: nb_<dominio>_<capa>_<accion>_<entidad> (Documento_Fabric.docx)

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_BRONZE = "lh_ventas_bronze"

CONFIG_TABLES = {
    "brand_rules": {
        "file": "Files/config/config_brand_rules.csv",
        "table": "brz_config_brand_rules",
        "key_col": "brand",
        "expected_min_rows": 4,
    },
    "vehicle_state": {
        "file": "Files/config/config_vehicle_state.csv",
        "table": "brz_config_vehicle_state",
        "key_col": "spart",
        "expected_min_rows": 6,
    },
}


# ── Cell 3: Extract config CSV ───────────────────────────────────────────────

def extract_config(spark, file_path: str) -> "DataFrame":
    """
    Read a config CSV file from the Bronze lakehouse Files area.

    Config files use comma separator and UTF-8 encoding.
    """
    return (
        spark.read
             .option("header", "true")
             .option("sep", ",")
             .option("encoding", "UTF-8")
             .csv(file_path)
    )


# ── Cell 4: Add audit columns ───────────────────────────────────────────────

def add_audit_columns(df: "DataFrame", source_file: str) -> "DataFrame":
    """Add _load_ts and _source for lineage tracking."""
    return (
        df
        .withColumn("_load_ts", F.current_timestamp())
        .withColumn("_source", F.lit(source_file))
    )


# ── Cell 5: Validate ────────────────────────────────────────────────────────

def validate(df: "DataFrame", name: str, key_col: str, expected_min: int) -> None:
    """
    Basic quality checks on config data.
    """
    total = df.count()
    if total == 0:
        raise ValueError(
            f"[config:{name}] CSV returned 0 rows. "
            "Check file path or content."
        )

    if total < expected_min:
        print(
            f"[config:{name}] WARNING: only {total} rows "
            f"(expected >= {expected_min})"
        )

    null_keys = df.filter(F.col(key_col).isNull()).count()
    if null_keys > 0:
        raise ValueError(
            f"[config:{name}] {null_keys} rows with NULL {key_col}. "
            "Check CSV file."
        )

    print(f"[config:{name}] Validation passed: {total} rows")


# ── Cell 6: Main ────────────────────────────────────────────────────────────

def run(spark) -> None:
    """Load all config CSVs into Bronze Delta tables (full overwrite)."""
    print("[config] Starting config load...")

    for name, cfg in CONFIG_TABLES.items():
        df = extract_config(spark, cfg["file"])
        df = add_audit_columns(df, cfg["file"])
        validate(df, name, cfg["key_col"], cfg["expected_min_rows"])

        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .saveAsTable(f"{LAKEHOUSE_BRONZE}.{cfg['table']}")
        )

        print(
            f"[config:{name}] Loaded → "
            f"{LAKEHOUSE_BRONZE}.{cfg['table']}"
        )

    print("[config] All config tables loaded successfully.")


run(spark)
