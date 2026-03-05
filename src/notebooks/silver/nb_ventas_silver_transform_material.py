# Notebook:  nb_ventas_silver_transform_material
# Domain:    ventas
# Layer:     silver
# Action:    transform
# Entity:    material
# Source:    lh_ventas_bronze.brz_sap_mm_material_raw
#            lh_ventas_bronze.brz_config_brand_rules
# Target:    lh_ventas_silver.material
# Load:      Full daily — MERGE upsert on material_id (D14)
# Run after: nb_ventas_bronze_ingest_material
# Run before: nb_ventas_gold_build_* (Gold notebooks)
#
# Key decisions:
#   D1  — extract wide, classify late (no brand filter in CDS)
#   D3  — brand rules from config table, never hard-coded
#   D15 — Silver is interface-agnostic
#
# SAP BW equivalent:
#   Source : 0MATERIAL_ATTR DataSource + Z-brand config
#   Target : Material master DSO
# SALFA naming: nb_<dominio>_<capa>_<accion>_<entidad> (Documento_Fabric.docx)

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_BRONZE  = "lh_ventas_bronze"
LAKEHOUSE_SILVER  = "lh_ventas_silver"
SOURCE_TABLE      = "brz_sap_mm_material_raw"
CONFIG_BRAND      = "brz_config_brand_rules"
TARGET_TABLE      = "material"
MERGE_KEYS        = ["material_id"]

# ── Cell 3: Load brand config ────────────────────────────────────────────────

def load_brand_rules(spark) -> "DataFrame":
    """
    Load brand classification rules from Bronze config table (D3/D17).
    Falls back to default JD rules if config table not yet initialised.
    """
    try:
        return spark.table(f"{LAKEHOUSE_BRONZE}.{CONFIG_BRAND}")
    except Exception:
        print(
            f"[material] Config table {CONFIG_BRAND} not found. "
            "Using default D3 rules."
        )
        return spark.createDataFrame([
            ("MFRNR",         "0000100005", "JD",  "PARTS"),
            ("MFRNR",         "0000100157", "JD",  "PARTS"),
            ("SPART_PATTERN", "M%",         "JD",  "MACHINERY"),
        ], ["rule_type", "rule_value", "brand", "category"])


# ── Cell 4: Transform ────────────────────────────────────────────────────────
# Transformation rules (golden_fixtures.md F03 / F04):
#   MATNR → material_id    strip leading zeros
#   MAKTX → description    trim
#   MATKL → material_group as-is
#   SPART → division       as-is (R0=parts, M0/M2/M4=machinery)
#   MFRNR → brand_code     strip zeros, null if empty
#   MFRPN → oem_part_number null if empty
#   MEINS → base_uom       as-is
#   WERKS → plant          as-is
#   LVORM → is_deleted     'X' → True
#   brand / category derived from config_brand_rules (D3)

def transform(
    df: "DataFrame",
    df_brand_rules: "DataFrame"
) -> "DataFrame":
    """
    Apply Silver transformation rules to Bronze material data.

    Args:
        df: Raw Bronze material DataFrame.
        df_brand_rules: Brand classification config DataFrame.

    Returns:
        Transformed Silver material DataFrame.
    """
    # ── Step 1: field transformations ─────────────────────────────────────────
    df_base = (
        df
        .withColumn(
            "material_id",
            F.regexp_replace("MATNR", r"^0+", "")
        )
        .withColumn("description",    F.trim(F.col("MAKTX")))
        .withColumn("material_group", F.trim(F.col("MATKL")))
        .withColumn("division",       F.trim(F.col("SPART")))
        .withColumn(
            "brand_code",
            F.when(
                F.trim(F.col("MFRNR")) == "",
                F.lit(None).cast(StringType())
            ).otherwise(F.regexp_replace("MFRNR", r"^0+", ""))
        )
        .withColumn(
            "oem_part_number",
            F.when(
                F.trim(F.col("MFRPN")) == "",
                F.lit(None).cast(StringType())
            ).otherwise(F.trim(F.col("MFRPN")))
        )
        .withColumn("base_uom", F.trim(F.col("MEINS")))
        .withColumn("plant",    F.trim(F.col("WERKS")))
        .withColumn(
            "is_deleted",
            F.when(F.trim(F.col("LVORM")) == "X", F.lit(True))
             .otherwise(F.lit(False))
        )
    )

    # ── Step 2: brand classification (D3) ────────────────────────────────────
    jd_mfrnr_codes = [
        row["rule_value"]
        for row in df_brand_rules
            .filter(F.col("rule_type") == "MFRNR")
            .select("rule_value")
            .collect()
    ]

    df_classified = (
        df_base
        .withColumn(
            "brand",
            F.when(
                F.col("MFRNR").isin(jd_mfrnr_codes),
                F.lit("JD")
            ).when(
                F.col("division").startswith("M") &
                (F.col("material_group") == "0005"),
                F.lit("JD")
            ).otherwise(F.lit("OTHER"))
        )
        .withColumn(
            "category",
            F.when(
                F.col("MFRNR").isin(jd_mfrnr_codes),
                F.lit("PARTS")
            ).when(
                F.col("division").startswith("M") &
                (F.col("material_group") == "0005"),
                F.lit("MACHINERY")
            ).otherwise(F.lit("OTHER"))
        )
    )

    # ── Step 3: select final Silver columns ───────────────────────────────────
    return (
        df_classified
        .withColumn("_silver_ts", F.current_timestamp())
        .select(
            "material_id", "description", "material_group",
            "division", "brand_code", "oem_part_number",
            "base_uom", "plant", "is_deleted",
            "brand", "category", "_silver_ts",
        )
    )


# ── Cell 5: Validate ─────────────────────────────────────────────────────────

def validate(df: "DataFrame") -> None:
    total = df.count()
    if total == 0:
        raise ValueError("[material] Silver transform returned 0 rows.")

    dupes = (
        df.groupBy("material_id")
          .count()
          .filter(F.col("count") > 1)
          .count()
    )
    if dupes > 0:
        raise ValueError(
            f"[material] {dupes} duplicate material_id values."
        )

    print("[material] Brand distribution:")
    df.groupBy("brand", "category").count().show()
    print(f"[material] Silver validation passed: {total:,} rows")


# ── Cell 6: Upsert ───────────────────────────────────────────────────────────

def upsert(spark, df: "DataFrame") -> None:
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

    print(f"[material] Upsert complete → {target}")


# ── Cell 7: Main ─────────────────────────────────────────────────────────────

def run(spark) -> None:
    print("[material] Starting Silver transform...")

    df_bronze      = spark.table(f"{LAKEHOUSE_BRONZE}.{SOURCE_TABLE}")
    df_brand_rules = load_brand_rules(spark)
    df_silver      = transform(df_bronze, df_brand_rules)

    validate(df_silver)
    upsert(spark, df_silver)

    print("[material] Silver transform complete.")


run(spark)
