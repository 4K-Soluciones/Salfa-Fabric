# Notebook:  nb_ventas_silver_transform_equipment
# Domain:    ventas
# Layer:     silver
# Action:    transform
# Entity:    equipment
# Source:    lh_ventas_bronze.brz_sap_pm_equipment_raw
# Target:    lh_ventas_silver.equipment
# Load:      Full daily — MERGE upsert on equipment_id (D14)
# Run after: nb_ventas_bronze_ingest_equipment
# Run before: nb_ventas_gold_build_* (Gold notebooks)
#
# Key decisions:
#   D4 — VIN = EQUI.SERNR = EQUNR at SALFA
#   D5 — CHARG preserved as batch (billing → equipment JOIN key)
#   D6 — records without SERNR excluded (accessories have no VIN)
#   D7 — vehicle_state populated in Gold via JOIN to material table
#
# SAP BW equivalent:
#   Source : 0PM_EQUI_ATTR DataSource
#   Target : Equipment master DSO  key = EQUNR
# SALFA naming: nb_<dominio>_<capa>_<accion>_<entidad> (Documento_Fabric.docx)

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_BRONZE  = "lh_ventas_bronze"
LAKEHOUSE_SILVER  = "lh_ventas_silver"
SOURCE_TABLE      = "brz_sap_pm_equipment_raw"
TARGET_TABLE      = "equipment"
MERGE_KEYS        = ["equipment_id"]

# ── Cell 3: Transform ────────────────────────────────────────────────────────
# Transformation rules (golden_fixtures.md F05):
#   EQUNR → equipment_id       as-is (= VIN at SALFA, D4)
#   SERNR → vin                as-is (= EQUNR at SALFA, D4)
#   MATNR → material_id        strip leading zeros
#   EQKTX → description        null if blank
#   HERST → manufacturer       null if blank
#   SWERK → plant              null if blank (ISSUE-03: blank in fixture)
#   INBDT → installation_date  YYYYMMDD → yyyy-MM-dd
#   BAUJJ → year_of_construction integer
#   CHARG → batch              preserved for billing JOIN (D5)
#   LVORM → is_deleted         'X' → True
#   vehicle_state              '' — populated in Gold via material JOIN (D7)

def transform(df: "DataFrame") -> "DataFrame":
    """
    Apply Silver transformation rules to Bronze equipment data.

    Args:
        df: Raw Bronze equipment DataFrame.

    Returns:
        Transformed Silver equipment DataFrame.
    """
    return (
        df

        # ── filter: only records with VIN (D4 / D6) ──────────────────────────
        .filter(
            F.col("SERNR").isNotNull() &
            (F.trim(F.col("SERNR")) != "")
        )

        # ── equipment_id and VIN (D4: equal at SALFA) ─────────────────────────
        .withColumn("equipment_id", F.trim(F.col("EQUNR")))
        .withColumn("vin",          F.trim(F.col("SERNR")))

        # ── material reference ─────────────────────────────────────────────────
        .withColumn(
            "material_id",
            F.regexp_replace("MATNR", r"^0+", "")
        )

        # ── description ───────────────────────────────────────────────────────
        .withColumn(
            "description",
            F.when(
                F.trim(F.col("EQKTX")) == "",
                F.lit(None).cast(StringType())
            ).otherwise(F.trim(F.col("EQKTX")))
        )

        # ── manufacturer ──────────────────────────────────────────────────────
        .withColumn(
            "manufacturer",
            F.when(
                F.trim(F.col("HERST")) == "",
                F.lit(None).cast(StringType())
            ).otherwise(F.trim(F.col("HERST")))
        )

        # ── plant (ISSUE-03: blank in current fixture — stored as null) ───────
        .withColumn(
            "plant",
            F.when(
                F.col("SWERK").isNull() |
                (F.trim(F.col("SWERK")) == ""),
                F.lit(None).cast(StringType())
            ).otherwise(F.trim(F.col("SWERK")))
        )

        # ── installation_date: YYYYMMDD → yyyy-MM-dd ─────────────────────────
        .withColumn(
            "installation_date",
            F.when(
                F.col("INBDT").isin("00000000", "0", "", " ") |
                F.col("INBDT").isNull(),
                F.lit(None).cast(StringType())
            ).otherwise(
                F.to_date(F.col("INBDT"), "yyyyMMdd").cast(StringType())
            )
        )

        # ── year of construction ──────────────────────────────────────────────
        .withColumn(
            "year_of_construction",
            F.when(
                F.col("BAUJJ").isNull() |
                (F.trim(F.col("BAUJJ")) == ""),
                F.lit(None).cast(IntegerType())
            ).otherwise(F.col("BAUJJ").cast(IntegerType()))
        )

        # ── batch (D5: JOIN key for billing → equipment) ──────────────────────
        .withColumn(
            "batch",
            F.when(
                F.trim(F.col("CHARG")) == "",
                F.lit(None).cast(StringType())
            ).otherwise(F.trim(F.col("CHARG")))
        )

        # ── deletion flag ─────────────────────────────────────────────────────
        .withColumn(
            "is_deleted",
            F.when(F.trim(F.col("LVORM")) == "X", F.lit(True))
             .otherwise(F.lit(False))
        )

        # ── vehicle_state: populated in Gold via material JOIN (D7) ──────────
        .withColumn("vehicle_state", F.lit(""))

        # ── audit ─────────────────────────────────────────────────────────────
        .withColumn("_silver_ts", F.current_timestamp())

        # ── final Silver columns ──────────────────────────────────────────────
        .select(
            "equipment_id", "vin", "material_id", "description",
            "manufacturer", "plant", "installation_date",
            "year_of_construction", "batch", "is_deleted",
            "vehicle_state", "_silver_ts",
        )
    )


# ── Cell 4: Validate ─────────────────────────────────────────────────────────

def validate(df: "DataFrame") -> None:
    total = df.count()
    if total == 0:
        raise ValueError("[equipment] Silver transform returned 0 rows.")

    dupes = (
        df.groupBy("equipment_id")
          .count()
          .filter(F.col("count") > 1)
          .count()
    )
    if dupes > 0:
        raise ValueError(
            f"[equipment] {dupes} duplicate equipment_id values."
        )

    # D4: validate VIN = equipment_id assumption at SALFA
    mismatch = df.filter(F.col("vin") != F.col("equipment_id")).count()
    if mismatch > 0:
        print(
            f"[equipment] WARNING: {mismatch} records where "
            "VIN != equipment_id. Review D4 assumption."
        )

    print(f"[equipment] Silver validation passed: {total:,} rows")


# ── Cell 5: Upsert ───────────────────────────────────────────────────────────

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

    print(f"[equipment] Upsert complete → {target}")


# ── Cell 6: Main ─────────────────────────────────────────────────────────────

def run(spark) -> None:
    print("[equipment] Starting Silver transform...")

    df_bronze = spark.table(f"{LAKEHOUSE_BRONZE}.{SOURCE_TABLE}")
    df_silver = transform(df_bronze)

    validate(df_silver)
    upsert(spark, df_silver)

    print("[equipment] Silver transform complete.")


run(spark)
