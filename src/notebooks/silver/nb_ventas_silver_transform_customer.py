# Notebook:  nb_ventas_silver_transform_customer
# Domain:    ventas
# Layer:     silver
# Action:    transform
# Entity:    customer
# Source:    lh_ventas_bronze.brz_sap_sd_customer_raw
# Target:    lh_ventas_silver.customer
# Load:      Full daily — MERGE upsert on customer_id (D14)
# Run after: nb_ventas_bronze_ingest_customer
# Run before: nb_ventas_gold_build_* (Gold notebooks)
#
# Key decisions:
#   D15 — Silver is interface-agnostic (feeds Seedz, JDPrism, ELIPS)
#   D20 — email = '' pending O6 (CDS01 must expose ADRNR for ADR6 JOIN)
#
# SAP BW equivalent:
#   Source : PSA / Bronze DSO
#   Target : Core DSO key = customer_id
#   Type   : Write-optimized DSO → activated table
# SALFA naming: nb_<dominio>_<capa>_<accion>_<entidad> (Documento_Fabric.docx)

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_BRONZE  = "lh_ventas_bronze"
LAKEHOUSE_SILVER  = "lh_ventas_silver"
SOURCE_TABLE      = "brz_sap_sd_customer_raw"
TARGET_TABLE      = "customer"
MERGE_KEYS        = ["customer_id"]

# ── Cell 3: Transform ────────────────────────────────────────────────────────
# Transformation rules (golden_fixtures.md F01 / F02):
#   KUNNR        → customer_id   strip leading zeros for pure numerics
#   NAME1        → name          trim
#   STCD1        → rut           as-is (Chilean RUT format)
#   KTOKD        → account_group as-is
#   LAND1        → country       as-is
#   REGIO        → region_code   as-is
#   ERDAT        → created_date  YYYYMMDD → yyyy-MM-dd
#   LOEVM        → is_deleted    'X' → True
#   STREET       → address_street null if blank
#   CITY1        → address_city   null if blank
#   POST_CODE1   → postal_code   null if blank
#   TEL_NUMBER   → phone         null if blank
#   email        → ''            pending D20 / O6

def transform(df: "DataFrame") -> "DataFrame":
    """
    Apply Silver transformation rules to Bronze customer data.

    Args:
        df: Raw Bronze customer DataFrame.

    Returns:
        Transformed Silver customer DataFrame.
    """
    return (
        df

        # ── customer_id: strip leading zeros for purely numeric KUNNR ─────────
        .withColumn(
            "customer_id",
            F.when(
                F.col("KUNNR").rlike(r"^\d+$"),
                F.regexp_replace("KUNNR", r"^0+", "")
            ).otherwise(F.col("KUNNR"))
        )

        # ── name ──────────────────────────────────────────────────────────────
        .withColumn("name", F.trim(F.col("NAME1")))

        # ── rut (Chilean tax ID) — keep as-is including dash ─────────────────
        .withColumn(
            "rut",
            F.when(
                F.trim(F.col("STCD1")) == "",
                F.lit(None).cast(StringType())
            ).otherwise(F.trim(F.col("STCD1")))
        )

        # ── account_group ─────────────────────────────────────────────────────
        .withColumn("account_group", F.trim(F.col("KTOKD")))

        # ── country / region ──────────────────────────────────────────────────
        .withColumn("country",     F.trim(F.col("LAND1")))
        .withColumn("region_code", F.trim(F.col("REGIO")))

        # ── created_date: YYYYMMDD → yyyy-MM-dd ──────────────────────────────
        .withColumn(
            "created_date",
            F.when(
                F.col("ERDAT").isin("00000000", "", " ") |
                F.col("ERDAT").isNull(),
                F.lit(None).cast(StringType())
            ).otherwise(
                F.to_date(F.col("ERDAT"), "yyyyMMdd").cast(StringType())
            )
        )

        # ── is_deleted: LOEVM = 'X' → True ───────────────────────────────────
        .withColumn(
            "is_deleted",
            F.when(F.trim(F.col("LOEVM")) == "X", F.lit(True))
             .otherwise(F.lit(False))
        )

        # ── address ───────────────────────────────────────────────────────────
        .withColumn(
            "address_street",
            F.when(
                F.trim(F.col("STREET")) == "",
                F.lit(None).cast(StringType())
            ).otherwise(F.trim(F.col("STREET")))
        )
        .withColumn(
            "address_city",
            F.when(
                F.trim(F.col("CITY1")) == "",
                F.lit(None).cast(StringType())
            ).otherwise(F.trim(F.col("CITY1")))
        )
        .withColumn(
            "postal_code",
            F.when(
                F.trim(F.col("POST_CODE1")) == "",
                F.lit(None).cast(StringType())
            ).otherwise(F.trim(F.col("POST_CODE1")))
        )

        # ── phone ─────────────────────────────────────────────────────────────
        .withColumn(
            "phone",
            F.when(
                F.trim(F.col("TEL_NUMBER")) == "",
                F.lit(None).cast(StringType())
            ).otherwise(F.trim(F.col("TEL_NUMBER")))
        )

        # ── email: pending O6 (CDS01 ADRNR → ADR6 join not yet deployed) ─────
        .withColumn("email", F.lit(""))

        # ── audit ─────────────────────────────────────────────────────────────
        .withColumn("_silver_ts", F.current_timestamp())

        # ── final Silver columns ──────────────────────────────────────────────
        .select(
            "customer_id", "name", "rut", "account_group",
            "country", "region_code", "created_date", "is_deleted",
            "address_street", "address_city", "postal_code",
            "phone", "email", "_silver_ts",
        )
    )


# ── Cell 4: Validate ─────────────────────────────────────────────────────────

def validate(df: "DataFrame") -> None:
    """Quality checks on Silver output."""
    total = df.count()
    if total == 0:
        raise ValueError("[customer] Silver transform returned 0 rows.")

    dupes = (
        df.groupBy("customer_id")
          .count()
          .filter(F.col("count") > 1)
          .count()
    )
    if dupes > 0:
        raise ValueError(
            f"[customer] {dupes} duplicate customer_id values detected."
        )

    print(f"[customer] Silver validation passed: {total:,} rows")


# ── Cell 5: Upsert ───────────────────────────────────────────────────────────

def upsert(spark, df: "DataFrame") -> None:
    """
    MERGE into Silver customer table. Creates on first run.
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

    print(f"[customer] Upsert complete → {target}")


# ── Cell 6: Main ─────────────────────────────────────────────────────────────

def run(spark) -> None:
    print("[customer] Starting Silver transform...")

    df_bronze = spark.table(f"{LAKEHOUSE_BRONZE}.{SOURCE_TABLE}")
    df_silver = transform(df_bronze)

    validate(df_silver)
    upsert(spark, df_silver)

    print("[customer] Silver transform complete.")


run(spark)
