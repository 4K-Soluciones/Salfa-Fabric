# Notebook:  nb_ventas_gold_build_sup_property
# Domain:    ventas
# Layer:     gold
# Action:    build
# Entity:    sup_property (Superación → /v1/property)
# Source:    lh_ventas_silver.customer
# Target:    lh_ventas_gold.gold_seedz_sup_property
# Load:      Full overwrite (daily after Silver refresh)
# Run after: nb_ventas_silver_transform_customer
#
# Key decisions:
#   D11 — All STRING, no NULLs, dates as dd-MM-yyyy HH:mm:ss
#   D12 — identification = {company_rut}_{plant_code}
#
# Note: In the Chile/SALFA context, property = customer (1:1 mapping).
#       Seedz requires both entities for compatibility with Brazilian model.

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
from pyspark.sql import functions as F

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_SILVER = "lh_ventas_silver"
LAKEHOUSE_GOLD   = "lh_ventas_gold"
SOURCE_TABLE     = "customer"
TARGET_TABLE     = "gold_seedz_sup_property"

COMPANY_RUT   = "96541530-9"
DEFAULT_PLANT = "1000"

# ── Cell 3: Transform ───────────────────────────────────────────────────────

def _to_gold_date(col_name: str) -> F.Column:
    return (
        F.when(
            F.col(col_name).isNull() | (F.col(col_name) == ""),
            F.lit("")
        )
        .otherwise(
            F.date_format(
                F.to_date(F.col(col_name), "yyyy-MM-dd"),
                "dd-MM-yyyy 00:00:00"
            )
        )
    )


def _empty_if_null(col_name: str) -> F.Column:
    return F.coalesce(F.col(col_name).cast("string"), F.lit(""))


def transform(df: "DataFrame") -> "DataFrame":
    """Build Gold property payload for Seedz /v1/property."""
    return (
        df
        .withColumn("identification",
            F.lit(f"{COMPANY_RUT}_{DEFAULT_PLANT}")
        )
        .withColumn("id", _empty_if_null("customer_id"))
        .withColumn("createdAt", _to_gold_date("created_date"))
        .withColumn("updatedAt",
            F.date_format(F.col("_silver_ts"), "dd-MM-yyyy HH:mm:ss")
        )
        .withColumn("clientId", _empty_if_null("customer_id"))
        .withColumn("companyName", _empty_if_null("name"))
        .withColumn("identificationProperty", F.lit(COMPANY_RUT))

        .select(
            "identification", "id", "createdAt", "updatedAt",
            "clientId", "companyName", "identificationProperty",
        )
    )


# ── Cell 4: Validate ────────────────────────────────────────────────────────

def validate(df: "DataFrame") -> None:
    total = df.count()
    if total == 0:
        raise ValueError("[gold:sup_property] Transform returned 0 rows.")

    nulls = sum(
        df.filter(F.col(c).isNull()).count()
        for c in df.columns
    )
    if nulls > 0:
        raise ValueError(
            f"[gold:sup_property] {nulls} NULL values found (D11)."
        )

    print(f"[gold:sup_property] Validation passed: {total:,} rows")


# ── Cell 5: Write ───────────────────────────────────────────────────────────

def run(spark) -> None:
    print("[gold:sup_property] Starting Gold build...")

    df = spark.table(f"{LAKEHOUSE_SILVER}.{SOURCE_TABLE}")
    df_gold = transform(df)
    validate(df_gold)

    (
        df_gold.write
               .format("delta")
               .mode("overwrite")
               .option("overwriteSchema", "true")
               .saveAsTable(f"{LAKEHOUSE_GOLD}.{TARGET_TABLE}")
    )

    print(f"[gold:sup_property] Complete → {LAKEHOUSE_GOLD}.{TARGET_TABLE}")


run(spark)
