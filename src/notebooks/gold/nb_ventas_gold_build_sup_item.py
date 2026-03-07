# Notebook:  nb_ventas_gold_build_sup_item
# Domain:    ventas
# Layer:     gold
# Action:    build
# Entity:    sup_item (Superación → /v1/item)
# Source:    lh_ventas_silver.material
# Target:    lh_ventas_gold.gold_seedz_sup_item
# Load:      Full overwrite (daily after Silver refresh)
# Run after: nb_ventas_silver_transform_material
#
# Key decisions:
#   D3  — Only JD brand materials sent to Seedz (brand = 'JD')
#   D11 — All STRING, no NULLs

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
from pyspark.sql import functions as F

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_SILVER = "lh_ventas_silver"
LAKEHOUSE_GOLD   = "lh_ventas_gold"
SOURCE_TABLE     = "material"
TARGET_TABLE     = "gold_seedz_sup_item"

COMPANY_RUT   = "96541530-9"
DEFAULT_PLANT = "1000"

# ── Cell 3: Transform ───────────────────────────────────────────────────────

def _empty_if_null(col_name: str) -> F.Column:
    return F.coalesce(F.col(col_name).cast("string"), F.lit(""))


def transform(df: "DataFrame") -> "DataFrame":
    """Build Gold item payload for Seedz /v1/item. JD brand only (D3)."""
    return (
        df
        # Filter: only JD brand materials
        .filter(F.col("brand") == "JD")

        .withColumn("identification",
            F.lit(f"{COMPANY_RUT}_{DEFAULT_PLANT}")
        )
        .withColumn("id", _empty_if_null("material_id"))
        .withColumn("createdAt",
            F.date_format(F.col("_silver_ts"), "dd-MM-yyyy HH:mm:ss")
        )
        .withColumn("updatedAt",
            F.date_format(F.col("_silver_ts"), "dd-MM-yyyy HH:mm:ss")
        )
        .withColumn("sku", _empty_if_null("material_id"))
        .withColumn("description", _empty_if_null("description"))

        .select(
            "identification", "id", "createdAt", "updatedAt",
            "sku", "description",
        )
    )


# ── Cell 4: Validate ────────────────────────────────────────────────────────

def validate(df: "DataFrame") -> None:
    total = df.count()
    if total == 0:
        raise ValueError("[gold:sup_item] Transform returned 0 rows.")

    nulls = sum(
        df.filter(F.col(c).isNull()).count()
        for c in df.columns
    )
    if nulls > 0:
        raise ValueError(
            f"[gold:sup_item] {nulls} NULL values found (D11)."
        )

    print(f"[gold:sup_item] Validation passed: {total:,} rows (JD only)")


# ── Cell 5: Write ───────────────────────────────────────────────────────────

def run(spark) -> None:
    print("[gold:sup_item] Starting Gold build...")

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

    print(f"[gold:sup_item] Complete → {LAKEHOUSE_GOLD}.{TARGET_TABLE}")


run(spark)
