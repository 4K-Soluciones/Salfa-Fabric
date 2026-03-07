# Notebook:  nb_ventas_gold_build_sup_customer
# Domain:    ventas
# Layer:     gold
# Action:    build
# Entity:    sup_customer (Superación → /v1/customer)
# Source:    lh_ventas_silver.customer
# Target:    lh_ventas_gold.gold_seedz_sup_customer
# Load:      Full overwrite (daily after Silver refresh)
# Run after: nb_ventas_silver_transform_customer
#
# Key decisions:
#   D11 — All STRING, no NULLs, dates as dd-MM-yyyy HH:mm:ss
#   D12 — identification = {company_rut}_{plant_code}
#
# SAP BW equivalent:
#   CompositeProvider / BEx query on top of DSO
# SALFA naming: nb_<dominio>_<capa>_<accion>_<entidad> (Documento_Fabric.docx)

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
from pyspark.sql import functions as F

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_SILVER = "lh_ventas_silver"
LAKEHOUSE_GOLD   = "lh_ventas_gold"
SOURCE_TABLE     = "customer"
TARGET_TABLE     = "gold_seedz_sup_customer"

# Company identification (D12) — confirm with SALFA
COMPANY_RUT  = "96541530-9"    # SALFA's RUT — TODO: confirm
DEFAULT_PLANT = "1000"          # Default plant — TODO: confirm

# ── Cell 3: Transform ───────────────────────────────────────────────────────

def _to_gold_date(col_name: str) -> F.Column:
    """Silver date (yyyy-MM-dd) → Gold format (dd-MM-yyyy HH:mm:ss)."""
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
    """NULL → empty string (D11)."""
    return F.coalesce(F.col(col_name).cast("string"), F.lit(""))


def transform(df: "DataFrame") -> "DataFrame":
    """Build Gold customer payload for Seedz /v1/customer."""
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
        .withColumn("companyName", _empty_if_null("name"))
        .withColumn("clientIdentification", _empty_if_null("rut"))
        .withColumn("phones", _empty_if_null("phone"))
        .withColumn("email", _empty_if_null("email"))

        .select(
            "identification", "id", "createdAt", "updatedAt",
            "companyName", "clientIdentification", "phones", "email",
        )
    )


# ── Cell 4: Validate ────────────────────────────────────────────────────────

def validate(df: "DataFrame") -> None:
    total = df.count()
    if total == 0:
        raise ValueError("[gold:sup_customer] Transform returned 0 rows.")

    nulls = sum(
        df.filter(F.col(c).isNull()).count()
        for c in df.columns
    )
    if nulls > 0:
        raise ValueError(
            f"[gold:sup_customer] {nulls} NULL values found — "
            "Gold layer must have no NULLs (D11)."
        )

    print(f"[gold:sup_customer] Validation passed: {total:,} rows")


# ── Cell 5: Write ───────────────────────────────────────────────────────────

def run(spark) -> None:
    print("[gold:sup_customer] Starting Gold build...")

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

    print(f"[gold:sup_customer] Complete → {LAKEHOUSE_GOLD}.{TARGET_TABLE}")


run(spark)
