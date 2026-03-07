# Notebook:  nb_ventas_gold_build_cens_payload
# Domain:    ventas
# Layer:     gold
# Action:    build
# Entity:    cens_payload (Censacional → /v1/censacional)
# Source:    lh_ventas_silver.billing_document   (base)
#            + lh_ventas_silver.customer         (customer name)
#            + lh_ventas_silver.material         (brand filter, category)
#            + lh_ventas_silver.equipment         (VIN, model, vehicleState)
# Target:    lh_ventas_gold.gold_seedz_cens_payload
# Load:      Full overwrite
# Run after: ALL Silver transforms (billing, customer, material, equipment)
#
# This is the MOST COMPLEX Gold notebook — multi-table JOIN.
#
# Key decisions:
#   D3  — Only JD brand materials
#   D5  — Equipment JOIN via batch (CHARG = HESSION), 94% coverage
#   D6  — Empty CHARG = parts/accessories (no equipment JOIN)
#   D7  — Vehicle state from division: M0/M2/M4→'N', M1/M3/M5→'U'
#   D11 — All STRING, no NULLs
#   D13 — Censacional = billing line items enriched with equipment
#
# SAP BW equivalent:
#   MultiProvider: billing + equipment + customer + material
#   BEx query with all master data attributes resolved

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
from pyspark.sql import functions as F

# ── Cell 2: Configuration ────────────────────────────────────────────────────
LAKEHOUSE_SILVER = "lh_ventas_silver"
LAKEHOUSE_GOLD   = "lh_ventas_gold"
TARGET_TABLE     = "gold_seedz_cens_payload"

DEALER_CODE = "SALFA_CL"  # TODO: confirm with Seedz

# ── Cell 3: Helpers ──────────────────────────────────────────────────────────

def _e(col_name: str) -> F.Column:
    """Empty if null (D11)."""
    return F.coalesce(F.col(col_name).cast("string"), F.lit(""))


def _to_gold_date_short(col_name: str) -> F.Column:
    return (
        F.when(
            F.col(col_name).isNull() | (F.col(col_name) == ""),
            F.lit("")
        )
        .otherwise(
            F.date_format(
                F.to_date(F.col(col_name), "yyyy-MM-dd"),
                "dd-MM-yyyy"
            )
        )
    )


def _decimal_to_str(col_name: str) -> F.Column:
    return (
        F.when(F.col(col_name).isNull(), F.lit("0.00"))
         .otherwise(F.format_number(F.col(col_name), 2))
    )


# ── Cell 4: Transform ───────────────────────────────────────────────────────

def transform(spark) -> "DataFrame":
    """
    Build Gold Censacional payload for Seedz /v1/censacional.

    Join chain:
      1. Start: billing_document WHERE is_cancelled = False
      2. INNER JOIN material ON material_id (filter brand = 'JD')
      3. LEFT JOIN customer ON customer_id
      4. LEFT JOIN equipment ON batch + plant (D5, 94% coverage)

    For parts (empty batch): chassi="", model="", vehicleState=""
    For machinery with no equipment match: same empty fields
    """
    df_bill = spark.table(f"{LAKEHOUSE_SILVER}.billing_document")
    df_cust = spark.table(f"{LAKEHOUSE_SILVER}.customer")
    df_mat  = spark.table(f"{LAKEHOUSE_SILVER}.material")
    df_equi = spark.table(f"{LAKEHOUSE_SILVER}.equipment")

    # ── 1. Base: non-cancelled billing items ─────────────────────────────
    df_base = df_bill.filter(F.col("is_cancelled") == False)

    # ── 2. INNER JOIN material (JD brand only — D3) ─────────────────────
    df_mat_slim = (
        df_mat
        .filter(F.col("brand") == "JD")
        .select(
            F.col("material_id").alias("_mat_id"),
            F.col("description").alias("_mat_desc"),
            F.col("category").alias("_mat_category"),
        )
        .dropDuplicates(["_mat_id"])
    )

    df = df_base.join(
        df_mat_slim,
        df_base["material_id"] == df_mat_slim["_mat_id"],
        "inner"  # Only JD materials
    )

    # ── 3. LEFT JOIN customer ────────────────────────────────────────────
    df_cust_slim = df_cust.select(
        F.col("customer_id").alias("_cust_id"),
        F.col("name").alias("_cust_name"),
    )

    df = df.join(
        df_cust_slim,
        df["customer_id"] == df_cust_slim["_cust_id"],
        "left"
    )

    # ── 4. LEFT JOIN equipment ON batch + plant (D5) ─────────────────────
    #    Only when batch is not empty (D6)
    df_equi_slim = df_equi.select(
        F.col("batch").alias("_equi_batch"),
        F.col("plant").alias("_equi_plant"),
        F.col("vin").alias("_equi_vin"),                  # VIN from EQUI.SERNR (D4)
        F.col("description").alias("_equi_model"),
        F.col("vehicle_state").alias("_equi_vs"),
    ).dropDuplicates(["_equi_batch", "_equi_plant"])

    # D6: Only JOIN equipment when batch is non-empty (parts have no batch)
    df = df.join(
        df_equi_slim,
        (F.col("batch").isNotNull()) &
        (F.col("batch") != "") &
        (df["batch"] == df_equi_slim["_equi_batch"]) &
        (df["plant"] == df_equi_slim["_equi_plant"]),
        "left"
    )

    # ── 5. Determine isMachinery ─────────────────────────────────────────
    is_machinery = (
        F.when(
            F.col("_mat_category") == "MACHINERY", F.lit("true")
        ).otherwise(F.lit("false"))
    )

    # ── 6. Build payload — all STRING, no NULLs (D11) ───────────────────
    return (
        df
        .withColumn("dealerCode", F.lit(DEALER_CODE))
        .withColumn("billingDocument", _e("billing_id"))
        .withColumn("billingItem", _e("billing_item"))
        .withColumn("billingDate", _to_gold_date_short("billing_date"))
        .withColumn("customerNumber", _e("customer_id"))
        .withColumn("customerName",
            F.coalesce(F.col("_cust_name").cast("string"), F.lit(""))
        )
        .withColumn("materialNumber", _e("material_id"))
        .withColumn("materialDescription", _e("item_description"))
        .withColumn("quantity", _decimal_to_str("billed_qty"))
        .withColumn("netValue", _decimal_to_str("net_value_item"))
        .withColumn("currency", _e("currency"))
        .withColumn("plant", _e("plant"))

        # Equipment fields: populated for machinery with match, empty otherwise
        .withColumn("chassi",
            F.coalesce(F.col("_equi_vin").cast("string"), F.lit(""))
        )
        .withColumn("model",
            F.coalesce(F.col("_mat_desc").cast("string"), F.lit(""))
        )
        .withColumn("vehicleState",
            F.coalesce(F.col("_equi_vs").cast("string"), F.lit(""))
        )

        .withColumn("sellerCode", _e("seller_id"))
        .withColumn("invoiceType", _e("invoice_type"))
        .withColumn("isMachinery", is_machinery)

        .select(
            "dealerCode", "billingDocument", "billingItem", "billingDate",
            "customerNumber", "customerName",
            "materialNumber", "materialDescription",
            "quantity", "netValue", "currency", "plant",
            "chassi", "model", "vehicleState",
            "sellerCode", "invoiceType", "isMachinery",
        )
    )


# ── Cell 5: Validate ────────────────────────────────────────────────────────

def validate(df: "DataFrame") -> None:
    total = df.count()
    if total == 0:
        raise ValueError("[gold:cens_payload] Transform returned 0 rows.")

    nulls = sum(
        df.filter(F.col(c).isNull()).count()
        for c in df.columns
    )
    if nulls > 0:
        raise ValueError(
            f"[gold:cens_payload] {nulls} NULL values found (D11)."
        )

    # Report machinery vs parts
    machinery = df.filter(F.col("isMachinery") == "true").count()
    parts = total - machinery
    print(
        f"[gold:cens_payload] Machinery: {machinery:,} | "
        f"Parts: {parts:,}"
    )

    # Report equipment coverage for machinery
    if machinery > 0:
        with_vin = df.filter(
            (F.col("isMachinery") == "true") &
            (F.col("chassi") != "")
        ).count()
        print(
            f"[gold:cens_payload] Machinery with VIN: "
            f"{with_vin:,} / {machinery:,} "
            f"({100 * with_vin / machinery:.1f}%)"
        )

    print(f"[gold:cens_payload] Validation passed: {total:,} rows")


# ── Cell 6: Write ───────────────────────────────────────────────────────────

def run(spark) -> None:
    print("[gold:cens_payload] Starting Gold build...")

    df_gold = transform(spark)
    validate(df_gold)

    (
        df_gold.write
               .format("delta")
               .mode("overwrite")
               .option("overwriteSchema", "true")
               .saveAsTable(f"{LAKEHOUSE_GOLD}.{TARGET_TABLE}")
    )

    print(
        f"[gold:cens_payload] Complete → "
        f"{LAKEHOUSE_GOLD}.{TARGET_TABLE}"
    )


run(spark)
