# Notebook:  nb_common_utils_seedz
# Domain:    common (transversal — used by all ventas notebooks)
# Layer:     utils
# Purpose:   Shared utility functions for Bronze / Silver / Gold notebooks
# Run with:  %run ../utils/nb_common_utils_seedz
#
# SAP BW equivalent: ABAP routine library used across transformations
# SALFA naming: nb_common_utils_<scope> (Documento_Fabric.docx)

# ── Cell 1: Imports ──────────────────────────────────────────────────────────
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DecimalType
from typing import Optional


# ── Cell 2: Date utilities ───────────────────────────────────────────────────

def sap_date_to_iso(col_name: str) -> F.Column:
    """
    Convert SAP YYYYMMDD string to ISO date (yyyy-MM-dd).
    '00000000' and '' → None.

    SAP BW equivalent: date conversion routine in transformation rules.

    Args:
        col_name: Name of the source column.

    Returns:
        Column expression with date as yyyy-MM-dd string or None.
    """
    return (
        F.when(
            F.col(col_name).isin("00000000", "0", "", " ") |
            F.col(col_name).isNull(),
            F.lit(None)
        )
        .otherwise(
            F.to_date(F.col(col_name), "yyyyMMdd")
             .cast(StringType())
        )
    )


def sap_date_to_gold(col_name: str) -> F.Column:
    """
    Convert SAP YYYYMMDD to Gold format: dd-MM-yyyy HH:mm:ss.
    '00000000' → empty string (D11: no NULLs in Gold).

    Args:
        col_name: Name of the source column.

    Returns:
        Column expression with Gold-formatted date string.
    """
    return (
        F.when(
            F.col(col_name).isin("00000000", "0", "", " ") |
            F.col(col_name).isNull(),
            F.lit("")
        )
        .otherwise(
            F.date_format(
                F.to_date(F.col(col_name), "yyyyMMdd"),
                "dd-MM-yyyy 00:00:00"
            )
        )
    )


# ── Cell 3: Numeric utilities ────────────────────────────────────────────────

def safe_cast_decimal(
    col_name: str,
    precision: int = 18,
    scale: int = 2
) -> F.Column:
    """
    Cast SAP amount/quantity string to Decimal.
    Handles trailing minus (D18): '1234.56-' → -1234.56.
    Empty/null/non-numeric → None.

    SAP BW equivalent: Amount keyfigure with sign handling.

    Args:
        col_name: Name of the source column.
        precision: Decimal precision (default 18).
        scale: Decimal scale (default 2).

    Returns:
        Column expression as DecimalType.
    """
    return (
        F.when(
            F.col(col_name).isNull() |
            (F.trim(F.col(col_name)) == ""),
            F.lit(None).cast(DecimalType(precision, scale))
        )
        .when(
            F.col(col_name).endswith("-"),
            F.concat(F.lit("-"), F.regexp_replace(col_name, "-$", ""))
             .cast(DecimalType(precision, scale))
        )
        .otherwise(
            F.col(col_name).cast(DecimalType(precision, scale))
        )
    )


def strip_leading_zeros(col_name: str) -> F.Column:
    """
    Strip leading zeros from purely numeric strings.
    Non-numeric strings (e.g. RUT '10054913-1') returned as-is.

    SAP BW equivalent: Conversion exit ALPHA for characteristic values.

    Args:
        col_name: Name of the source column.

    Returns:
        Column with leading zeros removed for numeric strings only.
    """
    return (
        F.when(
            F.col(col_name).rlike(r"^\d+$"),
            F.regexp_replace(col_name, r"^0+", "")
        )
        .otherwise(F.col(col_name))
    )


# ── Cell 4: String utilities ─────────────────────────────────────────────────

def null_if_blank(col_name: str) -> F.Column:
    """
    Return None if string is empty or whitespace-only.

    Args:
        col_name: Name of the source column.

    Returns:
        Column with None for blank values.
    """
    return (
        F.when(
            F.trim(F.col(col_name)) == "",
            F.lit(None)
        )
        .otherwise(F.trim(F.col(col_name)))
    )


def empty_if_null(col_name: str) -> F.Column:
    """
    Return empty string if value is None (D11: no NULLs in Gold).

    Args:
        col_name: Name of the source column.

    Returns:
        Column with empty string for None values.
    """
    return F.coalesce(F.col(col_name), F.lit(""))


# ── Cell 5: Watermark utilities ──────────────────────────────────────────────

def get_watermark(
    spark,
    lakehouse: str,
    entity: str
) -> Optional[str]:
    """
    Read last successful load watermark for an entity.
    Returns None if no watermark exists (= full load).

    SAP BW equivalent: Delta process last timestamp (RSA7 queue).

    Args:
        spark: Active SparkSession.
        lakehouse: Lakehouse name (e.g. 'lh_ventas_bronze').
        entity: Entity name (e.g. 'customer').

    Returns:
        Watermark string (YYYYMMDD) or None.
    """
    try:
        df = spark.sql(
            f"SELECT watermark FROM {lakehouse}.brz_watermark "
            f"WHERE entity = '{entity}'"
        )
        row = df.orderBy(F.col("updated_at").desc()).first()
        return row["watermark"] if row else None
    except Exception:
        return None


def set_watermark(
    spark,
    lakehouse: str,
    entity: str,
    watermark: str
) -> None:
    """
    Upsert watermark after a successful load.

    Args:
        spark: Active SparkSession.
        lakehouse: Lakehouse name.
        entity: Entity name.
        watermark: New watermark value (YYYYMMDD).
    """
    spark.sql(f"""
        MERGE INTO {lakehouse}.brz_watermark AS t
        USING (
            SELECT '{entity}'   AS entity,
                   '{watermark}' AS watermark,
                   current_timestamp() AS updated_at
        ) AS s
        ON t.entity = s.entity
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# ── Cell 6: Delta write utilities ────────────────────────────────────────────

def write_bronze(
    df: DataFrame,
    lakehouse: str,
    table_name: str,
    mode: str = "overwrite"
) -> None:
    """
    Write DataFrame to Bronze Delta table.
    Full load = overwrite. Delta load = append.

    SAP BW equivalent: PSA write (Persistent Staging Area).

    Args:
        df: Source DataFrame.
        lakehouse: Target lakehouse name.
        table_name: Target table name.
        mode: Write mode ('overwrite' or 'append').
    """
    (
        df.write
          .format("delta")
          .mode(mode)
          .option("overwriteSchema", "true")
          .saveAsTable(f"{lakehouse}.{table_name}")
    )


def write_silver(
    spark,
    df: DataFrame,
    lakehouse: str,
    table_name: str,
    merge_keys: list
) -> None:
    """
    Upsert DataFrame into Silver Delta table using MERGE.
    Creates table on first run if it does not exist.

    SAP BW equivalent: DSO activation (delta merge into active table).

    Args:
        spark: Active SparkSession.
        df: Source DataFrame with transformed data.
        lakehouse: Target lakehouse name.
        table_name: Target table name (e.g. 'customer').
        merge_keys: List of key column names for MERGE ON clause.
    """
    from delta.tables import DeltaTable

    target = f"{lakehouse}.{table_name}"
    merge_condition = " AND ".join(
        [f"t.{k} = s.{k}" for k in merge_keys]
    )

    if DeltaTable.isDeltaTable(spark, target):
        (
            DeltaTable.forName(spark, target)
                      .alias("t")
                      .merge(df.alias("s"), merge_condition)
                      .whenMatchedUpdateAll()
                      .whenNotMatchedInsertAll()
                      .execute()
        )
    else:
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .saveAsTable(target)
        )
