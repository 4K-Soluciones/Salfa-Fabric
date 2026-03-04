# Business Rules — SALFA Seedz

## 1. Invoice Type Derivation (invoice_type)

Determines whether a billing document is a **debit** (sale) or **credit** (return/cancellation).

```python
# SDDocCategory (VBTYP) determines the type:
#   'M' = Debit invoice (sale)          → invoice_type = '1' (Nota de saída)
#   'O' = Credit memo (return)          → invoice_type = '0' (Nota de entrada)
#   'N' = Cancellation invoice          → invoice_type = '0'
#   'P' = Debit memo (price adjustment) → invoice_type = '1'
#   Other                               → invoice_type = '1' (default to debit)

def derive_invoice_type(vbtyp):
    return F.when(F.col(vbtyp).isin('O', 'N'), F.lit('0')) \
            .otherwise(F.lit('1'))
```

## 2. Invoice Status Derivation (invoice_status) — Priority Chain

Status determination uses a **priority chain**. First condition that matches wins.

```python
# Priority chain (check in this order):
#   1. fksto = 'X'              → '0' (Cancelada)
#   2. sfakn != ''              → '0' (Cancelada — has cancellation reference)
#   3. rfbsk = 'C'              → '2' (Concluída — fully processed)
#   4. rfbsk = 'B'              → '1' (Pendente — partially processed)
#   5. rfbsk = 'A'              → '1' (Pendente — open)
#   6. rfbsk = ''               → '2' (Concluída — no billing status = completed)
#   7. DEFAULT                  → '1' (Pendente)

def derive_invoice_status(df):
    return df.withColumn("invoice_status",
        F.when(F.col("fksto") == "X", F.lit("0"))
         .when(F.col("sfakn") != "", F.lit("0"))
         .when(F.col("rfbsk") == "C", F.lit("2"))
         .when(F.col("rfbsk").isin("A", "B"), F.lit("1"))
         .when(F.col("rfbsk") == "", F.lit("2"))
         .otherwise(F.lit("1"))
    )
```

> **NOTE:** The `rfbsk` values (A, B, C) are SALFA-specific. Run discovery query `SELECT DISTINCT rfbsk FROM vbrk` to confirm actual values used.

## 3. Order Status Derivation (order_status)

```python
# Seedz order status values:
#   '0' = Pendente de faturamento (pending billing)
#   '1' = Faturado (billed)
#   '2' = Cancelado (cancelled)
#   '3' = Venda Perdida (lost sale)

# Priority chain:
#   1. abgru != '' (rejection reason exists)  → '3' (lost sale) or '2' (cancelled)
#      - abgru determines whether it's cancelled or lost. Both valid.
#      - For simplicity: abgru != '' → '2' (cancelled)
#   2. Has billing reference (check VBFA or billing exists) → '1' (billed)
#   3. DEFAULT → '0' (pending)

def derive_order_status(df):
    return df.withColumn("status",
        F.when(F.col("rejection_reason") != "", F.lit("2"))
         .when(F.col("has_billing") == True, F.lit("1"))
         .otherwise(F.lit("0"))
    )
```

## 4. Accounts Receivable Status Derivation (ar_status)

```python
# Seedz AR status values (Portuguese):
#   'Aberto'     = Open, not yet due
#   'Liquidado'  = Paid/Cleared
#   'Atrasado'   = Overdue (past due, not cleared)
#   'Protestado' = Protested (legal action — not used at SALFA)
#   'Devolvido'  = Returned (bounced — not used at SALFA)
#   'Cobrança'   = In collection (not used at SALFA)

# Priority chain:
#   1. is_cleared = True          → 'Liquidado'
#   2. is_overdue = True          → 'Atrasado'
#   3. DEFAULT                    → 'Aberto'

def derive_ar_status(df):
    return df.withColumn("status",
        F.when(F.col("is_cleared") == True, F.lit("Liquidado"))
         .when(F.col("is_overdue") == True, F.lit("Atrasado"))
         .otherwise(F.lit("Aberto"))
    )
```

## 5. Brand Classification Rules

```python
# John Deere Parts:
#   MFRNR IN ('0000100005', '0000100157')
#   → brand = 'JD', item_category = 'PARTS'

# John Deere Machinery:
#   SPART LIKE 'M%' AND MATKL = '0005'
#   (NOTE: MFRNR is EMPTY for machines at SALFA! Don't use it.)
#   → brand = 'JD', item_category = 'MACHINERY'

# Everything else:
#   → brand = 'OTHER', item_category = 'OTHER'
```

## 6. Vehicle State Mapping

```python
# MARA.SPART → vehicleState
#   M0 → 'N' (Nuevo / New)
#   M2 → 'N'
#   M4 → 'N'
#   M1 → 'U' (Usado / Used)
#   M3 → 'U'
#   M5 → 'U'
#   Others → '' (empty — not a vehicle)
```

## 7. Date Formatting

```python
# SAP dates come as: YYYYMMDD (8 chars) or YYYY-MM-DD (10 chars)
# Seedz expects:     dd-MM-yyyy HH:mm:ss

# Special cases:
#   '00000000' → '' (empty string, not NULL)
#   NULL       → '' (empty string)
#   ''         → '' (empty string)
#   '99991231' → '' (empty string — SAP "forever" date)

# For Gold layer: ALL dates must include time component
#   Date only → append ' 00:00:00'
#   Example: '2024-03-15' → '15-03-2024 00:00:00'
```

## 8. Amount / Decimal Formatting

```python
# SAP amounts may have TRAILING MINUS:
#   '1234.56-' means -1234.56
#   '0.00' means zero
#   '' or NULL means 0

# ⚠️ CRITICAL: Run trailing minus discovery query FIRST!
# SELECT netwr FROM vbrp WHERE vbtyp = 'O' LIMIT 10
# If results show '1234.56-' → trailing minus is active
# If results show '-1234.56' → standard format

# For Gold layer: amounts as STRING with 2 decimal places
#   1234.56 → '1234.56'
#   -500.00 → '-500.00'
#   0       → '0.00'
```

## 9. Identification Field (Chile-specific)

```python
# Seedz 'identification' field = company identifier + branch
# For Chile: RUT (Rol Único Tributario) + plant/branch code
# Format: "{company_rut}_{plant_code}"
#
# Example: "76123456-7_1010" where:
#   76123456-7 = SALFA's RUT
#   1010       = Plant code from T001W
#
# ⚠️ Get SALFA's RUT from config or business team
# ⚠️ Confirm exact format with Seedz documentation

# companyIdentification (without branch):
# Just the RUT: "76123456-7"
```

## 10. NULL Handling in Gold Layer

```python
# ABSOLUTE RULE: No NULL values in Gold tables.
# Every NULL must be replaced:
#   STRING fields → '' (empty string)
#   DATE fields   → '' (empty string, NOT '00-00-0000')
#   NUMERIC fields → '0' or '0.00'

# Apply at the end of every Gold notebook:
def null_to_empty(df):
    """Replace all NULLs with empty strings for Gold layer."""
    for col_name in df.columns:
        df = df.withColumn(col_name,
            F.when(F.col(col_name).isNull(), F.lit(""))
             .otherwise(F.col(col_name).cast("string"))
        )
    return df
```

## 11. Equipment-Billing JOIN Logic (Censacional)

```python
# The JOIN between billing and equipment uses BATCH (CHARG/HESSION):
#   billing_document.batch = equipment.batch
#   AND billing_document.plant = equipment.plant
#
# Coverage: 94.1% of machinery billing items have matching equipment
# The remaining 5.9% will have empty chassi/model/vehicleState
#
# This is a LEFT OUTER JOIN — never lose billing records
# If no equipment match: chassi = "", model = "", vehicleState = ""
#
# Filter: Only join when batch is not empty (CHARG <> '')
#   Items with empty batch are accessories/parts, not serialized equipment
```

## 12. Seller Partner Function

```python
# Seller is identified by Partner Function 'VE' (Vendedor):
#   VBPA.PARVW = 'VE' AND VBPA.POSNR = '000000' (header level)
#
# The PARNR field contains the seller's partner number
# This is joined to the seller master (ZES229_VENDEDORES) for seller details
#
# If no VE partner exists → seller_id = '' (empty string)
```
