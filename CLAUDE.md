# CLAUDE.md — SALFA Seedz Fabric Project

## Project Identity

- **Client:** SALFA (John Deere dealer, Chile)
- **Project:** Seedz data integration (Superación + Censacional)
- **Platform:** Microsoft Fabric — Medallion Architecture
- **Source:** SAP ECC on HANA → CDS Views (ZFB_CDS01–CDS07) → OData → OPDG → Fabric
- **Target:** Seedz REST API (api-prod.seedz.ag)
- **Language:** PySpark (Fabric Notebooks)
- **Future:** S/4HANA RISE migration in 2027 (CDS views are S/4-ready)

## What You're Building

28 PySpark notebooks that transform SAP ECC data through 4 layers:

```
SAP ECC → [CDS/OData] → Landing → Bronze → Silver → Gold → Seedz API
                         (raw)    (typed)  (clean)  (API format)
```

**Superación:** 8 entities → individual API endpoints
**Censacional:** 1 flat payload per billing line → /v1/censacional

## Critical Rules

1. **Read `agent_docs/` before writing any code.** Schemas, business rules, and golden fixtures are your source of truth.
2. **Follow naming conventions exactly.** See Naming section below.
3. **Never hard-code business rules.** Use config tables (config_brand_rules, config_vehicle_state).
4. **All Gold layer values must be STRING type.** Dates as `dd-MM-yyyy HH:mm:ss`. No NULLs — use empty string `""`.
5. **PEP 8 + SOLID + Functional style.** See `agent_docs/coding_standards.md`.
6. **Test against golden fixtures.** Every Silver/Gold notebook must have assertions comparing output to expected values.
7. **Chile-specific:** RUT is the tax ID format. SALFA company code = probably `1000` or `CL01` (confirm with config).

## Repository Structure

```
Salfa-Fabric/
├── CLAUDE.md                          ← YOU ARE HERE
├── agent_docs/
│   ├── schemas_reference.md           ← All Bronze/Silver/Gold table schemas
│   ├── task_queue.md                  ← 28 tasks with dependencies and acceptance criteria
│   ├── golden_fixtures.md             ← Real SAP data: input → expected output (TODO: fill after Eclipse extraction)
│   ├── business_rules.md              ← SALFA-specific transformation rules
│   └── seedz_api_payloads.md          ← Seedz API endpoint formats and field mappings
├── config/
│   ├── config_brand_rules.csv         ← Brand classification rules (JD parts, JD machines, future brands)
│   └── config_vehicle_state.csv       ← SPART → vehicleState mapping (N/U)
├── src/
│   ├── notebooks/
│   │   ├── bronze/                    ← 7 ingestion notebooks (Landing → Bronze)
│   │   ├── silver/                    ← 7 transformation notebooks (Bronze → Silver)
│   │   ├── gold/                      ← 11 build notebooks (Silver → Gold)
│   │   └── utils/                     ← Shared utilities (delta, logging, API auth)
│   └── tests/
│       ├── test_bronze_customer.py
│       ├── test_silver_billing.py
│       └── ...
└── .gitignore
```

## Naming Conventions (MANDATORY)

### Lakehouses
```
lh_ventas_landing
lh_ventas_bronze
lh_ventas_silver
lh_ventas_gold
```

### Tables
```
Bronze:  brz_sap_{module}_{entity}_raw     → brz_sap_sd_billing_raw
Silver:  {entity}                           → billing_document, customer, equipment
Gold:    gold_seedz_{program}_{entity}      → gold_seedz_sup_customer, gold_seedz_cens_payload
Config:  config_{purpose}                   → config_brand_rules, config_vehicle_state
```

### Notebooks
```
nb_seedz_{layer}_{action}_{entity}
  Bronze:  nb_seedz_bronze_ingest_customer
  Silver:  nb_seedz_silver_transform_billing
  Gold:    nb_seedz_gold_build_sup_customer
  Gold:    nb_seedz_gold_build_cens_payload
  Utils:   nb_common_utils_delta
```

### Pipelines
```
pl_seedz_{layer}_{scope}
  pl_seedz_landing_full
  pl_seedz_landing_delta
  pl_seedz_bronze_process
  pl_seedz_silver_process
  pl_seedz_gold_superacion
  pl_seedz_gold_censacional
  pl_seedz_api_send
  pl_seedz_master              ← Orchestrates everything
```

## Notebook Template

Every notebook MUST follow this structure:

```python
# Notebook: nb_seedz_{layer}_{action}_{entity}
# Layer: Bronze | Silver | Gold
# Description: {one-line description}
# Source: {input table(s)}
# Target: {output table}
# Author: Claude Code + Human review
# Date: {YYYY-MM-DD}

# ── Cell 1: Configuration ──────────────────────────
LAKEHOUSE_SOURCE = "lh_ventas_{source_layer}"
LAKEHOUSE_TARGET = "lh_ventas_{target_layer}"
TABLE_SOURCE = "{source_table}"
TABLE_TARGET = "{target_table}"

# ── Cell 2: Imports ────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ── Cell 3: Read Source ────────────────────────────
df_source = spark.read.format("delta").load(
    f"Tables/{TABLE_SOURCE}"
)
print(f"Source rows: {df_source.count()}")

# ── Cell 4: Transformations ────────────────────────
# (layer-specific logic here)

# ── Cell 5: Write Target ───────────────────────────
df_result.write.format("delta").mode("overwrite").saveAsTable(TABLE_TARGET)
print(f"Target rows: {df_result.count()}")

# ── Cell 6: Validation ────────────────────────────
# Assert row counts, null checks, golden fixture comparisons
assert df_result.count() > 0, "Output is empty!"
assert df_result.filter(F.col("key_field").isNull()).count() == 0, "NULL keys found!"
```

## Common Utility Functions

These functions live in `nb_common_utils_delta` and are referenced via `%run` in other notebooks:

```python
def safe_cast_date(col_name, input_format="yyyyMMdd", output_format="dd-MM-yyyy HH:mm:ss"):
    """SAP date (YYYYMMDD or YYYY-MM-DD) → Seedz format (dd-MM-yyyy HH:mm:ss).
    Returns empty string for NULL, '00000000', or invalid dates."""
    ...

def safe_cast_decimal(col_name):
    """SAP amount field → clean decimal string.
    Handles trailing minus (e.g., '1234.56-' → '-1234.56').
    Returns '0' for NULL or empty."""
    ...

def add_audit_columns(df):
    """Add _loaded_at (timestamp) and _source_file (string) columns."""
    ...

def get_watermark(table_name):
    """Read last successful watermark for delta processing."""
    ...

def set_watermark(table_name, watermark_value):
    """Update watermark after successful processing."""
    ...
```

## How to Work

### When I say "Task X.Y":
1. Look up the task in `agent_docs/task_queue.md`
2. Read the relevant schema from `agent_docs/schemas_reference.md`
3. Check business rules in `agent_docs/business_rules.md`
4. If Gold layer, check API format in `agent_docs/seedz_api_payloads.md`
5. Check golden fixtures in `agent_docs/golden_fixtures.md` for expected I/O
6. Generate the complete notebook following the template above
7. Write a test file in `src/tests/`

### When I say "Generate all Bronze notebooks":
1. Loop through tasks 1.1–1.7 in task_queue
2. Generate each notebook following the Bronze pattern
3. Each should read from Landing, add audit columns, write to Bronze

### When I say "Test against fixtures":
1. Load golden_fixtures.md
2. Create a PySpark test that reads the fixture input, applies the notebook transformations, and asserts the output matches expected values

## SAP BW Context (for your understanding)

If I use BW terminology, here's what it means in Fabric:

| BW Concept | Fabric Equivalent |
|---|---|
| InfoPackage + DTP | Copy Activity + Notebook |
| PSA | Landing Lakehouse |
| DSO (staging) | Bronze tables |
| DSO (clean) | Silver tables |
| CompositeProvider / BEx Query | Gold tables |
| Process Chain | Data Factory Pipeline |
| Start/End Routines | PySpark transformation cells |
| RSRT (debug) | Notebook with assertions |
| Transport (dev→qas→prd) | Git branches + Deployment Pipelines |

## Key Design Decisions

1. **Extract Wide, Filter Late:** CDS views have NO brand filters. Silver config tables control classification.
2. **Config over Code:** Brand rules, vehicle state, status mappings are all in config tables.
3. **Delta via Watermarks:** Transactional tables (billing, orders, AR) use `erdat >= last_watermark`.
4. **Full Load for Master Data:** Customer, material, equipment, seller — daily full refresh.
5. **VIN from EQUI.SERNR:** Not SERGE (empty at SALFA). Model from MAKT.MAKTX (not EQUI.TYPBZ).
6. **Seller via VBPA.PARVW='VE':** On billing/order documents. Master table TBD (ZES229_VENDEDORES).
7. **Equipment-Billing join via CHARG:** VBRP.CHARG = EQUI.HESSION (batch). 94.1% coverage for machinery.
