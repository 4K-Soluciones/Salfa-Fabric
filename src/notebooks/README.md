# Notebooks

Organized by Medallion layer. Each notebook follows the SALFA naming convention:
`nb_<dominio>_<capa>_<accion>_<entidad>` (Documento_Fabric.docx)

## Directories

- `utils/`  — Shared utilities (delta, logging, API auth)
- `bronze/` — Landing → Bronze ingestion (7 notebooks)
- `silver/` — Bronze → Silver transformation (7 notebooks)
- `gold/`   — Silver → Gold API formatting (11 notebooks)

## Execution Order

```
utils/nb_common_utils_seedz.py                  ← Run first (shared functions)

bronze/nb_ventas_bronze_ingest_customer.py       ─┐
bronze/nb_ventas_bronze_ingest_seller.py          │
bronze/nb_ventas_bronze_ingest_material.py        │ Phase 1 — full load
bronze/nb_ventas_bronze_ingest_equipment.py       │ parallelizable
bronze/nb_ventas_bronze_ingest_sales_order.py     │
bronze/nb_ventas_bronze_ingest_billing.py         │ delta load
bronze/nb_ventas_bronze_ingest_acct_recv.py      ─┘
bronze/nb_ventas_bronze_load_config.py            ← config tables (no dependency)

silver/nb_ventas_silver_transform_customer.py    ─┐
silver/nb_ventas_silver_transform_seller.py       │ blocked — pending seller
silver/nb_ventas_silver_transform_material.py     │ definition from SALFA
silver/nb_ventas_silver_transform_equipment.py    │ Phase 2
silver/nb_ventas_silver_transform_sales_order.py  │
silver/nb_ventas_silver_transform_billing.py      │
silver/nb_ventas_silver_transform_acct_recv.py   ─┘

gold/nb_ventas_gold_build_customer.py            ─┐
gold/nb_ventas_gold_build_seller.py               │
gold/nb_ventas_gold_build_item.py                 │
gold/nb_ventas_gold_build_property.py             │ Phase 3
gold/nb_ventas_gold_build_order.py                │ Superación entities
gold/nb_ventas_gold_build_order_item.py           │
gold/nb_ventas_gold_build_invoice.py              │
gold/nb_ventas_gold_build_invoice_item.py         │
gold/nb_ventas_gold_build_acct_recv.py           ─┘

gold/nb_ventas_gold_build_censacional.py          ← Censacional (7-JOIN)
```

## Status

| Notebook | Status |
|---|---|
| nb_common_utils_seedz | ✅ Ready |
| nb_ventas_bronze_ingest_customer | ✅ Ready |
| nb_ventas_bronze_ingest_material | ✅ Ready |
| nb_ventas_bronze_ingest_equipment | ✅ Ready |
| nb_ventas_silver_transform_customer | ✅ Ready |
| nb_ventas_silver_transform_material | ✅ Ready |
| nb_ventas_silver_transform_equipment | ✅ Ready |
| nb_ventas_bronze_ingest_seller | ⏳ Blocked — seller join TBD |
| nb_ventas_silver_transform_seller | ⏳ Blocked — seller join TBD |
| nb_ventas_bronze_ingest_billing | ⏳ Blocked — seller join TBD |
| nb_ventas_bronze_ingest_sales_order | ⏳ Blocked — seller join TBD |
| nb_ventas_bronze_ingest_acct_recv | ⏳ Pending |
| All Gold notebooks | ⏳ Pending Silver completion |

## How Claude Should Generate Notebooks

1. Read `CLAUDE.md` for project context and naming conventions
2. Read `agent_docs/task_queue.md` for the specific task
3. Read `agent_docs/schemas_reference.md` for input/output schemas
4. Read `agent_docs/business_rules.md` for transformation logic
5. Read `agent_docs/golden_fixtures.md` for expected input/output values
6. Generate `.py` file following naming convention above
7. Generate matching test in `src/tests/`
