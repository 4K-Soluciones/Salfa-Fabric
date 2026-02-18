# Z4K_FB — CDS Views for Microsoft Fabric Extraction

## Overview

Generic, brand-agnostic CDS views designed to extract SAP ECC data into Microsoft Fabric's Medallion architecture. These views replace the project-specific `ZJD_CDS*` and `ZEL_CDS*` views with a unified extraction layer that serves **all** downstream projects (Seedz, JDPrism, ELIPS, PMManage) from a single set of extractions.

## Design Principles

1. **Extract Wide, Filter Late** — No business filters in CDS (no MFRNR, MATKL, SPART filters). Brand/project classification happens in Fabric Silver layer.
2. **ODP Delta where it matters** — Transactional tables (billing, orders, AR) use `@Analytics.dataExtraction.delta` annotations for incremental extraction via SAP ODP framework.
3. **Full Snapshot for master data** — Small tables (customers, materials, equipment, sellers) use full load on each run.
4. **S/4HANA ready** — Views use standard SAP table structures compatible with both ECC and S/4HANA RISE (2027 migration).

## CDS View Catalog

| View | SQL View | Source Tables | Strategy | Description |
|------|----------|--------------|----------|-------------|
| `ZFB_CDS01` | ZFBCDS01 | KNA1 + ADRC | Full | Customer master + addresses |
| `ZFB_CDS02` | ZFBCDS02 | ZES229_VENDEDORES | Full | Seller master (**TBD with analyst**) |
| `ZFB_CDS03` | ZFBCDS03 | VBAK + VBAP + VBPA | ODP Delta | Sales orders + seller partner |
| `ZFB_CDS04` | ZFBCDS04 | MARA + MAKT + MARC | Full | Material master — ALL brands |
| `ZFB_CDS05` | ZFBCDS05 | VBRK + VBRP + VBPA | ODP Delta | Billing documents + seller |
| `ZFB_CDS06` | ZFBCDS06 | EQUI + EQKT | Full | Equipment master (VIN, batch) |
| `ZFB_CDS07` | ZFBCDS07 | BSID + BSAD | ODP Delta | Accounts receivable (open + cleared) |

## Prerequisites

- SAP ECC 6.0 EHP8, NetWeaver 7.5 SPS20+ (confirmed at SALFA)
- ODP framework active (verify with `ODQMON` and `RODPS_REPL_TEST`)
- abapGit installed

## Installation via abapGit

1. Create package `Z4K_FB` in SE80 under your transport layer
2. In abapGit → **+ Online** → paste this repo URL
3. Select package `Z4K_FB` → **Pull**
4. Activate all objects
5. Register OData services in `SEGW` for each CDS view (see OData Setup below)

## OData Setup (SEGW)

For each CDS view, create an OData service via `SEGW`:

1. Transaction `SEGW` → Create project `ZFB_CDS_XX_SRV`
2. Data Model → Redefine → **ODP Extraction**
3. Context: **ABAP Core Data Services**
4. ODP Name: `ZFB_CDSXX` (SQL view name: `ZFBCDSXX`)
5. Generate runtime → Register in `/IWFND/MAINT_SERVICE`

For delta-enabled views (03, 05, 07):
- First call returns full snapshot + creates ODQ subscription
- Subsequent calls return only delta (inserts/updates/deletes)
- Monitor in `ODQMON`

## Fabric Landing

| CDS View | Fabric Pipeline | Bronze Table | Load Frequency |
|----------|----------------|-------------|----------------|
| ZFB_CDS01 | Copy Activity (Full) | `bronze.customer_raw` | Daily 6:00 AM |
| ZFB_CDS02 | Copy Activity (Full) | `bronze.seller_raw` | Daily 6:00 AM |
| ZFB_CDS03 | SAP CDC Connector | `bronze.sales_order_raw` | Every 4 hours |
| ZFB_CDS04 | Copy Activity (Full) | `bronze.material_raw` | Daily 6:00 AM |
| ZFB_CDS05 | SAP CDC Connector | `bronze.billing_raw` | Every 4 hours |
| ZFB_CDS06 | Copy Activity (Full) | `bronze.equipment_raw` | Daily 6:00 AM |
| ZFB_CDS07 | SAP CDC Connector | `bronze.acct_recv_raw` | Every 4 hours |

## SALFA-Specific Findings

These views incorporate discoveries from the SALFA data analysis:

- **VIN**: Use `EQUI.SERNR` (not SERGE — empty at SALFA)
- **Model**: Use `MAKT.MAKTX` (not EQUI.TYPBZ — empty at SALFA)
- **vehicleState**: Derive from `MARA.SPART` (M0/M2/M4→N, M1/M3/M5→U)
- **JD Machines**: `SPART LIKE 'M%'` + `MATKL = 0005` (MFRNR is empty for machines)
- **JD Parts**: `MFRNR IN ('0000100005','0000100157')`
- **EQUI-CHARGE join**: 94.1% coverage for machinery billing
- **Seller link**: `VBPA.PARVW = 'VE'` on billing/order (master source TBD)

## Git Flow

Following 4K Soluciones Git Flow strategy:

```
main     ← Production (transport to PRD)
develop  ← Integration branch
feature/ ← New CDS views or modifications
release/ ← Pre-production validation
hotfix/  ← Critical fixes
```

## Open Items

- [ ] **ZFB_CDS02**: Confirm seller table structure with SALFA business analyst
- [ ] **ZFB_CDS02**: Confirm VBPA.PARNR → ZES229_VENDEDORES link
- [ ] **ODP validation**: Run `RODPS_REPL_TEST` for CDS05 (billing)
- [ ] **S/4 compatibility**: Validate CDS syntax against S/4HANA RISE (2027)

## Related Repos

- `Z4K_JD` — John Deere specific ABAP (legacy, being replaced)
- `Z4K_ELIPS` — ELIPS interface ABAP objects
