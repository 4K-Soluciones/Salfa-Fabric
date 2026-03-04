# SALFA Seedz — Critical Design Decisions

> This file consolidates ALL architectural decisions made across 27 design sessions.
> Every decision here is FINAL and must be followed in implementation.
> If in doubt, this file wins over any other document.

## D1: Extract Wide, Filter Late
CDS views (ZFB_CDS01–07) extract ALL data from SAP with NO brand filters.
Unlike the old ZJD_CDS/ZEL_CDS views which had hard-coded MFRNR and MATKL filters,
our CDS views are brand-agnostic. Brand classification (JD vs Hitachi vs CAT)
happens in Fabric Silver layer via `config_brand_rules` table.
**Why:** Adding a new brand = add rows to config table. Zero code changes.

## D2: CDS Views Are Untouchable
The 7 CDS views in GitHub `z4k_fb` repo are FINAL. Do not add WHERE clauses,
do not add JOINs, do not add calculated fields. All enrichment happens in Fabric.
**Exception:** O6 — adding `kna1.adrnr` to CDS01 for email resolution (pending transport).

## D3: Brand Classification Rules
- **JD Parts:** `MFRNR IN ('0000100005', '0000100157')` → brand='JD', category='PARTS'
- **JD Machinery:** `SPART LIKE 'M%' AND MATKL = '0005'` → brand='JD', category='MACHINERY'
- **Note:** MFRNR is EMPTY for machines at SALFA. Don't use MFRNR for machinery detection.
- **Everything else:** brand='OTHER'
- **Implementation:** `config_brand_rules` table in Bronze, lookup in Silver.

## D4: VIN Comes from EQUI.SERNR, Not SER*
At SALFA, serial numbers live in `EQUI.SERNR` (equipment master), NOT in SER01/SER02/SER03.
SERGE table is EMPTY at SALFA. This was validated in discovery phase.
Model name comes from `MAKT.MAKTX` (material description), NOT from `EQUI.TYPBZ`.

## D5: Equipment-Billing JOIN via CHARG (Batch)
The link between billing documents and equipment is:
`VBRP.CHARG = EQUI.HESSION` (batch number), scoped by plant.
Coverage: ~94% of machinery billing items have matching equipment.
This is a LEFT OUTER JOIN — never lose billing records. Missing equipment → empty chassi/model/vehicleState.

## D6: Batch Filter for Accessories
Items with empty CHARG are accessories (GPS antennas, tech kits), not serialized equipment.
Filter: `CHARG <> ''` when joining to equipment. This eliminates false empty-VIN records.

## D7: Vehicle State from SPART
`MARA.SPART` → vehicleState mapping:
- M0, M2, M4 → 'N' (Nuevo/New)
- M1, M3, M5 → 'U' (Usado/Used)
- Others → '' (not a vehicle)
Stored in `config_vehicle_state` table.

## D8: Invoice Type from VBTYP (sd_doc_category)
- VBTYP = 'M' → invoice_type = '1' (debit/sale/nota de saída)
- VBTYP = 'O' or 'N' → invoice_type = '0' (credit/return/nota de entrada)
- Default → '1'

## D9: Invoice Status Priority Chain
Check in this order (first match wins):
1. `fksto = 'X'` → '0' (Cancelada)
2. `sfakn != ''` → '0' (Cancelada — has cancellation reference)
3. `rfbsk = 'C'` → '2' (Concluída)
4. `rfbsk IN ('A','B')` → '1' (Pendente)
5. `rfbsk = ''` → '2' (Concluída — no status = completed)
6. Default → '1' (Pendente)

## D10: Seller via Partner Function VE
Seller is identified by `VBPA.PARVW = 'VE'` at header level (`POSNR = '000000'`).
CDS views 03 and 05 already include the seller JOIN.
Seller master data comes from CDS02 (ZES229_VENDEDORES — structure TBD with SALFA).

## D11: Gold Layer — All STRING, No NULLs
Every field in Gold tables is STRING type.
Dates formatted as `dd-MM-yyyy HH:mm:ss`.
Every NULL replaced with empty string `""`.
Every numeric formatted with 2 decimal places as string.

## D12: Identification Field = RUT + Plant
For Chile: `identification = "{company_rut}_{plant_code}"`
Example: `"76123456-7_1010"`
`companyIdentification = "{company_rut}"` (without plant)
**Pending:** Confirm exact SALFA RUT format with business team.

## D13: Censacional = Billing Line Items Enriched
Censacional is NOT a separate entity. It's billing line items (from Silver.billing_document)
enriched with equipment data (chassi, model, vehicleState) via the CHARG JOIN.
For parts: equipment fields are empty strings. For machinery: populated from equipment table.
One Gold notebook (`nb_seedz_gold_build_cens_payload`) with 7-JOIN chain.

## D14: Delta Strategy
- **Full load daily (06:00 CLT):** Customer, Material, Equipment, Seller (CDS01,02,04,06)
- **Delta every 4 hours:** Sales Orders, Billing, AR (CDS03,05,07) via watermark on ERDAT/BUDAT
- **Watermark table:** `brz_watermark` in Bronze lakehouse tracks last successful extraction per CDS.

## D15: Silver Is Interface-Agnostic
Silver entities (customer, material, billing_document, etc.) are NOT Seedz-specific.
They serve ALL downstream projects: Seedz, JDPrism, ELIPS, PMManage.
Seedz-specific formatting happens ONLY in Gold layer.

## D16: Naming Conventions (SALFA IT Standard)
- Lakehouses: `lh_ventas_{layer}` (lh_ventas_bronze, lh_ventas_silver, etc.)
- Bronze tables: `brz_sap_{module}_{entity}_raw`
- Silver tables: `{entity}` (customer, billing_document, equipment)
- Gold tables: `gold_seedz_{program}_{entity}`
- Notebooks: `nb_seedz_{layer}_{action}_{entity}`
- Pipelines: `pl_seedz_{layer}_{scope}`
- All snake_case, no special characters, no tildes.

## D17: Config Tables in Bronze
Business rules stored as Delta tables in `lh_ventas_bronze`:
- `brz_config_brand_rules` — brand classification rules
- `brz_config_vehicle_state` — SPART → vehicleState mapping
- `brz_config_company` — SALFA RUT, dealer code, tenantId
- `brz_config_rfbsk` — billing status code mappings
- `brz_config_plants` — plant codes and names

## D18: Trailing Minus Handling
SAP may store negative amounts as `1234.56-` (trailing minus) instead of `-1234.56`.
The `safe_cast_decimal` utility function must handle both formats.
**Pending confirmation:** Run discovery query on VBRP.NETWR for credit memos to confirm SALFA's format.

## D19: CDS02 Seller — Placeholder Until Confirmed
CDS02 is a placeholder. The actual seller master table at SALFA may be `ZES229_VENDEDORES`
(custom Z table). Until SALFA business confirms the table structure:
- CDS02 uses assumed fields: SellerID, SellerName, Document, Phone, Email
- Silver seller notebook (Task 2.2) is BLOCKED
- All other entities can proceed without seller

## D20: Email Resolution — Pending CDS Change (O6)
CUSTOMER.email requires `kna1.adrnr` → `ADR6.SMTP_ADDR` resolution.
CDS01 currently doesn't include ADRNR. This is the ONE pending CDS addition (O6).
Until O6 is deployed: `email = ""` in Silver customer table.
