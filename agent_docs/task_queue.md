# Task Queue — SALFA Seedz Notebooks

## How to Use This File

Each task = 1 notebook. When the developer says "Task X.Y", generate the complete notebook.
Check dependencies first — some tasks need other tasks' outputs.

---

## Phase 1: Bronze Layer (Landing → Bronze)

### Task 1.0 — Utility Notebooks
```
Notebook: nb_common_utils_delta
Output:   Reusable functions (referenced via %run)
Contains: safe_cast_date, safe_cast_decimal, add_audit_columns, get/set_watermark
Depends:  Nothing
Priority: DO THIS FIRST — all other notebooks reference it
```

### Task 1.1 — Bronze Customer
```
Notebook: nb_seedz_bronze_ingest_customer
Source:   lh_ventas_landing / ZFB_CDS01 data
Target:   lh_ventas_bronze / brz_sap_sd_customer_raw
Strategy: Full load (overwrite)
Logic:    Read landing → cast all to StringType → add _loaded_at, _source = 'ZFB_CDS01' → write
Depends:  Task 1.0
```

### Task 1.2 — Bronze Seller
```
Notebook: nb_seedz_bronze_ingest_seller
Source:   lh_ventas_landing / ZFB_CDS02 data
Target:   lh_ventas_bronze / brz_sap_sd_seller_raw
Strategy: Full load
Logic:    Same pattern as 1.1
Depends:  Task 1.0
⚠️ NOTE:  CDS02 structure TBD — generate with placeholder columns
```

### Task 1.3 — Bronze Sales Order
```
Notebook: nb_seedz_bronze_ingest_sales_order
Source:   lh_ventas_landing / ZFB_CDS03 data
Target:   lh_ventas_bronze / brz_sap_sd_sales_order_raw
Strategy: Delta (watermark on erdat)
Logic:    Read landing WHERE erdat >= last_watermark → add audit cols → MERGE into Bronze → update watermark
Depends:  Task 1.0
```

### Task 1.4 — Bronze Material
```
Notebook: nb_seedz_bronze_ingest_material
Source:   lh_ventas_landing / ZFB_CDS04 data
Target:   lh_ventas_bronze / brz_sap_mm_material_raw
Strategy: Full load
Logic:    Same pattern as 1.1. Filter SPRAS = 'S' (Spanish descriptions)
Depends:  Task 1.0
```

### Task 1.5 — Bronze Billing ⚡ CRITICAL
```
Notebook: nb_seedz_bronze_ingest_billing
Source:   lh_ventas_landing / ZFB_CDS05 data
Target:   lh_ventas_bronze / brz_sap_sd_billing_raw
Strategy: Delta (watermark on erdat)
Logic:    Read landing WHERE erdat >= last_watermark → add audit cols → MERGE → update watermark
Depends:  Task 1.0
Note:     Largest volume table. Test pagination from OData.
```

### Task 1.6 — Bronze Equipment
```
Notebook: nb_seedz_bronze_ingest_equipment
Source:   lh_ventas_landing / ZFB_CDS06 data
Target:   lh_ventas_bronze / brz_sap_pm_equipment_raw
Strategy: Full load
Logic:    Same pattern as 1.1
Depends:  Task 1.0
```

### Task 1.7 — Bronze Accounts Receivable
```
Notebook: nb_seedz_bronze_ingest_acct_recv
Source:   lh_ventas_landing / ZFB_CDS07 data
Target:   lh_ventas_bronze / brz_sap_fi_acct_recv_raw
Strategy: Delta (watermark on budat)
Logic:    Read landing WHERE budat >= last_watermark → MERGE → update watermark
Depends:  Task 1.0
```

### Task 1.8 — Config Tables
```
Notebook: nb_seedz_bronze_load_config
Source:   config/config_brand_rules.csv, config/config_vehicle_state.csv
Target:   lh_ventas_bronze / config_brand_rules, config_vehicle_state
Strategy: Full overwrite from CSV
Logic:    Read CSV → write as Delta table
Depends:  Nothing (but needed by Silver)
```

---

## Phase 2: Silver Layer (Bronze → Silver)

### Task 2.1 — Silver Customer
```
Notebook: nb_seedz_silver_transform_customer
Source:   lh_ventas_bronze / brz_sap_sd_customer_raw
Target:   lh_ventas_silver / customer
Logic:
  1. Read Bronze
  2. LTRIM zeros from customer_number → customer_id
  3. CONCAT name1 + ' ' + name2 → name (TRIM)
  4. REGEXP_REPLACE(stcd1, '[^0-9]', '') → tax_id
  5. REGEXP_REPLACE(tel_number, '[^0-9]', '') → phone
  6. LOWER(TRIM(email)) → email
  7. safe_cast_date(erdat) → created_date
  8. loevm = 'X' → is_deleted = True
  9. Filter: WHERE is_deleted = False (optional — discuss with business)
  10. Write as Delta, overwrite
Depends:  Task 1.1
Validate: Golden fixtures customer records
```

### Task 2.2 — Silver Seller
```
Notebook: nb_seedz_silver_transform_seller
Source:   lh_ventas_bronze / brz_sap_sd_seller_raw
Target:   lh_ventas_silver / seller
Logic:    Similar to customer. Clean phone/email.
Depends:  Task 1.2
⚠️ NOTE:  Blocked until seller definition confirmed by SALFA
```

### Task 2.3 — Silver Material ⚡ CRITICAL
```
Notebook: nb_seedz_silver_transform_material
Source:   lh_ventas_bronze / brz_sap_mm_material_raw + config_brand_rules
Target:   lh_ventas_silver / material
Logic:
  1. Read Bronze material + config_brand_rules
  2. LTRIM zeros from matnr → material_id
  3. TRIM(maktx) → description
  4. Brand classification:
     - is_jd_part = MFRNR IN ('0000100005', '0000100157')
     - is_jd_machine = SPART LIKE 'M%' AND MATKL = '0005'
     - brand = CASE WHEN is_jd_part OR is_jd_machine THEN 'JD' ELSE 'OTHER'
     - item_category = CASE WHEN is_jd_machine THEN 'MACHINERY' ELSE 'PARTS'
  5. Write as Delta
Depends:  Task 1.4, Task 1.8
Validate: Golden fixtures material records (1 machine, 1 part)
```

### Task 2.4 — Silver Equipment
```
Notebook: nb_seedz_silver_transform_equipment
Source:   lh_ventas_bronze / brz_sap_pm_equipment_raw + silver.material + config_vehicle_state
Target:   lh_ventas_silver / equipment
Logic:
  1. Read Bronze equipment
  2. LTRIM zeros from equnr → equipment_id
  3. TRIM(sernr) → serial_number (THIS IS THE VIN!)
  4. JOIN silver.material ON material_id → get model_name (description)
  5. JOIN config_vehicle_state ON division → vehicle_state (N/U)
     - M0, M2, M4 → 'N' (Nuevo)
     - M1, M3, M5 → 'U' (Usado)
  6. TRIM(hession) → batch (JOIN key for billing)
  7. Write as Delta
Depends:  Task 1.6, Task 2.3 (needs silver.material for model_name)
Validate: Golden fixtures (1 with VIN, 1 without)
```

### Task 2.5 — Silver Sales Order
```
Notebook: nb_seedz_silver_transform_sales_order
Source:   lh_ventas_bronze / brz_sap_sd_sales_order_raw
Target:   lh_ventas_silver / sales_order
Logic:
  1. Read Bronze
  2. LTRIM zeros, safe_cast_date, safe_cast_decimal
  3. Status derivation (see business_rules.md — order_status)
  4. Write as Delta, MERGE on order_id + order_item
Depends:  Task 1.3
```

### Task 2.6 — Silver Billing ⚡ MOST COMPLEX
```
Notebook: nb_seedz_silver_transform_billing
Source:   lh_ventas_bronze / brz_sap_sd_billing_raw
Target:   lh_ventas_silver / billing_document
Logic:
  1. Read Bronze billing
  2. LTRIM zeros, safe_cast_date, safe_cast_decimal (watch trailing minus!)
  3. Derive invoice_type (see business_rules.md)
  4. Derive invoice_status using priority chain (see business_rules.md)
  5. Write as Delta, MERGE on billing_id + billing_item
Depends:  Task 1.5
Validate: Golden fixtures (normal, cancelled, credit memo, parts)
NOTE:     This is the MOST IMPORTANT notebook. Billing feeds both Superación AND Censacional.
```

### Task 2.7 — Silver Accounts Receivable
```
Notebook: nb_seedz_silver_transform_acct_recv
Source:   lh_ventas_bronze / brz_sap_fi_acct_recv_raw
Target:   lh_ventas_silver / account_receivable
Logic:
  1. Read Bronze
  2. LTRIM zeros, safe_cast_date, safe_cast_decimal
  3. is_cleared = augbl != ''
  4. is_overdue = due_date < current_date() AND NOT is_cleared
  5. Derive ar_status (see business_rules.md)
  6. Write as Delta, MERGE on doc_number + fiscal_year + line_item
Depends:  Task 1.7
```

---

## Phase 3: Gold Layer (Silver → Gold)

### Task 3.1 — Gold Superación Customer
```
Notebook: nb_seedz_gold_build_sup_customer
Source:   lh_ventas_silver / customer
Target:   lh_ventas_gold / gold_seedz_sup_customer
Logic:    Map silver fields → Seedz CUSTOMER format. All STRING. See schemas_reference.md.
Depends:  Task 2.1
```

### Task 3.2 — Gold Superación Property
```
Notebook: nb_seedz_gold_build_sup_property
Source:   lh_ventas_silver / customer (same source as 3.1)
Target:   lh_ventas_gold / gold_seedz_sup_property
Logic:    Map to PROPERTY format. identification = company_rut + plant_code.
Depends:  Task 2.1
```

### Task 3.3 — Gold Superación Item
```
Notebook: nb_seedz_gold_build_sup_item
Source:   lh_ventas_silver / material
Target:   lh_ventas_gold / gold_seedz_sup_item
Logic:    Map to ITEM format. Filter JD only (brand = 'JD').
Depends:  Task 2.3
```

### Task 3.4 — Gold Superación Seller
```
Notebook: nb_seedz_gold_build_sup_seller
Source:   lh_ventas_silver / seller
Target:   lh_ventas_gold / gold_seedz_sup_seller
Depends:  Task 2.2
```

### Task 3.5 — Gold Superación Order
```
Notebook: nb_seedz_gold_build_sup_order
Source:   lh_ventas_silver / sales_order + silver.customer (JOIN for clientIdentification)
Target:   lh_ventas_gold / gold_seedz_sup_order
Depends:  Task 2.5, Task 2.1
```

### Task 3.6 — Gold Superación Order Item
```
Notebook: nb_seedz_gold_build_sup_order_item
Source:   lh_ventas_silver / sales_order + silver.customer
Target:   lh_ventas_gold / gold_seedz_sup_order_item
Depends:  Task 2.5, Task 2.1
```

### Task 3.7 — Gold Superación Invoice
```
Notebook: nb_seedz_gold_build_sup_invoice
Source:   lh_ventas_silver / billing_document + silver.customer
Target:   lh_ventas_gold / gold_seedz_sup_invoice
Logic:    Header-level: one row per billing_id. JOIN customer for clientDocument.
Depends:  Task 2.6, Task 2.1
```

### Task 3.8 — Gold Superación Invoice Item
```
Notebook: nb_seedz_gold_build_sup_invoice_item
Source:   lh_ventas_silver / billing_document + silver.customer + silver.material + silver.seller
Target:   lh_ventas_gold / gold_seedz_sup_invoice_item
Logic:    Item-level with sellerIdentification and category enrichment.
Depends:  Task 2.6, Task 2.1, Task 2.3, Task 2.2
```

### Task 3.9 — Gold Superación Account Receivable
```
Notebook: nb_seedz_gold_build_sup_acct_recv
Source:   lh_ventas_silver / account_receivable + silver.customer
Target:   lh_ventas_gold / gold_seedz_sup_acct_recv
Depends:  Task 2.7, Task 2.1
```

### Task 3.10 — Gold Censacional ⚡ MOST COMPLEX (7-JOIN)
```
Notebook: nb_seedz_gold_build_cens_payload
Source:   silver.billing_document
         + silver.customer       (JOIN on customer_id)
         + silver.material       (JOIN on material_id)
         + silver.equipment      (JOIN on batch — LEFT OUTER, 94.1% coverage)
         + silver.seller         (JOIN on seller_id)
         + config_brand_rules    (filter JD only)
         + config_vehicle_state  (already in equipment)
Target:   lh_ventas_gold / gold_seedz_cens_payload
Logic:
  1. Start from billing_document WHERE is_cancelled = False
  2. LEFT JOIN equipment ON billing.batch = equipment.batch AND billing.plant = equipment.plant
  3. INNER JOIN customer ON billing.customer_id = customer.customer_id
  4. INNER JOIN material ON billing.material_id = material.material_id WHERE brand = 'JD'
  5. LEFT JOIN seller ON billing.seller_id = seller.seller_id
  6. Map all fields to STRING
  7. For non-machinery items: chassi = "", model = "", vehicleState = ""
  8. Write as Delta
Depends:  Task 2.6, Task 2.1, Task 2.3, Task 2.4, Task 2.2
NOTE:     This is THE critical notebook. Must handle:
          - Machinery with equipment (chassi/model/vehicleState populated)
          - Parts without equipment (chassi/model/vehicleState = "")
          - Missing equipment for some machinery (94.1% coverage → 5.9% will have empty VIN)
```

---

## Phase 4: API Integration

### Task 4.1 — API Push Superación
```
Notebook: nb_seedz_api_send_superacion
Source:   All gold_seedz_sup_* tables
Target:   Seedz API endpoints (9 POST calls)
Logic:
  1. Authenticate → get token
  2. For each entity: read Gold table → chunk into batches of N records
  3. POST to /v1/{entity} with JSON body
  4. Handle 429 (rate limit) with exponential backoff
  5. Log success/failure per batch
  6. Write send_log table with timestamps and response codes
Depends:  All Task 3.x
```

### Task 4.2 — API Push Censacional
```
Notebook: nb_seedz_api_send_censacional
Source:   gold_seedz_cens_payload
Target:   Seedz API /v1/censacional
Logic:    Same pattern as 4.1 but single endpoint.
Depends:  Task 3.10
```
