# Golden Fixtures — Expected Input/Output

> ⚠️ **STATUS: PLACEHOLDER — Fill with real SAP data after running Eclipse SQL Console queries**
>
> Run the queries from `golden_fixture_queries.sql` in Eclipse ADT Open SQL Console,
> export the results as CSV, and paste them into the sections below.

## How to Use

Each fixture shows:
1. **Bronze Input** — raw data as it arrives from SAP (after Landing → Bronze)
2. **Silver Expected** — what the Silver notebook should produce
3. **Gold Expected** — what the Gold notebook should produce

Claude Code should generate assertions in each notebook that compare actual output to these expected values.

---

## Config Discovery (fill first — needed by all fixtures)

### MFRNR Values (John Deere manufacturer numbers)
```
-- TODO: Paste results of:
-- SELECT DISTINCT mfrnr, COUNT(*) FROM mara WHERE mfrnr <> '' GROUP BY mfrnr ORDER BY COUNT(*) DESC
```

### SPART Patterns (machinery divisions)
```
-- TODO: Paste results of:
-- SELECT DISTINCT spart, COUNT(*) FROM mara WHERE spart LIKE 'M%' GROUP BY spart ORDER BY spart
```

### RFBSK Values (billing status codes)
```
-- TODO: Paste results of:
-- SELECT DISTINCT rfbsk, COUNT(*) FROM vbrk GROUP BY rfbsk ORDER BY rfbsk
```

### Trailing Minus Check ⚠️ CRITICAL
```
-- TODO: Paste results of:
-- SELECT vbeln, netwr, CASE WHEN netwr < 0 THEN 'NEGATIVE' ELSE 'POSITIVE' END FROM vbrp WHERE vbtyp = 'O' LIMIT 10
-- This determines safe_cast_decimal implementation!
```

### Plant List
```
-- TODO: Paste results of:
-- SELECT werks, name1 FROM t001w WHERE bukrs = '{SALFA_company_code}' ORDER BY werks
```

---

## Fixture 1: Customer — Active

### Bronze Input (brz_sap_sd_customer_raw)
```
-- TODO: Paste 1 active customer from CDS01 query
customer_number | name1 | name2 | street | city | region | postal_code | country | tel_number | email | stcd1 | stcd2 | loevm | erdat
```

### Silver Expected (customer)
```
-- TODO: Fill expected Silver values after applying transformations
customer_id | name | tax_id | street | city | region | postal_code | country | phone | email | is_deleted | created_date
```

### Gold Expected (gold_seedz_sup_customer)
```
-- TODO: Fill expected Gold values (all STRING, dates dd-MM-yyyy HH:mm:ss)
identification | id | createdAt | updatedAt | companyName | clientIdentification | phones | email
```

---

## Fixture 2: Customer — Deleted

### Bronze Input
```
-- TODO: Paste 1 deleted customer (LOEVM = 'X') from CDS01 query
```

### Silver Expected
```
-- Should be filtered out (is_deleted = True) or flagged
```

---

## Fixture 3: Material — JD Machine

### Bronze Input (brz_sap_mm_material_raw)
```
-- TODO: Paste 1 JD machine (SPART LIKE 'M%', MATKL = '0005')
matnr | mtart | matkl | spart | meins | mfrnr | maktx | spras | werks | ersda
```

### Silver Expected (material)
```
material_id | description | material_type | material_group | division | base_uom | manufacturer | plant | brand | item_category | is_jd_part | is_jd_machine
-- brand should be 'JD', item_category = 'MACHINERY', is_jd_machine = True
```

---

## Fixture 4: Material — JD Part

### Bronze Input
```
-- TODO: Paste 1 JD part (MFRNR IN ('0000100005','0000100157'))
```

### Silver Expected
```
-- brand = 'JD', item_category = 'PARTS', is_jd_part = True
```

---

## Fixture 5: Billing — Normal Machinery Invoice ⚡ CRITICAL

### Bronze Input (brz_sap_sd_billing_raw)
```
-- TODO: Paste 1 normal machinery billing (VBTYP = 'M', FKSTO = '', CHARG <> '')
vbeln | posnr | fkart | fkdat | vkorg | vtweg | spart | kunag | kunrg | netwr_h | waerk | bukrs | vbtyp | rfbsk | fksto | sfakn | matnr | arktx | fkimg | vrkme | netwr_i | mwsbp | werks | matkl | pstyv | aubel | charg | parnr | erdat
```

### Silver Expected (billing_document)
```
-- invoice_type = '1' (debit), invoice_status = '2' (completed), is_cancelled = False
```

### Gold Expected (gold_seedz_sup_invoice)
```
-- type = '1', status = '2', statusId = '2'
```

### Gold Expected (gold_seedz_cens_payload)
```
-- chassi should be populated from equipment JOIN via batch
-- model should be populated from equipment.model_name
-- vehicleState should be 'N' or 'U'
```

---

## Fixture 6: Billing — Cancelled Invoice

### Bronze Input
```
-- TODO: Paste 1 cancelled billing (FKSTO = 'X')
```

### Silver Expected
```
-- invoice_status = '0' (cancelled), is_cancelled = True
```

---

## Fixture 7: Billing — Credit Memo

### Bronze Input
```
-- TODO: Paste 1 credit memo (VBTYP = 'O')
```

### Silver Expected
```
-- invoice_type = '0' (credit/entrada)
-- ⚠️ Check amount sign (trailing minus?)
```

---

## Fixture 8: Billing — Parts Invoice

### Bronze Input
```
-- TODO: Paste 1 parts billing (CHARG = '' — no batch/equipment)
```

### Gold Expected (gold_seedz_cens_payload)
```
-- chassi = '', model = '', vehicleState = '', isMachinery = 'false'
```

---

## Fixture 9: Equipment — With VIN

### Bronze Input (brz_sap_pm_equipment_raw)
```
-- TODO: Paste 1 equipment with SERNR populated
equnr | eqktx | sernr | matnr | hession | swerk | erdat
```

### Silver Expected (equipment)
```
-- serial_number should contain the VIN
-- model_name should come from MAKT JOIN
-- vehicle_state from config lookup
```

---

## Fixture 10: Accounts Receivable — Open Overdue

### Bronze Input (brz_sap_fi_acct_recv_raw)
```
-- TODO: Paste 1 open AR item where due date < today
bukrs | kunnr | belnr | gjahr | buzei | budat | bldat | zfbdt | dmbtr | waers | augbl | augdt | zterm | xblnr
```

### Silver Expected
```
-- is_cleared = False, is_overdue = True, status = 'Atrasado'
```

---

## Fixture 11: Accounts Receivable — Cleared

### Bronze Input
```
-- TODO: Paste 1 cleared AR item (AUGBL populated)
```

### Silver Expected
```
-- is_cleared = True, is_overdue = False, status = 'Liquidado'
```
