-- ================================================================
-- SALFA Seedz — Golden Fixture Extraction Queries
-- ================================================================
-- Run these in Eclipse ADT → Open SQL Console (ABAP SQL)
-- Target: SAP ECC on HANA (SALFA production or quality)
--
-- Each query mirrors the logic of the corresponding CDS view
-- (ZFB_CDS01–ZFB_CDS08) but queries standard tables directly.
-- No transport needed.
--
-- HOW TO USE:
--   1. Open Eclipse → ABAP Development Tools
--   2. Right-click your SAP system connection → Open SQL Console
--   3. Paste each query and execute (F8)
--   4. Export results: right-click result → Export → CSV or clipboard
--   5. Paste into the golden_fixtures_template.xlsx (blue columns)
--
-- IMPORTANT: These queries use WHERE clauses to pick specific
-- records that exercise transformation rules. Adjust the
-- filter values to match your real data.
-- ================================================================


-- ════════════════════════════════════════
-- CDS01: CUSTOMER — KNA1 + ADRC
-- Pick: 1 normal, 1 deleted (LOEVM='X'), 1 with minimal data
-- ════════════════════════════════════════

SELECT
  k~kunnr            AS CustomerNumber,
  k~name1            AS Name1,
  k~name2            AS Name2,
  k~stcd1            AS TaxNumber1,
  k~stcd2            AS TaxNumber2,
  k~ktokd            AS AccountGroup,
  k~land1            AS Country,
  k~regio            AS Region,
  k~spras            AS Language,
  k~brsch            AS IndustrySector,
  k~kukla            AS CustomerClass,
  k~erdat            AS CreatedDate,
  k~ernam            AS CreatedBy,
  k~loevm            AS DeletionFlag,
  a~street           AS Street,
  a~house_num1       AS HouseNumber,
  a~city1            AS City,
  a~city2            AS District,
  a~post_code1       AS PostalCode,
  a~region           AS AddrRegion,
  a~country          AS AddrCountry,
  a~tel_number       AS Phone,
  a~fax_number       AS Fax
FROM kna1 AS k
  LEFT OUTER JOIN adrc AS a
    ON k~adrnr = a~addrnumber
    AND a~date_from <= sy-datum
    AND a~nation = ' '
WHERE k~kunnr IN ( '0000012345', '0000067890', '0000099999' )
-- ^^^ Replace with 3 real customer numbers:
--     1 active, 1 deleted (LOEVM='X'), 1 with sparse data
ORDER BY k~kunnr
;


-- ════════════════════════════════════════
-- CDS02: SELLER — Custom table or VBPA + KNA1
-- Adjust based on SALFA's actual seller source
-- (ZES229_VENDEDORES or equivalent)
-- ════════════════════════════════════════

-- Option A: If seller comes from a custom table
-- SELECT * FROM zes229_vendedores
-- WHERE partner IN ( '0000000100', '0000000200' );

-- Option B: If seller comes from VBPA partner function VE
SELECT DISTINCT
  p~kunnr            AS SellerPartner,
  k~stcd1            AS Document,
  k~name1            AS SellerName,
  a~tel_number       AS Phone,
  -- email requires ADR6 join (see CDS note)
  ' '                AS Email,
  k~erdat            AS CreatedDate
FROM vbpa AS p
  INNER JOIN kna1 AS k ON p~kunnr = k~kunnr
  LEFT OUTER JOIN adrc AS a ON k~adrnr = a~addrnumber AND a~nation = ' '
WHERE p~parvw = 'VE'                      -- Partner function = Seller
  AND p~vbeln IN ( '0090001234' )         -- Pick from a known order
ORDER BY p~kunnr
;


-- ════════════════════════════════════════
-- CDS03: SALES ORDER — VBAK + VBAP + VBPA(VE)
-- Pick: 1 machinery order, 1 parts order, 1 cancelled (ABGRU≠'')
-- ════════════════════════════════════════

SELECT
  h~vbeln            AS SalesOrder,
  i~posnr            AS SalesOrderItem,
  h~auart            AS OrderType,
  h~vkorg            AS SalesOrg,
  h~vtweg            AS DistChannel,
  h~spart            AS Division,
  h~vkbur            AS SalesOffice,
  h~vkgrp            AS SalesGroup,
  h~kunnr            AS SoldToParty,
  h~erdat            AS CreatedDate,
  h~erzet            AS CreatedTime,
  h~ernam            AS CreatedBy,
  h~netwr            AS NetValueHeader,
  h~waerk            AS Currency,
  i~matnr            AS Material,
  i~arktx            AS ItemDescription,
  i~kwmeng           AS OrderQty,
  i~vrkme            AS SalesUnit,
  i~netwr            AS NetValueItem,
  i~werks            AS Plant,
  i~lgort            AS StorageLocation,
  i~matkl            AS MaterialGroup,
  i~pstyv            AS ItemCategory,
  i~abgru            AS RejectionReason,
  -- Seller from VBPA
  ( SELECT kunnr FROM vbpa
    WHERE vbeln = h~vbeln AND posnr = '000000' AND parvw = 'VE'
    ) AS SellerPartner,
  h~erdat            AS CreatedDateHeader
FROM vbak AS h
  INNER JOIN vbap AS i ON h~vbeln = i~vbeln
WHERE h~vbeln IN ( '0090001234', '0090001235', '0090001236' )
-- ^^^ Replace with:
--     1 machinery (SPART LIKE 'M%', MATKL='0005')
--     1 parts (MATKL NOT '0005')
--     1 cancelled (ABGRU != '')
ORDER BY h~vbeln, i~posnr
;


-- ════════════════════════════════════════
-- CDS04: MATERIAL — MARA + MAKT + MARC
-- Pick: 1 machine (SPART LIKE 'M%'), 1 JD part (MFRNR in config)
-- ════════════════════════════════════════

SELECT
  m~matnr            AS Material,
  t~maktx            AS MaterialDescription,
  m~mtart            AS MaterialType,
  m~matkl            AS MaterialGroup,
  m~mbrsh            AS IndustrySector,
  m~spart            AS Division,
  m~mfrnr            AS Manufacturer,
  m~meins            AS BaseUoM,
  m~brgew            AS GrossWeight,
  m~ntgew            AS NetWeight,
  m~gewei            AS WeightUnit,
  m~erdat            AS CreatedDate,
  m~ernam            AS CreatedBy,
  m~lvorm            AS DeletionFlag,
  c~werks            AS Plant,
  c~dismm            AS MRPType,
  c~prctr            AS ProfitCenter,
  c~maabc            AS ABCIndicator
FROM mara AS m
  INNER JOIN makt AS t ON m~matnr = t~matnr AND t~spras = 'S'
  LEFT OUTER JOIN marc AS c ON m~matnr = c~matnr AND c~werks = '1001'
WHERE m~matnr IN ( '000000000000123456', '000000000000789012' )
-- ^^^ Replace with:
--     1 machine: check SPART starts with M and MATKL = '0005'
--     1 JD part: check MFRNR IN ('0000100005','0000100157')
ORDER BY m~matnr
;


-- ════════════════════════════════════════
-- CDS05: BILLING — VBRK + VBRP + VBPA(VE)
-- Pick: 1 normal invoice, 1 cancelled (FKSTO='X'),
--       1 credit memo (VBTYP='O'), 1 parts invoice
-- THIS IS THE MOST IMPORTANT QUERY — exercises all transforms
-- ════════════════════════════════════════

SELECT
  h~vbeln            AS BillingDocument,
  i~posnr            AS BillingItem,
  h~fkart            AS BillingType,
  h~fkdat            AS BillingDate,
  h~vkorg            AS SalesOrg,
  h~vtweg            AS DistChannel,
  h~spart            AS Division,
  h~kunag            AS SoldToParty,
  h~kunrg            AS Payer,
  h~netwr            AS NetValueHeader,
  h~waerk            AS Currency,
  h~bukrs            AS CompanyCode,
  h~vbtyp            AS SDDocCategory,
  h~rfbsk            AS BillingStatus,
  h~fksto            AS CancelledFlag,
  h~sfakn            AS CancelledBilling,
  h~erdat            AS CreatedDate,
  h~erzet            AS CreatedTime,
  i~matnr            AS Material,
  i~arktx            AS ItemDescription,
  i~fkimg            AS BilledQty,
  i~netwr            AS NetValueItem,
  i~mwsbp            AS TaxAmount,
  i~werks            AS Plant,
  i~lgort            AS StorageLocation,
  i~matkl            AS MaterialGroup,
  i~pstyv            AS ItemCategory,
  i~aubel            AS SalesOrder,
  i~aupos            AS SalesOrderItem,
  i~charg            AS Batch,
  -- Seller
  ( SELECT kunnr FROM vbpa
    WHERE vbeln = h~vbeln AND posnr = '000000' AND parvw = 'VE'
    ) AS SellerPartner,
  h~sfakn            AS ReferenceDoc
FROM vbrk AS h
  INNER JOIN vbrp AS i ON h~vbeln = i~vbeln
WHERE h~vbeln IN ( '0090500001', '0090500002', '0090500003', '0090500004' )
-- ^^^ Replace with:
--     1 normal machinery invoice (SPART LIKE 'M%', MATKL='0005', CHARG != '')
--     1 cancelled (FKSTO = 'X')
--     1 credit memo (VBTYP = 'O' or 'N')
--     1 parts invoice (MATKL != '0005')
--
-- 🔍 HOW TO FIND GOOD RECORDS:
--   SELECT vbeln fkart spart fksto vbtyp rfbsk FROM vbrk
--   WHERE vkorg = '1000' AND fkdat >= '20240101'
--   ORDER BY fkdat DESCENDING
--   -- Then pick from results
ORDER BY h~vbeln, i~posnr
;


-- ════════════════════════════════════════
-- CRITICAL CHECK: Do amounts have trailing minus?
-- Run this to see the actual format in your system:
-- ════════════════════════════════════════

SELECT
  vbeln,
  netwr,
  fkimg,
  mwsbp,
  -- Check: is netwr negative for credit memos?
  CASE WHEN netwr < 0 THEN 'NEGATIVE' ELSE 'POSITIVE' END AS sign_check
FROM vbrp
WHERE vbeln IN ( SELECT vbeln FROM vbrk WHERE vbtyp = 'O' AND vkorg = '1000' )
  AND rownum <= 5
;
-- ^^^ This tells you whether SALFA stores credit memo amounts
--     as negative numbers or positive with trailing minus.
--     The answer determines whether safe_cast_decimal needs
--     the trailing-minus logic or not.


-- ════════════════════════════════════════
-- CDS06: EQUIPMENT — EQUI
-- Pick: 1 machine with SERNR (VIN), 1 without
-- ════════════════════════════════════════

SELECT
  e~equnr            AS EquipmentNumber,
  e~sernr            AS SerialNumber,       -- THIS is the VIN at SALFA
  e~matnr            AS Material,
  e~herst            AS Manufacturer,
  e~typbz            AS ManufacturerModel,  -- Check: is this empty at SALFA?
  e~swerk            AS Plant,
  e~inbdt            AS StartupDate,
  e~baujj            AS ConstructionYear,
  e~lvorm            AS DeletionFlag,
  e~charg            AS Batch              -- Links to billing CHARG
FROM equi AS e
WHERE e~equnr IN ( '000000010001', '000000010002' )
-- ^^^ Replace with:
--     1 with SERNR filled (this is VIN for Censacional)
--     1 without SERNR
--
-- 🔍 HOW TO FIND:
--   SELECT equnr sernr matnr herst typbz charg FROM equi
--   WHERE swerk = '1001' AND matnr != ''
--   ORDER BY equnr DESCENDING
--   -- Pick one with SERNR filled, one without
ORDER BY e~equnr
;


-- ════════════════════════════════════════
-- CDS07: ACCOUNTS RECEIVABLE — BSID (open) + BSAD (cleared)
-- Pick: 1 open (past due), 1 cleared, 1 open (not yet due)
-- ════════════════════════════════════════

-- Open items
SELECT
  'OPEN' AS item_type,
  bukrs AS CompanyCode,
  kunnr AS Customer,
  belnr AS AccountingDoc,
  gjahr AS FiscalYear,
  buzei AS LineItem,
  blart AS DocType,
  budat AS PostingDate,
  bldat AS DocumentDate,
  waers AS Currency,
  dmbtr AS AmountLocalCurrency,
  wrbtr AS AmountDocCurrency,
  shkzg AS DebitCredit,
  zfbdt AS BaselinePayDate,
  zbd1t AS PayTermDays1,
  zbd2t AS PayTermDays2,
  zbd3t AS PayTermDays3,
  vbeln AS BillingDocument,
  zuonr AS AssignmentNumber,
  sgtxt AS ItemText,
  ' '   AS ClearingDate,
  ' '   AS ClearingDoc
FROM bsid
WHERE bukrs = '1000'
  AND kunnr IN ( '0000012345', '0000067890' )
  AND budat >= '20240101'
ORDER BY budat DESCENDING
;

-- Cleared items
SELECT
  'CLEARED' AS item_type,
  bukrs AS CompanyCode,
  kunnr AS Customer,
  belnr AS AccountingDoc,
  gjahr AS FiscalYear,
  buzei AS LineItem,
  blart AS DocType,
  budat AS PostingDate,
  bldat AS DocumentDate,
  waers AS Currency,
  dmbtr AS AmountLocalCurrency,
  wrbtr AS AmountDocCurrency,
  shkzg AS DebitCredit,
  zfbdt AS BaselinePayDate,
  zbd1t AS PayTermDays1,
  zbd2t AS PayTermDays2,
  zbd3t AS PayTermDays3,
  vbeln AS BillingDocument,
  zuonr AS AssignmentNumber,
  sgtxt AS ItemText,
  augdt AS ClearingDate,
  augbl AS ClearingDoc
FROM bsad
WHERE bukrs = '1000'
  AND kunnr IN ( '0000012345', '0000067890' )
  AND budat >= '20240101'
ORDER BY budat DESCENDING
;


-- ════════════════════════════════════════
-- DOCUMENT FLOW — VBFA
-- Get the flow for the orders/invoices you picked
-- ════════════════════════════════════════

SELECT
  vbelv AS PrecedingDoc,
  posnv AS PrecedingItem,
  vbeln AS SubsequentDoc,
  posnn AS SubsequentItem,
  vbtyp_n AS SubsequentDocCategory,
  erdat AS CreatedDate
FROM vbfa
WHERE vbelv IN ( '0090001234', '0090001235' )
  -- ^^^ Use the SalesOrder numbers from CDS03
  AND vbtyp_n = 'M'  -- M = billing document
ORDER BY vbelv, erdat
;


-- ════════════════════════════════════════
-- CONFIG DATA — Get real values from SALFA
-- ════════════════════════════════════════

-- Brand rules: which MFRNR values = John Deere?
SELECT DISTINCT mfrnr AS Manufacturer
FROM mara
WHERE mfrnr != ''
  AND spart LIKE 'M%'
ORDER BY mfrnr
;

-- Vehicle state: what SPART values exist for machinery?
SELECT DISTINCT spart AS Division,
  CASE
    WHEN spart LIKE 'MN%' OR spart LIKE 'MU%' THEN 'Check pattern'
    WHEN spart LIKE 'M%' THEN 'Machinery'
    ELSE 'Other'
  END AS category
FROM mara
WHERE spart LIKE 'M%'
ORDER BY spart
;

-- Billing status codes actually used
SELECT DISTINCT rfbsk AS BillingStatus, COUNT(*) AS cnt
FROM vbrk
WHERE vkorg = '1000'
  AND fkdat >= '20230101'
GROUP BY rfbsk
ORDER BY cnt DESCENDING
;

-- Plants
SELECT DISTINCT werks AS Plant, name1 AS PlantName
FROM t001w
WHERE werks IN ( SELECT DISTINCT werks FROM vbrp WHERE werks != '' )
ORDER BY werks
;
