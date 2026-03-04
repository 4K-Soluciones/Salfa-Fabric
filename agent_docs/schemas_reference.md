# Schemas Reference — All Layers

## 1. CDS View → Bronze Mapping

### ZFB_CDS01 → brz_sap_sd_customer_raw (Customer)
| CDS Field | Bronze Column | SAP Type | PySpark Type |
|---|---|---|---|
| CustomerNumber | customer_number | CHAR(10) | StringType |
| CustomerName1 | name1 | CHAR(35) | StringType |
| CustomerName2 | name2 | CHAR(35) | StringType |
| Street | street | CHAR(60) | StringType |
| City | city | CHAR(40) | StringType |
| Region | region | CHAR(3) | StringType |
| PostalCode | postal_code | CHAR(10) | StringType |
| Country | country | CHAR(3) | StringType |
| TelNumber | tel_number | CHAR(30) | StringType |
| SmtpAddr | email | CHAR(241) | StringType |
| TaxNumber1 | stcd1 | CHAR(16) | StringType |
| TaxNumber2 | stcd2 | CHAR(11) | StringType |
| DeletionFlag | loevm | CHAR(1) | StringType |
| CreatedDate | erdat | DATS(8) | StringType |

### ZFB_CDS02 → brz_sap_sd_seller_raw (Seller — TBD)
| CDS Field | Bronze Column | SAP Type | PySpark Type |
|---|---|---|---|
| SellerID | seller_id | CHAR(10) | StringType |
| SellerName | seller_name | CHAR(40) | StringType |
| Document | document | CHAR(16) | StringType |
| Phone | phone | CHAR(30) | StringType |
| Email | email | CHAR(241) | StringType |
> ⚠️ **Open item:** Confirm ZES229_VENDEDORES structure with SALFA

### ZFB_CDS03 → brz_sap_sd_sales_order_raw (Sales Order)
| CDS Field | Bronze Column | SAP Type | PySpark Type |
|---|---|---|---|
| SalesOrder | vbeln | CHAR(10) | StringType |
| SalesOrderItem | posnr | NUMC(6) | StringType |
| OrderType | auart | CHAR(4) | StringType |
| SalesOrg | vkorg | CHAR(4) | StringType |
| DistChannel | vtweg | CHAR(2) | StringType |
| Division | spart | CHAR(2) | StringType |
| SoldToParty | kunnr | CHAR(10) | StringType |
| Material | matnr | CHAR(18) | StringType |
| Description | arktx | CHAR(40) | StringType |
| OrderQty | kwmeng | DEC | StringType |
| NetValue | netwr | CURR | StringType |
| Currency | waerk | CUKY(5) | StringType |
| OrderDate | audat | DATS(8) | StringType |
| RejectionReason | abgru | CHAR(2) | StringType |
| Plant | werks | CHAR(4) | StringType |
| SellerPartner | parnr | CHAR(10) | StringType |
| CreatedDate | erdat | DATS(8) | StringType |

### ZFB_CDS04 → brz_sap_mm_material_raw (Material)
| CDS Field | Bronze Column | SAP Type | PySpark Type |
|---|---|---|---|
| Material | matnr | CHAR(18) | StringType |
| MaterialType | mtart | CHAR(4) | StringType |
| MaterialGroup | matkl | CHAR(9) | StringType |
| Division | spart | CHAR(2) | StringType |
| BaseUOM | meins | UNIT(3) | StringType |
| Manufacturer | mfrnr | CHAR(10) | StringType |
| Description | maktx | CHAR(40) | StringType |
| Language | spras | LANG(1) | StringType |
| Plant | werks | CHAR(4) | StringType |
| CreatedDate | ersda | DATS(8) | StringType |

### ZFB_CDS05 → brz_sap_sd_billing_raw (Billing — MOST COMPLEX)
| CDS Field | Bronze Column | SAP Type | PySpark Type |
|---|---|---|---|
| BillingDocument | vbeln | CHAR(10) | StringType |
| BillingItem | posnr | NUMC(6) | StringType |
| BillingType | fkart | CHAR(4) | StringType |
| BillingDate | fkdat | DATS(8) | StringType |
| SalesOrg | vkorg | CHAR(4) | StringType |
| DistChannel | vtweg | CHAR(2) | StringType |
| Division | spart | CHAR(2) | StringType |
| SoldToParty | kunag | CHAR(10) | StringType |
| Payer | kunrg | CHAR(10) | StringType |
| NetValueHeader | netwr_h | CURR | StringType |
| Currency | waerk | CUKY(5) | StringType |
| CompanyCode | bukrs | CHAR(4) | StringType |
| SDDocCategory | vbtyp | CHAR(1) | StringType |
| BillingStatus | rfbsk | CHAR(1) | StringType |
| CancelledFlag | fksto | CHAR(1) | StringType |
| CancelledBilling | sfakn | CHAR(10) | StringType |
| Material | matnr | CHAR(18) | StringType |
| ItemDescription | arktx | CHAR(40) | StringType |
| BilledQty | fkimg | QUAN | StringType |
| SalesUnit | vrkme | UNIT(3) | StringType |
| NetValueItem | netwr_i | CURR | StringType |
| TaxAmount | mwsbp | CURR | StringType |
| Plant | werks | CHAR(4) | StringType |
| MaterialGroup | matkl | CHAR(9) | StringType |
| ItemCategory | pstyv | CHAR(4) | StringType |
| SalesOrder | aubel | CHAR(10) | StringType |
| Batch | charg | CHAR(10) | StringType |
| SellerPartner | parnr | CHAR(10) | StringType |
| CreatedDate | erdat | DATS(8) | StringType |

### ZFB_CDS06 → brz_sap_pm_equipment_raw (Equipment)
| CDS Field | Bronze Column | SAP Type | PySpark Type |
|---|---|---|---|
| Equipment | equnr | CHAR(18) | StringType |
| EquipDescription | eqktx | CHAR(40) | StringType |
| SerialNumber | sernr | CHAR(18) | StringType |
| Material | matnr | CHAR(18) | StringType |
| Batch | hession | CHAR(10) | StringType |
| Plant | swerk | CHAR(4) | StringType |
| CreatedDate | erdat | DATS(8) | StringType |

### ZFB_CDS07 → brz_sap_fi_acct_recv_raw (Accounts Receivable)
| CDS Field | Bronze Column | SAP Type | PySpark Type |
|---|---|---|---|
| CompanyCode | bukrs | CHAR(4) | StringType |
| Customer | kunnr | CHAR(10) | StringType |
| DocumentNumber | belnr | CHAR(10) | StringType |
| FiscalYear | gjahr | NUMC(4) | StringType |
| LineItem | buzei | NUMC(3) | StringType |
| PostingDate | budat | DATS(8) | StringType |
| DocumentDate | bldat | DATS(8) | StringType |
| DueDate | zfbdt | DATS(8) | StringType |
| Amount | dmbtr | CURR | StringType |
| Currency | waers | CUKY(5) | StringType |
| ClearingDoc | augbl | CHAR(10) | StringType |
| ClearingDate | augdt | DATS(8) | StringType |
| PaymentTerms | zterm | CHAR(4) | StringType |
| Reference | xblnr | CHAR(16) | StringType |

---

## 2. Silver Layer Schemas

### silver.customer
| Column | Type | Source | Transformation |
|---|---|---|---|
| customer_id | STRING | customer_number | LTRIM zeros |
| name | STRING | name1 + name2 | CONCAT + TRIM |
| tax_id | STRING | stcd1 | REGEXP: digits only |
| street | STRING | street | TRIM |
| city | STRING | city | TRIM |
| region | STRING | region | TRIM |
| postal_code | STRING | postal_code | TRIM |
| country | STRING | country | TRIM, default 'CL' |
| phone | STRING | tel_number | REGEXP: digits only |
| email | STRING | email | LOWER + TRIM |
| is_deleted | BOOLEAN | loevm | loevm = 'X' |
| created_date | DATE | erdat | safe_cast_date |
| _loaded_at | TIMESTAMP | — | current_timestamp() |

### silver.seller
| Column | Type | Source | Transformation |
|---|---|---|---|
| seller_id | STRING | seller_id | TRIM |
| name | STRING | seller_name | TRIM |
| document | STRING | document | TRIM |
| phone | STRING | phone | REGEXP: digits only |
| email | STRING | email | LOWER + TRIM |
| _loaded_at | TIMESTAMP | — | current_timestamp() |

### silver.material
| Column | Type | Source | Transformation |
|---|---|---|---|
| material_id | STRING | matnr | LTRIM zeros |
| description | STRING | maktx | TRIM (SPRAS='S' for Spanish) |
| material_type | STRING | mtart | TRIM |
| material_group | STRING | matkl | TRIM |
| division | STRING | spart | TRIM |
| base_uom | STRING | meins | TRIM |
| manufacturer | STRING | mfrnr | TRIM |
| plant | STRING | werks | TRIM |
| brand | STRING | config_brand_rules | Lookup: 'JD', 'OTHER' |
| item_category | STRING | config_brand_rules | 'MACHINERY' or 'PARTS' |
| is_jd_part | BOOLEAN | config | MFRNR IN ('0000100005','0000100157') |
| is_jd_machine | BOOLEAN | config | SPART LIKE 'M%' AND MATKL = '0005' |
| _loaded_at | TIMESTAMP | — | current_timestamp() |

### silver.equipment
| Column | Type | Source | Transformation |
|---|---|---|---|
| equipment_id | STRING | equnr | LTRIM zeros |
| description | STRING | eqktx | TRIM |
| serial_number | STRING | sernr | TRIM — this is the VIN |
| material_id | STRING | matnr | LTRIM zeros |
| batch | STRING | hession | TRIM — JOIN key to billing |
| plant | STRING | swerk | TRIM |
| model_name | STRING | MAKT.maktx | JOIN via material_id |
| vehicle_state | STRING | config_vehicle_state | SPART → N/U lookup |
| _loaded_at | TIMESTAMP | — | current_timestamp() |

### silver.sales_order
| Column | Type | Source | Transformation |
|---|---|---|---|
| order_id | STRING | vbeln | LTRIM zeros |
| order_item | STRING | posnr | LTRIM zeros |
| order_type | STRING | auart | TRIM |
| sales_org | STRING | vkorg | TRIM |
| dist_channel | STRING | vtweg | TRIM |
| division | STRING | spart | TRIM |
| customer_id | STRING | kunnr | LTRIM zeros |
| material_id | STRING | matnr | LTRIM zeros |
| description | STRING | arktx | TRIM |
| quantity | DECIMAL(13,3) | kwmeng | safe_cast_decimal |
| net_value | DECIMAL(15,2) | netwr | safe_cast_decimal |
| currency | STRING | waerk | TRIM |
| order_date | DATE | audat | safe_cast_date |
| rejection_reason | STRING | abgru | TRIM |
| plant | STRING | werks | TRIM |
| seller_id | STRING | parnr | LTRIM zeros |
| status | STRING | derived | See business_rules.md: order_status |
| created_date | DATE | erdat | safe_cast_date |
| _loaded_at | TIMESTAMP | — | current_timestamp() |

### silver.billing_document
| Column | Type | Source | Transformation |
|---|---|---|---|
| billing_id | STRING | vbeln | LTRIM zeros |
| billing_item | STRING | posnr | LTRIM zeros |
| billing_type | STRING | fkart | TRIM |
| billing_date | DATE | fkdat | safe_cast_date |
| sales_org | STRING | vkorg | TRIM |
| dist_channel | STRING | vtweg | TRIM |
| division | STRING | spart | TRIM |
| customer_id | STRING | kunag | LTRIM zeros |
| payer_id | STRING | kunrg | LTRIM zeros |
| net_value_header | DECIMAL(15,2) | netwr_h | safe_cast_decimal |
| currency | STRING | waerk | TRIM |
| company_code | STRING | bukrs | TRIM |
| sd_doc_category | STRING | vbtyp | TRIM |
| billing_status | STRING | rfbsk | TRIM |
| is_cancelled | BOOLEAN | fksto | fksto = 'X' |
| cancelled_billing | STRING | sfakn | TRIM |
| material_id | STRING | matnr | LTRIM zeros |
| item_description | STRING | arktx | TRIM |
| billed_qty | DECIMAL(13,3) | fkimg | safe_cast_decimal |
| sales_unit | STRING | vrkme | TRIM |
| net_value_item | DECIMAL(15,2) | netwr_i | safe_cast_decimal |
| tax_amount | DECIMAL(15,2) | mwsbp | safe_cast_decimal |
| plant | STRING | werks | TRIM |
| material_group | STRING | matkl | TRIM |
| item_category | STRING | pstyv | TRIM |
| sales_order_ref | STRING | aubel | LTRIM zeros |
| batch | STRING | charg | TRIM — JOIN key to equipment |
| seller_id | STRING | parnr | LTRIM zeros |
| invoice_type | STRING | derived | See business_rules.md |
| invoice_status | STRING | derived | See business_rules.md |
| created_date | DATE | erdat | safe_cast_date |
| _loaded_at | TIMESTAMP | — | current_timestamp() |

### silver.account_receivable
| Column | Type | Source | Transformation |
|---|---|---|---|
| company_code | STRING | bukrs | TRIM |
| customer_id | STRING | kunnr | LTRIM zeros |
| doc_number | STRING | belnr | TRIM |
| fiscal_year | STRING | gjahr | TRIM |
| line_item | STRING | buzei | TRIM |
| posting_date | DATE | budat | safe_cast_date |
| document_date | DATE | bldat | safe_cast_date |
| due_date | DATE | zfbdt | safe_cast_date |
| amount | DECIMAL(15,2) | dmbtr | safe_cast_decimal |
| currency | STRING | waers | TRIM |
| clearing_doc | STRING | augbl | TRIM |
| clearing_date | DATE | augdt | safe_cast_date |
| is_cleared | BOOLEAN | augbl | augbl != '' |
| is_overdue | BOOLEAN | derived | due_date < today AND NOT is_cleared |
| status | STRING | derived | See business_rules.md: ar_status |
| _loaded_at | TIMESTAMP | — | current_timestamp() |

---

## 3. Gold Layer Schemas

> **RULE:** All Gold fields are STRING type. Dates use `dd-MM-yyyy HH:mm:ss`. NULLs → `""`.

### gold_seedz_sup_customer (→ POST /v1/customer)
| Column | Seedz Field | Source | Derivation |
|---|---|---|---|
| identification | identification | RUT + branch | `{company_rut}_{plant_code}` |
| id | id | customer_id | Direct |
| createdAt | createdAt | created_date | Format dd-MM-yyyy HH:mm:ss |
| updatedAt | updatedAt | _loaded_at | Format dd-MM-yyyy HH:mm:ss |
| companyName | companyName | name | Direct |
| clientIdentification | clientIdentification | tax_id | Digits only (RUT) |
| phones | phones | phone | Digits only |
| email | email | email | Direct |

### gold_seedz_sup_property (→ POST /v1/property)
| Column | Seedz Field | Source |
|---|---|---|
| identification | identification | `{company_rut}_{plant_code}` |
| id | id | customer_id (same as customer) |
| createdAt | createdAt | created_date |
| updatedAt | updatedAt | _loaded_at |
| clientId | clientId | customer_id |
| companyName | companyName | name |
| identificationProperty | identificationProperty | `{company_rut}` |

### gold_seedz_sup_item (→ POST /v1/item)
| Column | Seedz Field | Source |
|---|---|---|
| identification | identification | `{company_rut}_{plant_code}` |
| id | id | material_id |
| createdAt | createdAt | created_date |
| updatedAt | updatedAt | _loaded_at |
| sku | sku | material_id |
| description | description | description |

### gold_seedz_sup_seller (→ POST /v1/seller)
| Column | Seedz Field | Source |
|---|---|---|
| identification | identification | seller_id |
| id | id | seller_id |
| document | document | document |
| companyName | companyName | name |
| phones | phones | phone |
| email | email | email |
| createdAt | createdAt | created_date |
| updatedAt | updatedAt | _loaded_at |

### gold_seedz_sup_order (→ POST /v1/order)
| Column | Seedz Field | Source |
|---|---|---|
| identification | identification | `{company_rut}_{plant_code}` |
| id | id | order_id |
| createdAt | createdAt | created_date |
| updatedAt | updatedAt | _loaded_at |
| issuedAt | issuedAt | order_date |
| status | status | status (0=pending, 1=billed, 2=cancelled, 3=lost) |
| clientId | clientId | customer_id |
| clientIdentification | clientIdentification | customer.tax_id (JOIN) |
| companyIdentification | companyIdentification | `{company_rut}` |
| sellers | sellers | seller_id |

### gold_seedz_sup_order_item (→ POST /v1/orderItem)
| Column | Seedz Field | Source |
|---|---|---|
| identification | identification | `{company_rut}_{plant_code}` |
| id | id | `{order_id}_{order_item}` |
| orderId | orderId | order_id |
| issuedAt | issuedAt | order_date |
| status | status | status (same as order) |
| clientIdentification | clientIdentification | customer.tax_id |
| idItem | idItem | material_id |
| itemSku | itemSku | material_id |
| itemDescription | itemDescription | description |
| itemMeasureUnit | itemMeasureUnit | sales_unit |
| quantity | quantity | quantity as string |
| netUnitValue | netUnitValue | net_value / quantity |
| totalValue | totalValue | net_value as string |
| createdAt | createdAt | created_date |
| updatedAt | updatedAt | _loaded_at |

### gold_seedz_sup_invoice (→ POST /v1/invoice)
| Column | Seedz Field | Source |
|---|---|---|
| identification | identification | `{company_rut}_{plant_code}` |
| id | id | billing_id |
| clientId | clientId | customer_id |
| clientDocument | clientDocument | customer.tax_id (JOIN) |
| number | number | billing_id |
| serie | serie | `"1"` (default for Chile) |
| issuedAt | issuedAt | billing_date |
| type | type | invoice_type (0=credit, 1=debit) |
| status | status | invoice_status (0=cancelled, 1=pending, 2=completed) |
| statusId | statusId | Same as status numeric |
| orderId | orderId | sales_order_ref |
| createdAt | createdAt | created_date |
| updatedAt | updatedAt | _loaded_at |

### gold_seedz_sup_invoice_item (→ POST /v1/invoiceItem)
| Column | Seedz Field | Source |
|---|---|---|
| identification | identification | `{company_rut}_{plant_code}` |
| id | id | `{billing_id}_{billing_item}` |
| number | number | billing_id |
| serie | serie | `"1"` |
| issuedAt | issuedAt | billing_date |
| orderId | orderId | sales_order_ref |
| type | type | invoice_type |
| clientId | clientId | customer_id |
| clientDocument | clientDocument | customer.tax_id |
| parentInvoiceId | parentInvoiceId | cancelled_billing (if credit note) |
| parentInvoice | parentInvoice | cancelled_billing |
| itemId | itemId | material_id |
| itemDescription | itemDescription | item_description |
| itemSku | itemSku | material_id |
| itemMeasureUnit | itemMeasureUnit | sales_unit |
| itemQuantity | itemQuantity | billed_qty |
| itemUnitValue | itemUnitValue | net_value_item / billed_qty |
| itemTotalValue | itemTotalValue | net_value_item |
| sellerIdentification | sellerIdentification | seller_id |
| category | category | material.item_category |
| status | status | invoice_status |
| statusId | statusId | invoice_status numeric |
| createdAt | createdAt | created_date |
| updatedAt | updatedAt | _loaded_at |

### gold_seedz_sup_acct_recv (→ POST /v1/accountReceivable)
| Column | Seedz Field | Source |
|---|---|---|
| identification | identification | `{company_rut}_{plant_code}` |
| id | id | `{doc_number}_{fiscal_year}_{line_item}` |
| titlenumber | titlenumber | doc_number |
| clientId | clientId | customer_id |
| supplierName | supplierName | customer.name (JOIN) |
| issuedAt | issuedAt | document_date |
| number | number | doc_number |
| serie | serie | fiscal_year |
| originalDueAt | originalDueAt | due_date |
| extendedDueAt | extendedDueAt | due_date (same unless extended) |
| netValue | netValue | amount |
| status | status | ar_status (Aberto, Liquidado, Atrasado, etc.) |
| paidAt | paidAt | clearing_date (only if cleared) |
| sellers | sellers | "" (not available at AR level) |
| createdAt | createdAt | posting_date |
| updatedAt | updatedAt | _loaded_at |

### gold_seedz_cens_payload (→ POST /v1/censacional)
| Column | Seedz Field | Source |
|---|---|---|
| dealerCode | dealerCode | SALFA dealer code (config) |
| billingDocument | billingDocument | billing_id |
| billingItem | billingItem | billing_item |
| billingDate | billingDate | billing_date |
| customerNumber | customerNumber | customer_id |
| customerName | customerName | customer.name (JOIN) |
| materialNumber | materialNumber | material_id |
| materialDescription | materialDescription | item_description |
| quantity | quantity | billed_qty |
| netValue | netValue | net_value_item |
| currency | currency | currency |
| plant | plant | plant |
| chassi | chassi | equipment.serial_number (VIN) — JOIN via batch |
| model | model | equipment.model_name — JOIN via batch |
| vehicleState | vehicleState | equipment.vehicle_state — JOIN via batch |
| sellerCode | sellerCode | seller_id |
| invoiceType | invoiceType | invoice_type |
| isMachinery | isMachinery | material.is_jd_machine |
