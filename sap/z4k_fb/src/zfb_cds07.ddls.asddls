@AbapCatalog.sqlViewName: 'ZFBCDS07'
@AbapCatalog.compiler.compareFilter: true
@AbapCatalog.preserveKey: true
@AccessControl.authorizationCheck: #NOT_REQUIRED
@EndUserText.label: 'FB: Accounts Receivable Open + Cleared'

@Analytics.dataCategory: #FACT
@Analytics.dataExtraction.enabled: true

-- ============================================================
-- Superacion ACCOUNTRECEIVABLE entity
-- Uses BSID (open items) UNION BSAD (cleared items)
-- ============================================================

define view ZFB_CDS07
  as select from bsid
{
  key bsid.bukrs             as CompanyCode,
  key bsid.kunnr             as Customer,
  key bsid.belnr             as AccountingDoc,
  key bsid.gjahr             as FiscalYear,
  key bsid.buzei             as LineItem,

      bsid.blart             as DocType,
      bsid.budat             as PostingDate,
      bsid.bldat             as DocumentDate,
      bsid.waers             as Currency,
      bsid.dmbtr             as AmountLocalCurrency,
      bsid.wrbtr             as AmountDocCurrency,
      bsid.shkzg             as DebitCredit,
      bsid.zfbdt             as BaselinePayDate,
      bsid.zbd1t             as PayTermDays1,
      bsid.zbd2t             as PayTermDays2,
      bsid.zbd3t             as PayTermDays3,
      bsid.rebzg             as InvoiceReference,
      bsid.vbeln             as BillingDocument,
      bsid.zuonr             as AssignmentNumber,
      bsid.sgtxt             as ItemText,

      bsid.augdt             as ClearingDate,
      bsid.augbl             as ClearingDoc
}

union all

select from bsad
{
  key bsad.bukrs             as CompanyCode,
  key bsad.kunnr             as Customer,
  key bsad.belnr             as AccountingDoc,
  key bsad.gjahr             as FiscalYear,
  key bsad.buzei             as LineItem,

      bsad.blart             as DocType,
      bsad.budat             as PostingDate,
      bsad.bldat             as DocumentDate,
      bsad.waers             as Currency,
      bsad.dmbtr             as AmountLocalCurrency,
      bsad.wrbtr             as AmountDocCurrency,
      bsad.shkzg             as DebitCredit,
      bsad.zfbdt             as BaselinePayDate,
      bsad.zbd1t             as PayTermDays1,
      bsad.zbd2t             as PayTermDays2,
      bsad.zbd3t             as PayTermDays3,
      bsad.rebzg             as InvoiceReference,
      bsad.vbeln             as BillingDocument,
      bsad.zuonr             as AssignmentNumber,
      bsad.sgtxt             as ItemText,

      bsad.augdt             as ClearingDate,
      bsad.augbl             as ClearingDoc
}
