@AbapCatalog.sqlViewName: 'ZFBCDS02'
@AbapCatalog.compiler.compareFilter: true
@AbapCatalog.preserveKey: true
@AccessControl.authorizationCheck: #NOT_REQUIRED
@EndUserText.label: 'FB: Seller Master Data'

@Analytics.dataCategory: #DIMENSION

-- ============================================================
-- MOCK / PLACEHOLDER — Replace once seller table is confirmed.
-- Uses VBPA partner function VE as interim seller source.
-- ============================================================

define view ZFB_CDS02
  as select distinct from vbpa
{
  key vbpa.parnr             as SellerPartner
}
where vbpa.parvw = 'VE'
