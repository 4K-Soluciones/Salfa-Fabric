@AbapCatalog.sqlViewName: 'ZFBCDS05'
@AbapCatalog.compiler.compareFilter: true
@AbapCatalog.preserveKey: true
@AccessControl.authorizationCheck: #NOT_REQUIRED
@EndUserText.label: 'FB: Billing Header + Item + Seller'

@Analytics.dataCategory: #FACT
@Analytics.dataExtraction.enabled: true

-- ============================================================
-- This is the CORE extraction for Seedz (Superación + Censacional),
-- ELIPS, and JDPrism. Extracts ALL billing documents.
-- Brand/project filtering happens in Fabric Silver/Gold.
--
-- Delta strategy: Watermark on ERDAT (Fabric pipeline stores
-- last loaded date, next run filters ERDAT >= lastWatermark).
-- Delete detection: FKSTO='X' flags cancellations, plus
-- ZFB_CDS08 (CDHDR/CDPOS) tracks all change operations.
-- ============================================================

define view ZFB_CDS05
  as select from vbrk
  inner join vbrp on vbrk.vbeln = vbrp.vbeln
  left outer join vbpa on  vbrk.vbeln = vbpa.vbeln
                        and vbpa.posnr = '000000'
                        and vbpa.parvw = 'VE'       // Seller on invoice
{
      // --- Keys ---
  key vbrp.vbeln             as BillingDocument,
  key vbrp.posnr             as BillingItem,

      // --- Header ---
      vbrk.fkart             as BillingType,
      vbrk.fkdat             as BillingDate,
      vbrk.vkorg             as SalesOrg,
      vbrk.vtweg             as DistChannel,
      vbrk.spart             as Division,
      vbrk.kunag             as SoldToParty,
      vbrk.kunrg             as Payer,
      vbrk.netwr             as NetValueHeader,
      vbrk.waerk             as Currency,
      vbrk.bukrs             as CompanyCode,
      vbrk.vbtyp             as SDDocCategory,
      vbrk.rfbsk             as BillingStatus,
      vbrk.fksto             as CancelledFlag,
      vbrk.sfakn             as CancelledBilling,
      vbrk.erdat             as CreatedDate,
      vbrk.erzet             as CreatedTime,

      // --- Item ---
      vbrp.matnr             as Material,
      vbrp.arktx             as ItemDescription,
      vbrp.fkimg             as BilledQty,
      vbrp.vrkme             as SalesUnit,
      vbrp.netwr             as NetValueItem,
      vbrp.mwsbp             as TaxAmount,
      vbrp.werks             as Plant,
      vbrp.lgort             as StorageLocation,
      vbrp.matkl             as MaterialGroup,
      vbrp.pstyv             as ItemCategory,
      vbrp.aubel             as SalesOrder,         // Reference order
      vbrp.aupos             as SalesOrderItem,
      vbrp.vgbel             as ReferenceDoc,        // Delivery ref
      vbrp.vgpos             as ReferenceItem,
      vbrp.charg             as Batch,               // EQUI join key!

      // --- Seller (VE) ---
      vbpa.parnr             as SellerPartner,

      // --- Delta Tracking ---
      vbrk.erdat             as DocCreatedDate
}
