@AbapCatalog.sqlViewName: 'ZFBCDS03'
@AbapCatalog.compiler.compareFilter: true
@AbapCatalog.preserveKey: true
@AccessControl.authorizationCheck: #NOT_REQUIRED
@EndUserText.label: 'FB: Sales Orders Header + Item'

@Analytics.dataCategory: #FACT
@Analytics.dataExtraction.enabled: true

define view ZFB_CDS03
  as select from vbak
  inner join vbap on vbak.vbeln = vbap.vbeln
  left outer join vbpa on  vbak.vbeln = vbpa.vbeln
                        and vbpa.posnr = '000000'
                        and vbpa.parvw = 'VE'       // Seller partner
{
      // --- Keys ---
  key vbap.vbeln             as SalesOrder,
  key vbap.posnr             as SalesOrderItem,

      // --- Header Data ---
      vbak.auart             as OrderType,
      vbak.vkorg             as SalesOrg,
      vbak.vtweg             as DistChannel,
      vbak.spart             as Division,
      vbak.vkbur             as SalesOffice,
      vbak.vkgrp             as SalesGroup,
      vbak.kunnr             as SoldToParty,
      vbak.erdat             as CreatedDate,
      vbak.erzet             as CreatedTime,
      vbak.ernam             as CreatedBy,
      vbak.netwr             as NetValueHeader,
      vbak.waerk             as Currency,

      // --- Item Data ---
      vbap.matnr             as Material,
      vbap.arktx             as ItemDescription,
      vbap.kwmeng            as OrderQty,
      vbap.vrkme             as SalesUnit,
      @Semantics.amount.currencyCode: 'Currency'
      vbap.netwr             as NetValueItem,
      vbap.werks             as Plant,
      vbap.lgort             as StorageLocation,
      vbap.matkl             as MaterialGroup,
      vbap.pstyv             as ItemCategory,
      vbap.abgru             as RejectionReason,

      // --- Seller (VE) ---
      vbpa.parnr             as SellerPartner,

      // --- Delta Tracking ---
      vbak.erdat             as CreatedDateHeader
}
