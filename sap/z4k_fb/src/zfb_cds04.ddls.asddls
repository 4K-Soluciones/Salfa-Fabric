@AbapCatalog.sqlViewName: 'ZFBCDS04'
@AbapCatalog.compiler.compareFilter: true
@AbapCatalog.preserveKey: true
@AccessControl.authorizationCheck: #NOT_REQUIRED
@EndUserText.label: 'FB: Material Master - All Brands'

@Analytics.dataCategory: #DIMENSION
@Analytics.dataExtraction.enabled: true

-- ============================================================
-- CRITICAL DESIGN DECISION: NO brand filters in this CDS.
-- Unlike ZJD_CDS01 which has: WHERE mfrnr IN ('0000100005',...)
-- This view extracts ALL materials. Brand classification
-- (JD parts, JD machines, CAT, Hitachi, etc.) happens in
-- Fabric Silver layer via config_brand_rules table.
-- ============================================================

define view ZFB_CDS04
  as select from mara
  inner join makt on  mara.matnr = makt.matnr
                   and makt.spras = 'S'
  left outer join marc on mara.matnr = marc.matnr
{
      // --- Keys ---
  key mara.matnr             as Material,
  key marc.werks             as Plant,

      // --- Material Classification ---
      mara.mtart             as MaterialType,
      mara.matkl             as MaterialGroup,
      mara.spart             as Division,          // M0-M6 = machinery
      mara.prdha             as ProductHierarchy,  // M... = machines
      mara.mfrnr             as Manufacturer,      // 100005 = JD parts
      mara.mfrpn             as MfgPartNumber,

      // --- Description ---
      makt.maktx             as MaterialDescription,

      // --- Master Attributes ---
      mara.meins             as BaseUOM,
      mara.brgew             as GrossWeight,
      mara.ntgew             as NetWeight,
      mara.gewei             as WeightUnit,
      mara.lvorm             as DeletionFlag,
      mara.ersda             as CreatedDate,
      mara.laeda             as LastChangeDate,

      // --- Plant-Level ---
      marc.dispo             as MRPController,
      marc.dismm             as MRPType,
      marc.plifz             as PlannedDelivTime,
      marc.eisbe             as SafetyStock,
      marc.lgrad             as ServiceLevel
}
