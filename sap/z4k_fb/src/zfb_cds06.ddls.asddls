@AbapCatalog.sqlViewName: 'ZFBCDS06'
@AbapCatalog.compiler.compareFilter: true
@AbapCatalog.preserveKey: true
@AccessControl.authorizationCheck: #NOT_REQUIRED
@EndUserText.label: 'FB: Equipment Master + Description'

@Analytics.dataCategory: #DIMENSION
@Analytics.dataExtraction.enabled: true

-- ============================================================
-- Equipment master for ALL brands (JD, Hitachi, Hamm, CAT, etc.)
-- Key fields for Censacional machinery enrichment:
--   SERNR  = VIN / chassis number  (maps to chassi)
--   CHARGE = join key to VBRP.CHARG (94.1% coverage confirmed)
--   MATNR  = material link for MAKT.MAKTX (model description)
--
-- SALFA-specific findings:
--   SERGE/TYPBZ are EMPTY at SALFA (don't use)
--   SERNR = EQUNR (100% identical at SALFA)
-- ============================================================

define view ZFB_CDS06
  as select from equi
  left outer join eqkt on  equi.equnr = eqkt.equnr
                        and eqkt.spras = 'S'
{
      // --- Keys ---
  key equi.equnr             as EquipmentNumber,

      // --- Equipment Identifiers ---
      equi.sernr             as SerialNumber,
      equi.serge             as MfgSerialNumber,
      equi.matnr             as Material,
      equi.charge            as Batch,

      // --- Classification ---
      equi.eqart             as EquipmentCategory,
      equi.typbz             as TypeDescription,
      equi.herst             as Manufacturer,

      // --- Status ---
      equi.aedat             as LastChangeDate,
      equi.lvorm             as DeletionFlag,

      // --- Description (EQKT) ---
      eqkt.eqktx             as EquipmentDescription
}
