@AbapCatalog.sqlViewName: 'ZFBCDS08'
@AbapCatalog.compiler.compareFilter: true
@AbapCatalog.preserveKey: true
@AccessControl.authorizationCheck: #NOT_REQUIRED
@EndUserText.label: 'FB: Change Documents for Delta Detection'

@Analytics.dataCategory: #FACT

-- ============================================================
-- CHANGE DOCUMENT LOG — Detects Inserts, Updates and Deletes.
--
-- Since ODP CDC annotations are NOT available on ECC NW 7.5,
-- this view reads SAP's native change document tables
-- (CDHDR + CDPOS) to capture I/U/D operations.
--
-- Fabric uses this as a "delta signal" table:
--   1. Load ZFB_CDS08 with watermark on UDATE
--   2. For each changed OBJECTID, re-read the affected record
--      from ZFB_CDS01/03/04/05/06 to get current state
--   3. MERGE into Silver tables
--
-- Key OBJECTCLAS values for our scope:
--   DEBI / DEBITOR = Customer master (KNA1)
--   MATERIAL       = Material master (MARA)
--   VERKBELEG      = Sales document (VBAK/VBAP)
--   FAKTBELEG      = Billing document (VBRK/VBRP)
--   EQUI           = Equipment master (EQUI)
--   FKKKO          = FI document
-- ============================================================

define view ZFB_CDS08
  as select from cdhdr
  inner join cdpos on  cdhdr.objectclas = cdpos.objectclas
                   and cdhdr.objectid   = cdpos.objectid
                   and cdhdr.changenr   = cdpos.changenr
{
      // --- Keys ---
  key cdhdr.objectclas       as ObjectClass,
  key cdhdr.objectid         as ObjectId,
  key cdhdr.changenr         as ChangeNumber,
  key cdpos.tabname          as TableName,
  key cdpos.tabkey           as TableKey,
  key cdpos.fname            as FieldName,

      // --- Change Header ---
      cdhdr.username         as ChangedBy,
      cdhdr.udate            as ChangeDate,
      cdhdr.utime            as ChangeTime,
      cdhdr.tcode            as TransactionCode,

      // --- Change Detail ---
      cdpos.chngind          as ChangeIndicator,    // I=Insert U=Update D=Delete
      cdpos.value_new        as NewValue,
      cdpos.value_old        as OldValue
}
where cdhdr.objectclas = 'DEBI'
   or cdhdr.objectclas = 'MATERIAL'
   or cdhdr.objectclas = 'VERKBELEG'
   or cdhdr.objectclas = 'FAKTBELEG'
   or cdhdr.objectclas = 'EQUI'
   or cdhdr.objectclas = 'FKKKO'
