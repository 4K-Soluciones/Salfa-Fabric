@AbapCatalog.sqlViewName: 'ZFBCDS01'
@AbapCatalog.compiler.compareFilter: true
@AbapCatalog.preserveKey: true
@AccessControl.authorizationCheck: #NOT_REQUIRED
@EndUserText.label: 'FB: Customer Master + Address'

@Analytics.dataCategory: #DIMENSION
@Analytics.dataExtraction.enabled: true

define view ZFB_CDS01
  as select from kna1
  left outer join adrc on  kna1.adrnr = adrc.addrnumber
                       and adrc.nation     = ''
{
      // --- Keys ---
  key kna1.kunnr           as CustomerNumber,

      // --- Master Data ---
      kna1.name1           as Name1,
      kna1.name2           as Name2,
      kna1.stcd1           as TaxNumber1,         // RUT in Chile
      kna1.stcd2           as TaxNumber2,
      kna1.ktokd           as AccountGroup,
      kna1.land1           as Country,
      kna1.regio           as Region,
      kna1.spras           as Language,
      kna1.brsch           as IndustrySector,
      kna1.kukla           as CustomerClass,
      kna1.erdat           as CreatedDate,
      kna1.ernam           as CreatedBy,
      kna1.loevm           as DeletionFlag,

      // --- Address (ADRC) ---
      adrc.street          as Street,
      adrc.house_num1      as HouseNumber,
      adrc.city1           as City,
      adrc.city2           as District,
      adrc.post_code1      as PostalCode,
      adrc.region          as AddrRegion,
      adrc.country         as AddrCountry,
      adrc.tel_number      as Phone,
      adrc.fax_number      as Fax
}
