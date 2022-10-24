if object_id('dbo.ExternalFileEUHotelFactLoadExpand') is null begin
    print 'Creating stored procedure ExternalFileEUHotelFactLoadExpand (placeholder)'
    execute('create procedure dbo.ExternalFileEUHotelFactLoadExpand as return 0')
end
go

print 'Altering stored procedure ExternalFileEUHotelFactLoadExpand'
go

alter procedure dbo.ExternalFileEUHotelFactLoadExpand 
    @pExternalFileID int
with recompile
as

/*
*********************************************************************
Copyright (C) 2006-2018 Expedia, Inc. All rights reserved.

Description:
     Updates ExternalFileEUHotelFact with most attributes

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2007-10-26  VBoerner        Created.
    2007-11-05  VBoerner        Updated per Code Review. 
    2007-12-19  VBoerner        Updated currency conversion
    2009-06-30  vboerner        Added BookingSourceID
    2010-10-16  BSimpson        Added HotelRatingNbr
    2012-01-06  vboerner        Added Hotel/Brand/ChainIDs for F3
    2013-06-17  Patrick Bressan Added PolicyReasonCodeID column
    2013-09-10  DMurugesan      Fixed Null BrandID and ChainID as "Unknown"
    2013-10-14  DMurugesan      GeographyTypeID override using GeographyTypeTravelProduct mapping table
    2014-01-15  DMurugesan      EGE-59458 Fixed Null BrandID and ChainID as "Unknown" for EU billback (BookingSourceID = 37)
    2014-03-18  a-jako          jira.EGE-63263 Update ExternalFileEUHotelFactLoadExpand to lookup BrandID
    2014-10-09  DMurugesan      EGE-73248 ItineraryTxt promoted to ExternalFileEUHotelFact
    2014-11-10  rakarnati       EGE-72650 - Fixed Hotel-Brand issue when hotels becomes independent
    2015-01-26  DMurugesan      EGE-79213 Update TRL from ItineraryTxt
    2015-02-03  rkarnati        EGE-79772 Hotel Rate Type Internal in the EU legacy path
    2015-04-06  rkarnati        EGE-83447 Enhanced Hotel Rate Type Internal in the EU legacy path for EAN booking sources
    2015-07-31  rkarnati        EGE-92424 Hotel brand setting to "Independent" when there is no branding information available
    2016-12-05  jappleberry     EGE-19143 set null GroupAccountDepartmentID to 0
	2018-04-26	Ramesh Karnati	Adding InvoiceSource (Apollo/BIAS) indicator as part of FSP/Oracle Integration
    2018-08-31  nrasmussen      Jira.EGE-202649 IRD add eu agentid (TUIDLogon)
    2018-10-03  nrasmussen      Jira.EGE-217181 IRD EU AgentID fix MetaDossierID overflow for int datatype when joining to EctWeb data
*********************************************************************
*/

set nocount on

---------------------------------------------------------------------
-- Declarations
---------------------------------------------------------------------
declare     -- Standard constants and variables
    @FALSE                          tinyint,
    @TRUE                           tinyint,
    @RC_FAILURE                     int,
    @RC_SUCCESS                     int,
    @Current_Timestamp              datetime,
    @Error                          int,
    @ErrorCode                      int,
    @ExitCode                       int,
    @ProcedureName                  sysname,
    @RC                             int,            -- Return code from called SP
    @Rowcount                       int,
    @SavePointName                  varchar(32),
    @TranStartedBool                tinyint,
    @TrancountSave                  int,
    @NonNumericString               varchar(10)

declare   -- Error message constants
    @ERRUNEXPECTED                  int,
    @ERRPARAMETER                   int,
    @MsgParm1                       varchar(100),
    @MsgParm2                       varchar(100),
    @MsgParm3                       varchar(100)

declare
    @BookingTypeIDPurchase                tinyint,
    @BookingTypeIDCancel                  tinyint,
    @BookingTypeIDReserve                 tinyint,
    @BookingTypeIDVoid                    tinyint,
    @BookingTypeIDExchange                tinyint,
    @BookingTypeIDRefund                  tinyint,
    @BookingTypeIDPRefund                 tinyint,
    @BookingTypeIDUTT                     tinyint,
    @BookingTypeIDVoidRefund              tinyint,
    @ExchangeRateNull                     money,
    @InternalExtractIDMax_LodgingCatalog  int

create table #ExternalFileEUHotelFact (
    ExternalFileID int,
    RecordKey varchar(110),
    BookingTypeID tinyint,
    CustomerSystemID tinyint,
    RecordKeyPrior varchar(110),
    RecordKeyOriginal varchar(110),
    TravelProductID int,
    GroupAccountID int,
    GroupAccountDepartmentID int,
    TUIDAccount int,
    TUIDTraveler int,
    TUIDLogon int,
    TUIDArranger int,
    TravelerNameID int,
    MetaDossierID varchar(20),
    SalesDocumentTypeID tinyint,
    SalesDocumentCode varchar(20),
    SalesDocumentCodePrior varchar(20),
    ItineraryTxt varchar(13),
    IssueDate datetime,
    InvoiceDate datetime,
    TravelDateStart smalldatetime,
    TravelDateEnd smalldatetime,
    TotalDaysCnt smallint,
    EUHotelID int,
    HotelName varchar(255),
    HotelCityName varchar(20),
    HotelProvinceName varchar(30),
    HotelCountryCode char(3),
    RateTypeID smallint,
    GeographyTypeID tinyint,
    ConfirmationNbr varchar(30),
    EUServiceLevelID tinyint,
    TravelerGroupPolicyID int,
    PolicyStatusID tinyint,
    OnlineBool bit,
    OfflineBookingTypeID tinyint,
    AgentAssistedBool bit,
    CurrencyCode char(3),
    CreditCardTypeID tinyint,
    DirectPaymentBool bit,
    CentralBillBool bit,
    CreditCardNbr varbinary(64),
    TransactionCnt smallint,
    IncrementCnt smallint,
    BookingAmtGross money,
    BookingAmtBase money,
    BookingAmtTax money,
    BookingAmtVat money,
    BookingAmtExtraPerson money,
    BookingAmtFee money,
    BookingAmtChangePenalty money,
    CustomDataElementTxt varchar(2000),
    ApprovalDate smalldatetime,
    TUIDApprover int,
    TravelerNameIDApproval int,
    ApprovalTxt varchar(255),
    UpdateDate smalldatetime,
    LastUpdatedBy varchar(32),
    BookingSourceID tinyint,
    HotelRatingNbr tinyint,
    HotelID int,
    HotelBrandID int,
    HotelChainID int,
    PolicyReasonCodeID int,
    CreditCardNbrBegin varchar(6),
    CreditCardNbrEnd varchar(4),
    TRL int,
    HotelRateTypeSupplyID tinyint,
    InvoiceSource varchar(10),
    IsNumericBool int
    )
---------------------------------------------------------------------
-- Initializations
---------------------------------------------------------------------

select   -- Standard constants
    @FALSE                          = 0,
    @TRUE                           = 1,
    @RC_FAILURE                     = -100,
    @RC_SUCCESS                     = 0,
    @NonNumericString               = '%[^0-9]%'

select   -- Standard variables
    @Current_Timestamp              = current_timestamp,
    @ExitCode                       = @RC_SUCCESS,
    @ProcedureName                  = object_name(@@ProcID),
    @SavePointName                  = '$' + cast(@@NestLevel as varchar(15))
                                    + '_' + cast(@@ProcID as varchar(15)),
    @TranStartedBool                = @FALSE

select   -- Error message constants
    @ERRUNEXPECTED                  = 200104,
    @ERRPARAMETER                   = 200110

select
    @BookingTypeIDPurchase                = 1,
    @BookingTypeIDCancel                  = 2,
    @BookingTypeIDReserve                 = 3,
    @BookingTypeIDVoid                    = 4,
    @BookingTypeIDExchange                = 5,
    @BookingTypeIDRefund                  = 7,
    @BookingTypeIDPRefund                 = 8,
    @BookingTypeIDUTT                     = 9,
    @BookingTypeIDVoidRefund              = 10,
    @ExchangeRateNull                     = 0.0

---------------------------------------------------------------------
-- Validation
---------------------------------------------------------------------
-- None, done by caller
---------------------------------------------------------------------
-- Processing
---------------------------------------------------------------------

insert into #ExternalFileEUHotelFact
select
    ExternalFileID,
    RecordKey,
    BookingTypeID,
    CustomerSystemID,
    RecordKeyPrior,
    RecordKeyOriginal,
    TravelProductID,
    GroupAccountID,
    GroupAccountDepartmentID,
    TUIDAccount,
    TUIDTraveler,
    TUIDLogon,
    TUIDArranger,
    TravelerNameID,
    MetaDossierID,
    SalesDocumentTypeID,
    SalesDocumentCode,
    SalesDocumentCodePrior,
    ItineraryTxt,
    IssueDate,
    InvoiceDate,
    TravelDateStart,
    TravelDateEnd,
    TotalDaysCnt,
    EUHotelID,
    HotelName,
    HotelCityName,
    HotelProvinceName,
    HotelCountryCode,
    RateTypeID,
    GeographyTypeID,
    ConfirmationNbr,
    EUServiceLevelID,
    TravelerGroupPolicyID,
    PolicyStatusID,
    OnlineBool,
    OfflineBookingTypeID,
    AgentAssistedBool,
    CurrencyCode,
    CreditCardTypeID,
    DirectPaymentBool,
    CentralBillBool,
    CreditCardNbr, 
    TransactionCnt,
    IncrementCnt,
    BookingAmtGross,
    BookingAmtBase,
    BookingAmtTax,
    BookingAmtVat,
    BookingAmtExtraPerson,
    BookingAmtFee,
    BookingAmtChangePenalty,
    CustomDataElementTxt,
    ApprovalDate,
    TUIDApprover,
    TravelerNameIDApproval,
    ApprovalTxt,
    UpdateDate,
    LastUpdatedBy,
    BookingSourceID,
    HotelRatingNbr,
    HotelID,
    HotelBrandID,
    HotelChainID,
    PolicyReasonCodeID,
    CreditCardNbrBegin,
    CreditCardNbrEnd,
    TRL,
    HotelRateTypeSupplyID,
    InvoiceSource,
    0 as IsNumericBool
from dbo.ExternalFileEUHotelFact
where 
    ExternalFileID = @pExternalFileID

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #ExternalFileEUHotelFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

create index temp_ix on #ExternalFileEUHotelFact (RecordKey, ExternalFileID, BookingTypeID)

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (index create on #ExternalFileEUHotelFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

update #ExternalFileEUHotelFact
   set
    CustomerSystemID = 2,
    RecordKeyPrior = b.RecordKeyPrior,
    RecordKeyOriginal = b.RecordKeyOriginal, 
    TravelProductID = b.TravelProductID, 
    GroupAccountID = 0,
    GroupAccountDepartmentID = 0,
    TUIDAccount = b.PercodeAccount, 
    TUIDTraveler = b.PercodeAccount, 
    TUIDLogon = case when b.LogonTypeID = 1 then b.LogonID
                     when b.LogonTypeID = 2 then -1 * b.LogonID
                     else null end, 
    TUIDArranger = b.PercodeArranger, 
    MetaDossierID = ltrim(rtrim(b.MetaDossierID)), 
    SalesDocumentTypeID = b.SalesDocumentTypeID, 
    SalesDocumentCode = ltrim(rtrim(b.SalesDocumentCode)), 
    SalesDocumentCodePrior = ltrim(rtrim(b.SalesDocumentCodePrior)), 
    IssueDate = b.IssueDate, 
    InvoiceDate = b.InvoiceDate, 
    TravelDateStart = b.TravelDateStart, 
    TravelDateEnd = b.TravelDateEnd, 
    TotalDaysCnt = case
            when a.BookingTypeID = @BookingTypeIDPurchase then abs(b.TotalDaysCnt)
            when a.BookingTypeID = @BookingTypeIDReserve then abs(b.TotalDaysCnt)
            when a.BookingTypeID = @BookingTypeIDCancel then -1 * abs(b.TotalDaysCnt)
            when a.BookingTypeID = @BookingTypeIDRefund then -1 * abs(b.TotalDaysCnt) end, 
    EUHotelID = b.EUHotelID,
    HotelName = ltrim(rtrim(b.HotelName)),
    HotelCityName = ltrim(rtrim(b.HotelCityName)),
    HotelProvinceName = ltrim(rtrim(b.HotelProvinceName)),
    HotelCountryCode = ltrim(rtrim(b.HotelCountryCode)),
    RateTypeID = b.RateTypeID, 
    GeographyTypeID = null,
    ConfirmationNbr = ltrim(rtrim(b.ConfirmationNbr)), 
    EUServiceLevelID = b.ServiceLevelID, 
    TravelerGroupPolicyID = b.TravelerCategoryID,
    PolicyStatusID = b.PolicyStatusID, 
    OnlineBool = b.OnlineBool, 
    OfflineBookingTypeID = b.OfflineBookingTypeID, 
    AgentAssistedBool = b.AgentAssistedBool, 
    CurrencyCode = c.CurrencyCodeStorage,
    CreditCardTypeID = b.CreditCardTypeID, 
    DirectPaymentBool = b.DirectPaymentBool, 
    CentralBillBool = case when b.CreditCardOwnerID = 1 then 1
                           when b.CreditCardOwnerID = 2 then 0
                           else null end,
    CreditCardNbr = b.CreditCardNbr, 
    TransactionCnt = 1, 
    IncrementCnt = case
            when a.BookingTypeID = @BookingTypeIDPurchase then 1
            when a.BookingTypeID = @BookingTypeIDReserve then 1
            when a.BookingTypeID = @BookingTypeIDCancel then -1
            when a.BookingTypeID = @BookingTypeIDRefund then -1 end, 
    BookingAmtGross = b.BookingAmtGross * coalesce(d.ExchangeRateUsed, @ExchangeRateNull), 
    BookingAmtBase = b.BookingAmtBase * coalesce(d.ExchangeRateUsed, @ExchangeRateNull), 
    BookingAmtTax = (b.BookingAmtTax + b.BookingAmtVAT) * coalesce(d.ExchangeRateUsed, @ExchangeRateNull), 
    BookingAmtVAT = b.BookingAmtVAT * coalesce(d.ExchangeRateUsed, @ExchangeRateNull), 
    BookingAmtExtraPerson = b.BookingAmtExtraPerson * coalesce(d.ExchangeRateUsed, @ExchangeRateNull), 
    BookingAmtFee = b.BookingAmtFee * coalesce(d.ExchangeRateUsed, @ExchangeRateNull), 
    BookingAmtChangePenalty = b.BookingAmtChangePenalty * coalesce(d.ExchangeRateUsed, @ExchangeRateNull), 
    CustomDataElementTxt = coalesce(ltrim(rtrim(b.CostCenterTxt)),''), 
    ApprovalDate = b.ApprovalDate, 
    TUIDApprover = b.PercodeApprover, 
    ApprovalTxt = ltrim(rtrim(b.ApprovalTxt)),
    BookingSourceID = case when e.ExternalFileTypeID = 9 then f.BookingSourceID  --EU IAN File
                           when e.ExternalFileTypeID = 10 then 13 end, --EU Amadeus File
    HotelRatingNbr = b.HotelRatingNbr,
    HotelID = case when f.BookingSourceID in (32,33,34,35,36) then b.EUHotelID else null end, 
    HotelBrandID = case when f.BookingSourceID in (32,33,34,35,36) then coalesce(b.HotelBrandID, 0) else -10000 end,
	PolicyReasonCodeID = b.PolicyReasonCodeID,
	ItineraryTxt = b.ItineraryTxt,
	TRL = case  when a.BookingSourceID IN  (37, 30, 31 ) then null  
	            WHEN ISNUMERIC(substring(b.ItineraryTxt,2, case when len(b.ItineraryTxt) > 2 then len(b.ItineraryTxt) - 3 else 0 end )) = 0     THEN Null
                WHEN substring(b.ItineraryTxt,2, case when len(b.ItineraryTxt) > 2 then len(b.ItineraryTxt) - 3 else 0 end ) LIKE '%[^-+ 0-9]%' THEN NULL
                WHEN CAST(substring(b.ItineraryTxt,2, case when len(b.ItineraryTxt) > 2 then len(b.ItineraryTxt) - 3 else 0 end ) AS NUMERIC(38, 0)) 
                NOT BETWEEN -2147483648. AND 2147483647. THEN NULL
           ELSE substring(b.ItineraryTxt,2, case when len(b.ItineraryTxt) > 2 then len(b.ItineraryTxt) - 3 else 0 end )
           END,
	InvoiceSource = b.Location_Code	     
  from #ExternalFileEUHotelFact a 
       inner join
       dbo.vExternalFileEUHotel b on a.ExternalFileID = b.ExternalFileID and a.RecordKey = b.RecordKey
       inner join 
       dbo.TravelProductDim c on b.TravelProductID = c.TravelProductID
       left join
       dbo.ExchangeRateDailyFull d on dbo.TimeIDFromDate(coalesce(b.InvoiceDate, b.IssueDate)) = d.TimeID and
            b.CurrencyCode = d.FromCurrencyCode and 
            c.CurrencyCodeStorage = d.ToCurrencyCode
       inner join
       dbo.ExternalFile e on a.ExternalFileID = e.ExternalFileID
       left join 
       dbo.ExternalFileTypeBookingSource f on e.ExternalFileTypeID = f.ExternalFileTypeID and
           b.BookingSourceCode = f.ExternalFileBookingSourceCode

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUHotelFact expand)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


-- Fetch the latest InternalExtract of LodgingCatalog 
-- Note: The source table "SkuGroupBrand" is a FULL extract now and we want to filter to the latest extract only.

select @InternalExtractIDMax_LodgingCatalog = max(InternalExtractID) 
from dbo.InternalExtract 
where SourceDatabaseName = 'LodgingCatalog'
    and InternalExtractStateID	= 3	
       
select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (fetch latest InternalExtract of LodgingCatalog, part of Expand HotelBrandID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


-- HotelBrandID
update #ExternalFileEUHotelFact
   set HotelBrandID = case when a.BookingSourceID in (32,33,34,35,36) then coalesce(B.BrandID, 0) else coalesce(B.BrandID, -10000) end
  from #ExternalFileEUHotelFact a 
       left join (
            select
                   SKUGroupCatalogItemID, 
                   BrandID,
                   BrandUseRank,
                   row_number() over (partition by SKUGroupCatalogItemID order by BrandUseRank, UpdateDate desc) RankNbr  --rank over UpdateDate to dedupe deleted records
              from dbo.vSkuGroupBrand 
             where BrandUseRank = 1
                    and InternalExtractIDMax = @InternalExtractIDMax_LodgingCatalog
            ) B on a.HotelID = B.SKUGroupCatalogItemID and B.RankNbr = 1
where a.ExternalFileID = @pExternalFileID
  and (a.HotelBrandID in (0,-10000) 
        or a.HotelBrandID is null)

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (Update #ExternalFileEUHotelFact.HotelBrandID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

update #ExternalFileEUHotelFact
 set GroupAccountID = c.GroupAccountID
from #ExternalFileEUHotelFact a 
   inner join
   dbo.vExternalFileEUHotel b on a.ExternalFileID = b.ExternalFileID and a.RecordKey = b.RecordKey
   inner join
   dbo.GroupAccountDim c on b.Comcode = c.Comcode

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUHotelFact look up GPID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


update #ExternalFileEUHotelFact
 set GroupAccountDepartmentID = isnull(d.GroupAccountDepartmentID, 0)
from #ExternalFileEUHotelFact a 
   inner join
   dbo.vExternalFileEUHotel b on a.ExternalFileID = b.ExternalFileID and a.RecordKey = b.RecordKey
   inner join
   dbo.GroupAccountDim c on b.Comcode = c.Comcode
   inner join    
   dbo.GroupAccountDepartmentDim d on c.GroupAccountID = d.GroupAccountID and b.CostCenterIDMain = d.MainCCValueID and d.CustomerSystemID = 2

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUHotelFact look up GADID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


update #ExternalFileEUHotelFact
 set TravelerGroupPolicyID = d.TravelerGroupPolicyID
from #ExternalFileEUHotelFact a 
   inner join
   dbo.vExternalFileEUHotel b on a.ExternalFileID = b.ExternalFileID and a.RecordKey = b.RecordKey
   inner join
   dbo.GroupAccountDim c on b.Comcode = c.Comcode
   inner join    
   dbo.TravelerGroupPolicyDim d on c.GroupAccountID = d.GroupAccountID and b.TravelerCategoryID = d.TravelerGroupPolicyID and d.CustomerSystemID = 2

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUHotelFact look up TGPID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


update #ExternalFileEUHotelFact
   set HotelChainID = coalesce(b.HotelChainID, -10000)
  from #ExternalFileEUHotelFact a 
       left join
       dbo.HotelChainHotelBrandDim b
           on a.HotelBrandID = b.HotelBrandID and a.IssueDate between b.EffectiveDateBegin and b.EffectiveDateEnd

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #ExternalFileEUHotelFact HotelChain)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


update #ExternalFileEUHotelFact
   set GeographyTypeID = coalesce(b.GeographyTypeID,c.GeographyTypeID)
  from #ExternalFileEUHotelFact a 
  left join GeographyTypeTravelProductCountry b on
      b.TravelProductID = a.TravelProductID and
      b.LineofBusinessID = 2 and 
      a.HotelCountryCode = b.CountryCode
  left join GeographyTypeTravelProductCountry c on
      c.TravelProductID = a.TravelProductID and
      c.LineofBusinessID = 2 and 
      c.CountryCode = ''


select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #ExternalFileEUHotelFact GeographyTypeID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

	
-- Set HotelRateTypeSupplyID for Billback/Private/Premier only, rest will be sourced from OMS booking messages
update a
   set a.HotelRateTypeSupplyID = case a.BookingSourceID 
        when 32 then        -- Atlantis Merchant
            case a.RateTypeID 
                when 6 then 9       -- RateTypeID 6-EPR to HotelRateTypeSupplyID 9-EPR Merchant
            else 7 end              -- RateTypeID 4 ESR, or any to HotelRateTypeSupplyID 7-ESR Merchant 
        when 33 then        -- Atlantis WSPN
            case a.RateTypeID 
                when 2 then 12      -- RateTypeID 2 Negotiated Rate to HotelRateTypeSupplyID 12 CNR WSPN Negotiated                 
            else 1 end              -- RateTypeID 1-Published Rate, or any to HotelRateTypeSupplyID 1-Worldspan Published
        when 36 then 6      -- Atlantis Direct Agency -> ESR Hotel Collect Published
        when 30 then 13     -- Private 
        when 31 then 14     -- Premier            
        when 37 then        -- EU Billback
            case a.RateTypeID 
                when 1 then 1       -- RateTypeID 1-Published Rate to HotelRateTypeSupplyID 1-Worldspan Published
                when 2 then 12      -- RateTypeID 2 Negotiated Rate to HotelRateTypeSupplyID 12 CNR WSPN Negotiated 
                when 4 then 7       -- RateTypeID 4 Expedia Special Rate to HotelRateTypeSupplyID 7 ESR Merchant 
            else null end 
        when 35 then 3      -- Atlantis Venere
        when 28 then        -- EAN Merchant
            case a.RateTypeID 
                    when 6 then 9       -- RateTypeID 6-EPR to HotelRateTypeSupplyID 9-EPR Merchant
                else 7 end              -- RateTypeID 4 ESR, or any to HotelRateTypeSupplyID 7-ESR Merchant 
        when 29 then        -- EAN Sabre
            case a.RateTypeID 
                when 2 then 12      -- RateTypeID 2 Negotiated Rate to HotelRateTypeSupplyID 12 CNR WSPN Negotiated                 
            else 1 end              -- RateTypeID 1-Published Rate, or any to HotelRateTypeSupplyID 1-Worldspan Published        
        else null end 
from #ExternalFileEUHotelFact a        


select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #ExternalFileEUHotelFact HotelRateTypeSupplyID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

------------------------------------------------------------
-- IRD/EU AgentID
------------------------------------------------------------
update #ExternalFileEUHotelFact
    set IsNumericBool = case
                            when isnumeric(MetaDossierID) = 0 then 0
                            when MetaDossierID like @NonNumericString then 0
                            when cast(MetaDossierID as numeric(38, 0)) not between -2147483648. and 2147483647. then 0
                            else 1
                        end

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update IsNumericBool #ExternalFileEUHotelFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

create index temp_ix2 on  #ExternalFileEUHotelFact (MetaDossierID,IsNumericBool)

select @Error = @@Error
if (@Error <> 0)
begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (index create on  #ExternalFileEUHotelFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

--EUTravelConsultant prepare
if object_id('tempdb..#EUTravelConsultant') is not null begin
    drop table #EUTravelConsultant
end

create table #EUTravelConsultant (
    dossier_id varchar(20),
    event_date smalldatetime,
    travel_consultant_id int,
    event_count int)

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (create table #EUTravelConsultant)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

--getting EUTravelConsultantID from event_log
insert into #EUTravelConsultant
    select 
        e.dossier_id,
        convert(smalldatetime,convert(varchar,e.event_date,23),120) as event_date,
        e.travel_consultant_id * -1 as travel_consultant_id,
        count(*) as event_count
    from dbo.event_log_eu_agentid e
    inner join dbo.event_action a
        on (e.action_id = a.id)
    where
        e.travel_consultant_id is not null and
        a.LABEL_CODE in ('HCCANCEL',
                         'TCCHANGED',
                         'TCHOTELMODIFIED',
                         'TCHOTELADDED',
                         'HCBOOK',
                         'IANHOTELCANCEL',
                         'IANHOTELDATECHANGE',
                         'IANHOTELPRICECHANGE',
                         'HCADD',
                         'CHECKOUT_INSERTION') and
        -- exclude website automation
        e.travel_consultant_id <> 504141 and
        exists (select * 
                from #ExternalFileEUHotelFact 
                where 
                    MetaDossierID = e.dossier_id and 
                    IsNumericBool = 1)
    group by 
        e.dossier_id,
        convert(smalldatetime,convert(varchar,e.event_date,23),120),
        e.travel_consultant_id

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #EUTravelConsultant)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

create index temp_ix2 on #EUTravelConsultant (dossier_id, event_date)

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (index create on #EUTravelConsultant)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

--#NavTC
if object_id('tempdb..#NavTC') is not null begin
    drop table #NavTC
end

select distinct
    otc.ID * -1 as travel_consultant_id
,   alm.LAST_NAME + ' ' + alm.FIRST_NAME as fld_value
into #NavTC
from dbo.[OPST_TRAVEL_CONSULTANT] otc
inner join dbo.[AGENT_LOGIN_MAPPING] alm
    on alm.ID = otc.ECT_USER_ID
        
select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #NavTC)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

create index temp_ix3 on #NavTC (fld_value)

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (index create on #NavTC)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end
--end EU travel consultant prepare

--updating EU travel consultant
update f
    set TUIDLogon = case f.AgentAssistedBool
                        when 1 then
                            case f.IsNumericBool
                                when 1 then
                                    coalesce(c.travel_consultant_id,coalesce(d.nav_tc,-500000)) 
                                else
                                    case
                                        when left(f.metadossierid,3) = 'NMD' then -500000
                                        when left(f.metadossierid,1) = 'M' then coalesce(d.nav_tc,-500000)
                                        else -500000
                                    end
                            end
                        else
                            f.TUIDLogon
                    end
   from #ExternalFileEUHotelFact f 
       outer apply (select top 1 travel_consultant_id
                    from #EUTravelConsultant
                    where 
                        dossier_id = f.MetaDossierID and
                        event_date <= f.IssueDate
                    order by event_date desc, event_count desc) c
       outer apply (select top 1 
                        a.fld_value, 
                        b.travel_consultant_id as nav_tc
                    from dbo.Nav_IMPORT_METAID_FIELD_VALUE a
                    inner join #NavTC b
                        on (a.fld_value = b.fld_value)
                    where 
                        a.METAID = f.metadossierid and 
                        a.travelproductid = f.travelproductid and 
                        a.fld_key = 'TRAVEL_CONSULTANT') d
                        
select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUHotelFact look up TravelConsultantID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end
-- end EU AgentID


select @TrancountSave = @@Trancount
if (@TrancountSave = 0) begin transaction @SavePointName
else                    save  transaction @SavePointName
select @TranStartedBool = @TRUE
 
    update dbo.ExternalFileEUHotelFact
       set 
        CustomerSystemID = b.CustomerSystemID,
        RecordKeyPrior = b.RecordKeyPrior,
        RecordKeyOriginal = b.RecordKeyOriginal, 
        TravelProductID = b.TravelProductID, 
        GroupAccountID = b.GroupAccountID,
        GroupAccountDepartmentID = isnull(b.GroupAccountDepartmentID, 0),
        TUIDAccount = b.TUIDAccount, 
        TUIDTraveler = b.TUIDTraveler, 
        TUIDLogon = b.TUIDLogon, 
        TUIDArranger = b.TUIDArranger, 
        MetaDossierID = b.MetaDossierID, 
        SalesDocumentTypeID = b.SalesDocumentTypeID, 
        SalesDocumentCode = b.SalesDocumentCode, 
        SalesDocumentCodePrior = b.SalesDocumentCodePrior, 
        IssueDate = b.IssueDate, 
        InvoiceDate = b.InvoiceDate, 
        TravelDateStart = b.TravelDateStart, 
        TravelDateEnd = b.TravelDateEnd, 
        TotalDaysCnt = b.TotalDaysCnt,
        EUHotelID = b.EUHotelID,
        HotelName = b.HotelName,
        HotelCityName = b.HotelCityName,
        HotelProvinceName = b.HotelProvinceName,
        HotelCountryCode = b.HotelCountryCode,
        RateTypeID = b.RateTypeID, 
        GeographyTypeID = b.GeographyTypeID,
        ConfirmationNbr = b.ConfirmationNbr, 
        EUServiceLevelID = b.EUServiceLevelID, 
        TravelerGroupPolicyID = b.TravelerGroupPolicyID,
        PolicyStatusID = b.PolicyStatusID, 
        OnlineBool = b.OnlineBool, 
        OfflineBookingTypeID = b.OfflineBookingTypeID, 
        AgentAssistedBool = b.AgentAssistedBool, 
        CurrencyCode = b.CurrencyCode, 
        CreditCardTypeID = b.CreditCardTypeID, 
        DirectPaymentBool = b.DirectPaymentBool, 
        CentralBillBool = b.CentralBillBool,
        CreditCardNbr = b.CreditCardNbr, 
        TransactionCnt = b.TransactionCnt, 
        IncrementCnt = b.IncrementCnt, 
        BookingAmtGross = b.BookingAmtGross, 
        BookingAmtBase = b.BookingAmtBase, 
        BookingAmtTax = b.BookingAmtTax, 
        BookingAmtVAT = b.BookingAmtVAT, 
        BookingAmtExtraPerson = b.BookingAmtExtraPerson, 
        BookingAmtFee = b.BookingAmtFee, 
        BookingAmtChangePenalty = b.BookingAmtChangePenalty, 
        CustomDataElementTxt = b.CustomDataElementTxt, 
        ApprovalDate = b.ApprovalDate, 
        TUIDApprover = b.TUIDApprover, 
        ApprovalTxt = b.ApprovalTxt,
        BookingSourceID = b.BookingSourceID,
        HotelRatingNbr = b.HotelRatingNbr,
        HotelID = b.HotelID,
        HotelBrandID = b.HotelBrandID,
        HotelChainID = b.HotelChainID,
		PolicyReasonCodeID = b.PolicyReasonCodeID,
		ItineraryTxt = b.ItineraryTxt,
		TRL = b.TRL,
		InvoiceSource = b.InvoiceSource, 
        UpdateDate = @Current_TimeStamp, 
        HotelRateTypeSupplyID = b.HotelRateTypeSupplyID, 
        LastUpdatedBy = 'ExtFileEUHotelFactLoadExpand'
      from dbo.ExternalFileEUHotelFact a 
           inner join
           #ExternalFileEUHotelFact b on a.ExternalFileID = b.ExternalFileID and a.RecordKey = b.RecordKey

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update ExternalFileEUHotelFact expansion)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

if ((@TrancountSave = 0) and (@TranStartedBool = @TRUE))
    commit transaction @SavePointName

goto ExitProc

---------------------------------------------------------------------
-- Error Handler
---------------------------------------------------------------------
ErrorHandler:
    if (@TranStartedBool = @TRUE) rollback transaction @SavePointName
    select   @ExitCode = @RC_FAILURE
    goto ExitProc

---------------------------------------------------------------------
-- Exit Procedure
---------------------------------------------------------------------
ExitProc:
    return (@ExitCode)
go
 
/*
    select top 100 * 
    from ExternalFileEUHotelFact
    where ExternalFileID = 22253
    order by 1 desc
    
    delete
    from ExternalFileEUHotelFact
    where ExternalFileID = 16775

    exec dbo.ExternalFileEUHotelFactLoadInitial 22253
    exec dbo.ExternalFileEUHotelFactLoadExpand 22253

    select hotelbrandid,hotelchainid , bookingsourceid,count(1)
    from ExternalFileEUHotelFact
    where ExternalFileID = 16775
    group by hotelbrandid,hotelchainid , bookingsourceid

    select  *
    from ExternalFileEUHotelFact where bookingsourceid = 37
    
    select top 100 *
    from ExternalFile where ExternalFileTypeID = 10	
    order by 1 desc 
    
*/