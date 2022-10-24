if object_id('dbo.ExternalFileEUAirFactLoadExpand') is null begin
    print 'Creating stored procedure ExternalFileEUAirFactLoadExpand (placeholder)'
    execute('create procedure dbo.ExternalFileEUAirFactLoadExpand as return 0')
end
go

print 'Altering stored procedure ExternalFileEUAirFactLoadExpand'
go

alter procedure dbo.ExternalFileEUAirFactLoadExpand 
    @pExternalFileID int
with recompile
as

/*
*********************************************************************
Copyright (C) 2006-2018 Expedia, Inc. All rights reserved.

Description:
     Updates ExternalFileEUAirFact with most attributes

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2006-07-03  BarryC          Created.
    2007-01-08  BarryC          Integrate RecordKeyPrior, RecordKeyOriginal
    2012-12-03  VBoerner        Add exchange rate handling for PL/CZ TPIDs
    2013-06-17  Patrick Bressan Added PolicyReasonCodeID column
    2013-10-14  DMurugesan      Setting GeographyTypeID to NULL for override in ExternalFileEUAirFactLoadDerived
    2013-11-04  VBoerner        Added SAS TP* AirFareTypeIDs
    2014-02-18  DMurugesan      EGE-53068 AdvancePurchaseDaysCnt set to TravelStartDate-IssueDate for Exchange Tickets
    2014-02-28  DMurugesan      Set AdvancePurchaseDaysCnt = -1 for all negative values.
    2014-04-08  Ramesh Karnati  Linking exchange penalty amounts with exchange itself - EGE-64393
    2014-08-26  jappleberry     EGE-71173 AdvancePurchaseDaysCnt set to TravelStartDate-coalesce(BookingDate,IssueDate)for Exchange Tickets
                                 and reclac AdvancePurchaseDaysCnt when BookingDate > IssueDate
    2016-12-05  jappleberry     EGE-19143 set null GroupAccountDepartmentID to 0
    2018-02-28  jappleberry     EGACP-1954 set AdvancePurchaseID = 99 when AdvancePurchaseDaysCnt < 0
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
    @TrancountSave                  int

declare   -- Error message constants
    @ERRUNEXPECTED                  int,
    @ERRPARAMETER                   int,
    @MsgParm1                       varchar(100),
    @MsgParm2                       varchar(100),
    @MsgParm3                       varchar(100)

declare
    @BookingTypeIDPurchase                tinyint,
    @BookingTypeIDReserve                 tinyint,
    @BookingTypeIDVoid                    tinyint,
    @BookingTypeIDExchange                tinyint,
    @BookingTypeIDRefund                  tinyint,
    @BookingTypeIDPRefund                 tinyint,
    @BookingTypeIDUTT                     tinyint,
    @BookingTypeIDVoidRefund              tinyint,
    @ExchangeRateNull                     money

---------------------------------------------------------------------
-- Initializations
---------------------------------------------------------------------

select   -- Standard constants
    @FALSE                          = 0,
    @TRUE                           = 1,
    @RC_FAILURE                     = -100,
    @RC_SUCCESS                     = 0

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

select *
  into #ExternalFileEUAirFact
  from dbo.ExternalFileEUAirFact
 where ExternalFileID = @pExternalFileID

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #ExternalFileEUAirFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

create index temp_ix on #ExternalFileEUAirFact (RecordKey, ExternalFileID, BookingTypeID)

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (index create on #ExternalFileEUAirFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

update #ExternalFileEUAirFact
   set
    CustomerSystemID = 2, 
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
    IATACode = ltrim(rtrim(b.IATACode)), 
    PNRCode = ltrim(rtrim(b.PNRCode)), 
    TicketCode = ltrim(rtrim(b.TicketCode)), 
    TicketCodePrior = coalesce(ltrim(rtrim(b.TicketCodePrior)),''), 
    TicketCodeEnding = coalesce(ltrim(rtrim(b.TicketCodeEnding)),''), 
    IssueDate = b.IssueDate, 
    InvoiceDate = b.InvoiceDate, 
    BillingSystemCreateDate = b.BillingSystemCreateDate, 
    BookingDate = b.BookingDate, 
    TravelDateStart = b.TravelDateStart, 
    TravelDateEnd = b.TravelDateEnd, 
    AirlineCode = ltrim(rtrim(b.AirlineCode)), 
    ConsolidatorCode = ltrim(rtrim(b.ConsolidatorCode)), 
    RouteTxt = b.RouteTxt, 
    SegmentCnt = b.SegmentCnt, 
    FlightCnt = b.FlightCnt, 
    AirFareBasisCode = ltrim(rtrim(b.AirFareBasisCode)),
    ClassOfServiceCode = b.ClassOfServiceCode, 
    CabinClassID = b.CabinClassID, 
    AirFareTypeID = case when b.BusinessSubCategoryID = 12 then 101 --TPC
                         when b.BusinessSubCategoryID = 14 then 102 --TPP
                         when b.BusinessSubCategoryID = 16 then 103 --TMP
                         when b.BusinessSubCategoryID = 13 then 104 --TPU
                         when b.BusinessSubCategoryID = 15 then 105 --TMU
                         else b.AirFareTypeID end, 
    TripTypeID = b.TripTypeID, 
    GeographyTypeID =  null,
    WithinCountryBool = b.WithinCountryBool, 
    SaturdayNightStayBool = b.SaturdayNightStayBool, 
    EUServiceLevelID = b.ServiceLevelID, 
    PolicyStatusID = b.PolicyStatusID, 
    OnlineBool = b.OnlineBool, 
    OfflineBookingTypeID = b.OfflineBookingTypeID, 
    AgentAssistedBool = b.AgentAssistedBool, 
    AdvancePurchaseDaysCnt = case when a.BookingTypeID = @BookingTypeIDPurchase and ( datediff(dd,coalesce(b.BookingDate,b.IssueDate),b.TravelDateStart) < -365 
                                      or datediff(dd,coalesce(b.BookingDate,b.IssueDate),b.TravelDateStart) > 365) then 0 
                                  when a.BookingTypeID = @BookingTypeIDPurchase and datediff(dd,b.BookingDate,b.IssueDate) < 0
                                      then datediff(dd,b.IssueDate,b.TravelDateStart)
                                  when a.BookingTypeID = @BookingTypeIDPurchase 
                                      then datediff(dd,coalesce(b.BookingDate,b.IssueDate),b.TravelDateStart) 
                                  when a.BookingTypeID = @BookingTypeIDExchange and datediff(dd,b.BookingDate,b.IssueDate) < 0 and datediff(dd,b.IssueDate,b.TravelDateStart) < 0 
                                      then -1  
                                  when a.BookingTypeID = @BookingTypeIDExchange and datediff(dd,b.BookingDate,b.IssueDate) < 0
                                      then datediff(dd,b.IssueDate,b.TravelDateStart) 
                                  when a.BookingTypeID = @BookingTypeIDExchange 
                                      then datediff(dd,coalesce(b.BookingDate,b.IssueDate),b.TravelDateStart) 
                                  else b.AdvancePurchaseDaysCnt end, 
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
            when a.BookingTypeID = @BookingTypeIDExchange then 0
            when a.BookingTypeID = @BookingTypeIDVoid then -1
            when a.BookingTypeID = @BookingTypeIDRefund then -1
            when a.BookingTypeID = @BookingTypeIDPRefund then 0 
            when a.BookingTypeID = @BookingTypeIDVoidRefund then 1 end, 
    TicketAmt = b.TicketAmt * coalesce(d.ExchangeRateUsed, @ExchangeRateNull), 
    TicketAmtBase = b.TicketAmtBase * coalesce(d.ExchangeRateUsed, @ExchangeRateNull), 
    TicketAmtTax = (b.TicketAmtTax + b.TicketAmtVAT) * coalesce(d.ExchangeRateUsed, @ExchangeRateNull), 
    TicketAmtTaxVAT = b.TicketAmtVAT * coalesce(d.ExchangeRateUsed, @ExchangeRateNull), 
    TicketAmtExchangePenalty = b.TicketAmtPenalty * coalesce(d.ExchangeRateUsed, @ExchangeRateNull), 
    TicketAmtCommission = b.TicketAmtCommission * coalesce(d.ExchangeRateUsed, @ExchangeRateNull), 
    TicketAmtMarkup = b.TicketAmtMarkup * coalesce(d.ExchangeRateUsed, @ExchangeRateNull), 
    TicketAmtGross = b.TicketAmtGross * coalesce(d.ExchangeRateUsed, @ExchangeRateNull), 
    NetPricedFareBool = b.NetPricedFareBool, 
    TicketTypeID = b.TicketTypeID, 
    ETicketRefusedBool = b.ETicketRefusedBool, 
    GroupTicketBool = b.GroupTicketBool, 
    ApprovalDate = b.ApprovalDate, 
    TUIDApprover = b.PercodeApprover, 
    ApprovalTxt = ltrim(rtrim(b.ApprovalTxt)), 
    AgentErrorBool = b.ECTErrorBool, 
    CustomDataElementTxt = coalesce(ltrim(rtrim(b.CostCenterTxt)),''), 
    LowFareChosenBool = case when b.LowFareChoiceTypeID = 1 then 1
                             when b.LowFareChoiceTypeID = 0 then 0
                             else null end,
    SavingsReasonCode = ltrim(rtrim(b.SavingsReasonCode)), 
    FareAmtLowestInPolicy = b.FareAmtLowestInPolicy * coalesce(d.ExchangeRateUsed, @ExchangeRateNull), 
    AirlineCodeLowestInPolicy = ltrim(rtrim(b.AirlineCodeLowestInPolicy)), 
    ClassOfServiceCodeLowestInPolicy = ltrim(rtrim(b.ClassOfServiceCodeLowestInPolicy)), 
    CabinClassIDLowestInPolicy = b.CabinClassIDLowestInPolicy, 
    AirFareTypeIDLowestInPolicy = b.AirFareTypeIDLowestInPolicy, 
    AirFareBasisCodeLowestInPolicy = ltrim(rtrim(b.AirFareBasisCodeLowestInPolicy)), 
    FareAmtPublished = b.FareAmtPublished * coalesce(d.ExchangeRateUsed, @ExchangeRateNull), 
    AirFareBasisCodePublished = ltrim(rtrim(b.AirFareBasisCodePublished)), 
    ClassOfServiceCodePublished = ltrim(rtrim(b.ClassOfServiceCodePublished)), 
    CabinClassIDPublished = b.CabinClassIDPublished, 
    FareAmtNegotiated = b.FareAmtNegotiated * coalesce(d.ExchangeRateUsed, @ExchangeRateNull), 
    AirFareBasisCodeNegotiated = ltrim(rtrim(b.AirFareBasisCodeNegotiated)), 
    CabinClassIDNegotiated = b.CabinClassIDNegotiated,
    RecordKeyPrior = b.RecordKeyPrior,
    RecordKeyOriginal = b.RecordKeyOriginal,
	PolicyReasonCodeID = b.PolicyReasonCodeID
  from #ExternalFileEUAirFact a 
       inner join
       dbo.ExternalFileEUAirTicket b on a.ExternalFileID = b.ExternalFileID and a.RecordKey = b.RecordKey
       inner join 
       dbo.TravelProductDim c on b.TravelProductID = c.TravelProductID
       left join
       dbo.ExchangeRateDailyFull d on dbo.TimeIDFromDate(b.InvoiceDate) = d.TimeID and
            b.CurrencyCode = d.FromCurrencyCode and 
            c.CurrencyCodeStorage = d.ToCurrencyCode

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUAirFact expand)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

    -- Linking exchange penalty amounts with exchange itself - EGE-64393
    update a 
        set 
         a.TicketAmtGross = a.TicketAmtGross + e.TicketAmtGross, 
         a.TicketAmtExchangePenalty = e.TicketAmtGross
    from #ExternalFileEUAirFact a 
        inner join 
        dbo.ExternalFileEUAirTicket b on a.ExternalFileID = b.ExternalFileID and a.RecordKey = b.RecordKey
        inner join 
        (select 
            b2.MetaDossierID, 
            b2.PerCodeAccount, 
            b2.TicketCodePrior, 
            coalesce(b2.TicketAmtGross, 0) * coalesce(d.ExchangeRateUsed, @ExchangeRateNull) TicketAmtGross, 
            row_number() over(partition by b2.MetaDossierID, b2.PerCodeAccount, b2.TicketCodePrior order by b2.RecordKey) PenaltyRank
        from dbo.ExternalFileEUAirTicket b2 
            inner join 
            dbo.TravelProductDim c on b2.TravelProductID = c.TravelProductID 
            left join
            dbo.ExchangeRateDailyFull d on dbo.TimeIDFromDate(b2.InvoiceDate) = d.TimeID and b2.CurrencyCode = d.FromCurrencyCode 
                and c.CurrencyCodeStorage = d.ToCurrencyCode                
        where 
            b2.ExternalFileID = @pExternalFileID
            and b2.BusinessCategoryID = 6        -- Ancillary Service
            and b2.ExternalRecordStatusID = 9    -- Air Exchange Penalty (based on BusinessSubCategoryID in the sproc ExternalFileDataVldEUAir_) 
            and b2.BookingTypeID = 1             -- Ancillary Service Purchase
            and isnull(b2.TicketAmtGross, 0) > 0    -- Exchange Penalty Gross > 0 
        ) e on b.MetaDossierID = e.MetaDossierID and b.PerCodeAccount = e.PerCodeAccount and b.TicketCode = e.TicketCodePrior
    where 
        b.ExternalFileID = @pExternalFileID
        and b.BookingTypeID = 5         -- Exchange 
        and e.PenaltyRank = 1 

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (Linking exchange penalty amounts with exchange itself)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end
    
update #ExternalFileEUAirFact
 set GroupAccountID = c.GroupAccountID
from #ExternalFileEUAirFact a 
   inner join
   dbo.ExternalFileEUAirTicket b on a.ExternalFileID = b.ExternalFileID and a.RecordKey = b.RecordKey
   inner join
   dbo.GroupAccountDim c on b.Comcode = c.Comcode

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUAirFact look up GPID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


update #ExternalFileEUAirFact
 set GroupAccountDepartmentID = isnull(d.GroupAccountDepartmentID, 0)
from #ExternalFileEUAirFact a 
   inner join
   dbo.ExternalFileEUAirTicket b on a.ExternalFileID = b.ExternalFileID and a.RecordKey = b.RecordKey
   inner join
   dbo.GroupAccountDim c on b.Comcode = c.Comcode
   inner join    
   dbo.GroupAccountDepartmentDim d on c.GroupAccountID = d.GroupAccountID and b.CostCenterIDMain = d.MainCCValueID and d.CustomerSystemID = 2

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUAirFact look up GADID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


update #ExternalFileEUAirFact
 set TravelerGroupPolicyID = d.TravelerGroupPolicyID
from #ExternalFileEUAirFact a 
   inner join
   dbo.ExternalFileEUAirTicket b on a.ExternalFileID = b.ExternalFileID and a.RecordKey = b.RecordKey
   inner join
   dbo.GroupAccountDim c on b.Comcode = c.Comcode
   inner join    
   dbo.TravelerGroupPolicyDim d on c.GroupAccountID = d.GroupAccountID and b.TravelerCategoryID = d.TravelerGroupPolicyID and d.CustomerSystemID = 2

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUAirFact look up TGPID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

update #ExternalFileEUAirFact
    set AdvancePurchaseID = case when a.AdvancePurchaseDaysCnt is null then -1
                                 when a.AdvancePurchaseDaysCnt < 0 then 99
                                 else coalesce(b.AdvancePurchaseID, -1)
                             end 
from #ExternalFileEUAirFact a 
         left join
     dbo.AdvancePurchaseDim b 
        on a.AdvancePurchaseDaysCnt between b.StartDay and b.EndDay and
           b.LangID = 1033

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUAirFact look up APID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

select @TrancountSave = @@Trancount
if (@TrancountSave = 0) begin transaction @SavePointName
else                    save  transaction @SavePointName
select @TranStartedBool = @TRUE

    update dbo.ExternalFileEUAirFact
       set 
        CustomerSystemID = b.CustomerSystemID, 
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
        IATACode = b.IATACode, 
        PNRCode = b.PNRCode, 
        TicketCode = b.TicketCode, 
        TicketCodePrior = b.TicketCodePrior, 
        TicketCodeEnding = b.TicketCodeEnding, 
        IssueDate = b.IssueDate, 
        InvoiceDate = b.InvoiceDate, 
        BillingSystemCreateDate = b.BillingSystemCreateDate, 
        BookingDate = b.BookingDate, 
        TravelDateStart = b.TravelDateStart, 
        TravelDateEnd = b.TravelDateEnd, 
        AirlineCode = b.AirlineCode, 
        ConsolidatorCode = b.ConsolidatorCode, 
        RouteTxt = b.RouteTxt, 
        SegmentCnt = b.SegmentCnt, 
        FlightCnt = b.FlightCnt, 
        AirFareBasisCode = b.AirFareBasisCode,
        ClassOfServiceCode = b.ClassOfServiceCode, 
        CabinClassID = b.CabinClassID, 
        AirFareTypeID = b.AirFareTypeID, 
        TripTypeID = b.TripTypeID, 
        GeographyTypeID = b.GeographyTypeID, 
        WithinCountryBool = b.WithinCountryBool, 
        SaturdayNightStayBool = b.SaturdayNightStayBool, 
        EUServiceLevelID = b.EUServiceLevelID, 
        TravelerGroupPolicyID = b.TravelerGroupPolicyID, 
        PolicyStatusID = b.PolicyStatusID, 
        OnlineBool = b.OnlineBool, 
        OfflineBookingTypeID = b.OfflineBookingTypeID, 
        AgentAssistedBool = b.AgentAssistedBool, 
        AdvancePurchaseDaysCnt = b.AdvancePurchaseDaysCnt, 
        AdvancePurchaseID = b.AdvancePurchaseID, 
        CurrencyCode = b.CurrencyCode, 
        CreditCardTypeID = b.CreditCardTypeID, 
        DirectPaymentBool = b.DirectPaymentBool, 
        CentralBillBool = b.CentralBillBool, 
        CreditCardNbr = b.CreditCardNbr, 
        TransactionCnt = b.TransactionCnt, 
        IncrementCnt = b.IncrementCnt, 
        TicketAmt = b.TicketAmt, 
        TicketAmtBase = b.TicketAmtBase, 
        TicketAmtTax = b.TicketAmtTax, 
        TicketAmtTaxVAT = b.TicketAmtTaxVAT, 
        TicketAmtExchangePenalty = b.TicketAmtExchangePenalty, 
        TicketAmtCommission = b.TicketAmtCommission, 
        TicketAmtMarkup = b.TicketAmtMarkup, 
        TicketAmtGross = b.TicketAmtGross, 
        NetPricedFareBool = b.NetPricedFareBool, 
        TicketTypeID = b.TicketTypeID, 
        ETicketRefusedBool = b.ETicketRefusedBool, 
        GroupTicketBool = b.GroupTicketBool, 
        ApprovalDate = b.ApprovalDate, 
        TUIDApprover = b.TUIDApprover, 
        ApprovalTxt = b.ApprovalTxt, 
        AgentErrorBool = b.AgentErrorBool, 
        CustomDataElementTxt = b.CustomDataElementTxt, 
        LowFareChosenBool = b.LowFareChosenBool, 
        SavingsReasonCode = b.SavingsReasonCode, 
        FareAmtLowestInPolicy = b.FareAmtLowestInPolicy, 
        AirlineCodeLowestInPolicy = b.AirlineCodeLowestInPolicy, 
        ClassOfServiceCodeLowestInPolicy = b.ClassOfServiceCodeLowestInPolicy, 
        CabinClassIDLowestInPolicy = b.CabinClassIDLowestInPolicy, 
        AirFareTypeIDLowestInPolicy = b.AirFareTypeIDLowestInPolicy, 
        AirFareBasisCodeLowestInPolicy = b.AirFareBasisCodeLowestInPolicy, 
        FareAmtPublished = b.FareAmtPublished, 
        AirFareBasisCodePublished = b.AirFareBasisCodePublished, 
        ClassOfServiceCodePublished = b.ClassOfServiceCodePublished, 
        CabinClassIDPublished = b.CabinClassIDPublished, 
        FareAmtNegotiated = b.FareAmtNegotiated, 
        AirFareBasisCodeNegotiated = b.AirFareBasisCodeNegotiated, 
        CabinClassIDNegotiated = b.CabinClassIDNegotiated,
        UpdateDate = @Current_TimeStamp,
        LastUpdatedBy = 'EFEUAirFactLoadExpand',
        RecordKeyPrior = b.RecordKeyPrior,
        RecordKeyOriginal = b.RecordKeyOriginal,
		PolicyReasonCodeID = b.PolicyReasonCodeID
      from dbo.ExternalFileEUAirFact a 
           inner join
           #ExternalFileEUAirFact b on a.ExternalFileID = b.ExternalFileID and a.RecordKey = b.RecordKey

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update ExternalFileEUAirFact expansion)'
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
exec dbo.ExternalFileEUAirFactLoadExpand 25361
rollback transaction
*/
