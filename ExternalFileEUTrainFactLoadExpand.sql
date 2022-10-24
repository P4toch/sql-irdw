if object_id('dbo.ExternalFileEUTrainFactLoadExpand') is null begin
    print 'Creating stored procedure ExternalFileEUTrainFactLoadExpand (placeholder)'
    execute('create procedure dbo.ExternalFileEUTrainFactLoadExpand as return 0')
end
go

print 'Altering stored procedure ExternalFileEUTrainFactLoadExpand'
go

alter procedure dbo.ExternalFileEUTrainFactLoadExpand 
    @pExternalFileID int
with recompile
as

/*
*********************************************************************
Copyright (C) 2006-2018 Expedia, Inc. All rights reserved.

Description:
     Updates ExternalFileEUTrainFact with most attributes

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
    2013-10-14  DMurugesan      Setting GeographyTypeID to NULL for override in ExternalFileEUTrainFactLoadDerived
    2014-02-18  DMurugesan      EGE-53068 AdvancePurchaseDaysCnt set to TravelStartDate-IssueDate for Exchange Tickets
    2014-28-02  DMurugesan      Set AdvancePurchaseDaysCnt = -1 for all negative values.
    2014-08-26  jappleberry     EGE-71173 AdvancePurchaseDaysCnt set to TravelStartDate-coalesce(BookingDate,IssueDate)for Exchange Tickets
                                 and reclac AdvancePurchaseDaysCnt when BookingDate > IssueDate
    2016-12-05  jappleberry     EGE-19143 set null GroupAccountDepartmentID to 0
    2018-02-28  jappleberry     EGACP-1954 set AdvancePurchaseID = 99 when AdvancePurchaseDaysCnt < 0
    2018-03-15  pbressan        EGE-181127: Add CreditCardNbr infos for EU transactions
    2018-03-27  pbressan        EGE-190394: PolicyReasonCodeID is null if we backfill
    2018-04-10  nrasmussen      EGE-189992 IRD Enrich an agent assisted transaction with Travel Consultant TUID
    2018-05-16  nrasmussen      EGE-195465 IRD - replaced table event_log with event_log_eu_agentid 
    2018-06-07  nrasmussen      EGE-200489 IRD moving eu agentid section due to dataflow issues
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
    @ExchangeRateNull                     money,
    @NonNumericString                     varchar(10)

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
    @ExchangeRateNull                     = 0.0,
    @NonNumericString                     = '%[^0-9]%'

---------------------------------------------------------------------
-- Validation
---------------------------------------------------------------------
-- None, done by caller
---------------------------------------------------------------------
-- Processing
---------------------------------------------------------------------

-- create table
select *
into #ExternalFileEUTrainFact
from dbo.ExternalFileEUTrainFact
where 1 = 2

select @Error = @@Error
if (@Error <> 0)
begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (create #ExternalFileEUTrainFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

-- ensure datatype consistency for metadossierid
alter table #ExternalFileEUTrainFact alter column metadossierid varchar(20)

select @Error = @@Error
if (@Error <> 0)
begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (alter #ExternalFileEUTrainFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


insert into #ExternalFileEUTrainFact
select *
  from dbo.ExternalFileEUTrainFact
 where ExternalFileID = @pExternalFileID

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #ExternalFileEUTrainFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

create index temp_ix1 on #ExternalFileEUTrainFact (RecordKey, ExternalFileID, BookingTypeID, MetaDossierID)

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (index create on #ExternalFileEUFeeFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


update #ExternalFileEUTrainFact
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
    IssueDate = b.IssueDate, 
    InvoiceDate = b.InvoiceDate, 
    BillingSystemCreateDate = b.BillingSystemCreateDate, 
    BookingDate = b.BookingDate, 
    TravelDateStart = b.TravelDateStart, 
    TravelDateEnd = b.TravelDateEnd, 
    TrainVendorCode = ltrim(rtrim(b.TrainVendorCode)), 
    RouteTxt = b.RouteTxt, 
    SegmentCnt = b.SegmentCnt, 
    TripCnt = b.TripCnt, 
    ClassOfServiceCode = b.ClassOfServiceCode, 
    CabinClassID = b.CabinClassID, 
    TrainFareTypeID = b.TrainFareTypeID, 
    TripTypeID = b.TripTypeID, 
    GeographyTypeID = null, 
    WithinCountryBool = b.WithinCountryBool, 
    SaturdayNightStayBool = b.SaturdayNightStayBool, 
    EUServiceLevelID = b.ServiceLevelID, 
    PolicyStatusID = b.PolicyStatusID, 
    OnlineBool = b.OnlineBool, 
    OfflineBookingTypeID = b.OfflineBookingTypeID, 
    AgentAssistedBool = b.AgentAssistedBool, 
    AdvancePurchaseDaysCnt = case when a.BookingTypeID = @BookingTypeIDPurchase and ( datediff(dd,coalesce(b.BookingDate,b.IssueDate),b.TravelDateStart) < -365 
                                      or datediff(dd,coalesce(a.BookingDate,a.IssueDate),a.TravelDateStart) > 365) then 0 
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
    CreditCardNbr = null, 
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
    RecordKeyPrior = b.RecordKeyPrior,
    RecordKeyOriginal = b.RecordKeyOriginal,
	PolicyReasonCodeID = b.PolicyReasonCodeID,
    CreditCardNbrBegin = left(b.CreditCardNbr, 6),
    CreditCardNbrEnd = right(b.CreditCardNbr, 4)
  from #ExternalFileEUTrainFact a 
       inner join
       dbo.ExternalFileEUTrainTicket b on a.ExternalFileID = b.ExternalFileID and a.RecordKey = b.RecordKey
       inner join 
       dbo.TravelProductDim c on b.TravelProductID = c.TravelProductID
       left join
       dbo.ExchangeRateDailyFull d on dbo.TimeIDFromDate(b.InvoiceDate) = d.TimeID and
            b.CurrencyCode = d.FromCurrencyCode and 
            c.CurrencyCodeStorage = d.ToCurrencyCode

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUTrainFact expand)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


update #ExternalFileEUTrainFact
 set GroupAccountID = c.GroupAccountID
from #ExternalFileEUTrainFact a 
   inner join
   dbo.ExternalFileEUTrainTicket b on a.ExternalFileID = b.ExternalFileID and a.RecordKey = b.RecordKey
   inner join
   dbo.GroupAccountDim c on b.Comcode = c.Comcode

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUTrainFact look up GPID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


update #ExternalFileEUTrainFact
 set GroupAccountDepartmentID = isnull(d.GroupAccountDepartmentID, 0)
from #ExternalFileEUTrainFact a 
   inner join
   dbo.ExternalFileEUTrainTicket b on a.ExternalFileID = b.ExternalFileID and a.RecordKey = b.RecordKey
   inner join
   dbo.GroupAccountDim c on b.Comcode = c.Comcode
   inner join    
   dbo.GroupAccountDepartmentDim d on c.GroupAccountID = d.GroupAccountID and b.CostCenterIDMain = d.MainCCValueID and d.CustomerSystemID = 2

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUTrainFact look up GADID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


update #ExternalFileEUTrainFact
 set TravelerGroupPolicyID = d.TravelerGroupPolicyID
from #ExternalFileEUTrainFact a 
   inner join
   dbo.ExternalFileEUTrainTicket b on a.ExternalFileID = b.ExternalFileID and a.RecordKey = b.RecordKey
   inner join
   dbo.GroupAccountDim c on b.Comcode = c.Comcode
   inner join    
   dbo.TravelerGroupPolicyDim d on c.GroupAccountID = d.GroupAccountID and b.TravelerCategoryID = d.TravelerGroupPolicyID and d.CustomerSystemID = 2

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUTrainFact look up TGPID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


update #ExternalFileEUTrainFact
    set AdvancePurchaseID = case when a.AdvancePurchaseDaysCnt is null then -1
                                 when a.AdvancePurchaseDaysCnt < 0 then 99
                                 else coalesce(b.AdvancePurchaseID, -1)
                             end 
from #ExternalFileEUTrainFact a 
         left join
     dbo.AdvancePurchaseDim b 
        on a.AdvancePurchaseDaysCnt between b.StartDay and b.EndDay and
           b.LangID = 1033

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUTrainFact look up APID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


update #ExternalFileEUTrainFact
    set PolicyReasonCodeID = (select top 1 cast(SubQ.ReasonsTxt as int) as PolicyReasonCodeID
                                    from dbo.Nav_workflow_out_of_policy_reasons SubQ
                                           with (index(ixNav_workflow_out_of_policy_reasons_MDCode))
                                   where SubQ.ReasonType = 'WHY_CHOSEN_AS_OOP_ID' and
                                         SubQ.MdCode = a.MetaDossierID and
                                         SubQ.ServiceTypeID = 2 and
                                         SubQ.ReasonsTxt not like @NonNumericString and
                                         SubQ.PNR = a.PNRCode
                                   order by SubQ.InternalExtractID desc) 
from #ExternalFileEUTrainFact a
where PolicyStatusID = 2 -- Non compliant
and PolicyReasonCodeID is null

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUTrainFact look up PolicyReasonCodeID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

------------------------------------------------------------
-- IRD/EU AgentID
------------------------------------------------------------
alter table #ExternalFileEUTrainFact add IsNumericBool int default 0

update #ExternalFileEUTrainFact
    set IsNumericBool = case
                            when isnumeric(MetaDossierID) = 0 then 0
                            when MetaDossierID like @NonNumericString then 0
                            when cast(MetaDossierID as numeric(38, 0)) not between -2147483648. and 2147483647. then 0
                            else 1
                        end

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update IsNumericBool #ExternalFileEUTrainFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

create index temp_ix on  #ExternalFileEUTrainFact (MetaDossierID,IsNumericBool)

select @Error = @@Error
if (@Error <> 0)
begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (index create on  #ExternalFileEUTrainFact)'
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
        a.LABEL_CODE in ('BOOK',
                         'CANCEL',
                         'TRAINBOOK',
                         'TRAINCANCEL',
                         'TCCHANGED',
                         'WAVEMODIF',
                         'TCAIRMODIFIED',
                         'TCCARMODIFIED',
                         'TCHOTELMODIFIED',
                         'TCAIRDELETED',
                         'TCAIRADDED',
                         'TCCARADDED',
                         'TCHOTELADDED',
                         'TCAIRPRICEMODIFIED',
                         'HCBOOK',
                         'IANHOTELCANCEL',
                         'IANHOTELDATECHANGE',
                         'IANHOTELPRICECHANGE',
                         'AIRINSERTION',
                         'TRAININSERTION',
                         'CARINSERTION',
                         'CHARGEMODIF',
                         'CHARGEMODIFCANCEL',
                         'TRAINUPDATE',
                         'TRAINDELETE',
                         'TRAINREFUND',
                         'TRAINEXCHANGE',
                         'TRAVREMOVED',
                         'HCADD',
                         'TRAINADD',
                         'RCARADD',
                         'WAVEADD',
                         'BOOKING_NEW',
                         'BOOKING_NEW_TC_CHECK',
                         'BOOKING_UPDATED',
                         'BOOKING_CANCELLED',
                         'BOOKING_EXCHANGED',
                         'CARBOOK',
                         'CARCANCEL',
                         'CHECKOUT_INSERTION',
                         'GROUNDADD',
                         'GROUNDUPDATE',
                         'GROUNDCANCEL') and
        -- exclude website automation 
		e.travel_consultant_id <> 504141 and
        exists (select * 
                from #ExternalFileEUTrainFact 
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
   from #ExternalFileEUTrainFact f 
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
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUTrainFact look up TravelConsultantID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end
-- end EU AgentID



select @TrancountSave = @@Trancount
if (@TrancountSave = 0) begin transaction @SavePointName
else                    save  transaction @SavePointName
select @TranStartedBool = @TRUE


    update dbo.ExternalFileEUTrainFact
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
        IssueDate = b.IssueDate, 
        InvoiceDate = b.InvoiceDate, 
        BillingSystemCreateDate = b.BillingSystemCreateDate, 
        BookingDate = b.BookingDate, 
        TravelDateStart = b.TravelDateStart, 
        TravelDateEnd = b.TravelDateEnd, 
        TrainVendorCode = b.TrainVendorCode, 
        RouteTxt = b.RouteTxt, 
        SegmentCnt = b.SegmentCnt, 
        TripCnt = b.TripCnt, 
        ClassOfServiceCode = b.ClassOfServiceCode, 
        CabinClassID = b.CabinClassID, 
        TrainFareTypeID = b.TrainFareTypeID, 
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
        UpdateDate = @Current_TimeStamp,
        LastUpdatedBy = 'EFEUTrainFactLoadExpand',
        RecordKeyPrior = b.RecordKeyPrior,
        RecordKeyOriginal = b.RecordKeyOriginal,
		PolicyReasonCodeID = b.PolicyReasonCodeID,
        CreditCardNbrBegin = b.CreditCardNbrBegin,
        CreditCardNbrEnd = b.CreditCardNbrEnd
      from dbo.ExternalFileEUTrainFact a 
           inner join
           #ExternalFileEUTrainFact b on a.ExternalFileID = b.ExternalFileID and a.RecordKey = b.RecordKey

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update ExternalFileEUTrainFact expansion)'
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
exec dbo.ExternalFileEUTrainFactLoadExpand 25366
Rollback transaction
BookingDate = case when b.BookingDate is null then b.IssueDate 
                   when datediff(dd,b.BookingDate,b.IssueDate) < 0 then b.IssueDate else b.BookingDate end,

*/
