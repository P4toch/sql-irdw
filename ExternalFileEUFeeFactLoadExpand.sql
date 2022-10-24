if object_id('dbo.ExternalFileEUFeeFactLoadExpand') is null begin
    print 'Creating stored procedure ExternalFileEUFeeFactLoadExpand (placeholder)'
    execute('create procedure dbo.ExternalFileEUFeeFactLoadExpand as return 0')
end
go

print 'Altering stored procedure ExternalFileEUFeeFactLoadExpand'
go

alter procedure dbo.ExternalFileEUFeeFactLoadExpand 
    @pExternalFileID int
with recompile
as

/*
*********************************************************************
Copyright (C) 2008-2018 Expedia, Inc. All rights reserved.

Description:
     Updates ExternalFileEUFeeFact with most attributes

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2008-03-15  BarryC          Created.
    2009-07-26  BarryC          Add BookingSourceID
    2016-12-05  jappleberry     EGE-19143 set null GroupAccountDepartmentID to 0
    2018-03-19  lmundal         EGE-181129: Add CreditCardNbr info for EU transactions
    2018-04-03  nrasmussen      EGE-189992 IRD - Enrich an agent assisted transaction with Travel Consultant TUID
    2018-05-16  nrasmussen      EGE-195465 IRD - replaced table event_log with event_log_eu_agentid 
    2018-05-29	Ramesh Karnati	Adding InvoiceSource (Apollo/BIAS) indicator as part of FSP/Oracle Integration (Resolved Merge conflicts)
    2018-06-07  nrasmussen      EGE-200489 IRD moving eu agentid section due to dataflow issues
    2018-08-29  nrasmussen      Jira.EGE-189822 Adding columns BusinessCategoryID and BusinessSubCategoryID
    2018-09-06  nrasmussen      Jira.EGE-204727 IRD FeeFact add pnrcode
    2018-10-02  nrasmussen      Jira.EGE-189822 Fixing overflow int column for MetaDossierID for IsNumericBool flag and changed logic for AgentAssistedBool
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
    @ExchangeRateNull                     money

create table #ExternalFileEUFeeFact (
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
    EUServiceLevelID tinyint,
    IssueDate datetime,
    InvoiceDate datetime,
    CreditCardTypeID tinyint,
    CentralBillBool bit,
    CreditCardNbr varbinary(64),
    TransactionCnt smallint,
    IncrementCnt smallint,
    EUFeeTypeCode varchar(30),
    CurrencyCode char(3),
    BookingAmtGross money,
    BookingAmtBase money,
    BookingAmtTax money,
    BookingAmtVat money,
    CustomDataElementTxt varchar(2000),
    UpdateDate smalldatetime,
    LastUpdatedBy varchar(32),
    BookingSourceID tinyint,
    CreditCardNbrBegin varchar(6),
    CreditCardNbrEnd varchar(4),
    InvoiceSource varchar(10),
    BusinessCategoryID smallint,
    BusinessSubCategoryID smallint,
    PNRCode varchar(35),
    Document_No_ varchar(20),
    Line_No_ int,
    AgentAssistedBool int,
    IsNumericBool int
    )

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
    @TranStartedBool                = @FALSE,
    @NonNumericString               = '%[^0-9]%'

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
insert into #ExternalFileEUFeeFact
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
    EUServiceLevelID,
    IssueDate,
    InvoiceDate,
    CreditCardTypeID,
    CentralBillBool,
    CreditCardNbr,
    TransactionCnt,
    IncrementCnt,
    EUFeeTypeCode,
    CurrencyCode,
    BookingAmtGross,
    BookingAmtBase,
    BookingAmtTax,
    BookingAmtVat,
    CustomDataElementTxt,
    UpdateDate,
    LastUpdatedBy,
    BookingSourceID,
    CreditCardNbrBegin,
    CreditCardNbrEnd,
    InvoiceSource,
    BusinessCategoryID,
    BusinessSubCategoryID,
    PNRCode,
    null as Document_No_,
    null as Line_No_,
    null as AgentAssistedBool,
    0 as IsNumericBool
from dbo.ExternalFileEUFeeFact
where ExternalFileID = @pExternalFileID

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #ExternalFileEUFeeFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

create index temp_ix1 on #ExternalFileEUFeeFact (RecordKey, ExternalFileID, BookingTypeID, MetaDossierID)

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (index create on #ExternalFileEUFeeFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

update #ExternalFileEUFeeFact
   set
    CustomerSystemID = 2,
    RecordKeyPrior = b.RecordKeyPrior,
    RecordKeyOriginal = b.RecordKeyOriginal, 
    TravelProductID = b.TravelProductID, 
    GroupAccountID = 0,
    GroupAccountDepartmentID = 0,
    TUIDAccount = nullif(b.PercodeAccount,0), 
    TUIDTraveler = nullif(b.PercodeAccount,0), 
    TUIDLogon = case when b.LogonTypeID = 1 then b.LogonID
                     when b.LogonTypeID = 2 then null -- -1 * b.LogonID
                     else null end,
    TUIDArranger = b.PercodeArranger, 
    MetaDossierID = ltrim(rtrim(b.MetaDossierID)), 
    SalesDocumentTypeID = b.SalesDocumentTypeID, 
    SalesDocumentCode = ltrim(rtrim(b.SalesDocumentCode)), 
    SalesDocumentCodePrior = ltrim(rtrim(b.SalesDocumentCodePrior)), 
    IssueDate = coalesce(b.IssueDate,b.InvoiceDate), 
    InvoiceDate = b.InvoiceDate,
    EUServiceLevelID = b.ServiceLevelID, 
    EUFeeTypeCode = b.EUFeeTypeCode, 
    CurrencyCode = c.CurrencyCodeStorage,
    CreditCardTypeID = b.CreditCardTypeID, 
    CentralBillBool = case when b.CreditCardOwnerID = 1 then 1
                           when b.CreditCardOwnerID = 2 then 0
                           else null end,
    CreditCardNbr = null, 
    TransactionCnt = 1, 
    IncrementCnt = case when a.BookingTypeID = @BookingTypeIDPurchase then 1
                        when a.BookingTypeID = @BookingTypeIDCancel then -1
                   end, 
    BookingAmtGross = b.BookingAmtGross * coalesce(d.ExchangeRateUsed, @ExchangeRateNull), 
    BookingAmtBase = b.BookingAmtBase * coalesce(d.ExchangeRateUsed, @ExchangeRateNull), 
    BookingAmtTax = (b.BookingAmtTax + b.BookingAmtVAT) * coalesce(d.ExchangeRateUsed, @ExchangeRateNull), 
    BookingAmtVAT = b.BookingAmtVAT * coalesce(d.ExchangeRateUsed, @ExchangeRateNull), 
    CustomDataElementTxt = coalesce(ltrim(rtrim(b.CostCenterTxt)),''),
    CreditCardNbrBegin = left(b.CreditCardNbr, 6),
    CreditCardNbrEnd = right(b.CreditCardNbr, 4),
	InvoiceSource = b.Location_Code, 
    Document_No_ = left(a.recordkey,charindex('-',a.recordkey,1)-1), -- use for Nav_ join later
    Line_No_ = right(a.recordkey,charindex('-',reverse(a.recordkey),1)-1) -- use for Nav_ join later
  from #ExternalFileEUFeeFact a 
       inner join
       dbo.ExternalFileEUFee b on a.ExternalFileID = b.ExternalFileID and a.RecordKey = b.RecordKey
       inner join 
       dbo.TravelProductDim c on b.TravelProductID = c.TravelProductID
       left join
       dbo.ExchangeRateDailyFull d on dbo.TimeIDFromDate(coalesce(b.InvoiceDate, b.IssueDate)) = d.TimeID and
            b.CurrencyCode = d.FromCurrencyCode and 
            c.CurrencyCodeStorage = d.ToCurrencyCode

       
select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUFeeFact expand)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

-- businesscategoryid/businesssubcategoryid/pnrcode
update a
    set a.BusinessCategoryID = isnull(coalesce(b.business_category,c.business_category),0),
        a.BusinessSubCategoryID = isnull(coalesce(b.business_sub_category,c.business_sub_category),0),
        a.PNRCode = case -- added special logic for FR rail pnrcode
		                when coalesce(b.Service_Group,c.Service_group) = 2 and a.TravelProductID = '60020' then
                            case coalesce(b.C_R_S,c.C_R_S)
                                when 'AIR208_RAIL' then
                                    case substring(coalesce(b.National_System_PNR,c.National_System_PNR), 4, 6) 
			 	                        when '' then coalesce(b.ID_2,c.ID_2) 
			 				            else substring(coalesce(b.National_System_PNR,c.National_System_PNR), 4, 6) 
						            end
                                else -- AIRRAIL
                                    case replace(coalesce(b.National_System_PNR,c.National_System_PNR), '2C ', '')
							            when '' then coalesce(b.ID_2,c.ID_2) 
							            else replace(coalesce(b.National_System_PNR,c.National_System_PNR), '2C ', '') 
						            end
                            end
					    else coalesce(b.ID_2,c.ID_2) 
                    end
    from #ExternalFileEUFeeFact a
    outer apply (select top 1 business_category, business_sub_category, ID_2, C_R_S, National_System_PNR, TravelProductID, Service_Group
                 from dbo.nav_sales_invoice_line
                 where Document_No_ = a.Document_No_
                 and   Line_No_ = a.Line_No_
                 order by internalextractid desc) b
    outer apply (select top 1 business_category, business_sub_category, ID_2, C_R_S, National_System_PNR, TravelProductID, Service_Group
                 from dbo.nav_sales_cr_memo_line
                 where Document_No_ = a.Document_No_
                 and   Line_No_ = a.Line_No_
                 order by internalextractid desc) c

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUFeeFact BusinessCateGoryID/BusinessSubCategoryID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


-- groupaccountid
update #ExternalFileEUFeeFact
   set GroupAccountID = c.GroupAccountID
  from #ExternalFileEUFeeFact a 
       inner join
       dbo.ExternalFileEUFee b on a.ExternalFileID = b.ExternalFileID and a.RecordKey = b.RecordKey
       inner join
       dbo.GroupAccountDim c on b.Comcode = c.Comcode

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUFeeFact look up GPID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


update #ExternalFileEUFeeFact
   set GroupAccountDepartmentID = isnull(d.GroupAccountDepartmentID, 0)
  from #ExternalFileEUFeeFact a 
       inner join
       dbo.ExternalFileEUFee b on a.ExternalFileID = b.ExternalFileID and a.RecordKey = b.RecordKey
       inner join
       dbo.GroupAccountDim c on b.Comcode = c.Comcode
       inner join    
       dbo.GroupAccountDepartmentDim d on c.GroupAccountID = d.GroupAccountID and b.CostCenterIDMain = d.MainCCValueID and d.CustomerSystemID = 2

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUFeeFact look up GADID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


update #ExternalFileEUFeeFact
   set BookingSourceID = case when b.StandAloneBool = 1 then 17
                              when b.StandaloneBool = 0 then 16 
                              else 17
                          end
  from #ExternalFileEUFeeFact a
       left join
       dbo.EUFeeTypeDim b on a.EUFeeTypeCode = b.EUFeeTypeCode and b.LangID = 1033

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUFeeFact look up BookingSourceID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

------------------------------------------------------------
-- IRD/EU AgentID
------------------------------------------------------------
update #ExternalFileEUFeeFact 
   set AgentAssistedBool = case substring(right(EUFeetypeCode,charindex('-',reverse(EUFeetypeCode),2)-1),2,1)
                              when '1' then 1 
                              when '0' then 1 
                              else 0
                           end  

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (updating new column #ExternalFileEUFeeFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end       

update #ExternalFileEUFeeFact
    set IsNumericBool = case
                            when isnumeric(MetaDossierID) = 0 then 0
                            when MetaDossierID like @NonNumericString then 0
                            when cast(MetaDossierID as numeric(38, 0)) not between -2147483648. and 2147483647. then 0
                            else 1
                        end

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update IsNumericBool #ExternalFileEUFeeFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

create index temp_ix on  #ExternalFileEUFeeFact (MetaDossierID,IsNumericBool)

select @Error = @@Error
if (@Error <> 0)
begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (index create on  #ExternalFileEUFeeFact)'
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
                from #ExternalFileEUFeeFact 
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
   from #ExternalFileEUFeeFact f 
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
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #ExternalFileEUFeeFact look up TravelConsultantID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end
-- end EU AgentID



select @TrancountSave = @@Trancount
if (@TrancountSave = 0) begin transaction @SavePointName
else                    save  transaction @SavePointName
select @TranStartedBool = @TRUE

update dbo.ExternalFileEUFeeFact
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
    EUServiceLevelID = b.EUServiceLevelID, 
    EUFeeTypeCode = b.EUFeeTypeCode, 
    CurrencyCode = b.CurrencyCode,
    CreditCardTypeID = b.CreditCardTypeID, 
    CentralBillBool = b.CentralBillBool,
    CreditCardNbr = b.CreditCardNbr , 
    TransactionCnt = b.TransactionCnt  , 
    IncrementCnt = b.IncrementCnt  , 
    BookingAmtGross = b.BookingAmtGross , 
    BookingAmtBase = b.BookingAmtBase , 
    BookingAmtTax =  b.BookingAmtTax , 
    BookingAmtVAT = b.BookingAmtVAT , 
    CustomDataElementTxt = b.CustomDataElementTxt ,
    BookingSourceID = b.BookingSourceID,
    CreditCardNbrBegin = b.CreditCardNbrBegin,
    CreditCardNbrEnd = b.CreditCardNbrEnd,
	InvoiceSource = b.InvoiceSource,
    BusinessCategoryID = b.BusinessCategoryID,
    BusinessSubCategoryID = b.BusinessSubCategoryID,
    PNRCode = b.PNRCode,
	UpdateDate = @Current_TimeStamp, 
	LastUpdatedBy = 'ExternalFileEUFeeFactLoadExpand'
      from dbo.ExternalFileEUFeeFact a 
           inner join
           #ExternalFileEUFeeFact b on a.ExternalFileID = b.ExternalFileID and a.RecordKey = b.RecordKey

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update ExternalFileEUFeeFact expansion)'
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

    delete
    from ExternalFileEUFeeFact
    where ExternalFileID = 2146

    exec dbo.ExternalFileEUFeeFactLoadInitial 2146
    exec dbo.ExternalFileEUFeeFactLoadExpand 2146

    select a.TravelProductID, a.InvoiceDate, a.CurrencyCode, a.BookingAmtGross, b.BookingAmtGross, a.BookingAmtBase, b.BookingAmtBase, *
    from ExternalFileEUFeeFact a
    join vExternalFileEUHotel b on a.RecordKey = b.RecordKey
    where a.ExternalFIleID = 2146

*/