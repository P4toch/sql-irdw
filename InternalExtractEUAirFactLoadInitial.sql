if object_id('dbo.InternalExtractEUAirFactLoadInitial') is null
begin
    print 'Creating stored procedure InternalExtractEUAirFactLoadInitial (placeholder)'
    execute ('create procedure dbo.InternalExtractEUAirFactLoadInitial as return 0')
end
go

print 'Altering stored procedure InternalExtractEUAirFactLoadInitial'
go

alter procedure [dbo].[InternalExtractEUAirFactLoadInitial] @pInternalExtractID int

as
/*
********************************************************************************************************
Copyright (C) 2014-2018 Expedia, Inc. All rights reserved.

Description:
     Inserts index set of EU air fact records for a particular
     extract.

     ToDo: RecordKeyOriginal/RecordKeyPrior lookups fail for exchanges when 
           dbo.[Nav_Sales_%_Line].[Originalticket] references multiple original tickets.
           Example: 3977125095-96 refers to 3977125095 and 3977125096.

           The following sample code splits them into two columns. We can use these two columns
           to join Nav_Service_Ledger_Entry:

            declare @NonNumericString varchar(10) = '%[^0-9-]%' 
            ;with t as (
            select top 10 --cast(ieef.TicketCode as varbinary(max)), datalength(ieef.TicketCode), 
                    ieef.TicketCode, ieef.TicketCodePrior, ieef.PNRCode,
                    case when ieef.TicketCodePrior like '[0-9][0-9][0-9][0-9]%-[0-9][0-9]' and
                              ieef.TicketCodePrior not like @NonNumericString
                        then 1 else 0 end as IsSplit
            from dbo.InternalExtractEUAirFact ieef 
            where ieef.TicketCodePrior like '[0-9][0-9][0-9][0-9]%-[0-9][0-9]' and
                              ieef.TicketCodePrior not like @NonNumericString
            )
            select *, 
                   case when IsSplit = 1 then 
                        left(TicketCodePrior, len(TicketCodePrior) -3)
                        else TicketCodePrior end as TickedCodePrior1, 
                   case when IsSplit = 1 then 
                        left(TicketCodePrior, len(TicketCodePrior) -5) + right(TicketCodePrior, 2)
                        else TicketCodePrior end as TickedCodePrior2
            from t

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------------------------------------------
    2014-04-28  JaredKo         Created.
    2015-03-31  JaredKo         Fix lookups for RecordKeyPrior/RecordKeyOriginal
    2015-06-04  JaredKo         Fix lookup from Nav_Sales_Cr_Memo_Header to Nav_Travel_Ledger_Entry
    2015-07-16  JaredKo         Fixing situations where RecordKeyOriginal or RecordKeyPrior is NULL
                                Code optimization to allow parallelism
    2017-11-14  manzeno         EGE-170657 Transaction type issue (Air Purchase with 0 as spend amount) 
    2018-02-15  nrasmussen      EGE-181461 Rollback of work done for EGE-170657                          
********************************************************************************************************
*/

set nocount on

---------------------------------------------------------------------
-- Declarations
---------------------------------------------------------------------

-- Standard constants and variables
declare @FALSE tinyint,
        @TRUE tinyint,
        @RC_FAILURE int,
        @RC_SUCCESS int,
        @Current_Timestamp datetime,
        @Error int,
        @ErrorCode int,
        @ExitCode int,
        @ProcedureName sysname,
        @RC int,            -- Return code from called SP
        @Rowcount int,
        @SavePointName varchar(32),
        @TranStartedBool tinyint,
        @TrancountSave int

-- Error message constants
declare @ERRUNEXPECTED int,
        @ERRPARAMETER int,
        @MsgParm1 varchar(100),
        @MsgParm2 varchar(100),
        @MsgParm3 varchar(100)

declare @BSIAir tinyint,
        @ExchangeRateNull money

declare @BookingID int,
        @BookingItemID int,
        @TravelProductID int,
        @TRL int,
        @BookingFulfillmentDateScheduled smalldatetime,
        @BookingItemAirTransactionStateID tinyint,
        @InternalExtractIDOther int,
        @BookingIDOther int,
        @BookingItemIDOther int,
        @pInternalExtractIDPrior int,
        @FactRecordStatusIDOK tinyint,
        @FactRecordStatusIDGPID tinyint,
        @FactRecordStatusIDLWD tinyint,
        @FactRecordStatusIDTRAV tinyint,
        @WorkingIdentifier int

--if object_id('tempdb..#Working') is not null drop table #Working
create table #Working (
    Identifier int identity (1, 1) primary key,
    InternalExtractID int not null,
    SalesDocumentCode varchar(20) not null, -- (PK of SalesInvoiceHeader)
    SalesDocumentLineNbr int not null, -- [SalesInvoiceLine].[Line_No_]
    BookingTypeID tinyint, -- NOT NULL after BookingTypeID lookup
    TravelProductID int,
    SalesDocumentTypeID tinyint,
    AgentAssistedBool bit, -- ToDo: Remove AgentAssistedBool calculation. It's determiend in InternalExtractEUAirFactLoadExpand
    RecordKey varchar(30),
    RecordKeyPrior varchar(30),
    RecordKeyOriginal varchar(30),
    SaleDocumentCodePrior varchar(20),
    TicketCode varchar(20),
    TicketCodePrior varchar(20)
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
    @TranStartedBool                = @FALSE

select   -- Error message constants
    @ERRUNEXPECTED                  = 200104,
    @ERRPARAMETER                   = 200110

select @FactRecordStatusIDOK           = 1,
       @FactRecordStatusIDGPID         = 7,
       @FactRecordStatusIDLWD          = 8,
       @FactRecordStatusIDTRAV         = 9

select @BSIAir                         = 7

select @pInternalExtractIDPrior = @pInternalExtractID - 1

---------------------------------------------------------------------
-- Validation
---------------------------------------------------------------------
-- Done by Caller
---------------------------------------------------------------------
-- Processing
---------------------------------------------------------------------

-- Get the set of extract records to work on.
-- This is based on Kettle's Populate_Second_Facts Air transformation
-- As the original transformation uses three similar queries with UNION operator,
-- Invoice CTE represents the parts that are common to each of the three SELECT statements.
-- BookingTypeIDs are pre-set for Exchange/Cancel but calculated later for regular Invoices
-- Credits (Nav_Sales_Cr_Memo_%) is processed after BookingTypeID Calculation


-- Invoices (Nav_Sales_Invoice_%)
   ;with Invoice as (
        select h.InternalExtractID,
               h.No_ as SalesDocumentCode, -- AKA Document_No_
               l.Line_No_ as SalesDocumentLineNbr,
               substring(h.No_ + '-' + convert(varchar(10), l.Line_No_), 1, 30) as RecordKey,
               h.TravelProductID,
               l.Ticket,
               l.Originalticket,
               l.ExchangeTicket,
               l.Ticket_Type,
               l.Principal_Ticket,
               l.Unit_of_Measure_Code,
               l.Amount_Including_VAT,
               case when l.Reservation_Mode = 2 
                   then 0 
                   else 1
                end as AgentAssistedBool,
               (select sum(SubQ.Amount_Including_VAT)
                       from dbo.Nav_Sales_Invoice_Line SubQ
                   where SubQ.Document_No_ = l.Document_No_ and
                       SubQ.Ticket = l.Principal_Ticket and
                       SubQ.Resource_Type = 0) as Sum_Amount_IncludingVAT
          from dbo.Nav_Sales_Invoice_Header h
               inner join
               dbo.Nav_Sales_Invoice_Line l on h.[No_] = l.[Document_No_] and
                                               h.InternalExtractID = l.InternalExtractID and
                                               h.TravelProductID = l.TravelProductID
               where l.Resource_Type = 0 and -- Primary Resource
                     l.Service_Group in (1, 6) and -- Air & Lowcost
                     l.Ticket_Type in (1, 2, 3) and -- BookingType = Issued,Repaid,Canceled
                     --l.Business_category <> 6 and -- Ancillary Service - Need this to calculate exchange penalties
                     isnumeric(h.Sell_to_Customer_No_) = 1 and -- ComCode is numeric
                     h.InternalExtractID = @pInternalExtractID and
                     l.Type = 3 and
                     -- Comment from Patrick: This filter for MICE was never used for air.
                     -- ToDo: Analysis and replace this criteria with new identification logic (remove as a part of future work item)
                     l.[group] <> 1 -- No MICE #EGE-68052

                   -- Don't include tickets where there are no travel segments
                   and exists (select * 
                                 from dbo.Nav_Travel_Ledger_Entry t
                                where t.InternalExtractID = l.InternalExtractID and
                                      t.TravelProductID = l.TravelProductID and
                                      t.Document_No_ = l.Document_No_ and
                                      t.Ticket_No = l.Ticket))
    insert into
        #Working(InternalExtractID,
                 SalesDocumentCode,
                 SalesDocumentLineNbr,
                 RecordKey,
                 TicketCode, 
                 TicketCodePrior,
                 TravelProductID,
                 SalesDocumentTypeID,
                 BookingTypeID,
                 AgentAssistedBool)

        -- INVOICES // Exchange = 0
          select i.InternalExtractID,
                 i.SalesDocumentCode,
                 i.SalesDocumentLineNbr,
                 i.RecordKey,
                 i.Ticket, 
                 i.Originalticket,
                 i.TravelProductID,
                 1 as SalesDocumentTypeID, -- 1 = Invoice
                 null as BookingTypeID,    -- Lookup in the next step
                 i.AgentAssistedBool
            from Invoice i
                 where ExchangeTicket = 0 and -- Not an exchange
                       Principal_Ticket = ''
                                        
        union all
        -- INVOICES // Exchange = 1
        select i.InternalExtractID,
               i.SalesDocumentCode,
               i.SalesDocumentLineNbr,
               i.RecordKey,
               i.Ticket, 
               i.Originalticket,
               i.TravelProductID,
               2 as SalesDocumentTypeID, -- 2 = Credit Note
               5 as BookingTypeID,       -- 5 = Exchange
               1 as AgentAssistedBool
            from Invoice i
        where ExchangeTicket = 1 and
            Ticket_Type = 1 and          -- 1 = Issued
            Principal_Ticket = ''
        union all

        -- CANCEL REPAID
        select i.InternalExtractID,
               i.SalesDocumentCode,
               i.SalesDocumentLineNbr,
               i.RecordKey,
               i.Ticket, 
               i.Originalticket,
               i.TravelProductID,
               0 as SalesDocumentTypeID, -- Unknown ?
               10 as BookingTypeID, -- Cancel Refund
               i.AgentAssistedBool
            from Invoice i
        where Ticket_Type = 4 and  -- BookingType = Cancel Repaid
              Principal_Ticket = ''


    -- Get BookingTypeID for Invoice
    -- Copies logic from \Navision\UDFs\dbo.GR_Get_BookingType_Period_AIR
    update tmp
        set tmp.BookingTypeID =      
            case
                when (l.[ticket_type] = 1 and -- Issued
                     l.ExchangeTicket = 1)
                    then 5 -- Exchange                   
                when l.[ticket_type] = 1 or
                     l.Amount_Including_VAT >= 0
                    then 1 -- Purchase
                when l.[ticket_type] = 4
                    then 10 -- Cancel and Refund
                when l.[ticket_type] = 3
                    then 4 -- Void (Listed as Cancel in Populate_Second_Facts)
                when (select sum(Amount_Including_VAT)
                        from dbo.Nav_Sales_Invoice_Line lSub
                             where lSub.InternalExtractID = @pInternalExtractID and
                                 lSub.TravelProductID = l.TravelProductID and
                                 lSub.[Document_No_]=l.[Document_No_] and
                                 lSub.[ID_2]=l.[ID_2] and
                                 lSub.[Ticket]=l.Ticket and
                                 lSub.[Ticket_Type] = 1 and
                                 lSub.[resource_type]=0) = (select sum(abs(Amount_Including_VAT))
                                                              from dbo.Nav_Sales_Invoice_Line lSub
                                                                   where lSub.InternalExtractID = @pInternalExtractID and
                                                                         lSub.TravelProductID = l.TravelProductID and
                                                                         lSub.[Document_No_]=l.[Document_No_] and
                                                                         lSub.[ID_2]=l.[ID_2] and
                                                                         lSub.[Ticket]=l.Ticket and
                                                                         lSub.[Ticket_Type] in (2, 3) and
                                                                         lSub.[resource_type]=0)
                    then 7 -- Refund
                else 8 -- Partial Refund
            end
        from #Working tmp
            join
            dbo.[Nav_Sales_Invoice_Line] l
                on tmp.SalesDocumentCode = l.[Document_No_] and
                    tmp.SalesDocumentLineNbr = l.Line_No_ and
                    tmp.InternalExtractID = l.InternalExtractID and
                    tmp.TravelProductID = l.TravelProductID
    where tmp.BookingTypeID is null

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (insert Sales Invoice into #InternalExtractEUAirFact)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- Credits (Nav_Sales_Cr_Memo_%)
    ;with Credit as (select h.InternalExtractID,
                            h.No_ as SalesDocumentCode,
                            l.Line_No_ as SalesDocumentLineNbr,
                            substring(h.No_ + '-' + convert(varchar(10), l.Line_No_), 1, 30) as RecordKey,
                            h.TravelProductID,
                            l.Ticket,
                            l.Originalticket,
                            l.ExchangeTicket,
                            l.Ticket_Type,
                            l.Principal_Ticket,
                            l.Unit_of_Measure_Code,
                            l.Amount_Including_VAT,
                            h.Issued_to_Doc_No_ as SaleDocumentCodePrior,
                            (select sum(SubQ.Amount_Including_VAT)
                                    from dbo.Nav_Sales_Cr_Memo_Line SubQ
                                where SubQ.InternalExtractID = @pInternalExtractID and
                                    SubQ.TravelProductID = l.TravelProductID and
                                    SubQ.Document_No_ = l.Document_No_ and
                                    SubQ.Ticket = l.Principal_Ticket and
                                    SubQ.Resource_Type = 0) as Sum_Amount_IncludingVAT
                       from dbo.Nav_Sales_Cr_Memo_Header h
                            inner join
                            dbo.Nav_Sales_Cr_Memo_Line l
                                on h.[No_] = l.[Document_No_] and
                                   h.InternalExtractID = l.InternalExtractID and
                                   h.TravelProductID = l.TravelProductID
                            where l.Resource_Type = 0 and -- Primary Resource
                                  l.Business_category in (1, 3, 10) and -- Ticket, Exchange, ATC
                                  l.Service_Group in (1, 6) and -- Air & Lowcost
                                  l.Ticket_Type in (1, 2, 3) and -- BookingType = Issued,Repaid,Canceled
                                  isnumeric(h.Sell_to_Customer_No_) = 1 and -- ComCode is numeric
                                  h.InternalExtractID = @pInternalExtractID and
                                  exists (select * 
                                            from dbo.Nav_Travel_Ledger_Entry t
                                           where t.InternalExtractID = l.InternalExtractID and
                                                 t.TravelProductID = l.TravelProductID and
                                                 t.Document_No_ = l.Document_No_ and
                                                 t.Ticket_No = l.Ticket)
                      )

        -- WITH Credit CTE
        insert into
            #Working(InternalExtractID,
                     SalesDocumentCode,
                     SalesDocumentLineNbr,
                     RecordKey,
                     TicketCode, 
                     TicketCodePrior,
                     TravelProductID,
                     SalesDocumentTypeID,
                     BookingTypeID,
                     AgentAssistedBool,
                     SaleDocumentCodePrior)
            select c.InternalExtractID,
                   c.SalesDocumentCode,
                   c.SalesDocumentLineNbr,
                   c.RecordKey,
                   c.Ticket, 
                   c.Originalticket,
                   c.TravelProductID,
                   2 as SalesDocumentTypeID, -- Credit Note
                   null as BookingTypeID,
                   1 as AgentAssistedBool,
                   SaleDocumentCodePrior
                from Credit c
                -- ToDo: Determine if we can remove Unit_of_Measure code. This may be here to remove Tax line items.
            where --c.Unit_of_Measure_Code in ('PRESTATION', 'PREST') and
                ((Principal_Ticket = '') -- Principal Ticket Segments
                or (Principal_Ticket <> '' and
                c.Sum_Amount_IncludingVAT <> 0))

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (insert Sales Credit into #InternalExtractEUAirFact)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    -- Get BookingTypeID for Credit
    -- Copies logic from \Navision\UDFs\dbo.GR_Get_BookingType_Back_AIR
    update tmp
        set tmp.BookingTypeID =      
            case
                when l.[ticket_type] in (1, 3)
                    then 4 -- Void (Listed as "Cancel" in Populate_Second_Facts)
                when l.[ticket_type] = 2 and
                    (select sum(Amount_Including_VAT)
                            from dbo.Nav_Sales_Invoice_Line lSub
                                 where lSub.[Document_No_]= h.Issued_to_Doc_No_ and
                                       lSub.[TravelProductID] = l.TravelProductID and
                                       lSub.[ID_2]=l.[ID_2] and
                                       lSub.[Ticket]=l.Ticket and
                                       lSub.[Ticket_Type] = 1 and
                                       lSub.[resource_type]=0)    = (select sum(Amount_Including_VAT)
                                                                          from dbo.Nav_Sales_Cr_Memo_Line lSub
                                                                               where lSub.[Document_No_]=l.[Document_No_] and
                                                                                     lSub.[TravelProductID] = l.TravelProductID and
                                                                                     lSub.[ID_2]=l.[ID_2] and
                                                                                     lSub.[Ticket]=l.Ticket and
                                                                                     lSub.[Ticket_Type] in (2, 3) and
                                                                                     lSub.[resource_type]=0)
                    then 7
                when l.[ticket_type] = 2
                    then 8
                -- ToDo: Determine why some BookingTypeIDs are NULL after connecting to prod data
                else 99
            end
        from #Working tmp
            inner join
            dbo.Nav_Sales_Cr_Memo_Header h
                on tmp.InternalExtractID = h.InternalExtractID and
                   tmp.TravelProductID = h.TravelProductID and
                   tmp.SalesDocumentCode = h.No_
            inner join
            dbo.Nav_Sales_Cr_Memo_Line l
                on tmp.SalesDocumentCode = l.[Document_No_] and
                    tmp.SalesDocumentLineNbr = l.Line_No_ and
                    tmp.InternalExtractID = l.InternalExtractID and
                    tmp.TravelProductID = l.TravelProductID
       where tmp.BookingTypeID is null and
             tmp.InternalExtractID = @pInternalExtractID

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update #InternalExtractEUAirFact.BookingTypeID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    -- Get RecordKey for Credit Memo
    update tmp
        -- ToDo: Question - Do Credit Memos have conjunctive tickets?
        set tmp.RecordKey =  tmp.SalesDocumentCode + '-' + convert(varchar(10), case
            when l.[principal_ticket] = ''
                then l.[Line_No_]
            else (select min(l2.[Line_No_])
                    from dbo.Nav_Sales_Cr_Memo_Line l2
                         where l2.[ticket]=l.[principal_ticket] and
                             l2.[Document_No_]=l.[Document_No_] and
                             l2.[Service_Group] in (1, 6) and --AIR  Low Cost
                             l2.[ticket_type] = l.[ticket_type] and
                             l2.[resource_type]= 0 and  -- Principal
                             l2.TravelProductID = l.TravelProductID)
        end)
        from dbo.#Working tmp
             inner join
             dbo.Nav_Sales_Cr_Memo_Line l on tmp.InternalExtractID = l.InternalExtractID and
                                             tmp.TravelProductID = l.TravelProductID and
                                             tmp.SalesDocumentCode = l.[Document_No_] and
                                             tmp.SalesDocumentLineNbr = l.Line_No_ and
                                             tmp.InternalExtractID = l.InternalExtractID 
             where tmp.InternalExtractID = @pInternalExtractID

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (Get RecordKey for Credit)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    -- Get RecordKeyOriginal / RecordKeyPrior
    -- SalesLineCTE is an alias for Sales_Invoice_Line and Sales_Cr_Memo_Line
    ;with SalesLineCTE as (
        select [InternalExtractID],
               [TravelProductID],
               [timestamp],
               [Document_No_],
               [Line_No_],
               [Sell_to_Customer_No_],
               [Ticket],
               [Ticket_Type],
               [Traveller_Code],
               [ID_2],
               [Originalticket],
               [Business_category],
               [ExchangeTicket],
               [Posting_Date],
               'I' as InvoiceType
          from dbo.Nav_Sales_Invoice_Line l
         union all
        select [InternalExtractID],
               [TravelProductID],
               [timestamp],
               [Document_No_],
               [Line_No_],
               [Sell_to_Customer_No_],
               [Ticket],
               [Ticket_Type],
               [Traveller_Code],
               [ID_2],
               [Originalticket],
               [Business_category],
               [ExchangeTicket],
               [Posting_Date],
               'C' as InvoiceType
          from dbo.Nav_Sales_Cr_Memo_Line
    ),PriorRecordKey as
    (
        select b.Document_No_ + '-' + convert(varchar,b.Line_No_) as RecordKey,
               a.InternalExtractID,
               a.TravelProductID,
               a.Document_No_,
               a.Line_No_,
               a.Business_category,
               a.ExchangeTicket,
               a.Originalticket,
               a.Ticket,
               b.Ticket_No,
               b.Entry_No_,
               b.Posting_Date,
               b.Entry_type,
               a.InvoiceType
               from SalesLineCTE a
                    join dbo.Nav_Service_Ledger_Entry b
                         on b.Ticket_No in (a.Ticket, a.Originalticket) and
                            b.TravelProductID = a.TravelProductID and
                            b.Traveller_Code = a.Traveller_Code and
                            b.Sell_to_Customer_No_ = a.Sell_to_Customer_No_ and
                            b.ID_2 = a.ID_2 and
                            b.Service_Group in (1, 6) and -- Air / Low Cost Air
                            b.Entry_type in (3, 4) and
                            b.Resource_Type = 0 and -- Primary
                            b.[Amount_(LCY)_including_VAT]<>0 and
                            -- Complex lookup required due to periodic invoicing. For example: We can't just filter to find a different Invoice Number for customers that have monthly billing.
                            -- When it's an invoice with no OriginalTicket, Sales_Invoice_Line.RecordKey = Service_Ledger_Entry.RecordKey
                                ( (a.InvoiceType = 'I' and b.Ticket_No = a.Ticket and a.Originalticket is null and b.Document_No_  = a.Document_No_ and a.Line_No_ = b.Line_No_) 
                                -- When there's an OriginalTicket (Exchange), omit the row with matching RecordKey (Sales_Invoice_Line.RecordKey <> Service_Ledger_Entry.RecordKey)
                                or (a.InvoiceType = 'I' and b.Ticket_No = a.Originalticket and b.Document_No_ + '-' + convert(varchar,b.Line_No_) <> a.Document_No_ + '-' + convert(varchar,a.Line_No_))
                                -- When it's a credit, match on (Sales_Cr_Memo_Line.Ticket_No = Service_Ledger_Entry.
                                or (a.InvoiceType = 'C' and b.Ticket_No = a.Ticket and b.Document_No_ + '-' + convert(varchar,b.Line_No_) <> a.Document_No_ + '-' + convert(varchar,a.Line_No_))
                                -- Exchange that results in Credit
                                or (a.InvoiceType = 'C' and b.Ticket_No = a.Originalticket and b.Document_No_ + '-' + convert(varchar,b.Line_No_) <> a.Document_No_ + '-' + convert(varchar,a.Line_No_))
                                    ) and
                            -- Old Logic:
                            --b.Document_No_ + '-' + convert(varchar,b.Line_No_) <> a.Document_No_ + '-' + convert(varchar,a.Line_No_) and
                            --b.Document_No_ <> a.Document_No_ and -- ToDo: does this work with Periodic Invoicing?
                            b.Posting_Date <= a.Posting_Date
    )
    update f
    set f.RecordKeyOriginal = coalesce((select top 1 aa.RecordKey
                                          from PriorRecordKey aa
                                         where aa.InternalExtractID = @pInternalExtractID and
                                               aa.TravelProductID = f.TravelProductID and
                                               aa.Document_No_ = f.SalesDocumentCode and
                                               aa.Line_No_ = f.SalesDocumentLineNbr and
                                               aa.Entry_No_ <= sle.Entry_No_ -- = Means there is no match so RecordKeyOrigional = RecordKey
                order by case
                             when aa.ExchangeTicket = 1 and aa.InvoiceType = 'I' and
                                  aa.Originalticket = aa.Ticket_No and 
                                  aa.Entry_type = 3
                             then 0
                             else 1
                         end,
                         case
                             when aa.Ticket = aa.Ticket_No
                             then 0
                             else 1
                         end,
                         aa.Entry_No_), f.RecordKey -- f.RecordKey is part of coalesce
                                         ), -- RecordKeyOriginal =
        f.RecordKeyPrior = coalesce((select top 1 aa.RecordKey
                                       from PriorRecordKey aa
                where aa.InternalExtractID = @pInternalExtractID and
                    aa.TravelProductID = f.TravelProductID and
                    aa.Document_No_ = f.SalesDocumentCode and
                    aa.Line_No_ = f.SalesDocumentLineNbr and
                    aa.Entry_No_ <= sle.Entry_No_
                order by case
                             when aa.Ticket = aa.Ticket_No
                             then 0
                             else 1
                         end,
                         case
                             when aa.Business_category = 3 and -- 1 = Ticket; 3 = Exchange; 10 = ATC
                             aa.Originalticket = aa.Ticket_No
                             then 0
                             else 1
                         end,
                         aa.Entry_No_ desc), f.RecordKey -- f.RecordKey is part of coalesce
                                      ) -- RecordKeyPrior =
    from dbo.#Working f
    join SalesLineCTE l
         on l.InternalExtractID = @pInternalExtractID
              and l.TravelProductID = f.TravelProductID
              and l.Document_No_ = f.SalesDocumentCode
              and l.Line_No_ = f.SalesDocumentLineNbr
    outer apply(select top 1 sle2.Entry_No_
                from dbo.Nav_Service_Ledger_Entry sle2 
                where   --sle2.Document_No_ = l.Document_No_ and
                        sle2.TravelProductID = l.TravelProductID and
                        sle2.Traveller_Code = l.Traveller_Code and
                        sle2.Sell_to_Customer_No_ = l.Sell_to_Customer_No_ and
                        sle2.ID_2 = l.ID_2 and
                        sle2.Service_Group in (1, 6) and -- Air / Low Cost Air
                        sle2.Entry_type in (3, 4) and
                        sle2.Resource_Type = 0 -- Primary
                        and ((l.ExchangeTicket = 0 and sle2.Document_No_ = l.Document_No_ and sle2.Ticket_No = l.Ticket) or
                             (l.ExchangeTicket = 1 and sle2.Ticket_No = l.Originalticket) ) 
                        and sle2.[Amount_(LCY)_including_VAT]<>0
                    order by sle2.InternalExtractID desc,
                             sle2.Entry_No_ desc
                            ) sle
    option(recompile)

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (Get RecordKeyPrior / RecordKeyOriginal)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    select @TrancountSave = @@Trancount
    if (@TrancountSave = 0)
        begin transaction @SavePointName
        else
            save transaction @SavePointName
        select @TranStartedBool = @TRUE

        insert
            dbo.InternalExtractEUAirFact(InternalExtractID,
                                         SalesDocumentCode,
                                         SalesDocumentLineNbr,
                                         SalesDocumentTypeID,
                                         BookingTypeID,
                                         TravelProductID,
                                         RecordKey,
                                         RecordKeyPrior,
                                         RecordKeyOriginal,
                                         SalesDocumentCodePrior,
                                         TicketCode, 
                                         TicketCodePrior,
                                         AgentAssistedBool,
                                         FactRecordStatusID,
                                         UpdateDate,
                                         LastUpdatedBy)
            select w.InternalExtractID,
                   w.SalesDocumentCode,
                   w.SalesDocumentLineNbr,
                   w.SalesDocumentTypeID,
                   isnull(w.BookingTypeID,199),
                   w.TravelProductID,
                   w.RecordKey,
                   w.RecordKeyPrior,
                   w.RecordKeyOriginal,
                   w.SaleDocumentCodePrior,
                   w.TicketCode, 
                   w.TicketCodePrior,
                   w.AgentAssistedBool,
                   @FactRecordStatusIDOK,
                   @Current_Timestamp,
                   @ProcedureName
                from #Working w

        select @Error = @@Error
        if (@Error <> 0)
        begin
            select @ErrorCode   = @ERRUNEXPECTED,
                   @MsgParm1    = cast(@Error as varchar(12)) + ' (insert InternalExtractEUAirFact Initial)'
            raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
            goto ErrorHandler
        end

    if ((@TrancountSave = 0)
        and (@TranStartedBool = @TRUE))
    commit transaction @SavePointName

goto ExitProc

---------------------------------------------------------------------
-- Error Handler
---------------------------------------------------------------------
ErrorHandler:
if (@TranStartedBool = @TRUE)
    rollback transaction @SavePointName
select @ExitCode = @RC_FAILURE
goto ExitProc

---------------------------------------------------------------------
-- Exit Procedure
---------------------------------------------------------------------
ExitProc:
return (@ExitCode)
go

-- select InternalExtractID, count(*) FROM InternalExtractEUAirFact group by InternalExtractID
-- select InternalExtractID, count(*) from dbo.Nav_Sales_Invoice_Line nsil group by InternalExtractID
-- select InternalExtractID, count(*) from dbo.Nav_Sales_Cr_Memo_Line nscml group by InternalExtractID
-- select * FROM dbo.InternalExtract ie where SourceDatabaseName = 'navision_europe'

/*
-- 37788
-- 39053
declare @InternalExtractID INT = 37788
 select RecordKey, RecordKeyPrior, RecordKeyOriginal, TicketCode, TicketCodePrior, BookingTypeID
 from InternalExtractEUAirFact where InternalExtractID = @InternalExtractID
   --and RecordKey IN ('FRC1673070-10000', 'FRI13136622-10000', 'FRI13364168-10000', 'FRI13364168-20010', 'FRI13364168-30020')

 begin tran 
 delete InternalExtractEUAirFact where InternalExtractID = @InternalExtractID
 
 exec [InternalExtractEUAirFactLoadInitial] @pInternalExtractID = @InternalExtractID
 --exec [InternalExtractEUAirFactLoadExpand] @pInternalExtractID = @InternalExtractID

select RecordKey, RecordKeyPrior, RecordKeyOriginal, TicketCode, TicketCodePrior, BookingTypeID
 from InternalExtractEUAirFact where InternalExtractID = @InternalExtractID
   --and RecordKey IN ('FRC1673070-10000', 'FRI13136622-10000', 'FRI13364168-10000', 'FRI13364168-20010', 'FRI13364168-30020')

rollback tran

*/


/*
begin tran
 select * from dbo.InternalExtractEUAirFact ieef 
 where recordkey in ('NOSI250152121-10000', 'NOSI250152125-10000', 'NOSI250152192-10000', 'NOSI250152193-10000', 'NOSI250152256-10000', 'NOSI250152260-10000', 'NOSI250152272-10000')
   and InternalExtractID = 38617

 select BookingTypeID, count(*) from dbo.InternalExtractEUAirFact where InternalExtractID = 38617 
 group by BookingTypeID order by BookingTypeID
 
 delete InternalExtractEUAirFact where InternalExtractID = 38617
 
 exec [InternalExtractEUAirFactLoadInitial] @pInternalExtractID = 38617
 
 select * from dbo.InternalExtractEUAirFact ieef where recordkey in ('NOSI250152121-10000', 'NOSI250152125-10000', 'NOSI250152192-10000', 'NOSI250152193-10000', 'NOSI250152256-10000', 'NOSI250152260-10000', 'NOSI250152272-10000')
 
 select BookingTypeID, count(*) from dbo.InternalExtractEUAirFact where InternalExtractID = 38617 group by BookingTypeID order by BookingTypeID
rollback tran
*/



--;with cte as (
--select l.Document_No_, l.Line_No_, l.Business_category, l.Business_sub_category, BookingTypeID
--from ECTDataStore_NavExtract.dbo.InternalExtractEUAirFact f
--join dbo.Nav_Sales_Invoice_Line l
--    on l.Document_No_ = f.SalesDocumentCode and l.Line_No_ = f.SalesDocumentLineNbr
--where l.Business_category in (1, 3, 10)
--            and l.Service_Group in (1, 6) -- Air & Lowcost
--            and l.Ticket_Type in (1, 2, 3) -- BookingType = Issued,Repaid,Canceled
--            and l.Resource_Type = 0  -- Primary Resource
--)
--select Business_category, Business_sub_category, BookingTypeID, count(*) Counts
--from cte
--group by grouping sets ((Business_category, Business_sub_category, BookingTypeID),())
--order by case when Business_category is null then 1 else 0 end, Business_category, Business_sub_category, BookingTypeID
