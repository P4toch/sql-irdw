if object_id('dbo.InternalExtractEUAirSegmentFactLoad') is null
begin
    print 'Creating stored procedure InternalExtractEUAirSegmentFactLoad (placeholder)'
    execute ('create procedure dbo.InternalExtractEUAirSegmentFactLoad as return 0')
end
go

print 'Altering stored procedure InternalExtractEUAirSegmentFactLoad'
go

alter procedure dbo.InternalExtractEUAirSegmentFactLoad @pInternalExtractID int
as

/*
*********************************************************************
Copyright (C) 2014-2018 Expedia, Inc. All rights reserved.

Description:
     Insert segments into InternalExtractEUAirSegmentFact

Notes:
    This uses code modified from Kettle's Populate_First_Facts transformation.
    Alternate method (which include Stopover_Indicator) can be found in Navision Function: dbo.IR_getTripDest

    ToDo: This should use dbo.InternalExtractEUAirFact as a source rather than copy logic from Populate_First_Facts

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2014-05-29  Jared Ko        Created
    2014-10-01  rakarnati       EGE-72755 - The attribute WithinCountryBool is populated from From/To Airport country
    2014-10-13  Jared Ko        Adjustment to EGE-72755 to put inline on first INSERT
                                Clean up ArrivalCityCode when len > 3
                                Clean up Corporation when len > 3 (found in Maui but not prod)
                                ArrivalDateTime isn't always > DepartureDateTime when crossing time zones code
                                    should not auto-correct
                                jira.EGE-73190 - EU Air: SegmentValue and FlightValue 
                                    null in Segment and Flight for some billback air
    2015-03-31  Jared Ko        Processing fails on dates outside of SmallDateTime data type range
    2015-06-02  Jared Ko        Add support for ConnectionBool
    2015-06-30  Jared Ko        Added check for AirportCode prior to inserting dbo.RouteDim
    2015-07-07  Jared Ko        Fix ConnectionBool calculation
    2015-09-11  Jared Ko        EGE-95147 - Segment BookingTypeID is 99 in some cases, resulting in 
                                    failure to populate flight data
    2015-11-16  minieto         Merging rakarnati Air Duration changes for EGE-96664
    2017-01-31  jappleberry     EGE-138798 BackOffice Air Redesign: 
                                 Air Integration Process in Navision 
    2018-05-21  pbressan        EGE-197474 Add column AirlineCodeOperating
    2018-06-26  pbressan        EGE-197474 Add column AirlineCodeMarketing
    2018-08-29  lmundal         EGE-208923 Update AirlineCodeOperating and AirlineCodeMarketing
    2018-09-21  pbressan        EGE-213316 Add column Flexibility Logic
    2018-10-17  GKoneru         EGE-215484 Removed the code that populates '---' when
                                           Arrival_City_Code & Departure_City_Code are same
    2018-10-23  yserir          EGE-187910 Fix for AF CabinClassID add of a custom logic
*********************************************************************
*/

set nocount on

---------------------------------------------------------------------
-- Declarations
---------------------------------------------------------------------

declare @FALSE tinyint, -- Standard constants and variables
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
 @TrancountSave int,
 @NonNumericString varchar(10)


declare @ERRUNEXPECTED int, -- Error message constants
        @ERRPARAMETER int,
        @MsgParm1 varchar(100),
        @MsgParm2 varchar(100),
        @MsgParm3 varchar(100)


declare @BSIAir tinyint, -- SP specific constants and variables
        @BookingTypeIDPurchase tinyint,
        @BookingTypeIDReserve tinyint,
        @BookingTypeIDVoid tinyint,
        @BookingTypeIDExchange tinyint,
        @BookingTypeIDRefund tinyint,
        @BookingTypeIDPRefund tinyint,
        @BookingTypeIDUTT tinyint,
        @TimeZoneIDFrom int

declare @AirFareFlexPriority table (
        [AirFareFlexibilityID] tinyint
    ,   [AirFareFlexibilityName] varchar(50)
    ,   [Priority] smallint
    )

---------------------------------------------------------------------
-- Initializations
---------------------------------------------------------------------

select   -- Standard constants
    @FALSE                          = 0,
    @TRUE                           = 1,
    @RC_FAILURE                     = -100,
    @RC_SUCCESS                     = 0,
    -- @NonNumericString is used to find integer values through exclusion
    -- Example: SELECT @ID = ID FROM tbl WHERE ID not like @NonNumericString
    -- Translated: Rows WHERE ID does NOT include values that are NOT 0-9.
    @NonNumericString               = '%[^0-9-]%' 

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

select   -- SP specific constants and variables
    @BSIAir = 7,
    @BookingTypeIDPurchase          = 1,
    @BookingTypeIDReserve           = 3,
    @BookingTypeIDVoid              = 4,
    @BookingTypeIDExchange          = 5,
    @BookingTypeIDRefund            = 7,
    @BookingTypeIDPRefund           = 8,
    @BookingTypeIDUTT               = 9,
    @TimeZoneIDFrom                 = 51

insert into @AirFareFlexPriority 
    select 100, 'TravelFusion',  -1 union all
    select   0, 'Unknown',        0 union all
    select   1, 'Non-flexible',   1 union all
    select   4, 'Changeable',     2 union all
    select   2, 'Refundable',     3 union all
    select   3, 'Fully-flexible', 4

-- if object_id('tempdb..#Segments') is not null drop table #Segments
create table #Segments (
    Identifier int identity (1, 1) primary key,
    InternalExtractID int not null,
    SalesDocumentCode varchar(20) not null, -- (PK of SalesInvoiceHeader)
    SalesDocumentLineNbr int not null, -- [SalesInvoiceLine].[Line_No_]
    BookingTypeID tinyint, -- NOT NULL after BookingTypeID lookup
    TravelProductID int,
    SalesDocumentTypeID tinyint,
    AgentAssistedBool bit,
    RecordKey varchar(30),
    RecordKeyPrior varchar(30),
    RecordKeyOriginal varchar(30),
    SaleDocumentCodePrior varchar(20)
)
---------------------------------------------------------------------
-- Validation
---------------------------------------------------------------------
-- None, done by caller
---------------------------------------------------------------------
-- Processing
---------------------------------------------------------------------

select @TrancountSave = @@Trancount
if (@TrancountSave = 0) begin transaction @SavePointName
else                    save  transaction @SavePointName
select @TranStartedBool = @TRUE


-- Get the set of extract records to work on.
-- Based on Kettle's "Populate_First_Facts"

    ;with cteSegments as (
        select h.InternalExtractID,
               h.No_ as SalesDocumentCode,
               l.Line_No_ as SalesDocumentLineNbr,
               h.TravelProductID,
               l.Ticket,
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
                   where SubQ.InternalExtractID = l.InternalExtractID and
                         SubQ.Document_No_ = l.Document_No_ and
                         SubQ.TravelProductID = l.TravelProductID and
                         SubQ.Ticket = l.Principal_Ticket and
                         SubQ.Resource_Type = 0) as Sum_Amount_IncludingVAT
          from dbo.Nav_Sales_Invoice_Header h
               inner join
               dbo.Nav_Sales_Invoice_Line l on h.[No_] = l.[Document_No_] and
                                               h.InternalExtractID = l.InternalExtractID and
                                               h.TravelProductID = l.TravelProductID
               where l.Resource_Type = 0  -- Primary Resource
                   and l.Service_Group in (1, 6) -- Air & Lowcost
                   and l.Ticket_Type in (1, 2, 3) -- BookingType = Issued,Repaid,Canceled
                   and isnumeric(h.Sell_to_Customer_No_) = 1 -- ComCode is numeric
                   and h.InternalExtractID = @pInternalExtractID
                   and l.Type = 3 -- ToDo: Redundant with ExtractObject code. Pick one place to use it.
                   and l.[group] <> 1 -- No MICE #EGE-68052
                   -- Join to Nav_Travel_Ledger_Entry so we're not including Tax line items
                   -- ToDo: Can we just say "WHERE Unit_of_Measure <> 'TX FEES'" ?
                   and exists (select * 
                                 from dbo.Nav_Travel_Ledger_Entry t
                                where t.InternalExtractID = l.InternalExtractID and
                                      t.TravelProductID = l.TravelProductID and
                                      t.Document_No_ = l.Document_No_ and
                                      t.Ticket_No = l.Ticket))
    insert into
        #Segments(InternalExtractID,
                 SalesDocumentCode,
                 SalesDocumentLineNbr,
                 TravelProductID,
                 SalesDocumentTypeID,
                 BookingTypeID,
                 AgentAssistedBool)
          select i.InternalExtractID,
                 i.SalesDocumentCode,
                 i.SalesDocumentLineNbr,
                 i.TravelProductID,
                 1 as SalesDocumentTypeID, -- Invoice
                 null as BookingTypeID, -- Lookup in the next step
                 i.AgentAssistedBool
            from cteSegments i
                 where ExchangeTicket = 0 and -- Not an exchange
                       (
                       (Principal_Ticket = '') 

-- ToDo: Determine if needed. This seems to be necessary for segments/flights but not for tickets
                       or
                            (Principal_Ticket <> '' and
                               i.Sum_Amount_IncludingVAT <> 0 and
                               -- No Conjunction Ticket Segments
                              exists (select *
                                        from dbo.Nav_Travel_Ledger_Entry t2
                                        where t2.InternalExtractID = @pInternalExtractID and
                                                t2.TravelProductID = i.TravelProductID and
                                                t2.Ticket_No = i.Principal_Ticket and
                                                t2.Document_No_ = i.SalesDocumentCode)))
                                        
        union all-- INVOICES // Exchange = 1
        select i.InternalExtractID,
               i.SalesDocumentCode,
               i.SalesDocumentLineNbr,
               i.TravelProductID,
               1 as SalesDocumentTypeID, -- Invoice
               5 as BookingTypeID, -- Exchange
               1 as AgentAssistedBool
            from cteSegments i
        where ExchangeTicket = 1 and
            Ticket_Type = 1 and -- BookingType = Issued
            ((Principal_Ticket = '') 
            or (Principal_Ticket <> '' and
            i.Sum_Amount_IncludingVAT <> 0))

        union all -- CANCEL REPAID
        select i.InternalExtractID,
               i.SalesDocumentCode,
               i.SalesDocumentLineNbr,
               i.TravelProductID,
               0 as SalesDocumentTypeID, -- Unknown ?
               10 as BookingTypeID, -- Cancel Refund
               i.AgentAssistedBool
            from cteSegments i
        where Ticket_Type = 4 and  -- BookingType = Cancel Repaid
            -- ToDo: Determine if we can remove Unit_of_Measure code. This may be here to remove Tax line items.
            --i.Unit_of_Measure_Code in ('PRESTATION', 'PREST') and
            ((Principal_Ticket = '') 
            or (Principal_Ticket <> '' and
            i.Sum_Amount_IncludingVAT <> 0))


    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (Insert Segments for Invoice Line)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    -- Get RecordKey for Invoice
    update tmp
        set tmp.RecordKey =  tmp.SalesDocumentCode + '-' + convert(varchar(10), case
            when l.[principal_ticket] = ''
                then l.[Line_No_]
            else (select min(l2.[Line_No_])
                    from dbo.Nav_Sales_Invoice_Line l2
                where l2.InternalExtractID = l.InternalExtractID and
                        l2.TravelProductID = l.TravelProductID and
                        l2.[ticket]=l.[principal_ticket] and
                        l2.[Document_No_]=l.[Document_No_] and
                        l2.[Service_Group] in (1, 6) and --AIR  Low Cost
                        l2.[ticket_type] = l.[ticket_type] and
                        l2.[resource_type]= 0) -- Principal
        end)
        from dbo.#Segments tmp
             inner join
             dbo.[Nav_Sales_Invoice_Line] l on tmp.InternalExtractID = l.InternalExtractID and
                                               tmp.TravelProductID = l.TravelProductID and
                                               tmp.SalesDocumentCode = l.[Document_No_] and
                                               tmp.SalesDocumentLineNbr = l.Line_No_

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (Get RecordKey for Invoice)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


   -- Credits (Nav_Sales_Cr_Memo_%)
    ;with Credit as (select h.InternalExtractID,
                            h.No_ as SalesDocumentCode,
                            l.Line_No_ as SalesDocumentLineNbr,
                            h.TravelProductID,
                            l.Ticket,
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
                                      t.Ticket_No = l.Ticket))
        -- WITH Credit CTE
        insert into
            #Segments(InternalExtractID,
                      SalesDocumentCode,
                      SalesDocumentLineNbr,
                      TravelProductID,
                      SalesDocumentTypeID,
                      BookingTypeID,
                      AgentAssistedBool,
                      SaleDocumentCodePrior)
            select c.InternalExtractID,
                   c.SalesDocumentCode,
                   c.SalesDocumentLineNbr,
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
               @MsgParm1    = cast(@Error as varchar(12)) + ' (Insert Segments for Sales Credit Line)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- Get RecordKey for Credit Memo
    update tmp
        set tmp.RecordKey =  tmp.SalesDocumentCode + '-' + convert(varchar(10), case
            when l.[principal_ticket] = ''
                then l.[Line_No_]
            else (select min(l2.[Line_No_])
                    from dbo.Nav_Sales_Cr_Memo_Line l2
                where l2.[ticket]=l.[principal_ticket] and
                    l2.[Document_No_]=l.[Document_No_] and
                    l2.[Service_Group] in (1, 6)  --AIR  Low Cost
                    and l2.[ticket_type] = l.[ticket_type] and
                    l2.[resource_type]= 0   -- Principal
                    and l2.TravelProductID = l.TravelProductID)
        end)
        from dbo.#Segments tmp
             inner join
             dbo.Nav_Sales_Cr_Memo_Line l on tmp.InternalExtractID = l.InternalExtractID and
                                             tmp.TravelProductID = l.TravelProductID and
                                             tmp.SalesDocumentCode = l.[Document_No_] and
                                             tmp.SalesDocumentLineNbr = l.Line_No_ and
                                             tmp.InternalExtractID = l.InternalExtractID 

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (Get RecordKey for Credit)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- Get BookingTypeID for Credit
  update tmp
         set tmp.BookingTypeID = af.BookingTypeID
    from dbo.#Segments tmp
         join dbo.InternalExtractEUAirFact af on
              af.InternalExtractID = tmp.InternalExtractID and
              af.TravelProductID = tmp.TravelProductID and
              af.RecordKey = tmp.RecordKey
   where tmp.BookingTypeID is null


    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (Get BookingTypeID for Credit)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end
    

    set dateformat 'dmy'


select         tle.TravelProductID,
               tle.Entry_No_,
               tle.Ticket_No,
               tle.Document_No_,
               ltrim(rtrim(tle.Classe)) as Classe,
               tle.No_Segment as No_Segment,
               tle.Departure_date,
               tle.Meta_ID,
               tle.Traveller_Code,
               tle.Billing_Unit_Entry_No_,
               tle.Transmission_number,
               tle.Fligh_Nbr,
               tle.Arrival_Date as Arrival_Date_String,
               cast(null as datetime) as Arrival_Date,
               cast(null as datetime) as ArrivalDateTime,
               tle.Arrival_Time,
               tle.Fly_Duration,
               cast(null as int) as FlightDurationMinutes,
               tle.Fare_Basis,
               tle.CabinClassID,
               tle.Corporation,
               tle.Tkt_Point_Mileage,
               tle.[Type],
               case -- Date part of DateTime
                   when tle.Departure_date between dateadd(year, -5, getdate()) and dateadd(year, 5, getdate()) -- Anything outside this range is most-definitely wrong
                        then tle.Departure_date
                   when tle.Issued_Date between dateadd(year, -5, getdate()) and dateadd(year, 5, getdate())
                        then tle.Issued_Date
                   else null
               end + 
               case -- Time part of DateTime
                   when (tle.Departure_Time like '[0-1][0-9][0-5][0-9]' or tle.Departure_Time like '2[0-3][0-5][0-9]')
                        then 
                            dateadd(minute, convert(integer, left(tle.Departure_Time, 2) * 60 +
                                     substring(tle.Departure_Time, 3, 2)), 0)
                       else dateadd(hour, 12, 0)
               end as DepartureDateTime,
               case
                   when len(tle.Departure_City_Code) > 3 
                        --or tle.Departure_City_Code = tle.Arrival_City_Code
                            then '---'
                   else tle.Departure_City_Code
               end as AirportCodeFrom,

               case
                   when len(tle.Arrival_City_Code) > 3
                        --or tle.Departure_City_Code = tle.Arrival_City_Code
                            then '---'
                   else tle.Arrival_City_Code
               end as AirportCodeTo,
               cast(0 as bit) as ConnectionBool,
               tle.ID_1,
               tle.[C_R_S],
               tle.Stopver_Indicator,
               substring(tle.[Corporation], 1, 3) as [Marketing_Carrier],
               substring(tle.[Operating_Carrier], 1, 3) as [Operating_Carrier],
               convert(tinyint, 0) as [AirFareFlexibilityID]
into #Travel_Ledger_Entry
            from dbo.Nav_Travel_Ledger_Entry tle
        where tle.InternalExtractID = @pInternalExtractID and
              exists(select * 
                       from dbo.InternalExtractEUAirFact af 
                      where tle.InternalExtractID = af.InternalExtractID and
                            tle.Document_No_ = af.SalesDocumentCode)
order by TravelProductID, Document_No_

    create clustered index ix_tmpTravel_Ledger_Entry__TravelProductID_Document_No_
    on #Travel_Ledger_Entry (TravelProductID, Document_No_)

update #Travel_Ledger_Entry
set Arrival_Date = case when isdate(Arrival_Date_String + cast(year(DepartureDateTime) as varchar)) = 1
                    then convert(datetime, Arrival_Date_String + cast(year(DepartureDateTime) as varchar))
                    else DepartureDateTime
               end

update #Travel_Ledger_Entry
set ArrivalDateTime = case -- Arrival_Time should be a 4-digit number that matches an HHMM format.
                      when Arrival_Time like '[0-1][0-9][0-5][0-9]' or
                           Arrival_Time like '2[0-3][0-5][0-9]' then 
                           dateadd(minute, convert(integer, left(Arrival_Time, 2)) * 60 +
                       convert(integer, substring(Arrival_Time, 3, 2)),
                       Arrival_Date
                       )
                   else dateadd(hour, 1, DepartureDateTime)
                   end,
    FlightDurationMinutes = case -- Duration should be a 4-digit number that matches an HHMM format.
                   when Fly_Duration like '[0-1][0-9][0-5][0-9]' or
                        Fly_Duration like '2[0-3][0-5][0-9]' then 
                           convert(integer, left(Fly_Duration, 2)) * 60 + convert(integer, substring(Fly_Duration, 3, 2))
                   else null
               end

update #Travel_Ledger_Entry
-- Because there is no arrival year, we use DepartureDateTime's year.
-- In some cases ArrivalDateTime may be <= DepartureDateTime 
--      when crossing time zones (Example: HEL to ARN is a 55-minute flight)
--      when crossing international date line (Example: NRT to SEA is about 12 hours and will arrive hours before it departs)
-- ArrivalDateTime should add one year if travel takes place over the new year (Departure in December and Arrival in January)
set ArrivalDateTime = case when ArrivalDateTime <= DepartureDateTime and 
                            year(DepartureDateTime) < year(dateadd(minute, FlightDurationMinutes, DepartureDateTime)) -- Duration takes it into a new year
                        then dateadd(year, 1,ArrivalDateTime)
                     else ArrivalDateTime
                end

    -- Get ConnectionBool
    update tle
     --set ConnectionBool = case when tas.Stopver_Indicator = 'X' 
     set ConnectionBool = case when tle.Stopver_Indicator = 'X' 
                            then 1
                            else 0
                         end
    from #Travel_Ledger_Entry tle
    join dbo.Nav_Sales_Invoice_Line l on
         l.TravelProductID = tle.TravelProductID and
         l.Document_No_ = tle.Document_No_ and
         l.Ticket = tle.Ticket_No
    -- Table dbo.Nav_Ticketed_Air_Segment___H_ no longer available
    --cross apply (select top 1 tas2.Stopver_Indicator 
    --               from dbo.Nav_Ticketed_Air_Segment___H_ tas2
    --              where tas2.Code_PNR = l.ID_2 and
    --                    tas2.File_Name = l.ID_1 and
    --                    tas2.Transmission_Nbr = tle.Transmission_number and
    --                    tas2.Seg_Nbr_PRN = tle.No_Segment and
    --                    tas2.TravelProductID = l.TravelProductID
    --              order by tas2.InternalExtractID desc
    --        ) tas
    where l.InternalExtractID = @pInternalExtractID
    
    update tle
    --set ConnectionBool = case when tas.Stopver_Indicator = 'X' 
    set ConnectionBool = case when tle.Stopver_Indicator = 'X' 
                            then 1
                            else 0
                         end
    from #Travel_Ledger_Entry tle
    join dbo.Nav_Sales_Cr_Memo_Line l on
         l.TravelProductID = tle.TravelProductID and
         l.Document_No_ = tle.Document_No_ and
         l.Ticket = tle.Ticket_No
    -- Table dbo.Nav_Ticketed_Air_Segment___H_ no longer available
    --cross apply (select top 1 tas2.Stopver_Indicator 
    --               from dbo.Nav_Ticketed_Air_Segment___H_ tas2
    --              where tas2.Code_PNR = l.ID_2 and
    --                    tas2.File_Name = l.ID_1 and
    --                    tas2.Transmission_Nbr = tle.Transmission_number and
    --                    tas2.Seg_Nbr_PRN = tle.No_Segment and
    --                    tas2.TravelProductID = l.TravelProductID
    --              order by tas2.InternalExtractID desc
    --        ) tas
    where l.InternalExtractID = @pInternalExtractID

    -- Flexibility Logic - #flex_level_1

    select
        fr.TravelProductID
    ,   fr.AirlineCode
    ,   fr.AirFareBasisCode
--  ,   coalesce(f.AirFareFlexibilityName, 'Unknown') as [AirFareFlexibilityName]
    ,   coalesce(fr.AirFareFlexibilityID, 0) as [AirFareFlexibilityID]
    ,   p.[Priority]
    ,   count(1) as [Count]
    ,   convert(smallint, 0) as [IsMaxCntBool]
    ,   newid() as [UniqueID]
    into #flex_level_1
    from [dbo].[AirFareFlexibilityRulesDim] fr
--  left outer join [dbo].[AirFareFlexibilityDim] f
--      on f.[AirFareFlexibilityID] = fr.AirFareFlexibilityID
--      and f.[LangID] = 1033
    left outer join @AirFareFlexPriority p
        on p.AirFareFlexibilityID = fr.AirFareFlexibilityID
    group by
        fr.TravelProductID
    ,   fr.AirlineCode
    ,   fr.AirFareBasisCode
--  ,   coalesce(f.AirFareFlexibilityName, 'Unknown')
    ,   coalesce(fr.AirFareFlexibilityID, 0)
    ,   p.[Priority]

    create index ix1 on #flex_level_1 (AirFareBasisCode, AirlineCode, TravelProductID, UniqueID)

    update fl1 set
        fl1.[IsMaxCntBool] = un.[IsMaxCntBool]
    from #flex_level_1 fl1
    inner join (
        select
            TravelProductID
        ,   AirlineCode
        ,   AirFareBasisCode
        ,   UniqueID
        ,   row_number() over (partition by TravelProductID, AirlineCode, AirFareBasisCode order by [Count] desc, [Priority] asc) as [IsMaxCntBool]
        from #flex_level_1
        ) un
        on un.TravelProductID = fl1.TravelProductID
        and un.AirlineCode = fl1.AirlineCode
        and un.AirFareBasisCode = fl1.AirFareBasisCode
        and un.UniqueID = fl1.UniqueID

    delete from #flex_level_1 where IsMaxCntBool <> 1

    -- Flexibility Logic - #flex_level_2

    select
        fl1.AirlineCode
    ,   fl1.AirFareBasisCode
--  ,   fl1.[AirFareFlexibilityName]
    ,   fl1.[AirFareFlexibilityID]
    ,   fl1.[Priority]
    ,   count(1) as [Count]
    ,   convert(smallint, 0) as [IsMaxCntBool]
    ,   newid() as [UniqueID]
    into #flex_level_2
    from #flex_level_1 fl1
    group by
        fl1.AirlineCode
    ,   fl1.AirFareBasisCode
--  ,   fl1.[AirFareFlexibilityName]
    ,   fl1.[AirFareFlexibilityID]
    ,   fl1.[Priority]

    create index ix1 on #flex_level_2 (AirFareBasisCode, AirlineCode, UniqueID)

    update fl2 set
        fl2.[IsMaxCntBool] = un.[IsMaxCntBool]
    from #flex_level_2 fl2
    inner join (
        select
            AirlineCode
        ,   AirFareBasisCode
        ,   UniqueID
        ,   row_number() over (partition by AirlineCode, AirFareBasisCode order by [Count] desc, [Priority] asc) as [IsMaxCntBool]
        from #flex_level_2
        ) un
        on un.AirlineCode = fl2.AirlineCode
        and un.AirFareBasisCode = fl2.AirFareBasisCode
        and un.UniqueID = fl2.UniqueID

    delete from #flex_level_2 where IsMaxCntBool <> 1

    -- Flexibility Logic - #flex_level_3
    select
        fl2.AirlineCode
    ,   left(fl2.AirFareBasisCode, 1) as [ClassOfServiceCode]
--  ,   fl2.[AirFareFlexibilityName]
    ,   fl2.[AirFareFlexibilityID]
    ,   fl2.[Priority]
    ,   count(1) as [Count]
    ,   convert(smallint, 0) as [IsMaxCntBool]
    ,   newid() as [UniqueID]
    into #flex_level_3
    from #flex_level_2 fl2
    group by
        fl2.AirlineCode
    ,   left(fl2.AirFareBasisCode, 1)
--  ,   fl2.[AirFareFlexibilityName]
    ,   fl2.[AirFareFlexibilityID]
    ,   fl2.[Priority]

    create index ix1 on #flex_level_3 (AirlineCode, ClassOfServiceCode, UniqueID)

    update fl3 set
        fl3.[IsMaxCntBool] = un.[IsMaxCntBool]
    from #flex_level_3 fl3
    inner join (
        select
            AirlineCode
        ,   ClassOfServiceCode
        ,   UniqueID
        ,   row_number() over (partition by AirlineCode, ClassOfServiceCode order by [Count] desc, [Priority] asc) as [IsMaxCntBool]
        from #flex_level_3
        ) un
        on un.AirlineCode = fl3.AirlineCode
        and un.ClassOfServiceCode = fl3.ClassOfServiceCode
        and un.UniqueID = fl3.UniqueID

    delete from #flex_level_3 where IsMaxCntBool <> 1

    -- Flexibility Logic - TLE udate
    update tle set
        AirFareFlexibilityID = coalesce(
            case when isnumeric(left(tle.Meta_ID, 2)) = 1 and tle.[C_R_S] = 'LOWCOST' and left(tle.[ID_1], 3) = 'LOW' then 100 end
        ,   fl1.AirFareFlexibilityID
        ,   fl2.AirFareFlexibilityID
        ,   fl3.AirFareFlexibilityID
        ,   0)
    from #Travel_Ledger_Entry tle
    left outer join #flex_level_1 fl1
        on fl1.TravelProductID = tle.TravelProductID
        and fl1.AirlineCode = convert(char(3), case when datalength(tle.Corporation) <= 3 then tle.Corporation else '00' end)
        and fl1.AirFareBasisCode = ltrim(rtrim(tle.Fare_Basis))
    left outer join #flex_level_2 fl2
        on fl2.AirlineCode = convert(char(3), case when datalength(tle.Corporation) <= 3 then tle.Corporation else '00' end)
        and fl2.AirFareBasisCode = ltrim(rtrim(tle.Fare_Basis))
    left outer join #flex_level_3 fl3
        on fl3.AirlineCode = convert(char(3), case when datalength(tle.Corporation) <= 3 then tle.Corporation else '00' end)
        and fl3.ClassOfServiceCode = left(ltrim(rtrim(tle.Fare_Basis)), 1)

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (Update AirFareFlexibilityID #Travel_Ledger_Entry)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end
        
    insert dbo.InternalExtractEUAirSegmentFact 
                (InternalExtractID,
                 SalesDocumentCode,
                 SalesDocumentLineNbr,
                 RecordKey,
                 BookingTypeID,
                 SegmentNbr,
                 SegmentNbrAdj,
                 TripNbr,
                 SequenceNbr,
                 AirlineCode,
                 FlightNumberTxt,
                 ClassOfServiceCode,
                 CabinClassID,
                 SegmentDateStart,
                 SegmentDateEnd,
                 AirportCodeFrom,
                 AirportCodeTo,
                 WithinCountryBool,
                 MileageCntGDS,
                 MileageCnt,
                 AirFareBasisCode,
                 IncrementCnt,
                 ConnectionBool,
                 CurrencyCode,
                 AirlineCodeMarketing,
                 AirlineCodeOperating,
                 AirFareFlexibilityID)
        select
            s.InternalExtractID,
            s.SalesDocumentCode,
            s.SalesDocumentLineNbr,
            f.RecordKey,
            f.BookingTypeID,
            -- SegmentNbr based on order entry of Sales Line Item and Travel Entry_No
            case
                when row_number() over (partition by f.RecordKey order by l.Line_No_, tle.Entry_No_) > 255
                then 255
                else row_number() over (partition by f.RecordKey order by l.Line_No_, tle.Entry_No_)
            end                   as SegmentNbr,

            -- SegmentNbrAdj based on DepartureDateTime with Sales Line Item and Travel Entry_No as tie breakers
            -- ToDo: Fix; This fails when traveling over international date line (example: SYD --> LAX). Using Segment_Nbr for now.
            -- case when row_number() over (partition by f.RecordKey order by tle.DepartureDateTime, l.Line_No_, tle.Entry_No_) > 255 then 255
            --else row_number() over (partition by f.RecordKey order by tle.DepartureDateTime, l.Line_No_, tle.Entry_No_) end as SegmentNbrAdj,
            case
                when row_number() over (partition by f.RecordKey order by tle.No_Segment, l.Line_No_, tle.Entry_No_) > 255
                then 255
                else row_number() over (partition by f.RecordKey order by tle.No_Segment, l.Line_No_, tle.Entry_No_)
            end                   as SegmentNbrAdj,

           1 as TripNbr, -- Updated in InternalExtractEUAirFlightFactLoad
           1 as SequenceNbr, -- Updated in InternalExtractEUAirFlightFactLoad
           case when datalength(tle.Corporation) <= 3
                then tle.Corporation
                else null end as AirlineCode,
           tle.Fligh_Nbr as FlightNumberTxt,
           CASE WHEN (tle.Classe IN ('-B', '-E', '-F', '-P')) 
                    THEN '-' 
                    ELSE substring(tle.classe, 1, 1) 
           END as ClassOfServiceCode,
           -- Define CabinClassID :
           CASE WHEN (tle.classe = '-B') THEN 2 
                WHEN (tle.classe = '-E') THEN 3 
                WHEN (tle.classe = '-F') THEN 1 
                WHEN (tle.classe = '-P') THEN 5 
                ELSE
                    CASE WHEN tle.Corporation = 'AF' THEN -- Add of some AF custom logic here, please see : https://confluence.expedia.biz/display/ECT/Air+France+%28AF%29+Cabin+Class+-+Fix+Proposal
                        CASE WHEN isnull(c.CountryCode, '---') + isnull(d.CountryCode, '---') = 'FRAFRA' THEN 3 --Eco -- Rule 1
                        WHEN substring(tle.classe, 1, 1) in ('A','S','W','P','F') AND (
                            ((isnull(c.CountryCode, '---') = 'FRA') AND isnull(d.CountryCode, '---') in ('ALB', 'AND', 'ARM', 'AUT',
                                                                                                         'AZE', 'BLR', 'BEL', 'BIH',
                                                                                                         'BGR', 'HRV', 'CZE', 'DNK',
                                                                                                         'EST', 'FRO', 'FIN', 'GEO',
                                                                                                         'DEU', 'GIB', 'GRC', 'HUN',
                                                                                                         'ISL', 'IRL', 'ITA', 'LVA',
                                                                                                         'LIE', 'LTU', 'LUX', 'MLT',
                                                                                                         'MDA', 'MCO', 'NLD', 'NOR',
                                                                                                         'POL', 'PRT', 'MKD', 'ROU',
                                                                                                         'RUS', 'SMR', 'SCG', 'SVK',
                                                                                                         'SVN', 'ESP', 'SJM', 'SWE',
                                                                                                         'CHE', 'TUR', 'UKR', 'GBR',
                                                                                                         'MNE', 'SRB', 'VAT', 'GRL',
                                                                                                         'CYP', 'ISR', 'DZA', 'MAR',
                                                                                                         'TUN'))
                         OR ((isnull(d.CountryCode, '---') = 'FRA') and isnull(c.CountryCode, '---') in ('ALB', 'AND', 'ARM', 'AUT',
                                                                                                         'AZE', 'BLR', 'BEL', 'BIH',
                                                                                                         'BGR', 'HRV', 'CZE', 'DNK',
                                                                                                         'EST', 'FRO', 'FIN', 'GEO',
                                                                                                         'DEU', 'GIB', 'GRC', 'HUN',
                                                                                                         'ISL', 'IRL', 'ITA', 'LVA',
                                                                                                         'LIE', 'LTU', 'LUX', 'MLT',
                                                                                                         'MDA', 'MCO', 'NLD', 'NOR',
                                                                                                         'POL', 'PRT', 'MKD','ROU',
                                                                                                         'RUS', 'SMR', 'SCG', 'SVK',
                                                                                                         'SVN', 'ESP','SJM', 'SWE',
                                                                                                         'CHE', 'TUR', 'UKR', 'GBR',
                                                                                                         'MNE','SRB', 'VAT', 'GRL',
                                                                                                         'CYP', 'ISR', 'DZA', 'MAR',
                                                                                                         'TUN'))
                            )
                            THEN 3 --Eco
                        WHEN substring(tle.classe, 1, 1) in ('A','S','W')
                            AND isnull(c.CountryCode, '---') + isnull(d.CountryCode, '---') <> 'FRAFRA'
                            THEN 5 --Premium Eco
                        WHEN substring(tle.classe, 1, 1) in ('P','F')
                            AND isnull(c.CountryCode, '---') + isnull(d.CountryCode, '---') <> 'FRAFRA'
                            THEN 1 --First
                        ELSE coalesce((select CabinClassID
                                 from dbo.ClassOfServiceDim SubQ 
                                where SubQ.AirlineCode = tle.Corporation and 
                                      SubQ.ClassOfServiceCode = substring(tle.classe, 1, 1)), 0) END
                    ELSE 
                        coalesce((select CabinClassID 
                                 from dbo.ClassOfServiceDim SubQ 
                                where SubQ.AirlineCode = tle.Corporation and 
                                      SubQ.ClassOfServiceCode = substring(tle.classe, 1, 1)), 0) 
                    END                  
          END as CabinClassID,
           tle.DepartureDateTime as SegmentDateStart,
           tle.ArrivalDateTime as SegmentDateEnd,
           tle.AirportCodeFrom,
           tle.AirportCodeTo,
           case when c.CountryCode = d.CountryCode 
                    then 1
                    else 0
                end as WithinCountryBool,
           Tkt_Point_Mileage as MileageCntGDS,
           (select top 1 Mileage
              from dbo.MileageFromLatLongAsTable(c.Latitude, c.Longitude, d.Latitude, d.Longitude)) *
               case
                   when f.BookingTypeID in (@BookingTypeIDVoid, @BookingTypeIDRefund, @BookingTypeIDPRefund) then -1.0
                   else 1.0
               end as MileageCnt,

           ltrim(rtrim(tle.Fare_Basis)), -- Populate_First_Facts is using Sales_Invoice_Line.Code_tarif when it 
                                         -- should be using Travel_Ledger_Entry.[Fare Basis]
                                         -- Related bug: https://jira/jira/browse/EGE-70173
           f.IncrementCnt,
           coalesce(tle.ConnectionBool, 0),
           f.CurrencyCode,
           case when datalength(tle.Corporation) <= 3
                then tle.Corporation
                else '00' end as AirlineCodeMarketing,
            case when datalength(tle.[Operating_Carrier]) between 1 and 3
                then ltrim(rtrim(tle.[Operating_Carrier]))
                else case when datalength(tle.Corporation) <= 3 -- Set AirlineCodeOperating = AirlineCodeMarketing if Operating_Carrier has no value
                    then tle.Corporation
                    else '00' end
                end as AirlineCodeOperating,
           tle.AirFareFlexibilityID
     from dbo.InternalExtractEUAirFact f
          inner join 
          dbo.#Segments s on
              f.RecordKey = s.RecordKey and
              f.TravelProductID = s.TravelProductID
          left join 
          dbo.Nav_Sales_Line(@pInternalExtractID) l on
              f.TravelProductID = l.TravelProductID and
              f.InternalExtractID = l.InternalExtractID and
              s.SalesDocumentCode = l.Document_No_ and
              s.SalesDocumentLineNbr = l.Line_No_ and
              l.Resource_Type = 0  and -- Primary Resource
              l.Service_Group in (1, 6) and -- Air & Lowcost
              l.Ticket_Type in (1, 2, 3) -- BookingType = Issued,Repaid,Canceled
          left join 
          #Travel_Ledger_Entry tle on
          --cteTravelLedgerEntry tle on
              f.TravelProductID = tle.TravelProductID and
              f.SalesDocumentCode = tle.Document_No_ and
              l.Ticket = tle.Ticket_No and
              -- Need additional matching on one of the following: 
              l.Traveller_Code = tle.Traveller_Code and
              -- Need more matches when the same traveler has multiple trips (Examples: FRI10596035, GEI1140119)
              l.ID_1 = tle.ID_1 and
              l.Ticket_Type = tle.[Type]
              --(tle.ID_1 = l.ID_1 or tle.Meta_ID = l.Meta_ID or tle.Billing_Unit_Entry_No_ = l.Entry_No_)
              -- f.MetaDossierID = tle.Meta_ID -- Fail on AUSI140029983
              -- l.Entry_No_ = tle.Billing_Unit_Entry_No_ -- Fails on chargeback

           left join
           dbo.AirportDim c on
               tle.AirportCodeFrom = c.AirportCode and c.LangID = 1033
           left join
           dbo.AirportDim d on
               tle.AirportCodeTo = d.AirportCode and d.LangID = 1033

    where f.InternalExtractId = @pInternalExtractID and
          f.FactRecordStatusID = 1

    -- ToDo: Document the location of original source code for this calculation

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (insert dbo.InternalExtractEUAirSegmentFact)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end
    
    ------ Update GeographyTypeID ------
    -- 1st Priority: When "Match To" and "Match From" THEN GeographyTypeID
    -- 2nd Priority: When a "Match To" and a "Match From" (Two joins) THEN Lowest Pri GeographyTypeID
    -- 3rd Priority: No "Match To" but there is a "Match From" -- Lower of EmptyString/Match
    -- 4th Priority: No "Match From" but there is a "Match To" -- Lower of EmptryString/Match
    -- 5th Priority: CountryCode = ''

    -- CTE/UPDATE statement for update to two related columns.
    ;with cteGeographyType as (
        select a.GeographyTypeID,
               case  when k.GeographyTypeID is not null then k.GeographyTypeID --- Highest priority for City Pair
                   else                                
                       case when f.PriorityOrderNbr  is not null and h.PriorityOrderNbr is not null then
                           case when f.PriorityOrderNbr < h.PriorityOrderNbr then f.GeographyTypeID
                                   else h.GeographyTypeID
                           end  -- 2nd Priority  
                           when f.PriorityOrderNbr is null and h.PriorityOrderNbr is not null then
                           case when j.PriorityOrderNbr < h.PriorityOrderNbr then j.GeographyTypeID
                               else f.PriorityOrderNbr
                               end -- 3rd Priority
                           when f.PriorityOrderNbr is not null and h.PriorityOrderNbr is null then
                           case when j.PriorityOrderNbr < f.PriorityOrderNbr then j.GeographyTypeID
                               else h.PriorityOrderNbr
                               end -- 4th Priority
                       else j.GeographyTypeID -- 5th Priority
                       end                          
               end as NewGeographyTypeID 
        from dbo.InternalExtractEUAirSegmentFact a
        inner join dbo.InternalExtractEUAirFact b on
            a.InternalExtractID = b.InternalExtractID and 
            a.SalesDocumentCode = b.SalesDocumentCode and
            a.RecordKey = b.RecordKey
        inner join dbo.AirportDim c on
            a.AirportCodeFrom = c.AirportCode and 
            c.LangID = 1033
        inner join dbo.AirportDim d on
            a.AirportCodeTo = d.AirportCode and 
            d.LangID = 1033
        left join dbo.GeographyTypeTravelProductCountry e on
            e.TravelProductID = b.TravelProductID and 
            e.LineofBusinessID = 1 /* Air */ and
            d.countrycode = e.Countrycode
        left join dbo.GeographyTypeTravelProduct f on
            f.TravelProductID = e.TravelProductID and 
            f.GeographyTypeID = e.GeographyTypeID and
            f.LineofBusinessID = e.LineofBusinessID 
        left join dbo.GeographyTypeTravelProductCountry g on
            g.TravelProductID = b.TravelProductID and 
            g.LineofBusinessID = 1 /* Air */ and
            c.countrycode = g.Countrycode
        left join dbo.GeographyTypeTravelProduct h on
            h.TravelProductID = g.TravelProductID and 
            h.GeographyTypeID = g.GeographyTypeID and
            h.LineofBusinessID = g.LineofBusinessID 
        left join dbo.GeographyTypeTravelProductCountry i on
            i.TravelProductID = b.TravelProductID and
            i.CountryCode = '' and
            i.LineofBusinessID = 1 /* Air */ 
        left join dbo.GeographyTypeTravelProduct j on
            j.TravelProductID = i.TravelProductID and
            j.GeographyTypeID = i.GeographyTypeID and 
            j.LineOfBusinessID = i.LineOfBusinessID
        left join dbo.GeographyTypeTravelProductCountry k on
            k.TravelProductID = b.TravelProductID and
            k.CountryCode = c.CountryCode and
            k.CountryCodeDestination = d.CountryCode and
            k.LineofBusinessID = 1 /* Air */
        left join dbo.GeographyTypeTravelProduct SortMatchOnEverything on
            SortMatchOnEverything.TravelProductID = k.TravelProductID and
            SortMatchOnEverything.GeographyTypeID = k.GeographyTypeID and 
            SortMatchOnEverything.LineOfBusinessID = k.LineOfBusinessID  
        where a.InternalExtractID = @pInternalExtractID and
              b.FactRecordStatusID = 1
    )
    update cteGeographyType
       set GeographyTypeID = NewGeographyTypeID


    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (Update GeographyTypeID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- Update travel start/end with time zone based on location OAG and calculate flight duration - EGE-96664
    update a
        set SegmentDateStartUTCOffset = convert(datetimeoffset(0), convert(varchar(19), a.SegmentDateStart, 121) + 
                case when isnull(d.UTCOffsetMinuteCnt, bt.UTCOffsetMinuteCnt) < 0 then '-' else '+' end + 
                RIGHT('0' + CAST((abs(isnull(d.UTCOffsetMinuteCnt, bt.UTCOffsetMinuteCnt)) / 60) AS VARCHAR(2)), 2) + ':' + 
                RIGHT('0' + CAST((abs(isnull(d.UTCOffsetMinuteCnt, bt.UTCOffsetMinuteCnt)) % 60) AS VARCHAR(2)), 2)),   -- SegmentDateStartUTCOffset            
            SegmentDateEndUTCOffset = convert(datetimeoffset(0), convert(varchar(19), a.SegmentDateEnd, 121) + 
                case when isnull(e.UTCOffsetMinuteCnt, ct.UTCOffsetMinuteCnt) < 0 then '-' else '+' end + 
                RIGHT('0' + CAST((abs(isnull(e.UTCOffsetMinuteCnt, ct.UTCOffsetMinuteCnt)) / 60) AS VARCHAR(2)), 2) + ':' + 
                RIGHT('0' + CAST((abs(isnull(e.UTCOffsetMinuteCnt, ct.UTCOffsetMinuteCnt)) % 60) AS VARCHAR(2)), 2))    -- SegmentDateEndUTCOffset    
    from 
    dbo.InternalExtractEUAirSegmentFact a
        inner join 
        dbo.vLocationOAG b on a.AirportCodeFrom = b.LocationCode
        inner join 
        dbo.vLocationOAG c on a.AirportCodeTo = c.LocationCode
        inner join 
        dbo.vTimeZoneOAG bt on b.TimeZoneOAGID = bt.TimeZoneOAGID
        inner join 
        dbo.vTimeZoneOAG ct on c.TimeZoneOAGID = ct.TimeZoneOAGID
        left join 
        dbo.vTimeZoneOAGDaylightSavingsOffset d on b.TimeZoneOAGID = d.TimeZoneOAGID and a.SegmentDateStart between d.EffectiveDateBegin and d.EffectiveDateEnd
        left join 
        dbo.vTimeZoneOAGDaylightSavingsOffset e on c.TimeZoneOAGID = e.TimeZoneOAGID and a.SegmentDateEnd between e.EffectiveDateBegin and e.EffectiveDateEnd
    where 
        a.InternalExtractID = @pInternalExtractID      -- For IE 39384 = 630; For IE 39365 = 20770 Vs 20768 (2 Airport codes are "---"
   
    
    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (Update SegmentDate Start/End as UTC)' 
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    update a 
        set SegmentDurationMinuteCnt = case when abs(DATEDIFF(DAY, SegmentDateStartUTCOffset, SegmentDateEndUTCOffset)) < 22 then 
                DATEDIFF(minute, SegmentDateStartUTCOffset, SegmentDateEndUTCOffset) 
                else null end * 
                    case when BookingTypeID in (@BookingTypeIDVoid, @BookingTypeIDRefund, @BookingTypeIDPRefund)
                        then -1.0
                        else 1.0
                    end
    from 
    dbo.InternalExtractEUAirSegmentFact a
    where 
        a.InternalExtractID = @pInternalExtractID 
    
    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (Update SegmentDurationMinuteCnt)' 
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end
    
    -- Mark incomplete rows
    update af
    set af.FactRecordStatusID = 10 -- Missing Extension Data
    from dbo.InternalExtractEUAirFact af
    join dbo.InternalExtractEUAirSegmentFact asf on 
         asf.InternalExtractID = af.InternalExtractID and
         asf.RecordKey = af.RecordKey
    where asf.AirportCodeFrom is null or asf.AirportCodeTo is null

    delete asf
    from dbo.InternalExtractEUAirSegmentFact asf
    where asf.AirportCodeFrom is null or asf.AirportCodeTo is null

    
    -- Add RouteID
    -- Insert new routes into dimension table
    insert into dbo.RouteDim (RouteCode, AirportCodeFrom, AirportCodeTo, RouteCodeBidirectional, AirportCodeFromBidirectional, AirportCodeToBidirectional, UpdateDate, LastUpdatedBy )
    select distinct
            upper(a.AirportCodeFrom) + ':' + upper(a.AirportCodeTo) as RouteCode,
            upper(a.AirportCodeFrom), 
            upper(a.AirportCodeTo),
            case when upper(a.AirportCodeFrom) < upper(a.AirportCodeTo) then upper(a.AirportCodeFrom) + ':' + upper(a.AirportCodeTo)
                else upper(a.AirportCodeTo) + ':' + upper(a.AirportCodeFrom) end as RouteCodeBidirectional,
            case when a.AirportCodeFrom < a.AirportCodeTo then upper(a.AirportCodeFrom)
                else upper(a.AirportCodeTo) end as AirportCodeFromBidirectional,
            case when a.AirportCodeFrom < a.AirportCodeTo then upper(a.AirportCodeTo)
                else upper(a.AirportCodeFrom) end as AirportCodeToBidirectional,
            UpdateDate = @Current_Timestamp,
            LastUpdatedBy = 'IDAirFactLoadRoute'
        from dbo.InternalExtractEUAirSegmentFact a 
            left join
            dbo.RouteDim b on
                a.AirportCodeTo = b.AirportCodeTo and
                a.AirportCodeFrom = b.AirportCodeFrom
        where a.InternalExtractID = @pInternalExtractID and
              b.AirportCodeTo is null

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (insert RouteDim)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- Update fact table with route id 
    update a
       set a.RouteID = b.RouteID
      from dbo.InternalExtractEUAirSegmentFact a
           inner join    
           dbo.RouteDim b  on
               a.AirportCodeTo = b.AirportCodeTo and
               a.AirportCodeFrom = b.AirportCodeFrom
     where a.InternalExtractID = @pInternalExtractID

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
                @MsgParm1    = cast(@Error as varchar(12)) + ' (update InternalExtractAirSegmentFact RouteID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


if ((@TrancountSave = 0)
    and (@TranStartedBool = @TRUE))
    and @@trancount > 0
begin
    commit transaction @SavePointName
end

goto ExitProc

---------------------------------------------------------------------
-- Error Handler
---------------------------------------------------------------------
ErrorHandler:
if (@TranStartedBool = @TRUE)
begin
    rollback transaction @SavePointName
end
select @ExitCode = @RC_FAILURE
goto ExitProc

---------------------------------------------------------------------
-- Exit Procedure
---------------------------------------------------------------------
ExitProc:
return (@ExitCode)
go

-- raiserror ('Executing InternalExtractEUAirSegmentFactLoad', 10, 1)

-- exec [InternalExtractEUAirSegmentFactLoad] @pInternalExtractID = 30657
-- select * from dbo.InternalExtractEUAirSegmentFact sf where sf.InternalExtractID = 30566 and salesdocumentcode = 'AUSI140026188'
-- exec [InternalExtractEUAirSegmentFactLoad] @pInternalExtractID = 30566
-- exec [InternalExtractEUAirSegmentFactLoad] @pInternalExtractID = 30568
-- exec [InternalExtractEUAirSegmentFactLoad] @pInternalExtractID = 30569
-- exec [InternalExtractEUAirSegmentFactLoad] @pInternalExtractID = 30571
-- exec [InternalExtractEUAirSegmentFactLoad] @pInternalExtractID = 30572

/*
    declare @InternalExtractID int = 37712

    select *
    from dbo.InternalExtractEUAirSegmentFact
    where InternalExtractID = @InternalExtractID
    order by SalesDocumentCode, SalesDocumentLineNbr

    begin tran

        delete dbo.InternalExtractEUAirSegmentFact where InternalExtractID = @InternalExtractID
        exec [InternalExtractEUAirSegmentFactLoad] @pInternalExtractID = @InternalExtractID
        
    select *
    from dbo.InternalExtractEUAirSegmentFact
    where InternalExtractID = @InternalExtractID
    order by SalesDocumentCode, SalesDocumentLineNbr

    rollback tran

*/