if object_id('dbo.InternalExtractEUAirFactLoadSegmentDerived') is null begin
    print 'Creating stored procedure InternalExtractEUAirFactLoadSegmentDerived (placeholder)'
    execute('create procedure dbo.InternalExtractEUAirFactLoadSegmentDerived as return 0')
end
go

print 'Altering stored procedure InternalExtractEUAirFactLoadSegmentDerived'
go

alter procedure dbo.InternalExtractEUAirFactLoadSegmentDerived
    @pInternalExtractID int
as

/*
*********************************************************************
Copyright (C) 2014-2018 Expedia, Inc. All rights reserved.

Description:
     Updates InternalExtractEUAirFact with the segment
     derived attributes

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2014-06-12  JaredKo         Created.
    2014-10-14  JaredKo         EGE-72755 - WithinCountryBool is 0 where 
                                 it should be 1 for EU billback air
    2015-03-27  JaredKo         EGE-82881 - Update AdvancePurchaseDays
                                 for exchanges. Adopt code from ExternalFileEUAirFactLoadExpand
                                 (EGE-71173)
    2015-06-15  JaredKo         EGE-87139 - Performance Optimizations
    2015-10-09  minieto         Added update for Air Duration columns
    2018-02-28  jappleberry     EGACP-1954 set AdvancePurchaseID = 99 when AdvancePurchaseDaysCnt < 0
    2018-09-21  pbressan        EGE-213316 Add column Flexibility Logic
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

declare     -- SP specific constants and variables
    @BSIAir tinyint,
    @BookingTypeIDPurchase                tinyint,
    @BookingTypeIDReserve                 tinyint,
    @BookingTypeIDVoid                    tinyint,
    @BookingTypeIDExchange                tinyint,
    @BookingTypeIDRefund                  tinyint,
    @BookingTypeIDPRefund                 tinyint,
    @BookingTypeIDUTT                     tinyint

declare @AirFareFlexPriority table (
        [AirFareFlexibilityID]            tinyint
    ,   [AirFareFlexibilityName]          varchar(50)
    ,   [Priority]                        smallint
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


select   -- SP specific constants and variables
    @BSIAir = 7,
    @BookingTypeIDPurchase                = 1,
    @BookingTypeIDReserve                 = 3,
    @BookingTypeIDVoid                    = 4,
    @BookingTypeIDExchange                = 5,
    @BookingTypeIDRefund                  = 7,
    @BookingTypeIDPRefund                 = 8,
    @BookingTypeIDUTT                     = 9

declare @CabinClassPriority table (CabinClassID tinyint, Priority tinyint)
insert into @CabinClassPriority
select 4, 1 union all
select 1, 2 union all
select 2, 3 union all
select 5, 4 union all
select 3, 5 union all
select 0, 10

insert into @AirFareFlexPriority
    select 100, 'TravelFusion',  -1 union all
    select   0, 'Unknown',        0 union all
    select   1, 'Non-flexible',   1 union all
    select   4, 'Changeable',     2 union all
    select   2, 'Refundable',     3 union all
    select   3, 'Fully-flexible', 4

---------------------------------------------------------------------
-- Validation
---------------------------------------------------------------------
-- None, done by caller
---------------------------------------------------------------------
-- Processing
---------------------------------------------------------------------

select @TrancountSave = @@trancount
if (@TrancountSave = 0) begin transaction @SavePointName
else                    save  transaction @SavePointName
select @TranStartedBool = @TRUE

    select s.RecordKey, SegmentNbrAdj, s.SegmentDateStart, s.SegmentDateEnd, s.TripNbr, AirportCodeFrom, AirportCodeTo
    into #InternalExtractEUAirSegmentFact
    from dbo.InternalExtractEUAirSegmentFact s 
            where s.BookingTypeID <> 99
            and s.InternalExtractID = @pInternalExtractID	
    order by RecordKey, SegmentNbrAdj

    create clustered index ix_s on #InternalExtractEUAirSegmentFact(RecordKey, SegmentNbrAdj)

    -- Get Travel Start/End Dates
    update f
       set f.TravelDateStart = (select top 1 s.SegmentDateStart
                                  from dbo.#InternalExtractEUAirSegmentFact s
                                 where s.RecordKey = f.RecordKey
                                 order by s.SegmentNbrAdj),
           f.TravelDateEnd =   (select top 1 s.SegmentDateEnd
                                  from dbo.#InternalExtractEUAirSegmentFact s
                                 where s.RecordKey = f.RecordKey
                                 order by s.SegmentNbrAdj desc)
      from dbo.InternalExtractEUAirFact f
     where f.InternalExtractID = @pInternalExtractID
    option(recompile)

    update a
       set SaturdayNightStayBool = 
            case
                when datepart(weekday, TravelDateStart) + datediff(dd, TravelDateStart, TravelDateEnd) >= 8
                    then 1
                else 0
            end,
        --AdvancePurchaseDaysCnt = datediff(dd, BookingDate, TravelDateStart)
            AdvancePurchaseDaysCnt = case when a.BookingTypeID = @BookingTypeIDPurchase and ( datediff(dd,coalesce(a.BookingDate,a.IssueDate),a.TravelDateStart) < -365 
                                      or datediff(dd,coalesce(a.BookingDate,a.IssueDate),a.TravelDateStart) > 365) then 0 
                                  when a.BookingTypeID = @BookingTypeIDPurchase and datediff(dd,a.BookingDate,a.IssueDate) < 0
                                      then datediff(dd,a.IssueDate,a.TravelDateStart)
                                  when a.BookingTypeID = @BookingTypeIDPurchase 
                                      then datediff(dd,coalesce(a.BookingDate,a.IssueDate),a.TravelDateStart) 
                                  when a.BookingTypeID = @BookingTypeIDExchange and datediff(dd,a.BookingDate,a.IssueDate) < 0 and datediff(dd,a.IssueDate,a.TravelDateStart) < 0 
                                      then -1  
                                  when a.BookingTypeID = @BookingTypeIDExchange and datediff(dd,a.BookingDate,a.IssueDate) < 0
                                      then datediff(dd,a.IssueDate,a.TravelDateStart) 
                                  when a.BookingTypeID = @BookingTypeIDExchange 
                                      then datediff(dd,coalesce(a.BookingDate,a.IssueDate),a.TravelDateStart) 
                                  else a.AdvancePurchaseDaysCnt end
      from dbo.InternalExtractEUAirFact a
     where a.InternalExtractID = @pInternalExtractID


    -- AdvancePurchaseID
    update dbo.InternalExtractEUAirFact
        set AdvancePurchaseID = case when a.AdvancePurchaseDaysCnt is null then -1
                                     when a.AdvancePurchaseDaysCnt < 0 then 99
                                     else coalesce(b.AdvancePurchaseID, -1)
                                 end 
      from dbo.InternalExtractEUAirFact a
           left join
           dbo.AdvancePurchaseDim b
               on a.AdvancePurchaseDaysCnt between b.StartDay and b.EndDay and
                  b.LangID = 1033
    where a.InternalExtractID = @pInternalExtractID

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update dbo.InternalExtractEUAirFact AdvancePurchaseID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- SegmentCnt / FlightCnt
    update dbo.InternalExtractEUAirFact
       set SegmentCnt = B.SegmentNbrAdj_count,
           FlightCnt = B.TripNbr_count,
           WithinCountryBool = B.WithinCountryBool
      from dbo.InternalExtractEUAirFact a 
           inner join
           (select a.InternalExtractID, a.BookingTypeID, a.RecordKey,
                   count(distinct a.TripNbr) as TripNbr_count,
                   count(a.SegmentNbrAdj) as SegmentNbrAdj_count,
                   min(cast(a.WithinCountryBool as tinyint)) as WithinCountryBool -- 0 if any segment is 0.
               from dbo.InternalExtractEUAirSegmentFact a
                    where a.InternalExtractID = @pInternalExtractID
                    group by a.InternalExtractID, a.RecordKey, a.BookingTypeID) AS B on
               a.InternalExtractID = B.InternalExtractID and
               a.BookingTypeID = B.BookingTypeID and
               a.RecordKey = B.RecordKey
      where a.InternalExtractID = @pInternalExtractID 

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
                @MsgParm1    = cast(@Error as varchar(12)) + ' (update SegmentCnt/FlightCnt)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end
    

    -- AirFareBasisCode
    update dbo.InternalExtractEUAirFact
       set AirFareBasisCode = (select top 1 b.AirFareBasisCode
                                 from dbo.InternalExtractEUAirSegmentFact b
                                      where b.InternalExtractID = @pInternalExtractID and
                                            b.BookingTypeID = a.BookingTypeID and
                                            b.SalesDocumentCode = a.SalesDocumentCode and
                                            b.RecordKey = a.RecordKey
                                      order by b.SegmentNbrAdj)
      from dbo.InternalExtractEUAirFact a 
      where a.InternalExtractID = @pInternalExtractID 

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
                @MsgParm1    = cast(@Error as varchar(12)) + ' (update AirFareBasisCode)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    -- AirFareFlexibilityID
    update dbo.InternalExtractEUAirFact
        set AirFareFlexibilityID = (select AirFareFlexibilityID from @AirFareFlexPriority where [Priority] =
                                   (select min(afp.[Priority])
                                      from dbo.InternalExtractEUAirSegmentFact b
                           left outer join @AirFareFlexPriority afp
                                        on afp.AirFareFlexibilityID = coalesce(b.AirFareFlexibilityID, 0)
                                     where b.InternalExtractID = @pInternalExtractID and
                                           b.BookingTypeID = a.BookingTypeID and
                                           b.SalesDocumentCode = a.SalesDocumentCode and
                                           b.RecordKey = a.RecordKey))
      from dbo.InternalExtractEUAirFact a 
      where a.InternalExtractID = @pInternalExtractID 

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
                @MsgParm1    = cast(@Error as varchar(12)) + ' (update AirFareFlexibilityID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    -- TripTypeID

    -- If number of legs is one, then one-way.
    -- If number of legs is two and AirportCodeFrom of leg one is equal to AirportCodeTo of leg two, then round trip.  
    -- Else multi-destination.
    update dbo.InternalExtractEUAirFact
       set TripTypeID = case when f.FlightCnt = 1 then 1 -- One Way
                             when f.FlightCnt = 2 and exists
                                                (
                                                 select * 
                                                   from dbo.#InternalExtractEUAirSegmentFact a
                                                        inner join 
                                                        dbo.#InternalExtractEUAirSegmentFact b on a.AirportCodeFrom = b.AirportCodeTo
                                                        where a.TripNbr = 1 and
                                                              b.TripNbr = 2 and
                                                              a.RecordKey = f.RecordKey and
                                                              b.RecordKey = f.RecordKey
                                                ) then 2
                            else 3
                        end
      from dbo.InternalExtractEUAirFact f
           where f.InternalExtractID = @pInternalExtractID 
           option (recompile)

    -- ClassOfServiceCode/CabinClassID
    ;with Flights as (
          select b.Priority, a.SalesDocumentCode, a.RecordKey, a.BookingTypeID, a.CabinClassID, a.ClassOfServiceCode, a.TripNbr
                 from dbo.InternalExtractEUAirFlightFact a
                      join @CabinClassPriority b on coalesce(a.CabinClassID, cast(0 as tinyint)) = b.CabinClassID
                      where a.InternalExtractID = @pInternalExtractID
          ),
    -- WITH
          FlightMatch as (
          select * from Flights a
                  where not exists(select * from Flights SubQ
                                           where SubQ.SalesDocumentCode = a.SalesDocumentCode and
                                                 SubQ.RecordKey = a.RecordKey and
                                                 SubQ.BookingTypeID = a.BookingTypeID and
                                                 (SubQ.Priority < a.Priority 
                                                    or (SubQ.Priority = a.Priority and SubQ.TripNbr < a.TripNbr)
                                                 )
                                   ) -- Not exists
                           ) -- FlightMatch CTE
    update f
       set ClassOfServiceCode = c.ClassOfServiceCode,
           CabinClassID = c.CabinClassID
           from dbo.InternalExtractEUAirFact f
                left join FlightMatch c on f.SalesDocumentCode = c.SalesDocumentCode and
                                           f.BookingTypeID = c.BookingTypeID and
                                           f.RecordKey = c.RecordKey
                where f.InternalExtractID = @pInternalExtractID
                option (recompile)

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
                @MsgParm1    = cast(@Error as varchar(12)) + ' (update ClassOfServiceCode/CabinClassID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    -- GeographyTypeID
    ;with cte as (
              select f.GeographyTypeID,
                    (select top 1 s.GeographyTypeID 
                      from dbo.InternalExtractEUAirSegmentFact s 
                           inner join GeographyTypeTravelProduct p on s.GeographyTypeID = p.GeographyTypeID and
                                                                      f.TravelProductID = p.TravelProductID and
                                                                      p.LineofBusinessID = 1 -- Air
                           where f.InternalExtractID = s.InternalExtractID and
                                 f.SalesDocumentCode = s.SalesDocumentCode and
                                 f.BookingTypeID = s.BookingTypeID and
                                 f.RecordKey = s.RecordKey
                           order by p.PriorityOrderNbr
                           ) as NewGeographyTypeID
      from dbo.InternalExtractEUAirFact f
           where f.InternalExtractID = @pInternalExtractID
)
    update cte
       set GeographyTypeID = NewGeographyTypeID
           option(recompile)


    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
                @MsgParm1    = cast(@Error as varchar(12)) + ' (update GeographyTypeID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    -- calculate and update the AirDuration fields
    update dbo.InternalExtractEUAirFact
    set 
        TravelDurationMinuteCnt = b.SumTravelDurationMinuteCnt,
        TravelDateStartUTCOffset = b.MinTravelDateStartUTCOffset,
        TravelDateEndUTCOffset = b.MaxTravelDateEndUTCOffset
    from dbo.InternalExtractEUAirFact a
    inner join (select b.InternalExtractID, b.RecordKey, b.BookingTypeID,
                    case
                        when abs(sum(b.TravelDurationMinuteCnt)) > 32767 then null  -- nullify if it exceeds smallint upper limit
                        else sum(b.TravelDurationMinuteCnt)
                    end as SumTravelDurationMinuteCnt,
                    min(b.TravelDateStartUTCOffset) as MinTravelDateStartUTCOffset,
                    max(b.TravelDateEndUTCOffset) as MaxTravelDateEndUTCOffset
                from dbo.InternalExtractEUAirFact a
                inner join dbo.InternalExtractEUAirFlightFact b on
                    b.InternalExtractID = a.InternalExtractID and
                    b.RecordKey = a.RecordKey and
                    b.BookingTypeID = a.BookingTypeID
                where b.InternalExtractID = @pInternalExtractID
                group by b.InternalExtractID, b.RecordKey, b.BookingTypeID
    ) b on
        b.InternalExtractID = a.InternalExtractID and
        b.RecordKey = a.RecordKey and
        b.BookingTypeID = a.BookingTypeID
    where b.InternalExtractID = @pInternalExtractID

    
    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update InternalExtractEUAirFact Air Duration)'
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
    print 'In the Error Handler'
    select @@trancount as TranCount
    if (@TranStartedBool = @TRUE) rollback transaction @SavePointName
    select   @ExitCode = @RC_FAILURE
    goto ExitProc
    select @@trancount as TranCount

---------------------------------------------------------------------
-- Exit Procedure
---------------------------------------------------------------------
ExitProc:
    return (@ExitCode)
go

-- exec [InternalExtractEUAirFactLoadSegmentDerived] @pInternalExtractID = 36901
-- exec [InternalExtractEUAirFactLoadSegmentDerived] @pInternalExtractID = 30568
-- exec [InternalExtractEUAirFactLoadSegmentDerived] @pInternalExtractID = 30569
-- exec [InternalExtractEUAirFactLoadSegmentDerived] @pInternalExtractID = 30571
-- exec [InternalExtractEUAirFactLoadSegmentDerived] @pInternalExtractID = 30572

--select SalesDocumentCode, SalesDocumentLineNbr, GeographyTypeID, WithinCountryBool, ClassOfServiceCode, CabinClassID,SegmentCnt, FlightCnt, * from InternalExtractEUAirFact where InternalExtractID = 30566 order by 1, 2
