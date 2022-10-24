if object_id('dbo.InternalExtractEUAirFactLoadRoutes') is null begin
    print 'Creating stored procedure InternalExtractEUAirFactLoadRoutes (placeholder)'
    execute('create procedure dbo.InternalExtractEUAirFactLoadRoutes as return 0')
end
go

print 'Altering stored procedure InternalExtractEUAirFactLoadRoutes'
go

alter procedure dbo.InternalExtractEUAirFactLoadRoutes
    @pInternalExtractID int
as

/*
*********************************************************************
Copyright (C) 2014 Expedia, Inc. All rights reserved.

Description:
     Updates InternalExtractEUAirFact, InternalExtractEUAirFlightFact,
        InternalExtractEUAirSegmentFact with RouteID.
     Adds rows to RouteDim. 

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2014-07-05  JaredKo         Created.
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
    @BookingTypeIDVoidRefund              tinyint

create table #AirFact (
   InternalExtractID int not null,
   RecordKey varchar(30) not null,
   BookingTypeID tinyint not null,
   RouteTxt varchar(150) null,
   AirportCodeFrom char(3) null,
   AirportCodeTo char(3) null,
   SegmentCnt tinyint null,
   FlightCnt tinyint null
)
create index temp_ix on #AirFact (InternalExtractID, RecordKey, BookingTypeID)


create table #RouteFlight (
   InternalExtractID int not null,
   RecordKey  varchar(30) not null,
   BookingTypeID int not null,
   TripNbr tinyint not null,
   AirportCodeFrom char(3) null,
   AirportCodeTo char(3) null
)
create index temp_ix on #RouteFlight (InternalExtractID, RecordKey, BookingTypeID, TripNbr)

create table #RouteSegment (
   InternalExtractID int not null,
   RecordKey  varchar(30) not null,
   BookingTypeID int not null,
   SegmentNbr tinyint not null,
   AirportCodeFrom char(3) null,
   AirportCodeTo char(3) null
)
create index temp_ix on #RouteSegment (InternalExtractID, RecordKey, BookingTypeID, SegmentNbr)


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
    @BookingTypeIDVoidRefund              = 10

---------------------------------------------------------------------
-- Validation
---------------------------------------------------------------------
-- None, done by caller
---------------------------------------------------------------------
-- Processing
---------------------------------------------------------------------

-- Rules for processing AirportCodeTo / AirportCodeFrom:

--If number of flights is 1, then AirportCodeFrom and AirportCodeTo of only flight.
--If number of flights is 2 and AirportCodeFrom of flight one is equal to AirportCodeTo of flight two, 
--      then AirportCodeFrom of flight 1 and AirportCodeTo of flight 1.
--If number of flights is 2 and AirportCodeFrom of flight one is not equal to AirportCodeTo of flight two and AirportCodeFrom of flight one is not equal to AirportCodeFrom of flight 2, 
--      then AirportCodeFrom of flight 1 and AirportCodeFrom of flight 2.
--If number of flights is 2 and AirportCodeFrom of flight one is not equal to AirportCodeTo of flight two and AirportCodeFrom of flight one is equal to AirportCodeFrom of flight 2, 
--      then AirportCodeFrom of flight 1 and AirportCodeTo of flight 2.
--If number of flights is 3 or more and AirportCodeFrom of flight one is not equal to AirportCodeTo of the last flight and AirportCodeFrom of flight one is not equal to AirportCodeFrom of the last flight, 
--      then AirportCodeFrom of flight 1 and AirportCodeFrom of the last flight.
--If number of flights is 3 or more and AirportCodeFrom of flight one is not equal to AirportCodeTo of the last flight and AirportCodeFrom of flight one is equal to AirportCodeFrom of the last flight, 
--      then AirportCodeFrom of flight 1 and AirportCodeTo of the last flight.

-- Build RouteTxt and determine AirportCodeTo / AirportCodeFrom
-- This Recursive CTE creates a graduating RouteTxt where each segment of RouteTxt is concatenated to the previous row (for matching RecordKeys).
-- All queries from the resulting temp table need to use "SELECT TOP 1... ORDER BY SegmentNbrAdj desc".

;with Segments as(
            select s.SalesDocumentCode,
                   s.SalesDocumentLineNbr,
                   s.RecordKey,
                   s.AirportCodeFrom, 
                   s.AirportCodeTo, 
                   s.AirportCodeFrom as RouteAirportCodeFrom,  -- AirportCodeFrom of first flight/segment.
                   s.AirportCodeFrom as FlightAirportCodeFrom, -- AirportCodeFrom (updates on each segment change)
                   s.AirportCodeTo as FlightAirportCodeTo,     -- AirportCodeFrom (updates on each TripNbr change)
                   cast(null as char(3)) as RouteAirportCodeTo,-- NULL unless matching pair for Airport return trip OR open leg.
                   cast(s.AirportCodeFrom + '/' + s.AirportCodeTo as varchar(150)) as RouteTxt, 
                   s.TripNbr,
                   s.SegmentNbrAdj
              from dbo.InternalExtractEUAirSegmentFact s
                   where s.InternalExtractID = @pInternalExtractID and
                         s.SegmentNbrAdj = cast(1 as tinyint)
            union all
            select s.SalesDocumentCode,
                   s.SalesDocumentLineNbr,
                   s.RecordKey,
                   s.AirportCodeFrom, 
                   s.AirportCodeTo, 
                   s1.RouteAirportCodeFrom,
                   case when s.TripNbr = s1.TripNbr 
                            then s1.AirportCodeFrom 
                            else s.AirportCodeFrom 
                        end as FlightAirportCodeFrom,
                   s.AirportCodeTo as FlightAirportCodeTo, -- Technically the same as s.AirportCodeTo
                   case when s1.RouteAirportCodeTo is null then
                        case when s.AirportCodeFrom = s1.AirportCodeTo and s.AirportCodeTo = s1.AirportCodeFrom
                                 -- This segment is a return segment based on 
                                 -- inverted Source/Dest AirportCodes in current and previous segment
                                 then s1.AirportCodeTo
                             when s.AirportCodeFrom <> s1.AirportCodeTo
                                 -- AirportCodeTo from previous segment doesn't match AirportCodeFrom in current
                                 -- segment. This is an open leg.
                                 then s1.AirportCodeTo
                             end
                        else s1.RouteAirportCodeTo 
                      end as RouteAirportCodeTo,
                   cast(s1.RouteTxt + case when s1.AirportCodeTo = s.AirportCodeFrom 
                                               then '/' + s.AirportCodeTo 
                                               else '_' + s.AirportCodeFrom + '/' + s.AirportCodeTo -- Open Leg
                                           end as varchar(150)) as RouteTxt, 
                   s.TripNbr,
                   s.SegmentNbrAdj
              from dbo.InternalExtractEUAirSegmentFact s 
                   inner join 
                   Segments s1 on s.SalesDocumentCode = s1.SalesDocumentCode and
                                  s.RecordKey = s1.RecordKey and
                                  s.SegmentNbrAdj = s1.SegmentNbrAdj + 1
                   where s.InternalExtractID = @pInternalExtractID and
                         s.SegmentNbrAdj > cast(1 as tinyint)
)

/* with Segments */
select * 
into #segments
from Segments s
option (recompile)

create index ix_tmp_segments_RecordKey_SegmentNumberAdj on #segments(RecordKey, SegmentNbrAdj desc)

-- Get record sets to work on
-- RouteAirportCodeTo already determined for Round-trips in previous query.
-- Still need to calculate AirportCodeTo when there isn't a transposed airport/segment pair or an open leg.
insert into #AirFact (InternalExtractID, RecordKey, BookingTypeID, RouteTxt, AirportCodeFrom, AirportCodeTo, SegmentCnt, FlightCnt)
select a.InternalExtractID, 
       a.RecordKey, 
       a.BookingTypeID, 
       b.RouteTxt, 
       b.RouteAirportCodeFrom, 
       -- If already calculated, use it...
       coalesce(b.RouteAirportCodeTo, 
                     -- A single segment trip automatically uses AirportCodeTo
                case when b.TripNbr  = 1 then b.AirportCodeTo
                     -- Pick an airport code from the final flight but NOT one that matches AirportCodeFrom.
                     when b.TripNbr >= 2 and c.RouteAirportCodeFrom <> c.FlightAirportCodeFrom then c.FlightAirportCodeFrom
                     when b.TripNbr >= 2 and c.RouteAirportCodeFrom =  c.FlightAirportCodeFrom then c.FlightAirportCodeTo
                     -- ToDo: This was fallback code. Don't think it's necessary anymore.
                     when b.TripNbr >= 3 then b.AirportCodeFrom
                     end
                ),
       b.SegmentNbrAdj,
       c.TripNbr as FlightCnt
  from dbo.InternalExtractEUAirFact a
       -- Get the LAST segment of the last flight
       cross apply(select top 1 * from #segments SubQ 
                   where SubQ.RecordKey = a.RecordKey 
                   order by SubQ.SegmentNbrAdj desc) b
       -- Get the FIRST segment of the last flight
       cross apply(select top 1 * from #segments SubQ 
                   where SubQ.RecordKey = a.RecordKey 
                   order by SubQ.SegmentNbrAdj desc
                   ) c
 where a.InternalExtractID = @pInternalExtractID
 option(recompile)
 

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #AirFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

-- Code Review Comment: Suggest we don't use a transaction here. 
-- A failure after dbo.RouteDim won't cause inconsistent state so Transactions are unnecessary.

--select @TrancountSave = @@Trancount
--if (@TrancountSave = 0) begin transaction @SavePointName
--else                    save  transaction @SavePointName
--select @TranStartedBool = @TRUE


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
           LastUpdatedBy = 'IEEUAirFactLoadRoutes'
      from #AirFact a 
           left join
           dbo.RouteDim b on
               a.AirportCodeTo = b.AirportCodeTo and
               a.AirportCodeFrom = b.AirportCodeFrom
     where b.AirportCodeTo is null
    
    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (insert RouteDim)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    -- Update ticket fact table with route id
    update dbo.InternalExtractEUAirFact
       set RouteID = c.RouteID,
           RouteTxt = b.RouteTxt,
           UpdateDate = @Current_TimeStamp,
           LastUpdatedBy = 'IEEUAirFactLoadRoutes'
      from dbo.InternalExtractEUAirFact a
           inner join    
           #AirFact b on
               a.InternalExtractID = b.InternalExtractID and
               a.RecordKey = b.RecordKey and
               a.BookingTypeID = b.BookingTypeID 
           inner join 
           dbo.RouteDim c  on
               b.AirportCodeTo = c.AirportCodeTo and
               b.AirportCodeFrom = c.AirportCodeFrom
     where a.InternalExtractID = @pInternalExtractID

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update InternalExtractEUAirFact RouteID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


-- See code review comment above.
--if ((@TrancountSave = 0) and (@TranStartedBool = @TRUE))
--    commit transaction @SavePointName


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
-- commit tran
-- begin tran
-- -- delete dbo.RouteDim where RouteCodeBidirectional like 'syd:%' or RouteCodeBidirectional like '%:syd'
-- exec InternalExtractEUAirFactLoadRoutes 30566
-- rollback tran

--select * from dbo.InternalExtractEUAirSegmentFact s where s.RecordKey = 'AUSI140026195-10000' order by s.SegmentNbrAdj
--select * from dbo.InternalExtractEUAirFlightFact ieeff where ieeff.SalesDocumentCode = 'AUSI140026195' 


--    Code Review Test Case
/*
--declare @SalesDocumentCode varchar(30) = 'AUSI140027272'
declare @SalesDocumentCode varchar(30) = 'AUSI140027063'

select l.Ticket, l.Principal_Ticket, * from dbo.Nav_Sales_Invoice_Line l
where l.Document_No_ = @SalesDocumentCode and l.Resource_Type = 0

select * from dbo.Nav_Travel_Ledger_Entry t 
where t.Document_No_ = @SalesDocumentCode and t.Ticket_No in ('4824107297')
 order by t.No_Segment
go
declare @SalesDocumentCode varchar(30) = 'AUSI140027063'
select top 10000 RouteID, RouteTxt, * from dbo.ExternalFileEUAirFact (nolock) 
where ExternalFileID > 23500 
  and SalesDocumentCode = @SalesDocumentCode

select * from dbo.ExternalFileEUAirFlightFact f
where f.ExternalFileID > 23500
  and RecordKey like @SalesDocumentCode + '%'

select * from dbo.ExternalFileEUAirSegmentFact f
where f.ExternalFileID > 23500
  and RecordKey like @SalesDocumentCode + '%'
go
declare @SalesDocumentCode varchar(30) = 'AUSI140027063'
select f.RouteID, f.RouteTxt, r.AirportCodeFrom, r.AirportCodeTo, f.FlightCnt, f.SegmentCnt, * from dbo.InternalExtractEUAirFact f
join dbo.RouteDim r on r.RouteID = f.RouteID
where SalesDocumentCode = @SalesDocumentCode

select * from dbo.InternalExtractEUAirFlightFact f
where RecordKey like @SalesDocumentCode + '%'

select GeographyTypeID, * from dbo.InternalExtractEUAirSegmentFact f
where SalesDocumentCode = @SalesDocumentCode


*/