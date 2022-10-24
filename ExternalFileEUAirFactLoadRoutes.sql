if object_id('dbo.ExternalFileEUAirFactLoadRoutes') is null begin
    print 'Creating stored procedure ExternalFileEUAirFactLoadRoutes (placeholder)'
    execute('create procedure dbo.ExternalFileEUAirFactLoadRoutes as return 0')
end
go

print 'Altering stored procedure ExternalFileEUAirFactLoadRoutes'
go

alter procedure dbo.ExternalFileEUAirFactLoadRoutes
    @pExternalFileID int
with recompile
as

/*
*********************************************************************
Copyright (C) 2006-2009 Expedia, Inc. All rights reserved.

Description:
     Updates ExternalFileEUAirFact, ExternalFileEUAirFlightFact,
        ExternalFileEUAirSegmentFact with RouteID.
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
    2006-07-04  BarryC          Created.
    2009-07-27  BarryC          Add new route processing at segment.
                                Add new RouteDim fields.
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

create table #RouteTicket (
   ExternalFileID int not null,
   RecordKey varchar(30) not null,
   BookingTypeID tinyint not null,
   AirportCodeFrom char(3) null,
   AirportCodeTo char(3) null
)
create index temp_ix on #RouteTicket (ExternalFileID, RecordKey, BookingTypeID)


create table #RouteFlight (
   ExternalFileID int not null,
   RecordKey  varchar(30) not null,
   BookingTypeID int not null,
   TripNbr tinyint not null,
   AirportCodeFrom char(3) null,
   AirportCodeTo char(3) null
)
create index temp_ix on #RouteFlight (ExternalFileID, RecordKey, BookingTypeID, TripNbr)

create table #RouteSegment (
   ExternalFileID int not null,
   RecordKey  varchar(30) not null,
   BookingTypeID int not null,
   SegmentNbr tinyint not null,
   AirportCodeFrom char(3) null,
   AirportCodeTo char(3) null
)
create index temp_ix on #RouteSegment (ExternalFileID, RecordKey, BookingTypeID, SegmentNbr)


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
-- Get record sets to work on
insert into #RouteTicket (ExternalFileID, RecordKey, BookingTypeID, AirportCodeFrom, AirportCodeTo)
select a.ExternalFileID, a.RecordKey, a.BookingTypeID, b.AirportCodeFrom, b.AirportCodeTo
  from dbo.ExternalFileEUAirFact a
       inner join
       dbo.ExternalFileEUAirTicket b 
           on a.ExternalFileID = b.ExternalFileID and
              a.RecordKey = b.RecordKey and
              a.BookingTypeID = b.BookingTypeID
 where a.ExternalFileID = @pExternalFileID

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #RouteTicket)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


insert into #RouteFlight (ExternalFileID, RecordKey, BookingTypeID, TripNbr, AirportCodeFrom, AirportCodeTo)
select a.ExternalFileID, a.RecordKey, a.BookingTypeID, a.TripNbr, b.AirportCodeFrom, b.AirportCodeTo
  from dbo.ExternalFileEUAirFlightFact a
       inner join
       dbo.ExternalFileEUAirTicketFlight b 
           on a.ExternalFileID = b.ExternalFileID and
              a.RecordKey = b.RecordKey and
              a.BookingTypeID = b.BookingTypeID and
              a.TripNbr = b.TripNbr
 where a.ExternalFileID = @pExternalFileID

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #RouteFlight)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

insert into #RouteSegment (ExternalFileID, RecordKey, BookingTypeID, SegmentNbr, AirportCodeFrom, AirportCodeTo)
select a.ExternalFileID, a.RecordKey, a.BookingTypeID, a.SegmentNbr, a.AirportCodeFrom, a.AirportCodeTo
  from dbo.ExternalFileEUAirSegmentFact a
 where a.ExternalFileID = @pExternalFileID

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #RouteFlight)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

select @TrancountSave = @@Trancount
if (@TrancountSave = 0) begin transaction @SavePointName
else                    save  transaction @SavePointName
select @TranStartedBool = @TRUE


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
           LastUpdatedBy = 'EFEUAirFactLoadRoutes'
      from #RouteTicket a 
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
           LastUpdatedBy = 'EFEUAirFactLoadRoutes'
      from #RouteFlight a 
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
           LastUpdatedBy = 'EFEUAirFactLoadRoutes'
      from #RouteSegment a 
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
    update dbo.ExternalFileEUAirFact
       set RouteID = c.RouteID,
           UpdateDate = @Current_TimeStamp,
           LastUpdatedBy = 'EFEUAirFactLoadRoutes'
      from dbo.ExternalFileEUAirFact a
           inner join    
           #RouteTicket b on
               a.ExternalFileID = b.ExternalFileID and
               a.RecordKey = b.RecordKey and
               a.BookingTypeID = b.BookingTypeID 
           inner join 
           dbo.RouteDim c  on
               b.AirportCodeTo = c.AirportCodeTo and
               b.AirportCodeFrom = c.AirportCodeFrom
     where a.ExternalFileID = @pExternalFileID

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update ExternalFileEUAirFact RouteID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- Update flight fact table with route id
    update dbo.ExternalFileEUAirFlightFact
       set RouteID = c.RouteID
      from dbo.ExternalFileEUAirFlightFact a
           inner join    
           #RouteFlight b on
               a.ExternalFileID = b.ExternalFileID and
               a.RecordKey = b.RecordKey and
               a.BookingTypeID = b.BookingTypeID and
               a.TripNbr = b.TripNbr
           inner join 
           dbo.RouteDim c  on
               b.AirportCodeTo = c.AirportCodeTo and
               b.AirportCodeFrom = c.AirportCodeFrom
     where a.ExternalFileID = @pExternalFileID

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update ExternalFileEUAirFlightFact RouteID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- Update segment fact table with route id
    update dbo.ExternalFileEUAirSegmentFact
       set RouteID = c.RouteID
      from dbo.ExternalFileEUAirSegmentFact a
           inner join    
           #RouteSegment b on
               a.ExternalFileID = b.ExternalFileID and
               a.RecordKey = b.RecordKey and
               a.BookingTypeID = b.BookingTypeID and
               a.SegmentNbr = b.SegmentNbr
           inner join 
           dbo.RouteDim c  on
               b.AirportCodeTo = c.AirportCodeTo and
               b.AirportCodeFrom = c.AirportCodeFrom
     where a.ExternalFileID = @pExternalFileID

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update ExternalFileEUAirSegmentFact RouteID)'
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

