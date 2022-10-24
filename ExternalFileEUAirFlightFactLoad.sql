if object_id('dbo.ExternalFileEUAirFlightFactLoad') is null begin
    print 'Creating stored procedure ExternalFileEUAirFlightFactLoad (placeholder)'
    execute('create procedure dbo.ExternalFileEUAirFlightFactLoad as return 0')
end
go

print 'Altering stored procedure ExternalFileEUAirFlightFactLoad'
go

alter procedure dbo.ExternalFileEUAirFlightFactLoad 
    @pExternalFileID int
with recompile
as

/*
*********************************************************************
Copyright (C) 2006-2013 Expedia, Inc. All rights reserved.

Description:
    Inserts/Updates ExternalFileEUAirTripFact with new and modified
        data

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
    2012-12-03  VBoerner        Add exchange rate handling for PL/CZ TPIDs
    2013-10-14  DMurugesan      GeographyTypeID override using GeographyTypeTravelProduct mapping table
*********************************************************************
*/

set nocount on
set ansi_warnings off
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

    @CounterMax                           int,
    @Counter                              int,
    @LineOfBusinessID                     tinyint

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
    @LineOfBusinessID                     = 1

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

insert into dbo.ExternalFileEUAirFlightFact (ExternalFileID,  RecordKey,  BookingTypeID,  TripNbr,  AirlineCode,  
    ClassOfServiceCode,  CabinClassID, TravelDateStart,  TravelDateEnd,  RouteID,  SegmentCnt,  MileageCnt, 
    GeographyTypeID,  WithinCountryBool, IncrementCnt,  CurrencyCode,  FlightValue) 
 select 
    b.ExternalFileID,  
    b.RecordKey,  
    b.BookingTypeID,  
    b.TripNbr,  
    b.AirlineCode,  
    b.ClassOfServiceCode,  
    b.CabinClassID, 
    b.TravelDateStart,  
    b.TravelDateEnd,  
    null as RouteID,  
    b.SegmentCnt,  
    dbo.MileageFromLatLong(c.Latitude, c.Longitude, d.Latitude, d.Longitude) *
        case when a.BookingTypeID in (@BookingTypeIDVoid, @BookingTypeIDRefund, @BookingTypeIDPRefund) then -1.0
             else 1.0 end as MileageCnt,  
    null as GeographyTypeID,  
    b.WithinCountryBool, 
    a.IncrementCnt,  
    e.CurrencyCodeStorage,  
    b.FlightValue * coalesce(f.ExchangeRateUsed, @ExchangeRateNull) 
  from dbo.ExternalFileEUAirFact a 
       inner join
       dbo.ExternalFileEUAirTicketFlight b on
            a.ExternalFileID = b.ExternalFileID and
            a.RecordKey = b.RecordKey and
            a.BookingTypeID = b.BookingTypeID
       left join
       dbo.AirportDim c
           on b.AirportCodeFrom = c.AirportCode and c.LangID = 1033
       left join
       dbo.AirportDim d 
           on b.AirportCodeTo = d.AirportCode and d.LangID = 1033
       inner join 
       dbo.TravelProductDim e on a.TravelProductID = e.TravelProductID
       left join
       dbo.ExchangeRateDailyFull f on dbo.TimeIDFromDate(a.InvoiceDate) = f.TimeID and
            a.CurrencyCode = f.FromCurrencyCode and 
            e.CurrencyCodeStorage = f.ToCurrencyCode
where a.ExternalFileID = @pExternalFileID 

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert ExternalFileEUAirFlightFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

--  FlightValue
update dbo.ExternalFileEUAirFlightFact
   set FlightValue = B.FlightValue
   from dbo.ExternalFileEUAirFlightFact a
        inner join 
        (select a.ExternalFileID, a.RecordKey, a.BookingTypeID, a.TripNbR,
                case when count(1) = count(b.SegmentValue) then sum(b.SegmentValue) else null end as FlightValue    
          from dbo.ExternalFileEUAirFlightFact a 
               inner join 
               dbo.ExternalFileEUAirSegmentFact b on
                   a.ExternalFileID = b.ExternalFileID and
                   a.RecordKey = b.RecordKey and     
                   a.BookingTypeID = b.BookingTypeID and   
                   a.TripNbR = b.TripNbr
         where a.ExternalFileID = @pExternalFileID
         group by a.ExternalFileID, a.RecordKey, a.BookingTypeID, a.TripNbr) as B on
               a.ExternalFileID = B.ExternalFileID and
               a.RecordKey = B.RecordKey and     
               a.BookingTypeID = B.BookingTypeID and
               a.TripNbr = B.TripNbr 
where a.ExternalFileId = @pExternalFileID 


-------- Update GeographyTypeID ------
Update A 
set  GeographyTypeID = C.GeographyTypeID 
from ExternalFileEUAirFlightFact A 
Inner join 
(select 
        a.ExternalFileID,
        a.RecordKey,
        a.BookingTypeID,
        a.TripNbR,
        c.TravelProductID,
        min(c.priorityordernbr) as priorityordernbr
 from dbo.ExternalFileEUAirSegmentFact a 
   inner join dbo.ExternalFileEUAirFact b on 
        a.ExternalFileID = b.ExternalFileID and  
        a.RecordKey = b.RecordKey and
        a.BookingTypeID = b.BookingTypeID
  inner join dbo.GeographyTypeTravelProduct c on
        c.TravelProductID = b.TravelProductID and 
        c.GeographyTypeID = a.GeographyTypeID and
        c.LineOfBusinessID = @LineOfBusinessID 
 where a.ExternalFileId = @pExternalFileID
 group by a.ExternalFileID,a.RecordKey,a.BookingTypeID,a.TripNbR,c.TravelProductID 
) as B on
  a.ExternalFileID = b.ExternalFileID and 
  a.RecordKey = b.RecordKey and
  a.BookingTypeID = b.BookingTypeID and   
  a.TripNbR = b.TripNbr
inner join dbo.GeographyTypeTravelProduct C  on
  c.TravelProductID = b.TravelProductID and  
  c.PriorityOrderNbr = b.PriorityOrderNbr and
  c.LineOfBusinessID = @LineOfBusinessID  
where a.ExternalFileId = @pExternalFileID         

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update ExternalFileEUAirFlightFact)'
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

