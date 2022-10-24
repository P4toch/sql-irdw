if object_id('dbo.ExternalFileEUTrainTripFactLoad') is null begin
    print 'Creating stored procedure ExternalFileEUTrainTripFactLoad (placeholder)'
    execute('create procedure dbo.ExternalFileEUTrainTripFactLoad as return 0')
end
go

print 'Altering stored procedure ExternalFileEUTrainTripFactLoad'
go

alter procedure dbo.ExternalFileEUTrainTripFactLoad 
    @pExternalFileID int
with recompile
as

/*
*********************************************************************
Copyright (C) 2006-2013 Expedia, Inc. All rights reserved.

Description:
    Inserts/Updates ExternalFileEUTrainTripFact with new and modified
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
    2009-09-14  BarryC          Take BookingTypeID from ticket level
    2012-12-03  VBoerner        Add exchange rate handling for PL/CZ TPIDs
    2013-10-15  DMurugesan      GeographyTypeID override using GeographyTypeTravelProduct mapping table
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
    @LineOfBusinessID                     = 5

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


insert into dbo.ExternalFileEUTrainTripFact (ExternalFileID,  RecordKey,  BookingTypeID,  TripNbr,  TrainVendorCode,  
    ClassOfServiceCode,  CabinClassID, TravelDateStart,  TravelDateEnd,  TrainRouteID,  SegmentCnt,  MileageCnt, 
    GeographyTypeID,  WithinCountryBool, IncrementCnt,  CurrencyCode,  TripValue) 
 select 
    b.ExternalFileID,  
    b.RecordKey,  
    a.BookingTypeID,  
    b.TripNbr,  
    b.TrainVendorCode,  
    b.ClassOfServiceCode,  
    b.CabinClassID, 
    b.TravelDateStart,  
    b.TravelDateEnd,  
    null as TrainRouteID,  
    b.SegmentCnt,  
    null as MileageCnt,  
    b.GeographyTypeID,  
    b.WithinCountryBool, 
    a.IncrementCnt,  
    c.CurrencyCodeStorage,  
    b.TripValue * coalesce(d.ExchangeRateUsed, @ExchangeRateNull)
  from dbo.ExternalFileEUTrainFact a 
       inner join
       dbo.ExternalFileEUTrainTicketTrip b on
            a.ExternalFileID = b.ExternalFileID and
            a.RecordKey = b.RecordKey 
       inner join 
       dbo.TravelProductDim c on a.TravelProductID = c.TravelProductID
       left join
       dbo.ExchangeRateDailyFull d on dbo.TimeIDFromDate(a.InvoiceDate) = d.TimeID and
            a.CurrencyCode = d.FromCurrencyCode and 
            c.CurrencyCodeStorage = d.ToCurrencyCode
where a.ExternalFileID = @pExternalFileID 

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert ExternalFileEUTrainTripFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

-------- Update GeographyTypeID ------
Update A 
set  GeographyTypeID = C.GeographyTypeID 
from ExternalFileEUTrainTripFact A 
Inner join 
(
    select 
        a.ExternalFileID,
        a.RecordKey,
        a.BookingTypeID,
        a.TripNbR,
        c.TravelProductID,
        min(c.priorityordernbr) as priorityordernbr
    from dbo.ExternalFileEUTrainSegmentFact a 
    inner join dbo.ExternalFileEUTrainFact b on 
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
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update ExternalFileEUTrainTripFact)'
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

