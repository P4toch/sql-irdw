if object_id('dbo.ExternalFileEUTrainSegmentFactLoad') is null begin
    print 'Creating stored procedure ExternalFileEUTrainSegmentFactLoad (placeholder)'
    execute('create procedure dbo.ExternalFileEUTrainSegmentFactLoad as return 0')
end
go

print 'Altering stored procedure ExternalFileEUTrainSegmentFactLoad'
go

alter procedure dbo.ExternalFileEUTrainSegmentFactLoad 
    @pExternalFileID int
with recompile
as

/*
*********************************************************************
Copyright (C) 2006-2016 Expedia, Inc. All rights reserved.

Description:
    Inserts/Updates ExternalFileEUTrainSegmentFact with new and modified
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
    2014-07-18  DMurugesan      EGE-68306 Fix GeographyTypeID choice 
   	2016-08-30  jappleberry     Update to use new TrainStationCodeToTrainStationID mapping table 
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

insert into dbo.ExternalFileEUTrainSegmentFact (ExternalFileID,  RecordKey,  BookingTypeID,  SegmentNbr,  TripNbr,  
    TicketCode, SequenceNbr,  TrainVendorCode,  TrainNumberTxt,  ClassOfServiceCode,  CabinClassID,  TrainSeatTypeID,  SegmentDateStart, 
    SegmentDateEnd,  TrainStationCodeFrom,  TrainStationCodeTo,  MileageCnt,  GeographyTypeID,  WithinCountryBool,  IncrementCnt, 
    CurrencyCode,  SegmentValue  ) 
 select 
    b.ExternalFileID,  
    ltrim(rtrim(b.RecordKey)),  
    a.BookingTypeID,  
    b.SegmentNbr,  
    b.TripNbr,  
    coalesce(ltrim(rtrim(b.TicketCode)),''), 
    b.SequenceNbr,  
    ltrim(rtrim(b.TrainVendorCode)),  
    ltrim(rtrim(b.TrainNumberTxt)),  
    ltrim(rtrim(b.ClassOfServiceCode)),  
    b.CabinClassID,  
    b.TrainSeatTypeID,  
    b.SegmentDateStart, 
    b.SegmentDateEnd,  
    case when (substring(ltrim(rtrim(b.TrainStationCodeFrom)),3,1) = '-')
         then substring(ltrim(rtrim(b.TrainStationCodeFrom)),4,16) 
         else ltrim(rtrim(b.TrainStationCodeFrom))
    end,
    case when (substring(ltrim(rtrim(b.TrainStationCodeTo)),3,1) = '-') 
         then substring(ltrim(rtrim(b.TrainStationCodeTo)),4,16) 
         else ltrim(rtrim(b.TrainStationCodeTo))
    end, 
    null as MileageCnt,  
    null as GeographyTypeID,  
    b.WithinCountryBool,  
    a.IncrementCnt, 
    c.CurrencyCodeStorage,  
    b.SegmentValue * coalesce(d.ExchangeRateUsed, @ExchangeRateNull)
  from dbo.ExternalFileEUTrainFact a 
       inner join
       dbo.ExternalFileEUTrainTicketSegment b on
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
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert ExternalFileEUTrainSegmentFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

------ Update GeographyTypeID ------   
Update A 
set GeographyTypeID = case  when k.GeographyTypeID is not null then k.GeographyTypeID --- Highest priority for City Pair
                            else                                
                             case when f.PriorityOrderNbr  is not null and h.PriorityOrderNbr is not null then
                                    case when f.PriorityOrderNbr < h.PriorityOrderNbr then f.GeographyTypeID
                                         else h.GeographyTypeID
                                    end    
                                  when f.PriorityOrderNbr is null and h.PriorityOrderNbr is not null then
                                    case when j.PriorityOrderNbr < h.PriorityOrderNbr then j.GeographyTypeID
                                        else h.GeographyTypeID
                                     end
                                  when f.PriorityOrderNbr is not null and h.PriorityOrderNbr is null then
                                    case when j.PriorityOrderNbr < f.PriorityOrderNbr then j.GeographyTypeID
                                        else f.GeographyTypeID
                                     end
                               else j.GeographyTypeID
                             end                          
                      end
from dbo.ExternalFileEUTrainSegmentFact a
inner join dbo.ExternalFileEUTrainFact b on
    a.ExternalFileID = b.ExternalFileID and 
    a.RecordKey = b.RecordKey and
    a.BookingTypeID = b.BookingTypeID
inner join dbo.ExternalFileEUTrainTicketSegment m on
    b.ExternalFileID = m.ExternalFileID and
    b.RecordKey = m.RecordKey
inner join TrainStationCodeToTrainStationID n on
    ltrim(rtrim(m.TrainStationCodeTo)) = n.TrainStationCodeLong
inner join dbo.TrainStationDim c on
    n.TrainStationCodeShort = c.TrainStationCode and
    n.CountryCode = c.CountryCode and
    c.LangID = 1033
inner join TrainStationCodeToTrainStationID o on
    ltrim(rtrim(m.TrainStationCodeFrom)) = o.TrainStationCodeLong
inner join dbo.TrainStationDim d on
    o.TrainStationCodeShort = d.TrainStationCode and
    o.CountryCode = d.CountryCode and
    d.LangID = 1033
left join dbo.GeographyTypeTravelProductCountry e on
    e.TravelProductID = b.TravelProductID and 
    e.LineofBusinessID = @LineOfBusinessID and
    c.countrycode = e.Countrycode
left join dbo.GeographyTypeTravelProduct f on
    f.TravelProductID = e.TravelProductID and 
    f.GeographyTypeID = e.GeographyTypeID and
    f.LineofBusinessID = e.LineofBusinessID 
left join dbo.GeographyTypeTravelProductCountry g on
    g.TravelProductID = b.TravelProductID and 
    g.LineofBusinessID = @LineOfBusinessID and
    d.countrycode = g.Countrycode
left join dbo.GeographyTypeTravelProduct h on
    h.TravelProductID = g.TravelProductID and 
    h.GeographyTypeID = g.GeographyTypeID and
    h.LineofBusinessID = g.LineofBusinessID 
left join dbo.GeographyTypeTravelProductCountry i on
    i.TravelProductID = b.TravelProductID and
    i.CountryCode = '' and
    i.LineofBusinessID = @LineOfBusinessID 
left join dbo.GeographyTypeTravelProduct j on
    j.TravelProductID = i.TravelProductID and
    j.GeographyTypeID = i.GeographyTypeID and 
    j.LineOfBusinessID = i.LineOfBusinessID
left join dbo.GeographyTypeTravelProductCountry k on
    k.TravelProductID = b.TravelProductID and
    k.CountryCode = d.CountryCode and
    k.CountryCodeDestination = c.CountryCode and
    k.LineofBusinessID = @LineOfBusinessID
left join dbo.GeographyTypeTravelProduct l on
    l.TravelProductID = k.TravelProductID and
    l.GeographyTypeID = k.GeographyTypeID and 
    l.LineOfBusinessID = k.LineOfBusinessID  
where a.ExternalFileId = @pExternalFileID
              
 select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (Update ExternalFileEUTrainSegmentFact)'
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

