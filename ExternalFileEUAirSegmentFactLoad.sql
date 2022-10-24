if object_id('dbo.ExternalFileEUAirSegmentFactLoad') is null begin
    print 'Creating stored procedure ExternalFileEUAirSegmentFactLoad (placeholder)'
    execute('create procedure dbo.ExternalFileEUAirSegmentFactLoad as return 0')
end
go

print 'Altering stored procedure ExternalFileEUAirSegmentFactLoad'
go

alter procedure dbo.ExternalFileEUAirSegmentFactLoad 
    @pExternalFileID int
with recompile
as

/*
*********************************************************************
Copyright (C) 2006-2013 Expedia, Inc. All rights reserved.

Description:
    Inserts/Updates ExternalFileEUAirSegmentFact with new and modified
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
    2012-12-04  VBoerner        Add exchange rate handling for PL/CZ TPIDs
    2013-10-14  DMurugesan      GeographyTypeID override using GeographyTypeTravelProduct mapping table
    2014-07-18  DMurugesan      EGE-68306 Fix GeographyTypeID choice
********************************************************************************************************
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

insert into dbo.ExternalFileEUAirSegmentFact (ExternalFileID,  RecordKey,  BookingTypeID,  SegmentNbr,  TripNbr,  
    SequenceNbr,  AirlineCode,  FlightNumberTxt,  ClassOfServiceCode,  CabinClassID,  SegmentDateStart, 
    SegmentDateEnd,  AirportCodeFrom,  AirportCodeTo,  MileageCnt, MileageCntGDS, AirFareBasisCode,
    GeographyTypeID,  WithinCountryBool,  ConnectionBool, IncrementCnt, CurrencyCode  ) 
 select 
    b.ExternalFileID,  
    ltrim(rtrim(b.RecordKey)),  
    b.BookingTypeID,  
    b.SegmentNbr,  
    b.TripNbr,  
    b.SequenceNbr,  
    ltrim(rtrim(b.AirlineCode)),  
    ltrim(rtrim(b.FlightNumberTxt)),  
    ltrim(rtrim(b.ClassOfServiceCode)),  
    b.CabinClassID,  
    b.SegmentDateStart, 
    b.SegmentDateEnd,  
    ltrim(rtrim(b.AirportCodeFrom)),  
    ltrim(rtrim(b.AirportCodeTo)),  
    dbo.MileageFromLatLong(c.Latitude, c.Longitude, d.Latitude, d.Longitude) *
        case when a.BookingTypeID in (@BookingTypeIDVoid, @BookingTypeIDRefund, @BookingTypeIDPRefund) then -1.0
             else 1.0 end as MileageCnt,
    b.MileageCntGDS,
    b.AirFareBasisCode,
    null as GeographyTypeID,  
    b.WithinCountryBool,
    b.ConnectionBool,
    a.IncrementCnt, 
    e.CurrencyCodeStorage
  from dbo.ExternalFileEUAirFact a 
       inner join
       dbo.ExternalFileEUAirTicketSegment b on
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
where a.ExternalFileID = @pExternalFileID 

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert ExternalFileEUAirSegmentFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

--  SegmentValue
update dbo.ExternalFileEUAirSegmentFact
   set SegmentValue = case when coalesce(C.MileageCntTotal,0.0) <> 0 then (a.MileageCnt/C.MileageCntTotal) * b.TicketAmtBase else null end
  from dbo.ExternalFileEUAirSegmentFact a
       inner join 
       dbo.ExternalFileEUAirFact b on 
            a.ExternalFileID = b.ExternalFileID and 
            a.RecordKey = b.RecordKey and
            a.BookingTypeID = b.BookingTypeID 
       inner join
       (select ExternalFileID, RecordKey, BookingTypeID,
              case when count(1) = count(MileageCnt) then sum(MileageCnt) else null end as MileageCntTotal
         from dbo.ExternalFileEUAirSegmentFact
        where ExternalFileId = @pExternalFileID
        group by ExternalFileID, RecordKey, BookingTypeID) as C on 
            a.ExternalFileID = C.ExternalFileID and 
            a.RecordKey = C.RecordKey and
            a.BookingTypeID = C.BookingTypeID 
where a.ExternalFileId = @pExternalFileID

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (Update ExternalFileEUAirSegmentFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

------ Update GeographyTypeID ------   
Update A 
set GeographyTypeID =  case  when k.GeographyTypeID is not null then k.GeographyTypeID --- Highest priority for City Pair
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
from dbo.ExternalFileEUAirSegmentFact a
inner join dbo.ExternalFileEUAirFact b on
    a.ExternalFileID = b.ExternalFileID and 
    a.RecordKey = b.RecordKey and
    a.BookingTypeID = b.BookingTypeID 
inner join dbo.AirportDim c on
    a.AirportCodeTo = c.AirportCode and 
    c.LangID = 1033
inner join dbo.AirportDim d on
    a.AirportCodeFrom = d.AirportCode and 
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
           @MsgParm1    = cast(@Error as varchar(12)) + ' (Update ExternalFileEUAirSegmentFact)'
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

