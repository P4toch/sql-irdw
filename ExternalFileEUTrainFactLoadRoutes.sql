if object_id('dbo.ExternalFileEUTrainFactLoadRoutes') is null begin
    print 'Creating stored procedure ExternalFileEUTrainFactLoadRoutes (placeholder)'
    execute('create procedure dbo.ExternalFileEUTrainFactLoadRoutes as return 0')
end
go

print 'Altering stored procedure ExternalFileEUTrainFactLoadRoutes'
go

alter procedure dbo.ExternalFileEUTrainFactLoadRoutes
    @pExternalFileID int
with recompile
as

/*
*********************************************************************
Copyright (C) 2006-2018 Expedia, Inc. All rights reserved.

Description:
     Updates ExternalFileEUTrainFact
             ExternalFileEUTrainTripFact
             ExternalFileEUTrainSegmentFact with RouteID.
     Adds rows to RouteDim.

     Updates ExternalFileEUTrainFact with RouteTxt

 

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
    2007-04-09  BarryC          Lengthed TrainStationCode(s)
    2009-07-25  BarryC          Added to segment level, new fields in TrainRouteDim
    2012-05-24  BSimpson        RAID1005784: Update TrainStationDim table to lengthen TrainStationCode field and add CityCode/Name fields
   	2016-08-30  jappleberry     Update to use new TrainStationCodeToTrainStationID mapping table 
                                 and added 4 StationIDs to TrainRouteDim
                                Updates ExternalFileEUTrainFact with RouteTxt
    2016-10-01  jappleberry     Bug fix - Updated to use new TrainStationCodeToTrainStationID instead
	                             of TrainStationDim joining on TrainStationCodeShort in join to
	                             get TrainStationIDs for creation of new TrainRouteDim Rows
    2016-10-10  jappleberry     Added routine to parse TrainStationCode from / to from the 
                                RouteTxt in ExternalFileEUTrainTicket because the 
                                Train Statin Codes are Truncated to 10 characters.
    2018-02-23 jappleberry      Modified ExternalFileEUTrainFactLoadRoutes
	                            To fix rout creation in #RouteSegment
    2018-03-29 pbressan         RouteTxt: Display city name when available
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
   TrainStationCodeFrom varchar(16) null,
   TrainStationCodeTo varchar(16) null,
   TrainStationCodeFromLong varchar(16) null,
   TrainStationCodeToLong varchar(16) null,
   RouteTxt varchar(500) null  
)
create index temp_ix on #RouteTicket (ExternalFileID, RecordKey, BookingTypeID)


create table #RouteTrip (
   ExternalFileID int not null,
   RecordKey  varchar(30) not null,
   BookingTypeID int not null,
   TripNbr tinyint not null,
   TrainStationCodeFrom varchar(16) null,
   TrainStationCodeTo varchar(16) null,
   TrainStationCodeFromLong varchar(16) null,
   TrainStationCodeToLong varchar(16) null
)
create index temp_ix on #RouteTrip (ExternalFileID, RecordKey, BookingTypeID, TripNbr)


create table #RouteSegment (
   ExternalFileID int not null,
   RecordKey  varchar(30) not null,
   BookingTypeID int not null,
   SegmentNbr tinyint not null,
   TrainStationCodeFrom varchar(16) null,
   TrainStationCodeTo varchar(16) null,
   TrainStationCodeFromLong varchar(16) null,
   TrainStationCodeToLong varchar(16) null,
   TrainRouteID int null
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
insert into #RouteTicket (ExternalFileID, RecordKey, BookingTypeID, TrainStationCodeFrom, TrainStationCodeTo, TrainStationCodeFromLong, TrainStationCodeToLong)
select a.ExternalFileID, a.RecordKey, a.BookingTypeID, 
    case when (substring(ltrim(rtrim(b.TrainStationCodeFrom)),3,1) = '-')
         then substring(ltrim(rtrim(b.TrainStationCodeFrom)),4,16) 
         else ltrim(rtrim(b.TrainStationCodeFrom))
    end,
    case when (substring(ltrim(rtrim(b.TrainStationCodeTo)),3,1) = '-') 
         then substring(ltrim(rtrim(b.TrainStationCodeTo)),4,16) 
         else ltrim(rtrim(b.TrainStationCodeTo))
    end, 
    ltrim(rtrim(b.TrainStationCodeFrom)),
    ltrim(rtrim(b.TrainStationCodeTo))
  from dbo.ExternalFileEUTrainFact a
       inner join
       dbo.ExternalFileEUTrainTicket b 
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

update #RouteTicket  
    set RouteTxt = left(l.RouteTxt,500)  -- limit is 500 characters
    from 
(select  distinct ExternalFileID, RecordKey, 
        (select  left(coalesce(f.CityName,f.DisplayNameLocal,'Unknown'),25) + '/'   -- take the first 25 characters of the Station Name
                -- test if the the StationCodeTo is not the same as the StationCodeFrom on the next segment put an '-' between the names
                +  case when (coalesce(h.CityName,g.TrainStationCodeLong) <> coalesce(j.CityName,i.TrainStationCodeLong)) then left(coalesce(h.CityName,h.DisplayNameLocal,'Unknown'),25) + '_' else '' end 
                -- test if the StationCodeFrom on the next segment is null we are done with segments so put this segment's StationCodeTo and the end of the RouteTxt
                +  case when (i.TrainStationCodeLong is null) then  left(coalesce(h.CityName,h.DisplayNameLocal,'Unknown'),25) else '' end
            as [text()]
        from dbo.ExternalFileEUTrainFact a
        join dbo.ExternalFileEUTrainTicketSegment b on  a.ExternalFileID = b.ExternalFileID and a.RecordKey = b.RecordKey  
        left join dbo.ExternalFileEUTrainTicketSegment c on  a.ExternalFileID = c.ExternalFileID and a.RecordKey = c.RecordKey
                                                                and b.SegmentNbr + 1 = c.SegmentNbr
        join dbo.TravelProductDim d on a.TravelProductId = d.TravelProductID
        left join dbo.TrainStationCodetoTrainStationID e on b.TrainStationCodeFrom = e.TrainStationCodeLong
        left Join dbo.TrainStationDim f on e.TrainStationID = f.TrainStationID and f.LangID = d.DefaultLangID
        left join dbo.TrainStationCodetoTrainStationID g on b.TrainStationCodeTo = g.TrainStationCodeLong
        left Join dbo.TrainStationDim h on g.TrainStationID = h.TrainStationID and h.LangID = d.DefaultLangID
        left join dbo.TrainStationCodetoTrainStationID i on c.TrainStationCodeFrom = i.TrainStationCodeLong
        left Join dbo.TrainStationDim j on i.TrainStationID = j.TrainStationID and j.LangID = d.DefaultLangID
        where a.ExternalFileID = k.ExternalFileID
            and a.RecordKey = k.RecordKey
        order by b.SegmentNbr
        for xml path ('')) as RouteTxt  
    from ExternalFileEUTrainFact k
    where k.ExternalFileID = @pExternalFileID
) l
where #RouteTicket.ExternalFileID = l.ExternalFileID
    and #RouteTicket.RecordKey = l.RecordKey

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #RouteTicket)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


update #RouteTicket
    set TrainStationCodeFrom = 
            case when (substring(ltrim(rtrim(b.RouteTxt)),3,1) = '-')
                then substring(ltrim(rtrim(b.RouteTxt)),4,charindex('/', b.RouteTxt) - 4)
		        else substring(ltrim(rtrim(b.RouteTxt)),1,charindex('/', b.RouteTxt) - 1)
	        end, 
        TrainStationCodeTo = 
            case when (len(b.RouteTxt) - len(replace(b.RouteTxt, '_', '')) > 0)
                then case when (substring(ltrim(rtrim(b.RouteTxt)), (charindex('/', b.RouteTxt)+3), 1) = '-')
                        then substring(ltrim(rtrim(b.RouteTxt)), len(b.RouteTxt) - charindex('/',reverse(b.RouteTxt), charindex('_',reverse(b.RouteTxt))) +5,
                                (len(b.RouteTxt) - charindex('_',reverse(b.RouteTxt))) - (len(b.RouteTxt) - charindex('/',reverse(b.RouteTxt), charindex('_',reverse(b.RouteTxt))) +4 ))
                        else substring(ltrim(rtrim(b.RouteTxt)), len(b.RouteTxt) - charindex('/',reverse(b.RouteTxt), charindex('_',reverse(b.RouteTxt))) +2,
                                (len(b.RouteTxt) - charindex('_',reverse(b.RouteTxt))) - (len(b.RouteTxt) - charindex('/',reverse(b.RouteTxt), charindex('_',reverse(b.RouteTxt))) +1 ))
	                    end 
                else case when len(b.RouteTxt) - len(replace(b.RouteTxt, '/', '')) < 2
                        then case when (substring(ltrim(rtrim(b.RouteTxt)), (charindex('/', b.RouteTxt)+3), 1) = '-')
                                    then substring(ltrim(rtrim(b.RouteTxt)), (charindex('/', b.RouteTxt) +4), charindex('/', replace(b.RouteTxt+'/','_','/'),charindex('/', b.RouteTxt) + 4) - (charindex('/', b.RouteTxt) +4))
                                    else substring(ltrim(rtrim(b.RouteTxt)), (charindex('/', b.RouteTxt) + 1),  charindex('/', replace(b.RouteTxt+'/','_','/'), (charindex('/', b.RouteTxt) + 1)) - (charindex('/', b.RouteTxt) +1))
	                            end 
                        else case when (substring(ltrim(rtrim(b.RouteTxt)), (charindex('/', b.RouteTxt)+3), 1) = '-')
                                    then substring(ltrim(rtrim(b.RouteTxt)), len(b.RouteTxt) - charindex('/',reverse(b.RouteTxt),charindex('/',reverse(b.RouteTxt))+1) + 5, 
                                        (len(b.RouteTxt) - charindex('/',reverse(b.RouteTxt))) -  (len(b.RouteTxt) - charindex('/',reverse(b.RouteTxt),charindex('/',reverse(b.RouteTxt))+1)+4))
                                    else substring(ltrim(rtrim(b.RouteTxt)), len(b.RouteTxt) - charindex('/',reverse(b.RouteTxt),charindex('/',reverse(b.RouteTxt))+1) + 2, 
                                        (len(b.RouteTxt) - charindex('/',reverse(b.RouteTxt))) -  (len(b.RouteTxt) - charindex('/',reverse(b.RouteTxt),charindex('/',reverse(b.RouteTxt))+1)+1))
	                            end 
                        end
                end,
        TrainStationCodeFromLong = 
            substring(ltrim(rtrim(b.RouteTxt)),1,charindex('/', b.RouteTxt) - 1),
        TrainStationCodeToLong = 
            case when (len(b.RouteTxt) - len(replace(b.RouteTxt, '_', '')) > 0)
                then substring(ltrim(rtrim(b.RouteTxt)), len(b.RouteTxt) - charindex('/',reverse(b.RouteTxt), charindex('_',reverse(b.RouteTxt))) +2,
                    (len(b.RouteTxt) - charindex('_',reverse(b.RouteTxt))) - (len(b.RouteTxt) - charindex('/',reverse(b.RouteTxt), charindex('_',reverse(b.RouteTxt))) +1 ))
                else case when len(b.RouteTxt) - len(replace(b.RouteTxt, '/', '')) < 2
                        then substring(ltrim(rtrim(b.RouteTxt)), (charindex('/', b.RouteTxt) + 1), charindex('/', replace(b.RouteTxt+'/','_','/'),charindex('/', b.RouteTxt) + 1) - (charindex('/', b.RouteTxt) +1))
                        else substring(ltrim(rtrim(b.RouteTxt)), len(b.RouteTxt) - charindex('/',reverse(b.RouteTxt),charindex('/',reverse(b.RouteTxt))+1) + 2, 
                            (len(b.RouteTxt) - charindex('/',reverse(b.RouteTxt))) -  (len(b.RouteTxt) - charindex('/',reverse(b.RouteTxt),charindex('/',reverse(b.RouteTxt))+1)+1))
                    end 
            end
  from dbo.ExternalFileEUTrainFact a
       inner join
       dbo.ExternalFileEUTrainTicket b 
           on a.ExternalFileID = b.ExternalFileID and
              a.RecordKey = b.RecordKey and
              a.BookingTypeID = b.BookingTypeID
 where #RouteTicket.ExternalFileID = a.ExternalFileID
   and #RouteTicket.RecordKey = b.RecordKey
   and #RouteTicket.BookingTypeID = b.BookingTypeID
   and left(a.RecordKey, 2) not in ('FR', 'ES')

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #RouteTicket)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

insert into #RouteTrip (ExternalFileID, RecordKey, BookingTypeID, TripNbr, TrainStationCodeFrom, TrainStationCodeTo, TrainStationCodeFromLong, TrainStationCodeToLong)
select a.ExternalFileID, a.RecordKey, a.BookingTypeID, a.TripNbr,
    case when (substring(ltrim(rtrim(b.TrainStationCodeFrom)),3,1) = '-')
         then substring(ltrim(rtrim(b.TrainStationCodeFrom)),4,16) 
         else ltrim(rtrim(b.TrainStationCodeFrom))
    end,
    case when (substring(ltrim(rtrim(b.TrainStationCodeTo)),3,1) = '-') 
         then substring(ltrim(rtrim(b.TrainStationCodeTo)),4,16) 
         else ltrim(rtrim(b.TrainStationCodeTo))
    end, 
    ltrim(rtrim(b.TrainStationCodeFrom)),
    ltrim(rtrim(b.TrainStationCodeTo))
  from dbo.ExternalFileEUTrainTripFact a
       inner join
       dbo.ExternalFileEUTrainTicketTrip b 
           on a.ExternalFileID = b.ExternalFileID and
              a.RecordKey = b.RecordKey and
              a.TripNbr = b.TripNbr
 where a.ExternalFileID = @pExternalFileID

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #RouteTrip)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

-- ExternalFileEUTrainSegmentFact already loaded with TrainStationCodeShort For TranStationCode From and To
insert into #RouteSegment (ExternalFileID, RecordKey, BookingTypeID, SegmentNbr, TrainStationCodeFrom, TrainStationCodeTo, TrainStationCodeFromLong, TrainStationCodeToLong, TrainRouteID)
select a.ExternalFileID, a.RecordKey, a.BookingTypeID, a.SegmentNbr, a.TrainStationCodeFrom, a.TrainStationCodeTo,
       ltrim(rtrim(b.TrainStationCodeFrom)),
       ltrim(rtrim(b.TrainStationCodeTo)), 
       TrainRouteID
  from dbo.ExternalFileEUTrainSegmentFact a
  inner join
       dbo.ExternalFileEUTrainTicketSegment b on
            a.ExternalFileID = b.ExternalFileID and
            a.RecordKey = b.RecordKey and
            --a.BookingTypeID = b.BookingTypeID and   --Refund not matching Partial Refund
            a.SegmentNbr = b.SegmentNbr 
 where a.ExternalFileID = @pExternalFileID

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #RouteSegment)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

select @TrancountSave = @@Trancount
if (@TrancountSave = 0) begin transaction @SavePointName
else                    save  transaction @SavePointName
select @TranStartedBool = @TRUE


    -- Insert new routes into dimension table
    insert into dbo.TrainRouteDim (TrainRouteCode, TrainStationCodeFrom, TrainStationCodeTo, TrainRouteCodeBidirectional, TrainStationCodeFromBidirectional, TrainStationCodeToBidirectional,
                                   TrainStationIDFrom, TrainStationIDTo, TrainStationIDFromBidirectional, TrainStationIDToBidirectional, UpdateDate, LastUpdatedBy )
    select distinct
           upper(a.TrainStationCodeFrom) + ':' + upper(a.TrainStationCodeTo) as TrainRouteCode,
           upper(a.TrainStationCodeFrom), 
           upper(a.TrainStationCodeTo),
           case when upper(a.TrainStationCodeFrom) < upper(a.TrainStationCodeTo) then upper(a.TrainStationCodeFrom) + ':' + upper(a.TrainStationCodeTo)
                else upper(a.TrainStationCodeTo) + ':' + upper(a.TrainStationCodeFrom) end as TrainRouteCodeBidirectional,
           case when a.TrainStationCodeFrom < a.TrainStationCodeTo then upper(a.TrainStationCodeFrom)
                else upper(a.TrainStationCodeTo) end as TrainStationCodeFromBidirectional,
           case when a.TrainStationCodeFrom < a.TrainStationCodeTo then upper(a.TrainStationCodeTo)
                else upper(a.TrainStationCodeFrom) end as TrainStationCodeToBidirectional,
           c.TrainStationID, 
           d.TrainStationID,
           case when a.TrainStationCodeFrom < a.TrainStationCodeTo then c.TrainStationID
                else d.TrainStationID end as TrainStationIDFromBidirectional,
           case when a.TrainStationCodeFrom < a.TrainStationCodeTo then d.TrainStationID
                else c.TrainStationID end as TrainStationIDToBidirectional,
           UpdateDate = @Current_Timestamp,
           LastUpdatedBy = 'EFEUTrainFactLoadRoutes'
      from #RouteTicket a 
           left join
           dbo.TrainRouteDim b on
               a.TrainStationCodeTo = b.TrainStationCodeTo and
               a.TrainStationCodeFrom = b.TrainStationCodeFrom
           inner join 
           dbo.TrainStationCodeToTrainStationID c on a.TrainStationCodeFromLong = c.TrainStationCodeLong
           inner join 
           dbo.TrainStationCodeToTrainStationID d on a.TrainStationCodeToLong = d.TrainStationCodeLong
     where b.TrainStationCodeTo is null
    
    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (insert TrainRouteDim)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- Insert new routes into dimension table
    insert into dbo.TrainRouteDim (TrainRouteCode, TrainStationCodeFrom, TrainStationCodeTo, TrainRouteCodeBidirectional, TrainStationCodeFromBidirectional, TrainStationCodeToBidirectional, 
                                   TrainStationIDFrom, TrainStationIDTo, TrainStationIDFromBidirectional, TrainStationIDToBidirectional, UpdateDate, LastUpdatedBy )
 
   select distinct
           upper(a.TrainStationCodeFrom) + ':' + upper(a.TrainStationCodeTo) as TrainRouteCode,
           upper(a.TrainStationCodeFrom), 
           upper(a.TrainStationCodeTo),
           case when upper(a.TrainStationCodeFrom) < upper(a.TrainStationCodeTo) then upper(a.TrainStationCodeFrom) + ':' + upper(a.TrainStationCodeTo)
                else upper(a.TrainStationCodeTo) + ':' + upper(a.TrainStationCodeFrom) end as TrainRouteCodeBidirectional,
           case when a.TrainStationCodeFrom < a.TrainStationCodeTo then upper(a.TrainStationCodeFrom)
                else upper(a.TrainStationCodeTo) end as TrainStationCodeFromBidirectional,
           case when a.TrainStationCodeFrom < a.TrainStationCodeTo then upper(a.TrainStationCodeTo)
                else upper(a.TrainStationCodeFrom) end as TrainStationCodeToBidirectional,
           c.TrainStationID, 
           d.TrainStationID,
           case when a.TrainStationCodeFrom < a.TrainStationCodeTo then c.TrainStationID
                else d.TrainStationID end as TrainStationIDFromBidirectional,
           case when a.TrainStationCodeFrom < a.TrainStationCodeTo then d.TrainStationID
                else c.TrainStationID end as TrainStationIDToBidirectional,
           UpdateDate = @Current_Timestamp,
           LastUpdatedBy = 'EFEUTrainFactLoadRoutes'
      from #RouteTrip a 
           left join
           dbo.TrainRouteDim b on
               a.TrainStationCodeTo = b.TrainStationCodeTo and
               a.TrainStationCodeFrom = b.TrainStationCodeFrom
           inner join 
           dbo.TrainStationCodeToTrainStationID c on a.TrainStationCodeFromLong = c.TrainStationCodeLong
           inner join 
           dbo.TrainStationCodeToTrainStationID d on a.TrainStationCodeToLong = d.TrainStationCodeLong
     where b.TrainStationCodeTo is null
    
    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (insert TrainRouteDim)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end    

    -- Insert new routes into dimension table
    insert into dbo.TrainRouteDim (TrainRouteCode, TrainStationCodeFrom, TrainStationCodeTo, TrainRouteCodeBidirectional, TrainStationCodeFromBidirectional, TrainStationCodeToBidirectional, 
                                   TrainStationIDFrom, TrainStationIDTo, TrainStationIDFromBidirectional, TrainStationIDToBidirectional, UpdateDate, LastUpdatedBy )
    select distinct
           upper(a.TrainStationCodeFrom) + ':' + upper(a.TrainStationCodeTo) as TrainRouteCode,
           upper(a.TrainStationCodeFrom), 
           upper(a.TrainStationCodeTo),
           case when upper(a.TrainStationCodeFrom) < upper(a.TrainStationCodeTo) then upper(a.TrainStationCodeFrom) + ':' + upper(a.TrainStationCodeTo)
                else upper(a.TrainStationCodeTo) + ':' + upper(a.TrainStationCodeFrom) end as TrainRouteCodeBidirectional,
           case when a.TrainStationCodeFrom < a.TrainStationCodeTo then upper(a.TrainStationCodeFrom)
                else upper(a.TrainStationCodeTo) end as TrainStationCodeFromBidirectional,
           case when a.TrainStationCodeFrom < a.TrainStationCodeTo then upper(a.TrainStationCodeTo)
                else upper(a.TrainStationCodeFrom) end as TrainStationCodeToBidirectional,
           c.TrainStationID, 
           d.TrainStationID,
           case when a.TrainStationCodeFrom < a.TrainStationCodeTo then c.TrainStationID
                else d.TrainStationID end as TrainStationIDFromBidirectional,
           case when a.TrainStationCodeFrom < a.TrainStationCodeTo then d.TrainStationID
                else c.TrainStationID end as TrainStationIDToBidirectional,
           UpdateDate = @Current_Timestamp,
           LastUpdatedBy = 'EFEUTrainFactLoadRoutes'
      from #RouteSegment a 
           left join
           dbo.TrainRouteDim b on
               a.TrainStationCodeTo = b.TrainStationCodeTo and
               a.TrainStationCodeFrom = b.TrainStationCodeFrom
           inner join 
           dbo.TrainStationCodeToTrainStationID c on a.TrainStationCodeFromLong = c.TrainStationCodeLong
           inner join 
           dbo.TrainStationCodeToTrainStationID d on a.TrainStationCodeToLong = d.TrainStationCodeLong
     where a.TrainRouteID is null
       and b.TrainStationCodeTo is null
    
    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (insert TrainRouteDim)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end 

    -- Update ticket fact table with route id
    update dbo.ExternalFileEUTrainFact
       set TrainRouteID = coalesce(c.TrainRouteID, e.TrainRouteID),
           RouteTxt = b.RouteTxt,
           UpdateDate = @Current_TimeStamp,
           LastUpdatedBy = 'EFEUTrainFactLoadRoutes'
      from dbo.ExternalFileEUTrainFact a
           inner join    
           #RouteTicket b on
               a.ExternalFileID = b.ExternalFileID and
               a.RecordKey = b.RecordKey and
               a.BookingTypeID = b.BookingTypeID 
           left join 
           dbo.TrainRouteDim c  on
               b.TrainStationCodeTo = c.TrainStationCodeTo and
               b.TrainStationCodeFrom = c.TrainStationCodeFrom
	       left join 
		   dbo.ExternalFileEUTrainTicket d on 
               a.ExternalFileID = d.ExternalFileID and
               a.RecordKey = d.RecordKey and
               a.BookingTypeID = d.BookingTypeID
           left join 
           dbo.TrainRouteDim e on
		       e.TrainStationCodeFrom =
		           case when (substring(ltrim(rtrim(d.RouteTxt)),3,1) = '-')
                       then substring(ltrim(rtrim(d.RouteTxt)),4,charindex('/', d.RouteTxt) - 4)
		               else  substring(ltrim(rtrim(d.RouteTxt)),1,charindex('/', d.RouteTxt) - 1)
		            end and
               e.TrainStationCodeTo =
	               case when (len(d.RouteTxt) - len(replace(d.RouteTxt, '_', '')) > 0)
                        then case when (substring(ltrim(rtrim(d.RouteTxt)), (charindex('/', d.RouteTxt)+3), 1) = '-')
                                then substring(ltrim(rtrim(d.RouteTxt)), len(d.RouteTxt) - charindex('/',reverse(d.RouteTxt), charindex('_',reverse(d.RouteTxt))) +5,
                                        (len(d.RouteTxt) - charindex('_',reverse(d.RouteTxt))) - (len(d.RouteTxt) - charindex('/',reverse(d.RouteTxt), charindex('_',reverse(d.RouteTxt))) +4 ))
                                else substring(ltrim(rtrim(d.RouteTxt)), len(d.RouteTxt) - charindex('/',reverse(d.RouteTxt), charindex('_',reverse(d.RouteTxt))) +2,
                                        (len(d.RouteTxt) - charindex('_',reverse(d.RouteTxt))) - (len(d.RouteTxt) - charindex('/',reverse(d.RouteTxt), charindex('_',reverse(d.RouteTxt))) +1 ))
	                            end 
                        else case when len(d.RouteTxt) - len(replace(d.RouteTxt, '/', '')) < 2
                                then case when (substring(ltrim(rtrim(d.RouteTxt)), (charindex('/', d.RouteTxt)+3), 1) = '-')
                                            then substring(ltrim(rtrim(d.RouteTxt)), (charindex('/', d.RouteTxt) +4), charindex('/', replace(d.RouteTxt+'/','_','/'),charindex('/', d.RouteTxt) + 4) - (charindex('/', d.RouteTxt) +4))
                                            else substring(ltrim(rtrim(d.RouteTxt)), (charindex('/', d.RouteTxt) + 1),  charindex('/', replace(d.RouteTxt+'/','_','/'), (charindex('/', d.RouteTxt) + 1)) - (charindex('/', d.RouteTxt) +1))
	                                    end 
                                else case when (substring(ltrim(rtrim(d.RouteTxt)), (charindex('/', d.RouteTxt)+3), 1) = '-')
                                            then substring(ltrim(rtrim(d.RouteTxt)), len(d.RouteTxt) - charindex('/',reverse(d.RouteTxt),charindex('/',reverse(d.RouteTxt))+1) + 5, 
                                                (len(d.RouteTxt) - charindex('/',reverse(d.RouteTxt))) -  (len(d.RouteTxt) - charindex('/',reverse(d.RouteTxt),charindex('/',reverse(d.RouteTxt))+1)+4))
                                            else substring(ltrim(rtrim(d.RouteTxt)), len(d.RouteTxt) - charindex('/',reverse(d.RouteTxt),charindex('/',reverse(d.RouteTxt))+1) + 2, 
                                                (len(d.RouteTxt) - charindex('/',reverse(d.RouteTxt))) -  (len(d.RouteTxt) - charindex('/',reverse(d.RouteTxt),charindex('/',reverse(d.RouteTxt))+1)+1))
	                                    end 
                                end
                        end
     where a.ExternalFileID = @pExternalFileID

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update ExternalFileEUTrainFact TrainRouteID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- Update trip fact table with route id
    update dbo.ExternalFileEUTrainTripFact
       set TrainRouteID = c.TrainRouteID
      from dbo.ExternalFileEUTrainTripFact a
           inner join    
           #RouteTrip b on
               a.ExternalFileID = b.ExternalFileID and
               a.RecordKey = b.RecordKey and
               a.TripNbr = b.TripNbr
           inner join 
           dbo.TrainRouteDim c  on
               b.TrainStationCodeTo = c.TrainStationCodeTo and
               b.TrainStationCodeFrom = c.TrainStationCodeFrom
     where a.ExternalFileID = @pExternalFileID

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update ExternalFileEUTrainTripFact TrainRouteID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- Update segment fact table with route id
    update dbo.ExternalFileEUTrainSegmentFact
       set TrainRouteID = c.TrainRouteID
      from dbo.ExternalFileEUTrainSegmentFact a
           inner join    
           #RouteSegment b on
               a.ExternalFileID = b.ExternalFileID and
               a.RecordKey = b.RecordKey and
               a.BookingTypeID = b.BookingTypeID  and
               a.SegmentNbr = b.SegmentNbr
           inner join 
           dbo.TrainRouteDim c  on
               b.TrainStationCodeTo = c.TrainStationCodeTo and
               b.TrainStationCodeFrom = c.TrainStationCodeFrom
     where a.ExternalFileID = @pExternalFileID

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update ExternalFileEUTrainSegmentFact TrainRouteID)'
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

GO

/*

select RouteTxt, * 
from dbo.ExternalFileEUTrainFact 
where TrainRouteId is null
order by ExternalFileID


exec [dbo].[ExternalFileEUTrainFactLoadRoutes_Fix] @pExternalFileID = 30995

drop procedure ExternalFileEUTrainFactLoadRoutes_fix

exec dbo.ExternalFileEUTrainFactLoadRoutes @pExternalFileID = 31038

*/


