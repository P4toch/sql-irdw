if object_id('dbo.InternalExtractEUAirFlightFactLoad') is null
begin
    print 'Creating stored procedure InternalExtractEUAirFlightFactLoad (placeholder)'
    execute ('create procedure dbo.InternalExtractEUAirFlightFactLoad as return 0')
end
go

print 'Altering stored procedure InternalExtractEUAirFlightFactLoad'
go

alter procedure dbo.InternalExtractEUAirFlightFactLoad @pInternalExtractID int, @pDebug bit = 0
as

    /*
    *********************************************************************
    Copyright (C) 2014-2015 Expedia, Inc. All rights reserved.
    
    Description:
         Updates InternalExtractEUAirFact with most attributes
         from the expanded/flattened BookingItem 
    
    Notes:
        
        -- SegmentNbrAdjSum --

        SegmentNbrAdjSum is a binary column representing all segments that make up a flight. 
        Since SegmentNbrAdj is a TinyInt, there is only a possibility of 255 segments.
        We use Bitwise & to store/compare Segment Numbers so Binary values might look like this:
            @Segment = 2 - or - sum(power(2, @Segment - 1)) - or - 00010
            @Segment = 3 - or - sum(power(2, @Segment - 1)) - or - 00100
            A Flight with Segments 2 and 3 would combine those:    00110
    

        -- GeographyType Priority --

        GeographyTypeTravelProduct determines which segment of a flight is the "Winner" in determining
        GeographyTypeID om situations where multiple segments have different GeographyTypeID

        We multiply PriorityOrderNbr by 100 and then choose the MIN value when we aggregate segments.
        From there, we use the MODULO (%) operator to remove the hundreds column. This leaves us with
        the actual GeographyTypeID
    
    Result Set:
        None
    
    Return values:
        0     Success
        -100  Failure
    
    Error codes:
    
    Change History:
        Date        Author          Description
        ----------  --------------- ------------------------------------
        2014-06-09  Jared Ko        Created
        2014-10-02  rakarnati       EGE-72755 - In some cases "AirportTo" can be NULL and can blow 
                                        on the concatenate function while filling RouteDim. Fixed it.
        2014-10-13  Jared Ko        jira.EGE-73190 - EU Air: SegmentValue and FlightValue 
                                        null in Segment and Flight for some billback air
        2015-06-05  Jared Ko        Updated to use dbo.MileageFromLatLongAsTable table function
                                        to support parallelism
        2015-09-11  Jared Ko        jira.EGE-95147 - Segment BookingTypeID is 99 in some cases, resulting in 
                                        failure to populate flight data.
        2015-11-16  minieto         Merging rakarnati Air Duration changes for EGE-96664 
        2015-11-25  minieto         Fixing typo for TravelDateEndUTCOffset
    *********************************************************************
    */

    set nocount on

    ---------------------------------------------------------------------
    -- Declarations
    ---------------------------------------------------------------------


    declare
        @FALSE tinyint, -- Standard constants and variables
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


    declare
        @ERRUNEXPECTED int, -- Error message constants
        @ERRPARAMETER int,
        @MsgParm1 varchar(100),
        @MsgParm2 varchar(100),
        @MsgParm3 varchar(100)


    declare
        @BSIAir tinyint, -- SP specific constants and variables
        @BookingTypeIDPurchase tinyint,
        @BookingTypeIDVoid tinyint,
        @BookingTypeIDExchange tinyint,
        @BookingTypeIDRefund tinyint,
        @BookingTypeIDPRefund tinyint,
        @TimeZoneIDFrom int

    ---------------------------------------------------------------------
    -- Initializations
    ---------------------------------------------------------------------

    select   -- Standard constants
        @FALSE = 0,
        @TRUE = 1,
        @RC_FAILURE = -100,
        @RC_SUCCESS = 0,
        -- @NonNumericString is used to find integer values through exclusion
        -- Example: SELECT @ID = ID FROM tbl WHERE ID not like @NonNumericString
        -- Translated: Rows WHERE ID does NOT include values that are NOT 0-9.
        @NonNumericString = '%[^0-9-]%'

    select   -- Standard variables
        @Current_Timestamp = current_timestamp,
        @ExitCode = @RC_SUCCESS,
        @ProcedureName = object_name(@@procid),
        @SavePointName = '$' + cast(@@nestlevel as varchar(15))
        + '_' + cast(@@procid as varchar(15)),
        @TranStartedBool = @FALSE

    select   -- Error message constants
        @ERRUNEXPECTED = 200104,
        @ERRPARAMETER = 200110

    select   -- SP specific constants and variables
        @BSIAir = 7,
        @TimeZoneIDFrom = 51,        
        @BookingTypeIDPurchase = 1,
        @BookingTypeIDVoid = 4,
        @BookingTypeIDExchange = 5,
        @BookingTypeIDRefund = 7,
        @BookingTypeIDPRefund = 8

    declare
        @flights table (
            InternalExtractID int,
            SalesDocumentCode varchar(20),
            RecordKey varchar(30),
            BookingTypeID tinyint,
            TripNbr int,
            AirlineCode char(3),
            ClassOfServiceCode varchar(2),
            CabinClassID tinyint,
            TravelDateStart smalldatetime,
            TravelDateEnd smalldatetime,
            AirportCodeFrom char(3),
            AirportCodeTo char(3),
            SegmentCnt int,
            MileageCnt float,
            MileageCntGDS float,
            GeographyTypeID tinyint,
            WithinCountryBool bit,
            IncrementCnt smallint,
            CurrencyCode char(3),
            RouteID int,
            SegmentNbrAdjSum varbinary(255), -- The sum of all segments that make up a flight
            TravelDurationMinuteCnt smallint, 
            TravelDateStartUTCOffset datetimeoffset(0),
            TravelDateEndUTCOffset datetimeoffset(0)
        )

    ---------------------------------------------------------------------
    -- Validation
    ---------------------------------------------------------------------
    -- None, done by caller
    ---------------------------------------------------------------------
    -- Processing
    ---------------------------------------------------------------------

    begin try

            select a.InternalExtractID,
                   a.RecordKey,
                   a.SalesDocumentCode,
                   a.SalesDocumentLineNbr,
                   a.BookingTypeID,
                   a.SegmentNbr,
                   a.SegmentDateStart,
                   a.SegmentDateEnd,
                   a.GeographyTypeID,
                   a.WithinCountryBool,
                   a.IncrementCnt,
                   a.CurrencyCode,
                   a.MileageCntGDS,
                   a.ClassOfServiceCode,
                   a.CabinClassID,
                   a.SegmentNbrAdj,
                   a.SegmentDateStartUTCOffset,
                   a.SegmentDateEndUTCOffset
              into #InternalExtractEUAirSegmentFact
              from dbo.InternalExtractEUAirSegmentFact a
             where a.InternalExtractID = @pInternalExtractID and
                   a.BookingTypeID <> 99
             order by InternalExtractID, SalesDocumentCode, SalesDocumentLineNbr, BookingTypeID, SegmentNbr

         create unique clustered index tmpInternalExtractEUAirSegmentFact_InternalExtractID_SalesDocumentCode_SalesDocumentLineNbr_BookingTypeID_SegmentNbr
         on #InternalExtractEUAirSegmentFact(InternalExtractID, SalesDocumentCode, SalesDocumentLineNbr, BookingTypeID, SegmentNbr)

        -- GeographyTypePriority CTE determines which GeographyTypeID will be used for the flight
        ;with Flights
        as (select a.InternalExtractID,
                   a.RecordKey,
                   a.SalesDocumentCode,
                   a.SalesDocumentLineNbr,
                   a.BookingTypeID,
                   a.SegmentNbr,
                   a.SegmentDateStart,
                   a.SegmentDateEnd,
                   p.PriorityOrderNbr * 100 + a.GeographyTypeID as PriorityAndGeographyTypeID,
                   a.GeographyTypeID,
                   a.WithinCountryBool,
                   datediff(minute, b.SegmentDateEnd, a.SegmentDateStart) / 60.0 as DurationHours,
                   1 as TripNbr,
                   a.IncrementCnt,
                   a.CurrencyCode,
                   a.MileageCntGDS,
                   a.ClassOfServiceCode,
                   a.CabinClassID,
                   a.SegmentNbrAdj,
                   a.SegmentDateStartUTCOffset,
                   a.SegmentDateEndUTCOffset
              from #InternalExtractEUAirSegmentFact a
                   inner join
                   dbo.InternalExtractEUAirFact f on a.InternalExtractID = f.InternalExtractID and
                                                     a.RecordKey = f.RecordKey and
                                                     a.BookingTypeID = f.BookingTypeID
                   inner join
                   dbo.GeographyTypeTravelProduct p on a.GeographyTypeID = p.GeographyTypeID and
                                                       f.TravelProductID = p.TravelProductID and
                                                       p.LineofBusinessID = 1 -- Air
                   left join
                   #InternalExtractEUAirSegmentFact b on a.InternalExtractID = b.InternalExtractID and
                                                         a.RecordKey = b.RecordKey and
                                                         a.SalesDocumentCode = b.SalesDocumentCode and
                                                         a.InternalExtractID = b.InternalExtractID and
                                                         a.BookingTypeID = b.BookingTypeID and
                                                         a.SegmentNbr = b.SegmentNbr + 1
            where a.InternalExtractID = @pInternalExtractID and
                  a.BookingTypeID <> 99 and
                  b.InternalExtractID is null -- First Segment
            union all
            select a.InternalExtractID,
                   a.RecordKey,
                   a.SalesDocumentCode,
                   a.SalesDocumentLineNbr,
                   a.BookingTypeID,
                   a.SegmentNbr,
                   a.SegmentDateStart,
                   a.SegmentDateEnd,
                   p.PriorityOrderNbr * 100 + a.GeographyTypeID as PriorityAndGeographyTypeID,
                   a.GeographyTypeID,
                   a.WithinCountryBool,
                   datediff(minute, b.SegmentDateEnd, a.SegmentDateStart) / 60.0 as DurationHours,
                   -- Increment TripNbr if:
                   --   The time between segments is more than 4 hours on a domestic segment
                   --   The time between segments is more than 6 hours on an international segment
                   case
                       when datediff(minute, b.SegmentDateEnd, a.SegmentDateStart) / 60.0 > 4.0 and
                            a.GeographyTypeID <> 2 and 
                            b.GeographyTypeID <> 2
                         then b.TripNbr + 1
                       when datediff(minute, b.SegmentDateEnd, a.SegmentDateStart) / 60.0 > 6.0 then b.TripNbr + 1
                         else b.TripNbr
                   end as TripNbr,
                   a.IncrementCnt,
                   a.CurrencyCode,
                   a.MileageCntGDS,
                   a.ClassOfServiceCode,
                   a.CabinClassID,
                   a.SegmentNbrAdj,
                   a.SegmentDateStartUTCOffset,
                   a.SegmentDateEndUTCOffset
              from #InternalExtractEUAirSegmentFact a
                   inner join
                   dbo.InternalExtractEUAirFact f on a.InternalExtractID = f.InternalExtractID and
                                                     a.RecordKey = f.RecordKey and 
                                                     a.BookingTypeID = f.BookingTypeID
                   inner join
                   dbo.GeographyTypeTravelProduct p on a.GeographyTypeID = p.GeographyTypeID and
                                                       f.TravelProductID = p.TravelProductID and
                                                       p.LineofBusinessID = 1 -- Air
                   inner join
                   Flights b on a.InternalExtractID = b.InternalExtractID and
                                a.RecordKey = b.RecordKey and
                                a.SalesDocumentCode = b.SalesDocumentCode and
                                a.InternalExtractID = b.InternalExtractID and
                                a.BookingTypeID = b.BookingTypeID and
                                a.SegmentNbr = b.SegmentNbr + 1
            -- ToDo: Also check Airport Code?
            where a.InternalExtractID = @pInternalExtractID and
                  a.BookingTypeID <> 99 
        )

        -- With CTE...
        insert @flights (InternalExtractID,
                         RecordKey,
                         SalesDocumentCode,
                         BookingTypeID,
                         TripNbr,
                         ClassOfServiceCode,
                         TravelDateStart,
                         TravelDateEnd,
                         SegmentCnt,
                         GeographyTypeID,
                         WithinCountryBool,
                         IncrementCnt,
                         CurrencyCode,
                         MileageCntGDS,
                         SegmentNbrAdjSum,
                         TravelDateStartUTCOffset,
                         TravelDateEndUTCOffset)

        select InternalExtractID,
               RecordKey,
               SalesDocumentCode,
               null as BookingTypeID,
               TripNbr,
               null as ClassOfServiceCode,
               min(SegmentDateStart) as TravelDateStart,
               max(SegmentDateEnd) as TravelDateEnd,
               count(*) as SegmentCnt,
               min(PriorityAndGeographyTypeID) % 100 as GeographyTypeID, -- Hundreds column is for sorting. Use modulo to remove it.
               min(cast(WithinCountryBool as tinyint)) as WithinCountryBool, -- 0 if any segment is 0.
               IncrementCnt,
               CurrencyCode,
               sum(MileageCntGDS) as MileageCntGDS,
               sum(power(2, SegmentNbrAdj - 1)) as SegmentNbrAdjSum, -- Bitmask - All Segments that make up this flight (see flowerbox notes)
               min(SegmentDateStartUTCOffset) as TravelDateStartUTCOffset,
               max(SegmentDateEndUTCOffset) as TravelDateEndUTCOffset
          from Flights
               group by InternalExtractID,
                        RecordKey,
                        SalesDocumentCode,
                        BookingTypeID,
                        TripNbr,
                        IncrementCnt,
                        CurrencyCode
    end try
    begin catch
        select @Error = @@error
        select @ErrorCode = @ERRUNEXPECTED,
                @MsgParm1 = cast(@Error as varchar(12)) + ' (insert @flights)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end catch

    -- Update Trip Number for InternalExtractEUAirSegmentFact
    begin try

        update sf
        set sf.TripNbr = f.TripNbr
        from @flights f
             inner join 
             dbo.InternalExtractEUAirSegmentFact sf on f.InternalExtractID = sf.InternalExtractID and
                                                       f.RecordKey = sf.RecordKey and
                                                       f.SalesDocumentCode = sf.SalesDocumentCode and
                                                       -- Find original Segment Number
                                                       power(2, sf.SegmentNbrAdj - 1) & f.SegmentNbrAdjSum = power(2, sf.SegmentNbrAdj - 1)
        option(recompile)

    end try
    begin catch

        select @Error = @@error
        select @ErrorCode = @ERRUNEXPECTED,
                @MsgParm1 = cast(@Error as varchar(12)) + ' (Update TripNbr)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)

        goto ErrorHandler

    end catch

    -- Update Sequence Number for InternalExtractEUAirSegmentFact
    begin try

        update sf
        set sf.SequenceNbr = (select count(*) from dbo.InternalExtractEUAirSegmentFact sf2
                              where sf2.InternalExtractID = sf.InternalExtractID and
                                    sf2.TripNbr = sf.TripNbr and
                                    sf2.RecordKey = sf.RecordKey and 
                                    sf2.SegmentNbrAdj <= sf.SegmentNbrAdj)
        from  dbo.InternalExtractEUAirSegmentFact sf 
        where sf.InternalExtractID = @pInternalExtractID
        option(recompile)

    end try
    begin catch

        select @Error = @@error
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Update SequenceNbr)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)

        goto ErrorHandler

    end catch

    -- CabinClassID, ClassOfServiceCode, BookingTypeID, AirlineCode
    -- The following columns are determined by the first segment of
    -- the correlated GeographyTypeID
    begin try

        update a
        set CabinClassID = b.CabinClassID,
            ClassOfServiceCode = b.ClassOfServiceCode,
            BookingTypeID = b.BookingTypeID,
            AirlineCode = b.AirlineCode
        from @flights a
        cross apply (select top 1 *
                       from dbo.InternalExtractEUAirSegmentFact SubQ
                      where SubQ.InternalExtractID = @pInternalExtractID and
                            SubQ.RecordKey = a.RecordKey and
                            SubQ.SalesDocumentCode = a.SalesDocumentCode and
                            SubQ.GeographyTypeID = a.GeographyTypeID and
                            SubQ.TripNbr = a.TripNbr
                      order by SubQ.SegmentNbrAdj) b
    
    end try
    begin catch
        select @Error = @@error
        select @ErrorCode = @ERRUNEXPECTED,
                @MsgParm1 = cast(@Error as varchar(12)) + ' (Lookup Class/BookingType)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)

        goto ErrorHandler
    end catch

    -- Get AirportCode To/From 
    begin try
    
        update a
        set AirportCodeFrom = b.AirportCodeFrom
        from @flights a
        cross apply (select top 1 *
                       from dbo.InternalExtractEUAirSegmentFact SubQ
                      where SubQ.InternalExtractID = @pInternalExtractID and
                            SubQ.RecordKey = a.RecordKey and
                            SubQ.SalesDocumentCode = a.SalesDocumentCode and
                            SubQ.TripNbr = a.TripNbr
                      order by SubQ.SegmentNbrAdj) b

        update a
        set AirportCodeTo = b.AirportCodeTo
        from @flights a
        cross apply (select top 1 *
                       from dbo.InternalExtractEUAirSegmentFact SubQ
                      where SubQ.InternalExtractID = @pInternalExtractID and
                            SubQ.RecordKey = a.RecordKey and
                            SubQ.SalesDocumentCode = a.SalesDocumentCode and
                            SubQ.TripNbr = a.TripNbr and
                            SubQ.AirportCodeTo <> a.AirportCodeFrom
                      order by SubQ.SegmentNbrAdj desc) b
        option(recompile)
    
    end try
    begin catch
        select @Error = @@error
        select @ErrorCode = @ERRUNEXPECTED,
                @MsgParm1 = cast(@Error as varchar(12)) + ' (Lookup AirportCode To/From)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)

        goto ErrorHandler
    end catch

    -- Get mileage between First and Last airport
    -- Note, this is NOT the sum of all segments that make up a flight
    begin try

        update a
        set MileageCnt = round(
                               (select top 1 Mileage
                                from dbo.MileageFromLatLongAsTable(b.Latitude, b.Longitude, c.Latitude, c.Longitude)
                                    ) * 
                                case when a.BookingTypeID in (@BookingTypeIDVoid, @BookingTypeIDRefund, @BookingTypeIDPRefund)
                                    then -1.0
                                    else 1.0
                                end,
                              4)
        from @flights a
        left join dbo.AirportDim b
            on a.AirportCodeFrom = b.AirportCode and
            b.LangID = 1033
        left join dbo.AirportDim c
            on a.AirportCodeTo = c.AirportCode and
            c.LangID = 1033
        option(recompile)

    end try
    begin catch
        select @Error = @@error
        select @ErrorCode = @ERRUNEXPECTED,
                @MsgParm1 = cast(@Error as varchar(12)) + ' (Lookup MileageCnt)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)

        goto ErrorHandler
    end catch

    -- Insert new routes into dimension table
    begin try

        insert into dbo.RouteDim (RouteCode,
                                  AirportCodeFrom,
                                  AirportCodeTo,
                                  RouteCodeBidirectional,
                                  AirportCodeFromBidirectional,
                                  AirportCodeToBidirectional,
                                  UpdateDate,
                                  LastUpdatedBy)
        select distinct upper(a.AirportCodeFrom) + ':' + upper(a.AirportCodeTo) as RouteCode,
                        upper(a.AirportCodeFrom),
                        upper(a.AirportCodeTo),
                        case
                            when upper(a.AirportCodeFrom) < upper(a.AirportCodeTo) then upper(a.AirportCodeFrom) + ':' + upper(a.AirportCodeTo)
                            else upper(a.AirportCodeTo) + ':' + upper(a.AirportCodeFrom)
                        end as RouteCodeBidirectional,
                        case
                            when a.AirportCodeFrom < a.AirportCodeTo then upper(a.AirportCodeFrom)
                            else upper(a.AirportCodeTo)
                        end as AirportCodeFromBidirectional,
                        case
                            when a.AirportCodeFrom < a.AirportCodeTo then upper(a.AirportCodeTo)
                            else upper(a.AirportCodeFrom)
                        end as AirportCodeToBidirectional,
                        UpdateDate = @Current_Timestamp,
                        LastUpdatedBy = 'IDAirFactLoadRoute'
            from @flights a
            left join
            dbo.RouteDim b on a.AirportCodeTo = b.AirportCodeTo and
                a.AirportCodeFrom = b.AirportCodeFrom
        where a.InternalExtractID = @pInternalExtractID and 
            a.AirportCodeTo is not null and 
            b.AirportCodeTo is null

    end try
    begin catch
        select @Error = @@error
        select @ErrorCode = @ERRUNEXPECTED,
                @MsgParm1 = cast(@Error as varchar(12)) + ' (Insert RouteDim)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)

        goto ErrorHandler
    end catch

    -- Add RouteID
    begin try

        update a
        set a.RouteID = b.RouteID
        from @flights a
        inner join dbo.RouteDim b
            on a.AirportCodeFrom = b.AirportCodeFrom and
            a.AirportCodeTo = b.AirportCodeTo
        option(recompile)

    end try
    begin catch
        select @Error = @@error
        select @ErrorCode = @ERRUNEXPECTED,
                @MsgParm1 = cast(@Error as varchar(12)) + ' (Add RouteID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end catch

    -- calcualte TravelDurationMinuteCnt
    begin try

        update a
        set a.TravelDurationMinuteCnt = case when abs(DATEDIFF(DAY, TravelDateStartUTCOffset, TravelDateEndUTCOffset)) < 22
                    then DATEDIFF(MINUTE, TravelDateStartUTCOffset, TravelDateEndUTCOffset) 
                else null end  * 
                                case when BookingTypeID in (@BookingTypeIDVoid, @BookingTypeIDRefund, @BookingTypeIDPRefund)
                                    then -1.0
                                    else 1.0
                                end 
        from @flights a

    end try
    begin catch
        select @Error = @@error
        select @ErrorCode = @ERRUNEXPECTED,
                @MsgParm1 = cast(@Error as varchar(12)) + ' (calcualte TravelDurationMinuteCnt)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end catch
    
    -- Insert InternalExtractEUAirFlightFact from @flights
    begin try

        insert dbo.InternalExtractEUAirFlightFact 
                        (InternalExtractID,
                         RecordKey,
                         SalesDocumentCode,
                         BookingTypeID,
                         TripNbr,
                         AirlineCode,
                         ClassOfServiceCode,
                         CabinClassID,
                         TravelDateStart,
                         TravelDateEnd,
                         RouteID,
                         SegmentCnt,
                         MileageCnt,
                         MileageCntGDS,
                         GeographyTypeID,
                         WithinCountryBool,
                         IncrementCnt,
                         CurrencyCode,
                         TravelDurationMinuteCnt, 
                         TravelDateStartUTCOffset,
                         TravelDateEndUTCOffset)
        select InternalExtractID,
               RecordKey,
               SalesDocumentCode,
               --SalesDocumentLineNbr,
               BookingTypeID,
               TripNbr,
               AirlineCode,
               ClassOfServiceCode,
               CabinClassID,
               TravelDateStart,
               TravelDateEnd,
               RouteID,
               SegmentCnt,
               MileageCnt,
               MileageCntGDS,
               GeographyTypeID,
               WithinCountryBool,
               IncrementCnt,
               CurrencyCode,
               TravelDurationMinuteCnt,
               TravelDateStartUTCOffset,
               TravelDateEndUTCOffset
          from @flights f
         where not exists(select * from dbo.InternalExtractEUAirFlightFact aff
                          where aff.InternalExtractID = @pInternalExtractID and
                                f.InternalExtractID = aff.InternalExtractID and
                                f.SalesDocumentCode = aff.SalesDocumentCode and
                                f.RecordKey = aff.RecordKey)
        option (recompile)
    end try
    begin catch
        select @Error = @@error
        select @ErrorCode = @ERRUNEXPECTED,
                @MsgParm1 = cast(@Error as varchar(12)) + ' (insert InternalExtractEUAirFlightFact)'

        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        if @pDebug = 1
        begin
            select ERROR_NUMBER() AS ErrorNumber,
                   ERROR_SEVERITY() AS ErrorSeverity,
                   ERROR_STATE() AS ErrorState,
                   ERROR_PROCEDURE() AS ErrorProcedure,
                   ERROR_LINE() AS ErrorLine,
                   ERROR_MESSAGE() AS ErrorMessage
        end
        goto ErrorHandler
    end catch

    if ((@TrancountSave = 0) and (@TranStartedBool = @TRUE))
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
-- set statistics io off
-- exec [InternalExtractEUAirFlightFactLoad] @pInternalExtractID = 30566
-- exec [InternalExtractEUAirFlightFactLoad] @pInternalExtractID = 30568
-- exec [InternalExtractEUAirFlightFactLoad] @pInternalExtractID = 30569
-- exec [InternalExtractEUAirFlightFactLoad] @pInternalExtractID = 30571
-- exec [InternalExtractEUAirFlightFactLoad] @pInternalExtractID = 30572

-- select *
--     from dbo.InternalExtractEUAirFlightFact ieeff
-- where SalesDocumentCode = 'FRI11490608' and
--     SalesDocumentLineNbr = 10000

-- select *
--     from InternalExtractEUAirSegmentFact
-- where SalesDocumentCode = 'FRI11490608' and
--     SalesDocumentLineNbr = 10000

-- select *
--     from dbo.Nav_Travel_Ledger_Entry ntle
-- where ntle.Document_No_ = '10000'


/*
declare @InternalExtractID int = 37712

select * from dbo.InternalExtractEUAirSegmentFact asf where InternalExtractID = @InternalExtractID order by SalesDocumentCode, SalesDocumentLineNbr
select * from dbo.InternalExtractEUAirFlightFact aff where InternalExtractID = @InternalExtractID order by SalesDocumentCode, SalesDocumentLineNbr

begin tran

    delete dbo.InternalExtractEUAirSegmentFact where InternalExtractID = @InternalExtractID
    exec [InternalExtractEUAirSegmentFactLoad] @pInternalExtractID = @InternalExtractID
    
    delete dbo.InternalExtractEUAirFlightFact where InternalExtractID = @InternalExtractID
    exec dbo.InternalExtractEUAirFlightFactLoad @pInternalExtractID = @InternalExtractID
        
select * from dbo.InternalExtractEUAirSegmentFact where InternalExtractID = @InternalExtractID order by SalesDocumentCode, SalesDocumentLineNbr
select * from dbo.InternalExtractEUAirFlightFact aff where InternalExtractID = @InternalExtractID order by SalesDocumentCode, SalesDocumentLineNbr

rollback tran

*/