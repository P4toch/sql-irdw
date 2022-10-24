if object_id('dbo.ExternalFileEUHotelFactLoadComparison') is null begin
    print 'Creating stored procedure ExternalFileEUHotelFactLoadComparison (placeholder)'
    execute('create procedure dbo.ExternalFileEUHotelFactLoadComparison as return 0')
end
go

print 'Altering stored procedure ExternalFileEUHotelFactLoadComparison'
go

-- Below must be there for indexed
-- views
set quoted_identifier on
go

alter procedure dbo.ExternalFileEUHotelFactLoadComparison
    @pExternalFileID int

as

/*
*********************************************************************
Copyright (C) 2007-2014 Expedia, Inc. All rights reserved.

Description:
     Adds rows to ExternalFileEUHotelFactComparison.

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2010-10-16  BSimpson        Created.
    2013-07-26  VBoerner        Default DescriptionTxt to ''.
    2014-09-08  rakarnati       EGE-69083 - Changed the source from dbo.vExternalFileEUHotel to TravelLogECT.dbo.HotelTransactionLog
                                    Also, back fill comparables data from EventCommnet when found in HotelTransactionLog 
    2014-09-23  rakarnati       EGE-72506 - Fixed currency conversion issue which should base on the HotelTransactionLog.CurrecnyCode
    2014-09-24  rakarnati       EGE-72505 - Back fill EU Hotel Comparisons only when NO comparison data is available in HotelTransactionComparisonLog 
    2014-09-26  rakarnati       EGE-69083 - Added filter to process same-hotel comparables only. 
    2014-10-14  rakarnati       EGE-73670 - Extract date boundary overlap can cause to load duplicate comparable with new source as the matching done on TRL
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

declare   -- Sproc specific constants and variables
    @HotelRateComparisonTypeIDESR   int,
    @HotelRateComparisonTypeIDEPR   int,
    @HotelRateComparisonTypeIDNeg   int,
    @HotelRateComparisonTypeIDPub   int,
    
    @HotelComparisonTypeIDSameHotel  tinyint 
    
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

select   -- Sproc specific constants and variables
    @HotelRateComparisonTypeIDESR   = 1,
    @HotelRateComparisonTypeIDEPR   = 2,
    @HotelRateComparisonTypeIDNeg   = 3,
    @HotelRateComparisonTypeIDPub   = 4,
    
    @HotelComparisonTypeIDSameHotel  = 1 

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

    -- Insert new comparables into fact source table
    insert into dbo.ExternalFileEUHotelFactComparison
        (   ExternalFileID, RecordKey, BookingTypeID, HotelRateComparisonTypeID, CurrencyCode, DescriptionTxt,
            TotalBaseAmtPrice,
            TotalTaxAmtPrice
            )
        select 
            a.ExternalFileID, a.RecordKey, a.BookingTypeID, D.HotelRateComparisonTypeID, tpd.CurrencyCodeStorage, coalesce(convert(varchar(255), D.RoomTypeDesc), '') DescriptionTxt,
            D.TotalBaseAmtPrice * erdf.ExchangeRateUsed,
            isnull( TotalTaxAmtPrice, 0.0 ) * erdf.ExchangeRateUsed 
        from dbo.ExternalFileEUHotelIAN a
            inner join 
            (select 
                c.HotelRateComparisonTypeID, b.TravelProductID, b.TRL, b.CurrencyCode, c.TotalBaseAmtPrice, c.TotalTaxAmtPrice, c.RoomTypeDesc,
                row_number() over (partition by b.TRL, b.TravelProductID, c.HotelRateComparisonTypeID order by b.InternalExtractID desc, b.HotelTransactionLogID desc) RankNbr
            from dbo.HotelTransactionLog b 
            inner join
            dbo.HotelTransactionComparisonLog c on b.InternalExtractID = c.InternalExtractID
                        and b.HotelTransactionLogID = c.HotelTransactionLogID   
                        and c.HotelComparisonTypeID = @HotelComparisonTypeIDSameHotel
                ) D ON isnumeric(a.ItineraryTxt) = 1 and PATINDEX('%[^0-9]%', a.ItineraryTxt) = 0 and -- to filter out non-numeric ItineraryTxt 
                    substring(a.ItineraryTxt, 2, 8) = D.TRL and a.TravelProductID = D.TravelProductID
            inner join 
            dbo.TravelProductDim tpd on a.TravelProductID = tpd.TravelProductID
            inner join 
            dbo.ExchangeRateDailyFull erdf
                on dbo.TimeIDFromDate(coalesce(a.InvoiceDate, a.IssueDate)) = erdf.TimeID                
                and D.CurrencyCode = erdf.FromCurrencyCode
                and tpd.CurrencyCodeStorage = erdf.ToCurrencyCode
        where ExternalFileID = @pExternalFileID and ExternalRecordStatusID = 1
            AND D.RankNbr = 1 

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (insert ExternalFileEUHotelFactComparison)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    -- Insert new comparables into fact source table from EventCommon where comparables were not found in the new source HotelTransactionLog
    insert into dbo.ExternalFileEUHotelFactComparison
        (   ExternalFileID, RecordKey, BookingTypeID, HotelRateComparisonTypeID, CurrencyCode, DescriptionTxt,
            TotalBaseAmtPrice,
            TotalTaxAmtPrice
            )
        select 
            a.ExternalFileID, a.RecordKey, a.BookingTypeID, a.HotelRateComparisonTypeID, tpd.CurrencyCodeStorage, coalesce(a.DescriptionTxt, ''),
            a.TotalBaseAmtPrice * erdf.ExchangeRateUsed,
            isnull(a.TotalTaxAmtPrice, 0.0) * erdf.ExchangeRateUsed            
            from (
                select TravelProductID, InvoiceDate, IssueDate, ExternalFileID, RecordKey, BookingTypeID,
                    HotelRateComparisonTypeID   = @HotelRateComparisonTypeIDEPR,
                    CurrencyCode                = CurrencyCodeEPR,
                    TotalBaseAmtPrice           = BaseAmtEPR,
                    TotalTaxAmtPrice            = TaxAmtEPR,
                    DescriptionTxt              = DescriptionTxtEPR
                    from dbo.vExternalFileEUHotel
                    where ExternalFileID = @pExternalFileID and ExternalRecordStatusID = 1
                    and nullif( BaseAmtEPR, 0.0 ) is not null and CurrencyCodeEPR is not null
                union all
                select TravelProductID, InvoiceDate, IssueDate, ExternalFileID, RecordKey, BookingTypeID,
                    HotelRateComparisonTypeID   = @HotelRateComparisonTypeIDESR,
                    CurrencyCode                = CurrencyCodeESR,
                    TotalBaseAmtPrice           = BaseAmtESR,
                    TotalTaxAmtPrice            = TaxAmtESR,
                    DescriptionTxt              = DescriptionTxtESR
                    from dbo.vExternalFileEUHotel
                    where ExternalFileID = @pExternalFileID and ExternalRecordStatusID = 1
                    and nullif( BaseAmtESR, 0.0 ) is not null and CurrencyCodeESR is not null
                union all
                select TravelProductID, InvoiceDate, IssueDate, ExternalFileID, RecordKey, BookingTypeID,
                    HotelRateComparisonTypeID   = @HotelRateComparisonTypeIDPub,
                    CurrencyCode                = CurrencyCodePublished,
                    TotalBaseAmtPrice           = BaseAmtPublished,
                    TotalTaxAmtPrice            = TaxAmtPublished,
                    DescriptionTxt              = DescriptionTxtPublished
                    from dbo.vExternalFileEUHotel
                    where ExternalFileID = @pExternalFileID and ExternalRecordStatusID = 1
                    and nullif( BaseAmtPublished, 0.0 ) is not null and CurrencyCodePublished is not null
                union all
                select TravelProductID, InvoiceDate, IssueDate, ExternalFileID, RecordKey, BookingTypeID,
                    HotelRateComparisonTypeID   = @HotelRateComparisonTypeIDNeg,
                    CurrencyCode                = CurrencyCodeNegotiated,
                    TotalBaseAmtPrice           = BaseAmtNegotiated,
                    TotalTaxAmtPrice            = TaxAmtNegotiated,
                    DescriptionTxt              = DescriptionTxtNegotiated
                    from dbo.vExternalFileEUHotel
                    where ExternalFileID = @pExternalFileID and ExternalRecordStatusID = 1
                    and nullif( BaseAmtNegotiated, 0.0 ) is not null and CurrencyCodeNegotiated is not null
                ) a
            join dbo.TravelProductDim tpd on a.TravelProductID = tpd.TravelProductID
            join dbo.ExchangeRateDailyFull erdf
                on dbo.TimeIDFromDate(coalesce(a.InvoiceDate, a.IssueDate)) = erdf.TimeID                
                and a.CurrencyCode = erdf.FromCurrencyCode
                and tpd.CurrencyCodeStorage = erdf.ToCurrencyCode
            left join 
            dbo.ExternalFileEUHotelFactComparison d on 
                a.ExternalFileID = d.ExternalFileID and 
                a.RecordKey = d.RecordKey and 
                a.BookingTypeID = d.BookingTypeID 
        where 
            d.ExternalFileID is null
        
    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (insert ExternalFileEUHotelFactComparison from EventCommon)'
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


/*
declare @pExternalFileID int
select @pExternalFileID = 20591 --  20413 -- 20395 --  

--select top 100 * from dbo.ExternalFileEUHotelFactComparison order by ExternalFileID desc
select * from dbo.ExternalFileEUHotelFactComparison where ExternalFileID = @pExternalFileID
delete dbo.ExternalFileEUHotelFactComparison where ExternalFileID = @pExternalFileID

exec dbo.ExternalFileEUHotelFactLoadComparison
    @pExternalFileID = @pExternalFileID

select * from dbo.ExternalFileEUHotelFactComparison where ExternalFileID = @pExternalFileID

*/