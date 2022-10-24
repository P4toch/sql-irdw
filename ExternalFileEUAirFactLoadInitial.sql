if object_id('dbo.ExternalFileEUAirFactLoadInitial') is null begin
    print 'Creating stored procedure ExternalFileEUAirFactLoadInitial (placeholder)'
    execute('create procedure dbo.ExternalFileEUAirFactLoadInitial as return 0')
end
go

print 'Altering stored procedure ExternalFileEUAirFactLoadInitial'
go

alter procedure dbo.ExternalFileEUAirFactLoadInitial 
    @pExternalFileID int
with recompile
as

/*
*********************************************************************
Copyright (C) 2006 Expedia, Inc. All rights reserved.

Description:
     Inserts index set of Air fact records for a particular
     external file.

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2006-07-03  BarryC          Created.
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


create table #ExternalFileEUAirFact (
    ExternalFileID int not null,
    RecordKey varchar(30) not null,
    BookingTypeID tinyint not null)
    

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
-- Done by Caller
---------------------------------------------------------------------
-- Processing
---------------------------------------------------------------------

insert into #ExternalFileEUAirFact (ExternalFileID, RecordKey, BookingTypeID)
select a.ExternalFileID, a.RecordKey, coalesce(a.BookingTypeID,0)
  from dbo.ExternalFileEUAirTicket a
 where a.ExternalFileID = @pExternalFileID 
       and a.ExternalRecordStatusID = 1
        
select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #ExternalFileAirFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

-- Delete records of unknown booking type
delete from #ExternalFileEUAirFact where BookingTypeID not in (select BookingTypeID from BookingTypeDim)

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (delete #ExternalFileAirFact)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


select @TrancountSave = @@Trancount
if (@TrancountSave = 0) begin transaction @SavePointName
else                    save  transaction @SavePointName
select @TranStartedBool = @TRUE

    insert into dbo.ExternalFileEUAirFact (ExternalFileID, RecordKey, BookingTypeID, UpdateDate, LastUpdatedBy)
    select a.ExternalFileID, a.RecordKey, a.BookingTypeID, @Current_TimeStamp, 'EFEUAirFactLoadIntial'
      from #ExternalFileEUAirFact a

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (insert to ExternalFileAirFact)'
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

