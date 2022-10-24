if object_id('dbo.ExternalFileEUHotelFactLoad') is null begin
    print 'Creating stored procedure ExternalFileEUHotelFactLoad (placeholder)'
    execute('create procedure dbo.ExternalFileEUHotelFactLoad as return 0')
end
go

print 'Altering stored procedure ExternalFileEUHotelFactLoad'
go

alter procedure dbo.ExternalFileEUHotelFactLoad @pExternalFileID int

as

/*
*********************************************************************
Copyright (C) 2007-2011 Expedia, Inc. All rights reserved.

Description:
    Inserts/Updates ExternalFileEUHotelFact with new and modified data

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2007-10-26  VBoerner        Created.
    2011-10-28  BSimpson        Added call to ExternalFileEUHotelFactCDFValueLoad.
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
    @BookingTypeIDVoidRefund              tinyint,

    @FactSourceIDThis                     int,
    @ExternalFileTypeIDThis               int,
    @ExternalFileType_IAN                 int,
    @FactSourceID_IAN                     int,
    @FactSourceID_AMADEUS                 int,
    @Timer                                datetime
            
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

select 
    @ExternalFileType_IAN                 = 9,
    @FactSourceID_IAN                     = 15,
    @FactSourceID_AMADEUS                 = 16  
  
---------------------------------------------------------------------
-- Validation
---------------------------------------------------------------------
if (@pExternalFileID is null) begin
    select @ErrorCode = @ERRPARAMETER,
           @MsgParm1  = '@pExternalFileID',
           @MsgParm2  = IsNull(convert(varchar(30),@pExternalFileID),'NULL')
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1, @MsgParm2)
    goto ErrorHandler
end

-- If extract processed or not in proper state, exit without error
if not exists (
    select *
      from dbo.ExternalFile  
     where ExternalFileStatusID = 2
        and ExternalFileID = @pExternalFileID)
begin
    goto ExitProc
end

---------------------------------------------------------------------
-- Processing
---------------------------------------------------------------------

select @ExternalFileTypeIDThis = ExternalFileTypeID
from dbo.ExternalFile where ExternalFileID = @pExternalFileID

select @FactSourceIDThis = case when @ExternalFileTypeIDThis = @ExternalFileType_IAN
    then @FactSourceID_IAN else @FactSourceID_AMADEUS end

----------------------------------------------------------------------
--  Initialize fact rows
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUHotelFactLoadInitial', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUHotelFactLoadInitial @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUHotelFactLoadInitial)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUHotelFactLoadInitial', null, @Timer, @ProcedureName
----------------------------------------------------------------------
--  Expand and get most attributes
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUHotelFactLoadExpand', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUHotelFactLoadExpand @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUHotelFactLoadExpand)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUHotelFactLoadExpand', null, @Timer, @ProcedureName
----------------------------------------------------------------------
--  Comparisons
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUHotelFactLoadComparison', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUHotelFactLoadComparison @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUHotelFactLoadComparison)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUHotelFactLoadComparison', null, @Timer, @ProcedureName
----------------------------------------------------------------------
--  Traveler Names
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUHotelFactLoadTravelerNames', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUHotelFactLoadTravelerNames @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUHotelFactLoadTravelerNames)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUHotelFactLoadTravelerNames', null, @Timer, @ProcedureName

----------------------------------------------------------------------
--  Add keys
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUHotelFactKeyLoad', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUHotelFactKeyLoad @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUHotelFactKeyLoad)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUHotelFactKeyLoad', null, @Timer, @ProcedureName

----------------------------------------------------------------------
--  Add CDFs
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUHotelFactCDFValueLoad', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUHotelFactCDFValueLoad @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUHotelFactCDFValueLoad)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUHotelFactCDFValueLoad', null, @Timer, @ProcedureName

goto ExitProc
---------------------------------------------------------------------
-- Error Handler
---------------------------------------------------------------------
ErrorHandler:
    while @@Trancount > 0 rollback tran
    select   @ExitCode = @RC_FAILURE
    goto ExitProc

---------------------------------------------------------------------
-- Exit Procedure
---------------------------------------------------------------------
ExitProc:
    return (@ExitCode)
go

