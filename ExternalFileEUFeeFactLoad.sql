if object_id('dbo.ExternalFileEUFeeFactLoad') is null begin
    print 'Creating stored procedure ExternalFileEUFeeFactLoad (placeholder)'
    execute('create procedure dbo.ExternalFileEUFeeFactLoad as return 0')
end
go

print 'Altering stored procedure ExternalFileEUFeeFactLoad'
go

alter procedure dbo.ExternalFileEUFeeFactLoad @pExternalFileID int

as

/*
*********************************************************************
Copyright (C) 2008-2011 Expedia, Inc. All rights reserved.

Description:
    Inserts/Updates ExternalFileEUFeeFact with new and modified data

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2008-03-15  BarryC          Created.
    2011-10-28  BSimpson        Added call to ExternalFileEUFeeFactCDFValueLoad.
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
    @ExternalFileTypeIDThis               tinyint,
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
    @FactSourceIDThis = 18,
    @ExternalFileTypeIDThis = 11 
  


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

----------------------------------------------------------------------
--  Initialize fact rows
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUFeeFactLoadInitial', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUFeeFactLoadInitial @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUFeeFactLoadInitial)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUFeeFactLoadInitial', null, @Timer, @ProcedureName
----------------------------------------------------------------------
--  Expand and get most attributes
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUFeeFactLoadExpand', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUFeeFactLoadExpand @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUFeeFactLoadExpand)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUFeeFactLoadExpand', null, @Timer, @ProcedureName
----------------------------------------------------------------------
--  Traveler Names
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUFeeFactLoadTravelerNames', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUFeeFactLoadTravelerNames @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUFeeFactLoadTravelerNames)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUFeeFactLoadTravelerNames', null, @Timer, @ProcedureName

----------------------------------------------------------------------
--  Add keys
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUFeeFactKeyLoad', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUFeeFactKeyLoad @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUFeeFactKeyLoad)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUFeeFactKeyLoad', null, @Timer, @ProcedureName

----------------------------------------------------------------------
--  Add CDFs
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUFeeFactCDFValueLoad', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUFeeFactCDFValueLoad @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUFeeFactCDFValueLoad)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUFeeFactCDFValueLoad', null, @Timer, @ProcedureName

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

