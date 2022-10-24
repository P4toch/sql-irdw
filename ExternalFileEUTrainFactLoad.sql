if object_id('dbo.ExternalFileEUTrainFactLoad') is null begin
    print 'Creating stored procedure ExternalFileEUTrainFactLoad (placeholder)'
    execute('create procedure dbo.ExternalFileEUTrainFactLoad as return 0')
end
go

print 'Altering stored procedure ExternalFileEUTrainFactLoad'
go

alter procedure dbo.ExternalFileEUTrainFactLoad @pExternalFileID int

as

/*
*********************************************************************
Copyright (C) 2006-2013 Expedia, Inc. All rights reserved.

Description:
    Inserts/Updates ExternalFileEUTrainFact with new and modified data

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
    2011-10-28  BSimpson        Added call to ExternalFileEUTrainFactCDFValueLoad.
    2013-10-16  DMurugesan      Added call to ExternalFileEUTrainFactLoadSegmentDerived
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

select @FactSourceIDThis = 13

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
      from dbo.ExternalFile a 
     where ExternalFileStatusID = 2)
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
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUTrainFactLoadInitial', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUTrainFactLoadInitial @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUTrainFactLoadInitial)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUTrainFactLoadInitial', null, @Timer, @ProcedureName
----------------------------------------------------------------------
--  Expand and get most attributes
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUTrainFactLoadExpand', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUTrainFactLoadExpand @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUTrainFactLoadExpand)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUTrainFactLoadExpand', null, @Timer, @ProcedureName
----------------------------------------------------------------------
--  Load Segments
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUTrainSegmentFactLoad', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUTrainSegmentFactLoad @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUTrainSegmentFactLoad)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUTrainSegmentFactLoad', null, @Timer, @ProcedureName
----------------------------------------------------------------------
--  Load Trips
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUTrainTripFactLoad', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUTrainTripFactLoad @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUTrainTripFactLoad)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUTrainTripFactLoad', null, @Timer, @ProcedureName
----------------------------------------------------------------------
--  RouteID
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUTrainFactLoadRoutes', @Timer, null, @ProcedureName

-- Get RouteID, insert newly seen routes to TrainRouteDim
exec @rc = dbo.ExternalFileEUTrainFactLoadRoutes @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select 'here'
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUTrainFactLoadRoutes)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUTrainFactLoadRoutes', null, @Timer, @ProcedureName

----------------------------------------------------------------------
--  Traveler Names
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUTrainFactLoadTravelerNames', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUTrainFactLoadTravelerNames @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUTrainFactLoadTravelerNames)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUTrainFactLoadTravelerNames', null, @Timer, @ProcedureName

----------------------------------------------------------------------
--  Add keys
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUTrainFactKeyLoad', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUTrainFactKeyLoad @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUTrainFactKeyLoad)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUTrainFactKeyLoad', null, @Timer, @ProcedureName

----------------------------------------------------------------------
--  Add CDFs
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUTrainFactCDFValueLoad', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUTrainFactCDFValueLoad @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUTrainFactCDFValueLoad)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUTrainFactCDFValueLoad', null, @Timer, @ProcedureName

----------------------------------------------------------------------
--  Dervied and get most attributes
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUTrainFactLoadSegmentDerived', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUTrainFactLoadSegmentDerived @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUTrainFactLoadSegmentDerived)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUTrainFactLoadSegmentDerived', null, @Timer, @ProcedureName

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

