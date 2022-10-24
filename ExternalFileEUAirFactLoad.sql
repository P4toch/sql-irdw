if object_id('dbo.ExternalFileEUAirFactLoad') is null begin
    print 'Creating stored procedure ExternalFileEUAirFactLoad (placeholder)'
    execute('create procedure dbo.ExternalFileEUAirFactLoad as return 0')
end
go

print 'Altering stored procedure ExternalFileEUAirFactLoad'
go

alter procedure dbo.ExternalFileEUAirFactLoad @pExternalFileID int

as

/*
*********************************************************************
Copyright (C) 2006-2013 Expedia, Inc. All rights reserved.

Description:
    Inserts/Updates ExternalFileEUAirFact with new and modified data

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
    2011-10-28  BSimpson        Added call to ExternalFileEUAirFactCDFValueLoad.
    2013-10-16  DMurugesan      Added call to ExternalFileEUAirFactLoadSegmentDerived.
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

select @FactSourceIDThis = 14

---------------------------------------------------------------------
-- Validation
---------------------------------------------------------------------
-- None
---------------------------------------------------------------------
-- Processing
---------------------------------------------------------------------

----------------------------------------------------------------------
--  Initialize fact rows
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUAirFactLoadInitial', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUAirFactLoadInitial @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUAirFactLoadInitial)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUAirFactLoadInitial', null, @Timer, @ProcedureName
----------------------------------------------------------------------
--  Expand and get most attributes
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUAirFactLoadExpand', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUAirFactLoadExpand @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUAirFactLoadExpand)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUAirFactLoadExpand', null, @Timer, @ProcedureName
----------------------------------------------------------------------
--  Load Segments
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUAirSegmentFactLoad', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUAirSegmentFactLoad @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUAirSegmentFactLoad)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUAirSegmentFactLoad', null, @Timer, @ProcedureName
----------------------------------------------------------------------
--  Load Flights
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUAirFlightFactLoad', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUAirFlightFactLoad @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUAirFlightFactLoad)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUAirFlightFactLoad', null, @Timer, @ProcedureName
----------------------------------------------------------------------
--  RouteID
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUAirFactLoadRoutes', @Timer, null, @ProcedureName

-- Get RouteID, insert newly seen routes to AirRouteDim
exec @rc = dbo.ExternalFileEUAirFactLoadRoutes @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select 'here'
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUAirFactLoadRoutes)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUAirFactLoadRoutes', null, @Timer, @ProcedureName

----------------------------------------------------------------------
--  Traveler Names
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUAirFactLoadTravelerNames', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUAirFactLoadTravelerNames @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUAirFactLoadTravelerNames)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUAirFactLoadTravelerNames', null, @Timer, @ProcedureName

----------------------------------------------------------------------
--  Add keys
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUAirFactKeyLoad', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUAirFactKeyLoad @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUAirFactKeyLoad)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUAirFactKeyLoad', null, @Timer, @ProcedureName

----------------------------------------------------------------------
--  Add CDFs
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUAirFactCDFValueLoad', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUAirFactCDFValueLoad @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUAirFactCDFValueLoad)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUAirFactCDFValueLoad', null, @Timer, @ProcedureName

----------------------------------------------------------------------
--  Dervied and get most attributes
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUAirFactLoadSegmentDerived', @Timer, null, @ProcedureName

exec @rc = dbo.ExternalFileEUAirFactLoadSegmentDerived @pExternalFileID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec ExternalFileEUAirFactLoadSegmentDerived)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pExternalFileID, 'ExternalFileEUAirFactLoadSegmentDerived', null, @Timer, @ProcedureName

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

