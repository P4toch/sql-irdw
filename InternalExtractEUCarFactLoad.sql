if object_id('dbo.InternalExtractEUCarFactLoad') is null begin
    print 'Creating stored procedure InternalExtractEUCarFactLoad (placeholder)'
    execute('create procedure dbo.InternalExtractEUCarFactLoad as return 0')
end
go

print 'Altering stored procedure InternalExtractEUCarFactLoad'
go

alter procedure dbo.InternalExtractEUCarFactLoad @pInternalExtractID int

as

/*
*********************************************************************
Copyright (C) 2013 Expedia, Inc. All rights reserved.

Description:
    Inserts/Updates InternalExtractEUCarFact with new and modified data

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2013-05-23  VBoerner        Created.
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

declare     -- SP specific constants and variables
    @BookingTypeIDReserve                 tinyint,
    @BookingTypeIDCancel                  tinyint,
    @FactSourceIDThis                     tinyint,
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

select   -- SP specific constants and variables
    @BookingTypeIDReserve                 = 3,
    @BookingTypeIDCancel                  = 2,

    @FactSourceIDThis                     = 27

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
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUCarFactLoadInitial', @Timer, null, @ProcedureName

exec @rc = dbo.InternalExtractEUCarFactLoadInitial @pInternalExtractID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec InternalExtractEUCarFactLoadInitial)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUCarFactLoadInitial', null, @Timer, @ProcedureName
----------------------------------------------------------------------
--  Expand/Flatten and get most attributes
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUCarFactLoadExpand', @Timer, null, @ProcedureName

exec @rc = dbo.InternalExtractEUCarFactLoadExpand @pInternalExtractID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec InternalExtractEUCarFactLoadExpand)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUCarFactLoadExpand', null, @Timer, @ProcedureName
----------------------------------------------------------------------
--  Traveler Name
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUCarFactLoadTravelerNames', @Timer, null, @ProcedureName

exec @rc = dbo.InternalExtractEUCarFactLoadTravelerNames @pInternalExtractID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec InternalExtractEUCarFactLoadTravelerNames)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUCarFactLoadTravelerNames', null, @Timer, @ProcedureName
----------------------------------------------------------------------
--  CDE
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUCarFactCDETxtLoad', @Timer, null, @ProcedureName

exec @rc = dbo.InternalExtractEUCarFactCDETxtLoad @pInternalExtractID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec InternalExtractEUCarFactCDETxtLoad)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUCarFactCDETxtLoad', null, @Timer, @ProcedureName
----------------------------------------------------------------------
--  Amount derived information
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUCarFactLoadAmounts', @Timer, null, @ProcedureName

exec @rc = dbo.InternalExtractEUCarFactLoadAmounts @pInternalExtractID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec InternalExtractEUCarFactLoadAmounts)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUCarFactLoadAmounts', null, @Timer, @ProcedureName
----------------------------------------------------------------------
--  Car special equipment
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUCarSpecialEquipmentFactLoad', @Timer, null, @ProcedureName

exec @rc = dbo.InternalExtractEUCarSpecialEquipmentFactLoad @pInternalExtractID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec InternalExtractEUCarSpecialEquipmentFactLoad)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUCarSpecialEquipmentFactLoad', null, @Timer, @ProcedureName
----------------------------------------------------------------------
--  Add keys
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUCarFactKeyLoad', @Timer, null, @ProcedureName

exec @rc = dbo.InternalExtractEUCarFactKeyLoad @pInternalExtractID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec InternalExtractEUCarFactKeyLoad)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUCarFactKeyLoad', null, @Timer, @ProcedureName

goto ExitProc
---------------------------------------------------------------------
-- Error Handler
---------------------------------------------------------------------
ErrorHandler:
    while @@TRANCOUNT > 0 ROLLBACK TRAN
    select   @ExitCode = @RC_FAILURE
    goto ExitProc

---------------------------------------------------------------------
-- Exit Procedure
---------------------------------------------------------------------
ExitProc:
    return (@ExitCode)
go

/*

exec dbo.InternalExtractEUCarFactLoad
    @pInternalExtractID = 16409 

select *
from InternalExtract

*/
