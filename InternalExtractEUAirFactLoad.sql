if object_id('dbo.InternalExtractEUAirFactLoad') is null begin
    print 'Creating stored procedure InternalExtractEUAirFactLoad (placeholder)'
    execute('create procedure dbo.InternalExtractEUAirFactLoad as return 0')
end
go

print 'Altering stored procedure InternalExtractEUAirFactLoad'
go

alter procedure dbo.InternalExtractEUAirFactLoad @pInternalExtractID int

as

/*
*********************************************************************
Copyright (C) 2016 Expedia, Inc. All rights reserved.

Description:
    Wrapper proc for Inserts/Updates to InternalExtractEUAir% table

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2014-06-24  JaredKo         Created.
    2016-01-14  pbressan        AirFactLoadEUAirSavings post processing step.
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
    @Timer                                datetime,
    @past_months                          tinyint
            
declare @FactSourceLookup table (FactSourceID tinyint, SourceDatabaseName varchar(255))

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

    -- Number of months for missed savings data search
    @past_months                          = 1

-- Populate a mapping between FactSourceID and Navision extract type
insert into @FactSourceLookup
    select 30, 'Navision_Europe' union all 
    select 33, 'Navision_VIA' 

select @FactSourceIDThis = b.FactSourceID
from dbo.InternalExtract a 
join @FactSourceLookup b on a.SourceDatabaseName = b.SourceDatabaseName
where InternalExtractID = @pInternalExtractID

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
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUAirFactLoadInitial', @Timer, null, @ProcedureName

exec @rc = dbo.InternalExtractEUAirFactLoadInitial @pInternalExtractID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec InternalExtractEUAirFactLoadInitial)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUAirFactLoadInitial', null, @Timer, @ProcedureName
----------------------------------------------------------------------
--  Expand and get most attributes
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUAirFactLoadExpand', @Timer, null, @ProcedureName

exec @rc = dbo.InternalExtractEUAirFactLoadExpand @pInternalExtractID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec InternalExtractEUAirFactLoadExpand)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUAirFactLoadExpand', null, @Timer, @ProcedureName
----------------------------------------------------------------------
--  Load Segments
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUAirSegmentFactLoad', @Timer, null, @ProcedureName

exec @rc = dbo.InternalExtractEUAirSegmentFactLoad @pInternalExtractID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec InternalExtractEUAirSegmentFactLoad)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUAirSegmentFactLoad', null, @Timer, @ProcedureName
----------------------------------------------------------------------
--  Load Flights
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUAirFlightFactLoad', @Timer, null, @ProcedureName

exec @rc = dbo.InternalExtractEUAirFlightFactLoad @pInternalExtractID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec InternalExtractEUAirFlightFactLoad)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUAirFlightFactLoad', null, @Timer, @ProcedureName


----------------------------------------------------------------------
--  RouteID
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUAirFactLoadRoutes', @Timer, null, @ProcedureName

-- Get RouteID, insert newly seen routes to AirRouteDim
exec @rc = dbo.InternalExtractEUAirFactLoadRoutes @pInternalExtractID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select 'here'
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec InternalExtractEUAirFactLoadRoutes)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUAirFactLoadRoutes', null, @Timer, @ProcedureName

----------------------------------------------------------------------
--  Traveler Names
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUAirFactLoadTravelerNames', @Timer, null, @ProcedureName

exec @rc = dbo.InternalExtractEUAirFactLoadTravelerNames @pInternalExtractID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec InternalExtractEUAirFactLoadTravelerNames)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUAirFactLoadTravelerNames', null, @Timer, @ProcedureName

----------------------------------------------------------------------
--  Add keys
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUAirFactKeyLoad', @Timer, null, @ProcedureName

exec @rc = dbo.InternalExtractEUAirFactKeyLoad @pInternalExtractID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec InternalExtractEUAirFactKeyLoad)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUAirFactKeyLoad', null, @Timer, @ProcedureName

----------------------------------------------------------------------
--  Add CDFs
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUAirFactCDFValueLoad', @Timer, null, @ProcedureName

exec @rc = dbo.InternalExtractEUAirFactCDFValueLoad @pInternalExtractID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec InternalExtractEUAirFactCDFValueLoad)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUAirFactCDFValueLoad', null, @Timer, @ProcedureName

----------------------------------------------------------------------
--  Dervied and get most attributes
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUAirFactLoadSegmentDerived', @Timer, null, @ProcedureName

exec @rc = dbo.InternalExtractEUAirFactLoadSegmentDerived @pInternalExtractID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec InternalExtractEUAirFactLoadSegmentDerived)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUAirFactLoadSegmentDerived', null, @Timer, @ProcedureName


----------------------------------------------------------------------
--  Amounts
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUAirFactLoadAmounts', @Timer, null, @ProcedureName

exec @rc = dbo.InternalExtractEUAirFactLoadAmounts @pInternalExtractID
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec InternalExtractEUAirFactLoadAmounts)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'InternalExtractEUAirFactLoadAmounts', null, @Timer, @ProcedureName


----------------------------------------------------------------------
--  AirFactLoadEUAirSavings
----------------------------------------------------------------------
set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'AirFactLoadEUAirSavings', @Timer, null, @ProcedureName

exec @rc = dbo.AirFactLoadEUAirSavings @past_months
select @Error = @@Error
if (@Error <> 0 or @rc <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (exec AirFactLoadEUAirSavings)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

set @Timer = getdate()
exec @rc = dbo.FactSourceProcessLogMrg @FactSourceIDThis, @pInternalExtractID, 'AirFactLoadEUAirSavings', null, @Timer, @ProcedureName


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
-- set statistics io on
-- set statistics io off
-- select @@trancount

-- delete InternalExtractEUAirFact          where InternalExtractID = 30637
-- delete InternalExtractEUAirFactCDFValue  where InternalExtractID = 30637
-- delete InternalExtractEUAirFactKey       where InternalExtractID = 30637
-- delete InternalExtractEUAirFlightFact    where InternalExtractID = 30637
-- delete InternalExtractEUAirSegmentFact   where InternalExtractID = 30637


-- exec [InternalExtractEUAirFactLoad] @pInternalExtractID = 30637
