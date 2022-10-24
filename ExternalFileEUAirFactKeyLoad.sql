if object_id('dbo.ExternalFileEUAirFactKeyLoad') is null begin
    print 'Creating stored procedure ExternalFileEUAirFactKeyLoad (placeholder)'
    execute('create procedure dbo.ExternalFileEUAirFactKeyLoad as return 0')
end
go

print 'Altering stored procedure ExternalFileEUAirFactKeyLoad'
go

alter procedure dbo.ExternalFileEUAirFactKeyLoad
    @pExternalFileID int

as

/*
*********************************************************************
Copyright (C) 2005-2006 Expedia, Inc. All rights reserved.

Description:
     Inserts records to ExternalFileEUAirFactKeyLoad 

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2005-08-09  BarryC          Created.
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
    @FactKeyTypeID tinyint

---------------------------- ----------------------------------------
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
    @FactKeyTypeID = 18 


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


    insert into dbo.ExternalFileEUAirFactKey (ExternalFileID, RecordKey, BookingTypeID)
    select ExternalFileID, RecordKey, BookingTypeID
      from dbo.ExternalFileEUAirFact
     where ExternalFileID = @pExternalFileID

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (insert ExternalFileEUAirFactKey)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    insert into dbo.AirFactKey (FactKeyTypeID, FactKeyID)
    select @FactKeyTypeID, ExternalFileEUAirFactID
      from dbo.ExternalFileEUAirFactKey
     where ExternalFileID = @pExternalFileID 

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (insert AirFactKey)'
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
