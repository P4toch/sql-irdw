if object_id('dbo.InternalExtractEUCarFactKeyLoad') is null begin
    print 'Creating stored procedure InternalExtractEUCarFactKeyLoad (placeholder)'
    execute('create procedure dbo.InternalExtractEUCarFactKeyLoad as return 0')
end
go

print 'Altering stored procedure InternalExtractEUCarFactKeyLoad'
go

alter procedure dbo.InternalExtractEUCarFactKeyLoad
    @pInternalExtractID int

as

/*
*********************************************************************
Copyright (C) 2013 Expedia, Inc. All rights reserved.

Description:
    Inserts records into InternalExtractCarFactKeyLoad.

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
    @RC                             int,            
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
    @FactKeyTypeID = 25

---------------------------------------------------------------------
-- Validation
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Processing
---------------------------------------------------------------------
select @TrancountSave = @@Trancount
if (@TrancountSave = 0) begin transaction @SavePointName
else                    save  transaction @SavePointName
select @TranStartedBool = @TRUE


    insert into dbo.InternalExtractEUCarFactKey (InternalExtractID, CarCartID, CarCartLogID, BookingTypeID, BookingSystemID)
    select InternalExtractID, CarCartID, CarCartLogID, BookingTypeID, BookingSystemID 
      from dbo.InternalExtractEUCarFact
     where InternalExtractID = @pInternalExtractID

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (insert InternalExtractEUCarFactKey)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    insert into dbo.CarFactKey (FactKeyTypeID, FactKeyID)
    select @FactKeyTypeID, InternalExtractEUCarFactID
      from dbo.InternalExtractEUCarFactKey
     where InternalExtractID = @pInternalExtractID 

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (insert CarFactKey)'
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

exec dbo.InternalExtractEUCarFactKeyLoad
    @pInternalExtractID = 16468 

select *
from dbo.InternalExtractEUCarFactKey 

*/