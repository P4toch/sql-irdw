if object_id('dbo.InternalExtractEUAirFactKeyLoad') is null begin
    print 'Creating stored procedure InternalExtractEUAirFactKeyLoad (placeholder)'
    execute('create procedure dbo.InternalExtractEUAirFactKeyLoad as return 0')
end
go

print 'Altering stored procedure InternalExtractEUAirFactKeyLoad'
go

alter procedure dbo.InternalExtractEUAirFactKeyLoad
    @pInternalExtractID int
as

/*
*********************************************************************
Copyright (C) 2014 Expedia, Inc. All rights reserved.

Description:
     Inserts records to InternalExtractEUAirFactKeyLoad 

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2014-07-21  JaredKo         Created.
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
    @FactKeyTypeID = 28 --  InternalExtractEUAirFactKey


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


    insert into dbo.InternalExtractEUAirFactKey (InternalExtractID, RecordKey, BookingTypeID)
    select InternalExtractID, RecordKey, BookingTypeID
      from dbo.InternalExtractEUAirFact
     where InternalExtractID = @pInternalExtractID

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (insert InternalExtractEUAirFactKey)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    insert into dbo.AirFactKey (FactKeyTypeID, FactKeyID)
    select @FactKeyTypeID, InternalExtractEUAirFactID
      from dbo.InternalExtractEUAirFactKey
     where InternalExtractID = @pInternalExtractID 

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

-- exec [InternalExtractEUAirFactKeyLoad] @pInternalExtractID = 30566

--select * from dbo.InternalExtractEUAirFact f where f.InternalExtractID = 30566 and f.RecordKey = 'AUSI140027455-10000' and f.BookingTypeID = 5

--select l.Resource_Type, ticket, l.Principal_Ticket, l.ExchangeTicket, l.Amount_Including_VAT, * 
--from dbo.Nav_Sales_Invoice_Line l 
--where l.Document_No_ = 'AUSI140027455'
--  and l.Resource_Type = 0

--select * from dbo.InternalExtractEUAirFact f where f.InternalExtractID = 30566 and f.RecordKey like 'AUSI140027455%'
