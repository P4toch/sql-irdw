if object_id('dbo.ExternalFileEUAirFactCDFValueLoad') is null begin
    print 'Creating stored procedure ExternalFileEUAirFactCDFValueLoad (placeholder)'
    execute('create procedure dbo.ExternalFileEUAirFactCDFValueLoad as return 0')
end
go

print 'Altering stored procedure ExternalFileEUAirFactCDFValueLoad'
go

alter procedure dbo.ExternalFileEUAirFactCDFValueLoad 
    @pExternalFileID int
with recompile
as

/*
*********************************************************************
Copyright (C) 2011 Expedia, Inc. All rights reserved.

Description:
    Inserts/Updates ExternalFileEUAirFactCDFValue with new and modified
        data

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2011-10-28  BSimpson        Created.
    2011-11-20  BSimpson        RAID 1005056 - Fact source processing fails if any EU transaction has multiple CDFs with same CDF ID
*********************************************************************
*/

set nocount on
set ansi_warnings off
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

insert into dbo.ExternalFileEUAirFactCDFValue( ExternalFileID, RecordKey, BookingTypeID, CustomDataFieldID, CustomDataFieldValue ) 
    select z.ExternalFileID, z.RecordKey, z.BookingTypeID, z.CustomDataFieldID, z.CustomDataFieldValue
        from (
            select 
                b.ExternalFileID,
                b.RecordKey,
                b.BookingTypeID,
                b.CustomDataFieldID,
                b.CustomDataFieldValue,
                ordinal = row_number()
                    over (
                        partition by b.ExternalFileID, b.RecordKey, b.BookingTypeID, b.CustomDataFieldID
                        order by b.CustomDataFieldName, b.CustomDataFieldValue
                        )
            from dbo.ExternalFileEUAirFact a 
            join dbo.vExternalFileEUAirTicketCDFUnpivot b
                on a.ExternalFileID = b.ExternalFileID
                and a.RecordKey = b.RecordKey
                and a.BookingTypeID = b.BookingTypeID
            where a.ExternalFileID = @pExternalFileID
                and b.CustomDataFieldValue is not null
            ) z where z.ordinal = 1
    union all
    select 
        a.ExternalFileID,
        a.RecordKey,
        a.BookingTypeID,
        0,
        b.GroupAccountDepartmentName        
    from dbo.ExternalFileEUAirFact a 
    join dbo.GroupAccountDepartmentDim b
        on a.GroupAccountDepartmentID = b.GroupAccountDepartmentID
    where a.ExternalFileID = @pExternalFileID
        and b.CustomerSystemID = 2

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert ExternalFileEUAirFactCDFValue)'
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

