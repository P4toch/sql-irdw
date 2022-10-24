if object_id('dbo.ExternalFileEUTrainFactLoadSegmentDerived') is null begin
    print 'Creating stored procedure ExternalFileEUTrainFactLoadSegmentDerived (placeholder)'
    execute('create procedure dbo.ExternalFileEUTrainFactLoadSegmentDerived as return 0')
end
go

print 'Altering stored procedure ExternalFileEUTrainFactLoadSegmentDerived'
go

alter procedure dbo.ExternalFileEUTrainFactLoadSegmentDerived
    @pExternalFileID int
                  
as

/*
*********************************************************************
Copyright (C) 2013 Expedia, Inc. All rights reserved.

Description:
     Updates ExternalFileEUTrainFact with the derived attributes

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2013-10-16  DMurugesan      Created.
    
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
    @MsgParm3                       varchar(100),
    @LineOfBusinessID               tinyint

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
    @TranStartedBool                = @FALSE,
    @LineOfBusinessID               = 5

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

Update A 
set  GeographyTypeID = C.GeographyTypeID 
from ExternalFileEUTrainFact A 
Inner join 
(
    select 
        a.ExternalFileID,
        a.RecordKey,
        c.TravelProductID,
        min(c.priorityordernbr) as priorityordernbr
    from dbo.ExternalFileEUTrainTripFact a 
    inner join ExternalFileEUTrainFact b on
        a.ExternalFileID = b.ExternalFileID and 
        a.RecordKey = b.RecordKey and
        a.BookingTypeID = b.BookingTypeID
    inner join dbo.GeographyTypeTravelProduct c on
        c.TravelProductID = b.TravelProductID and 
        c.GeographyTypeID = a.GeographyTypeID and
        c.LineOfBusinessID = @LineOfBusinessID  
     where a.ExternalFileId = @pExternalFileID
     group by a.ExternalFileID,a.RecordKey,c.TravelProductID
) as B on
    a.ExternalFileID = b.ExternalFileID and  
    a.RecordKey = b.RecordKey and
    a.TravelProductID = b.TravelProductID
inner join dbo.GeographyTypeTravelProduct C  on
    c.TravelProductID = b.TravelProductID and  
    c.PriorityOrderNbr = b.PriorityOrderNbr and
    c.LineOfBusinessID = @LineOfBusinessID  
where a.ExternalFileId = @pExternalFileID 

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update ExternalFileEUTrainFact look up GeographyTypeID)'
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
