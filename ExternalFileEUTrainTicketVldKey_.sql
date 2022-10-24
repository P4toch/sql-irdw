if object_id('dbo.ExternalFileEUTrainTicketVldKey_') is null begin
    execute('create procedure dbo.ExternalFileEUTrainTicketVldKey_ as return 0')
end
go

print 'Altering stored procedure ExternalFileEUTrainTicketVldKey_'
go

alter procedure dbo.ExternalFileEUTrainTicketVldKey_ (
    @pExternalFileID int,
    @pDuplicateCount int OUTPUT
)
as

/*
*********************************************************************
Copyright (C) 2006-2018 Expedia, Inc. All rights reserved.

Description:
    Checks deletes potential PK violations in the EU train ticket  
    staging table. Returns duplicate count/deleted rows.
    Based on expectations from external source this
    should never happen.

Result Set:  None

Return values:
    0     Success
    -100  Failure

Error codes:
    200104    SP: %s. Unexpected error. See previous error messages. Error number: %s.

Change History:
    Date        Author               Description
    ----------  -------------------  ------------------------------------
    2006-06-04   Barry Courtois      Created.
    2018-03-07   nrasmussen          EGE-169392 Bugfix for duplicate recordkey for seat and incorrect amount for ticket
    2018-03-07   nrasmussen          and modified duplicate record check away from using cursor 
    2018-03-22   nrasmussen          added seperate commit transaction for update section and subsequent begin transaction 
    2018-03-26   nrasmussen          added @ExternalFileID = @pExternalFileID
************************************************************************
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
    @Error                          int,
    @ErrorCode                      int,
    @ExitCode                       int,
    @ProcedureName                  sysname,
    @RC                             int,            -- Return code from called SP
    @Rowcount                       int,
    @SavePointName                  varchar(32),
    @TranStartedBool                tinyint,
    @TrancountSave                  int

declare     -- Error message constants and variables
    @ERRUNEXPECTED                  int,
    @MsgParm1                       varchar(100),
    @MsgParm2                       varchar(100),
    @MsgParm3                       varchar(100)

declare     -- Cursor constants and variables
    @CSRSTSCLOSED                   smallint,
    @CSRSTSNOTAPPL                  smallint,
    @CSRSTSNOTEXISTS                smallint,
    @CSRSTSOPEN0                    smallint,       -- Result set set with 0 rows
    @CSRSTSOPEN1                    smallint        -- Result set with 1 or more rows

declare     -- SP specific constants and variables
    @FieldName                      varchar(60),
    @LoopCount                      int,
    @UpdateCount                    int,
    @ExternalFileID                 int , 
    @RecordKey                      varchar(30)

---------------------------------------------------------------------
-- Initializations
---------------------------------------------------------------------
select   -- Standard constants
    @FALSE                          = 0,
    @TRUE                           = 1,
    @RC_FAILURE                     = -100,
    @RC_SUCCESS                     = 0

select   -- Standard variables
    @ExitCode                       = @RC_SUCCESS,
    @ProcedureName                  = object_name(@@ProcID),
    @SavePointName                  = '$' + cast(@@NestLevel as varchar(15))
                                    + '_' + cast(@@ProcID as varchar(15)),
    @TranStartedBool                = @FALSE

select   -- Error message constants
    @ERRUNEXPECTED                  = 200104

select     -- Cursor constants and variables
    @CSRSTSCLOSED                   = -1,
    @CSRSTSNOTAPPL                  = -2,
    @CSRSTSNOTEXISTS                = -3,
    @CSRSTSOPEN0                    = 0,
    @CSRSTSOPEN1                    = 1

select   -- SP specific constants and variables
    @LoopCount                      = 0,
    @UpdateCount                    = 0

select --@ExternalFileID = @pExternalFileID
    @ExternalFileID = @pExternalFileID
---------------------------------------------------------------------
-- Prepare - get seat reservations only
--------------------------------------------------------------------
if object_id('tempdb..#Seat') is not null begin
    drop table #Seat
end
   
select *
    into #Seat
from dbo.ExternalFileEUTrainTicketStaging 
where 
    BusinessCategoryID = 9 and --seat reservations only
    ExternalFileID = @ExternalFileID 

---------------------------------------------------------------------
-- Processing - Add seat amounts to ticket amounts
--------------------------------------------------------------------
select @TrancountSave = @@Trancount
if (@TrancountSave = 0) 
    begin transaction @SavePointName
else
    save transaction @SavePointName
select @TranStartedBool = @TRUE

update a
    set a.TicketAmt = coalesce(a.TicketAmt, 0) + coalesce(b.TicketAmt, 0),
        a.TicketAmtBase = coalesce(a.TicketAmtBase, 0) + coalesce(b.TicketAmtBase, 0),
        a.TicketAmtVat = coalesce(a.TicketAmtVat, 0) + coalesce(b.TicketAmtVat, 0),
        a.TicketAmtTax = coalesce(a.TicketAmtTax, 0) + coalesce(b.TicketAmtTax, 0),
        a.TicketAmtMarkup = coalesce(a.TicketAmtMarkup, 0) + coalesce(b.TicketAmtMarkup, 0),
        a.TicketAmtDifferential = coalesce(a.TicketAmtDifferential, 0) + coalesce(b.TicketAmtDifferential, 0),
        a.TicketAmtBaseDifferential = coalesce(a.TicketAmtBaseDifferential, 0) + coalesce(b.TicketAmtBaseDifferential, 0),
        a.TicketAmtVatDifferential = coalesce(a.TicketAmtVatDifferential, 0) + coalesce(b.TicketAmtVatDifferential, 0),    
        a.TicketAmtTaxDifferential = coalesce(a.TicketAmtTaxDifferential, 0) + coalesce(b.TicketAmtTaxDifferential, 0),
        a.TicketAmtMarkupDifferential = coalesce(a.TicketAmtMarkupDifferential, 0) + coalesce(b.TicketAmtMarkupDifferential, 0),
        a.TicketAmtPenalty = coalesce(a.TicketAmtPenalty, 0) + coalesce(b.TicketAmtPenalty, 0),    
        a.TicketAmtGross = coalesce(a.TicketAmtGross, 0) + coalesce(b.TicketAmtGross, 0),
        a.TicketAmtCommission = coalesce(a.TicketAmtCommission, 0) + coalesce(b.TicketAmtCommission, 0)
from dbo.ExternalFileEUTrainTicketStaging a
inner join #Seat b
    on (a.ExternalFileID = b.ExternalFileID and
        a.RecordKey = b.RecordKey and
        a.BusinessCategoryID = 1 --apply to ticket only
        )

select @Error = @@Error

if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update dbo.ExternalFileEUTrainTicketStaging amount columns for ticket)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

--------------------------------------------------------------------
-- Validations - Identifying duplicate records
---------------------------------------------------------------------
;with DuplicateCheck as (
    select 
        ExternalFileID, RecordKey, BusinessCategoryID, row_number() 
        over (partition by ExternalFileID, RecordKey 
              order by ExternalFileID, RecordKey, BusinessCategoryID --ticket is businesscategoryid 1
              ) as DuplicateCnt 
    from dbo.ExternalFileEUTrainTicketStaging
    where 
        ExternalFileID = @ExternalFileID
    )
    
---------------------------------------------------------------------
-- Validations - Removing duplicate records
--------------------------------------------------------------------
delete from a
    from dbo.ExternalFileEUTrainTicketStaging a
    where 
        exists (select
                    * 
                from DuplicateCheck b
                where 
                    b.ExternalFileID = a.ExternalFileID and
                    b.RecordKey = a.RecordKey and
                    b.BusinessCategoryID = a.BusinessCategoryID and
                    b.DuplicateCnt > 1) 

select @Error = @@Error, 
       @pDuplicateCount = @@ROWCOUNT  --deleted number of duplicate records

if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (removing duplicates from dbo.ExternalFileEUTrainTicketStaging)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

if ((@TrancountSave = 0) and
    (@TranStartedBool = @TRUE))
    commit transaction @SavePointName

goto ExitProc
---------------------------------------------------------------------
-- Error Handler
---------------------------------------------------------------------
ErrorHandler:
if (@TranStartedBool = @TRUE)
    rollback transaction @SavePointName
select 
    @ExitCode = @RC_FAILURE
goto ExitProc

---------------------------------------------------------------------
-- Exit Procedure
---------------------------------------------------------------------
ExitProc:
return (@ExitCode)
go





