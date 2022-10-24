if object_id('dbo.ExternalFileEUFeeTypeVldKey_') is null begin
    execute('create procedure dbo.ExternalFileEUFeeTypeVldKey_ as return 0')
end
go

print 'Altering stored procedure ExternalFileEUFeeTypeVldKey_'
go

alter procedure dbo.ExternalFileEUFeeTypeVldKey_ (
    @pExternalFileID int,
    @pDuplicateCount int OUTPUT
)
as

/*
*********************************************************************
Copyright (C) 2008 Expedia, Inc. All rights reserved.

Description:
    Checks deletes potential PK violations in the EU FeeType 
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
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2008-03-01  VBoerner        Created.
    2008-03-16  BarryC          Change primary key.
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
    @ExternalFileID                 int, 
    @EUFeeTypeCode                  varchar(30),
    @LangID                         smallint,
    @DuplicateCount                 int

declare @FeeType table
    (ExternalFileID int,
     EUFeeTypeCode varchar(30),  
     LangID smallint,
     DuplicateCount int)

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

---------------------------------------------------------------------
-- Validations
---------------------------------------------------------------------
-- None, done by caller

---------------------------------------------------------------------
-- Declare Cursor
---------------------------------------------------------------------
insert into @FeeType (ExternalFileID, EUFeeTypeCode, LangID, DuplicateCount)
select ExternalFileID, EUFeeTypeCode, LangID, count(*)
  from dbo.ExternalFileEUFeeTypeStaging
 where ExternalFileID = @pExternalFileID
 group by ExternalFileID, EUFeeTypeCode, LangID
having count(*) > 1

declare Key_Cursor cursor local dynamic for
select a.ExternalFileID, a.EUFeeTypeCode, a.LangID
  from dbo.ExternalFileEUFeeTypeStaging a
       inner join
       @FeeType b on a.ExternalFileID = b.ExternalFileID and 
                     a.EUFeeTypeCode = b.EUFeeTypeCode and
                     a.LangID = b.LangID 
for update

open Key_Cursor
select @Error = @@error
if( @Error <> 0 ) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = isnull(convert(varchar,@Error),'NULL')
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

---------------------------------------------------------------------
-- Processing
---------------------------------------------------------------------

select @TrancountSave = @@Trancount
if (@TrancountSave = 0) begin transaction @SavePointName
else                    save  transaction @SavePointName
select @TranStartedBool = @TRUE

-- Cursor Loop
while( 1=1 ) begin

    select @LoopCount = @LoopCount + 1

    fetch next from Key_Cursor into @ExternalFileID, @EUFeeTypeCode, @LangID
    select @Error = @@error
    if( @Error <> 0 ) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = isnull(convert(varchar,@Error),'NULL')
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    if( @@FETCH_STATUS = -1 ) begin
        break
    end
    if( @@FETCH_STATUS = -2 ) begin
        continue
    end
    
    if exists (select *
                 from @FeeType
                where ExternalFileID = @ExternalFileID and
                      EUFeeTypeCode = @EUFeeTypeCode and
                      LangID = @LangID and
                      DuplicateCount > 1)
        begin
            delete from dbo.ExternalFileEUFeeTypeStaging
            where current of Key_Cursor
            
            select @Error = @@error
            if( @Error <> 0 ) begin
                select @ErrorCode   = @ERRUNEXPECTED,
                       @MsgParm1    = isnull(convert(varchar,@Error),'NULL')
                raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
                goto ErrorHandler
            end

            update @FeeType
               set DuplicateCount = DuplicateCount - 1
             where ExternalFileID = @ExternalFileID and
                   EUFeeTypeCode = @EUFeeTypeCode and
                   LangID = @LangID

            set @UpdateCount = @UpdateCount + 1
        end

end

if ((@TrancountSave = 0) and (@TranStartedBool = @TRUE))
    commit transaction @SavePointName

set @pDuplicateCount = @UpdateCount

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
    /*
    -- print final status of cursor loop
    print convert(varchar(23),getdate(),121)
        + '. LoopCount: ' + convert(varchar(10), @LoopCount)
        + ' UpdateCount: ' + convert(varchar(10), @UpdateCount)
    */


    if cursor_status( 'local', 'Key_Cursor' )
        in ( @CSRSTSOPEN0, @CSRSTSOPEN1, @CSRSTSCLOSED ) begin
        deallocate Key_Cursor
    end

    return (@ExitCode)
go




