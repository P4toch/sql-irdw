if object_id('dbo.ExternalFileEUFeeFactLoadTravelerNames') is null begin
    print 'Creating stored procedure ExternalFileEUFeeFactLoadTravelerNames (placeholder)'
    execute('create procedure dbo.ExternalFileEUFeeFactLoadTravelerNames as return 0')
end
go

print 'Altering stored procedure ExternalFileEUFeeFactLoadTravelerNames'
go

-- Below must be there for indexed
-- views
set quoted_identifier on
go

alter procedure dbo.ExternalFileEUFeeFactLoadTravelerNames
    @pExternalFileID int

as

/*
*********************************************************************
Copyright (C) 2008-2018 Expedia, Inc. All rights reserved.

Description:
     Updates ExternalFileEUFeeFact with TravelerNameIDs
     Adds rows to TravelerNameDim.

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2008-03-15  BarryC          Created.
    2009-08-10  VBoerner        RAID121445 fixed join on BookingTypeID
    2016-09-21  JaredKo         EGE-126814: Apply InternalExtract
                                    logic to ExternalFile process
	2018-03-22	manzeno			EGE-189849: Apply fix to make sure we pick the last values after "-" in Recordkey field
								Changed substring(a.RecordKey, charindex('-', a.RecordKey) + 1, 10)
                                to  reverse(left(reverse(a.RecordKey), charindex('-', reverse(a.RecordKey)) - 1))
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


create table #NamesTraveler (
    ExternalFileID int not null,
    InternalExtractID int,
    TravelProductID int,
    RecordKey varchar(30) not null,
    BookingTypeID tinyint not null,
    TUID int not null,
    TitleTxt varchar(20),
    FirstName varchar(60),
    MiddleName varchar(60) default '',
    LastName varchar(60),
    SalesDocumentNo varchar(20), 
    SaleDocumentLineNo int)

create index temp_ix1 on #NamesTraveler (ExternalFileID, RecordKey, BookingTypeID)
create index temp_ix2 on #NamesTraveler (TUID, TitleTxt, FirstName, MiddleName, LastName)


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
-- Build unique tuid/name combinations
;with NewNames as (
            select distinct a.ExternalFileID, l.InternalExtractID, a.TravelProductID, a.RecordKey, a.BookingTypeID, a.TUIDTraveler, a.TravelerNameID, l.Document_No_, l.Line_No_
            from dbo.ExternalFileEUFeeFact a 
            outer apply (select top 1
                                l2.InternalExtractID, l2.TravelProductID, l2.Document_No_, l2.Line_No_ 
                            from dbo.Nav_Sales_Invoice_Line l2
                            where l2.Document_No_ = a.SalesDocumentCode
                            and l2.Line_No_ = reverse(left(reverse(a.RecordKey), charindex('-', reverse(a.RecordKey)) - 1))
                            union all 
                            select top 1
                                l2.InternalExtractID, l2.TravelProductID, l2.Document_No_, l2.Line_No_ 
                            from dbo.Nav_Sales_Cr_Memo_Line l2
                            where l2.Document_No_ = a.SalesDocumentCode
							and l2.Line_No_ = reverse(left(reverse(a.RecordKey), charindex('-', reverse(a.RecordKey)) - 1))
                            ) l
            where a.ExternalFileID = @pExternalFileID
              and a.TUIDTraveler is not null
              and a.TUIDTraveler <> 0
            )
insert #NamesTraveler (ExternalFileID, InternalExtractID, TravelProductID, RecordKey, BookingTypeID, TUID, SalesDocumentNo, SaleDocumentLineNo)
select distinct
         n.ExternalFileID, n.InternalExtractID, n.TravelProductID, n.RecordKey, n.BookingTypeID, n.TUIDTraveler, n.Document_No_, n.Line_No_
from NewNames n
option(recompile)

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #NamesTraveler)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

update a
set a.FirstName = ltrim(rtrim(b.FirstName)),
    a.LastName = ltrim(rtrim(b.LastName)),
    a.TitleTxt = ltrim(rtrim(coalesce(b.Title, '')))
from #NamesTraveler a
cross apply dbo.Nav_InternalExtractTravelerNames(a.InternalExtractID, a.TravelProductID, a.SalesDocumentNo, a.SaleDocumentLineNo) b

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #NamesTraveler with names)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

select @TrancountSave = @@Trancount
if (@TrancountSave = 0) begin transaction @SavePointName
else                    save  transaction @SavePointName
select @TranStartedBool = @TRUE


    -- Insert new names into dimension table
    insert into dbo.TravelerNameDim (TUID, CustomerSystemID, a.TitleTxt, FirstName, MiddleName, LastName, UpdateDate, LastUpdatedBy)
    select TUID, CustomerSystemID, a.TitleTxt, FirstName, MiddleName, LastName, UpdateDate, LastUpdatedBy
      from
         (select distinct a.TUID, 2 as CustomerSystemID, a.TitleTxt, a.FirstName, a.MiddleName, a.LastName, @Current_Timestamp as UpdateDate, 'EFEUFeeFactLoadNames' as LastUpdatedBy
           from #NamesTraveler a 
                left join
                dbo.TravelerNameDim b on 
                    b.CustomerSystemID = 2 and
                    a.TUID = b.TUID and   
                    a.TitleTxt = b.TitleTxt and  
                    a.FirstName = b.FirstName and
                    a.MiddleName = b.MiddleName and
                    a.LastName = b.LastName
          where b.CustomerSystemID is null) as A

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (insert TravelerNameDim)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    -- Update fact table with TravelerNameID
    update dbo.ExternalFileEUFeeFact
       set TravelerNameID = c.TravelerNameID 
      from dbo.ExternalFileEUFeeFact a
           inner join    
           #NamesTraveler b on
               a.ExternalFileID = b.ExternalFileID and
               a.RecordKey = b.RecordKey and
               a.BookingTypeID = b.BookingTypeID 
           inner join 
           dbo.TravelerNameDim c  on
                c.CustomerSystemID = 2 and
                b.TUID = c.TUID and
                b.TitleTxt = c.TitleTxt and    
                b.FirstName = c.FirstName and
                b.MiddleName = c.MiddleName and
                b.LastName = c.LastName              
     where a.ExternalFileID = @pExternalFileID 

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update ExternalFileEUFeeFact TravelerNameID)'
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


-- Test new name lookup code
declare @pExternalFileID int
select @pExternalFileID = max(ExternalFileID)
from ExternalFileEUFeeFact

--select RecordKey, BookingTypeID, a.TravelerNameID, TitleTxt, FirstName, MiddleName, LastName
--from ExternalFileEUFeeFact a
--join TravelerNameDim b on a.TravelerNameID = b.TravelerNameID
--where ExternalFileID = @pExternalFileID

begin tran
    exec ExternalFileEUFeeFactLoadTravelerNames @pExternalFileID

    select RecordKey, BookingTypeID, a.TravelerNameID, TitleTxt, FirstName, MiddleName, LastName
    from ExternalFileEUFeeFact a
    join TravelerNameDim b on a.TravelerNameID = b.TravelerNameID
    where ExternalFileID = @pExternalFileID

rollback tran

*/