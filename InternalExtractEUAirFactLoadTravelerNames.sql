if object_id('dbo.InternalExtractEUAirFactLoadTravelerNames') is null begin
    print 'Creating stored procedure InternalExtractEUAirFactLoadTravelerNames (placeholder)'
    execute('create procedure dbo.InternalExtractEUAirFactLoadTravelerNames as return 0')
end
go
print 'Altering stored procedure InternalExtractEUAirFactLoadTravelerNames'
go

-- Below must be there for indexed
-- views
set quoted_identifier on
go

alter procedure dbo.InternalExtractEUAirFactLoadTravelerNames
    @pInternalExtractID int

as

/*
*********************************************************************
Copyright (C) 2014-2016 Expedia, Inc. All rights reserved.

Description:
     Updates InternalExtractEUAirFact with TravelerNameIDs
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
    2014-07-15  JaredKo         Created.
    2015-10-26  DMurugesan      EGE-97318 Guest Traveler Name Check
    2015-10-26  JaredKo         Performance Enhancements
    2016-04-26  JaredKo         EGE-113866 Integrated Patrick's changes (3/15; 4/11) with prior code
                                    Lookup switched to a UDF for code reuse / debugging
                                    Found/Fixed bug looking up traveler name in Nav_IMPORT_METAID_FIELD_VALUE
    2016-11-04  JaredKo         EGE-129571 Added Support for Anonymous Traveler/Approver
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
    InternalExtractID int not null,
    TravelProductID int not null,
    RecordKey varchar(30) not null,
    BookingTypeID tinyint not null,
    TUID int not null,
    MetaDossierID varchar(20),
    TitleTxt varchar(20),
    FirstName varchar(60),
    MiddleName varchar(60),
    LastName varchar(60),
    Traveller_Code varchar(20))

create index temp_ix1 on #NamesTraveler (InternalExtractID, RecordKey, BookingTypeID)
create index temp_ix2 on #NamesTraveler (TUID, TitleTxt, FirstName, LastName)

create table #NamesApprover (
    InternalExtractID int not null,
    TravelProductID int not null,
    GroupAccountID int not null,
    RecordKey varchar(30) not null,
    BookingTypeID tinyint not null,
    TUID int not null,
    TravelerNameID int,
    TitleTxt varchar(20),
    FirstName varchar(60),
    MiddleName varchar(60),
    LastName varchar(60),
    Traveller_Code varchar(20))

create index temp_ix3 on #NamesApprover (InternalExtractID, RecordKey, BookingTypeID)
create index temp_ix4 on #NamesApprover (TUID, TitleTxt, FirstName, LastName)

create table #CustomerIDTUIDMatch (
    TravelerNameID int,
    TUID int not null,    
    TitleTxt varchar(20),
    FirstName varchar(60),
    MiddleName varchar(60),
    LastName varchar(60))

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

-- Name Selection based on original Kettle Code:
-- 1st Choice: Nav_Traveller.First_Name where IsOccasional = 0
-- 2nd Choice: Nav_Sales_Invoice_Line.[Traveller_Name_(Alias)]
-- 3rd Choice: Nav_IMPORT_METAID_FIELD_VALUE.


insert into #NamesTraveler (InternalExtractID, RecordKey, TravelProductID, BookingTypeID, TUID, MetaDossierID, TitleTxt, FirstName, MiddleName, LastName, Traveller_Code)
select @pInternalExtractID, 
       a.RecordKey,
       a.TravelProductID,
       a.BookingTypeID,
       a.TUIDTraveler,
       c.MetaDossierID,
       c.Title,
       c.FirstName,
       '' as MiddleName,
       c.LastName,
       c.Traveller_Code
from dbo.InternalExtractEUAirFact a
cross apply dbo.Nav_InternalExtractTravelerNames(a.InternalExtractID, a.TravelProductID, a.SalesDocumentCode, a.SalesDocumentLineNbr) c
where a.InternalExtractID = @pInternalExtractID

-- Code Review / ToDo: If we can't get a perfect match on any of the three tables, we should fall back to the
-- tables with the least blank/nulls. [NotAName] CTE should still be respected.

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #NamesTraveler)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


insert into #NamesApprover (InternalExtractID,
                            TravelProductID,
                            GroupAccountID,
                            RecordKey,
                            BookingTypeID,
                            TUID,
                            TravelerNameID,
                            TitleTxt,
                            FirstName,
                            MiddleName,
                            LastName)
    select a.InternalExtractID,
           a.TravelProductID,
           a.GroupAccountID,
           a.RecordKey,
           a.BookingTypeID,
           a.TUIDApprover,
           a.TravelerNameIDApproval,
           coalesce(ltrim(rtrim(c.Title)),''),
           coalesce(ltrim(rtrim(c.first_Name)),''),
           '' as MiddleName,
           coalesce(ltrim(rtrim(c.Name)),'')
           from dbo.InternalExtractEUAirFact a
               outer apply (select top 1 t.*               
                            from dbo.Nav_Traveller t
                                 where t.TravelProductID = a.TravelProductID and
                                       t.no_ = cast(a.TUIDApprover as varchar(20)) and
                                       t.InternalExtractID <= @pInternalExtractID
                                 order by t.InternalExtractID desc) c
               where a.InternalExtractID = @pInternalExtractID and
        a.TUIDApprover is not null
    union all
    select a.InternalExtractID,
           a.TravelProductID,
           a.GroupAccountID,
           a.RecordKey,
           a.BookingTypeID,
           a.TUIDApprover,
           a.TravelerNameIDApproval,
           coalesce(ltrim(rtrim(c.Title)),''),
           coalesce(ltrim(rtrim(c.first_Name)),''),
           '' as MiddleName,
           coalesce(ltrim(rtrim(c.Name)),'')
        from dbo.InternalExtractEUAirFact a
            outer apply (select top 1 t.*               
                         from dbo.Nav_Traveller t
                              where t.TravelProductID = a.TravelProductID and
                                    t.no_ = cast(a.TUIDApprover as varchar(20)) and
                                    t.InternalExtractID <= @pInternalExtractID
                              order by t.InternalExtractID desc) c
    where a.InternalExtractID = @pInternalExtractID and
        a.TUIDApprover is not null

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #NamesApprover)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

update na
   set na.FirstName = ta.FirstName,
       na.MiddleName = ta.MiddleName,
       na.LastName = ta.LastName,
       na.TitleTxt = ta.TitleTxt
    --output inserted.*, '<-- Inserted / Deleted -->', deleted.*
  from #NamesApprover na
  join dbo.TravelerAnonymous ta 
    on ta.TravelProductID = na.TravelProductID
   and ta.GroupAccountID = na.GroupAccountID

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #NamesApprover with anonymous names)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


select @TrancountSave = @@Trancount
if (@TrancountSave = 0) begin transaction @SavePointName
else                    save  transaction @SavePointName
select @TranStartedBool = @TRUE


    ;with NewNames as (
              select distinct a.TUID, 2 as CustomerSystemID, a.TitleTxt, a.FirstName, a.MiddleName, a.LastName, @Current_Timestamp as UpdateDate, 'IEUAirFactLoadNames' as LastUpdatedBy
               from #NamesTraveler a 
                    left join
                    dbo.TravelerNameDim b on 
                        b.CustomerSystemID = 2 and
                        a.TUID = b.TUID and   
                        a.TitleTxt = b.TitleTxt and  
                        a.FirstName = b.FirstName and
                        a.LastName = b.LastName
              where b.CustomerSystemID is null
             union 
             select distinct a.TUID, 2 as CustomerSystemID, a.TitleTxt, a.FirstName, a.MiddleName, a.LastName, @Current_Timestamp as UpdateDate, 'IEUAirFactLoadNames' as LastUpdatedBy
               from #NamesApprover a 
                    left join
                    dbo.TravelerNameDim b on 
                        b.CustomerSystemID = 2 and
                        a.TUID = b.TUID and   
                        a.TitleTxt = b.TitleTxt and  
                        a.FirstName = b.FirstName and
                        a.LastName = b.LastName
              where b.CustomerSystemID is null
              )
    insert into dbo.TravelerNameDim (TUID, CustomerSystemID, a.TitleTxt, FirstName, MiddleName, LastName, UpdateDate, LastUpdatedBy)
    select TUID, CustomerSystemID, TitleTxt, FirstName, MiddleName, LastName, UpdateDate, LastUpdatedBy
    from NewNames n
    order by TUID

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
                @MsgParm1    = cast(@Error as varchar(12)) + ' (insert TravelerNameDim)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    -- Update fact table with TravelerNameID
    update a
       set TravelerNameID = c.TravelerNameID 
      from dbo.InternalExtractEUAirFact a
           inner join    
           #NamesTraveler b on
               a.InternalExtractID = b.InternalExtractID and
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
     where a.InternalExtractID = @pInternalExtractID 

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update InternalExtractEUAirFact TravelerNameID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- Update fact table with TravelerNameID
    update dbo.InternalExtractEUAirFact
       set TravelerNameIDApproval = c.TravelerNameID 
      from dbo.InternalExtractEUAirFact a
           inner join    
           #NamesApprover b on
               a.InternalExtractID = b.InternalExtractID and
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
     where a.InternalExtractID = @pInternalExtractID 

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update InternalExtractEUAirFact TravelerNameIDApproval)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- Update fact table with TravelerNameID
    update dbo.InternalExtractEUAirFact
       set FactRecordStatusID = 9 -- Missing TRAVELLERS
      from dbo.InternalExtractEUAirFact a
     where a.InternalExtractID = @pInternalExtractID and
          (a.TravelerNameID is null or
                (a.TravelerNameIDApproval is null and 
                 a.TUIDApprover is not null)
            )

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update InternalExtractEUAirFact Missing TravelerNameIDApproval)'
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
-- Check TravelerNameID counts before and after execution
begin tran
    select count(*) as TotalRowCount,
           sum(case when TravelerNameID is null then 1 else 0 end) as TravelerNameIDNullCount,
           sum(case when TravelerNameIDApproval is null and a.TUIDApprover is not null then 1 else 0 end) as TravelerNameIDApprovalNullCount
    from InternalExtractEUAirFact a
    where a.InternalExtractID = 37676 

    update InternalExtractEUAirFact set TravelerNameID = null where InternalExtractID = 37676
    exec InternalExtractEUAirFactLoadTravelerNames 37676

    select count(*) as TotalRowCount,
           sum(case when TravelerNameID is null then 1 else 0 end) as TravelerNameIDNullCount,
           sum(case when TravelerNameIDApproval is null and a.TUIDApprover is not null then 1 else 0 end) as TravelerNameIDApprovalNullCount
    from InternalExtractEUAirFact a
    where a.InternalExtractID = 37676 

rollback tran

*/
