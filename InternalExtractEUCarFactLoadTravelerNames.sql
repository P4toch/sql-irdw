if object_id('dbo.InternalExtractEUCarFactLoadTravelerNames') is null begin
    print 'Creating stored procedure InternalExtractEUCarFactLoadTravelerNames (placeholder)'
    execute('create procedure dbo.InternalExtractEUCarFactLoadTravelerNames as return 0')
end
go

print 'Altering stored procedure InternalExtractEUCarFactLoadTravelerNames'
go

-- Below must be there for indexed
-- views
set quoted_identifier on
go

alter procedure dbo.InternalExtractEUCarFactLoadTravelerNames
    @pInternalExtractID int

as

/*
*********************************************************************
Copyright (C) 2013-2016 Expedia, Inc. All rights reserved.

Description:
     Updates InternalExtractEUCarFact with TravelerNameIDs
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
    2013-05-23  VBoerner        Created.
    2016-12-06  JaredKo         EGE-129571 - Anonymize names reflected
                                    in TravelerAnonymous
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
    @BookingTypeIDReserve                 tinyint,
    @BookingTypeIDCancel                  tinyint,
    @BookingSystemID_ECTWeb               tinyint,
    @CarCartID                            int,
    @TravelProductID                      int

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
    @BookingTypeIDReserve           = 3,
    @BookingTypeIDCancel            = 2,
    @BookingSystemID_ECTWeb         = 22

create table #NamesTraveler (
    InternalExtractID int not null,
    CarCartID int not null,
    CarCartLogID int not null,
    BookingTypeID tinyint not null,
    TUID int not null,
    TitleTxt varchar(20) not null,
    FirstName varchar(60) not null,
    MiddleName varchar(60) not null,
    LastName varchar(60) not null)

create index temp_ix1 on #NamesTraveler (InternalExtractID, CarCartID, CarCartLogID, BookingTypeID)
create index temp_ix2 on #NamesTraveler (TUID, TitleTxt, FirstName, MiddleName, LastName)

create table #NamesApprover (
    InternalExtractID int not null,
    CarCartID int not null,
    CarCartLogID int not null,
    BookingTypeID tinyint not null,
    TUID int not null,
    TitleTxt varchar(20) not null,
    FirstName varchar(60) not null,
    MiddleName varchar(60) not null,
    LastName varchar(60) not null)

create index temp_ix3 on #NamesApprover (InternalExtractID, CarCartID, CarCartLogID, BookingTypeID)
create index temp_ix4 on #NamesApprover (TUID, TitleTxt, FirstName, MiddleName, LastName)

---------------------------------------------------------------------
-- Validation
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Processing
---------------------------------------------------------------------

-- Build unique tuid/name combinations
insert into #NamesTraveler (InternalExtractID, CarCartID, CarCartLogID, BookingTypeID, TUID, TitleTxt, FirstName, MiddleName, LastName)
select distinct cf.InternalExtractID, 
                cf.CarCartID,
                cf.CarCartLogID,
                cf.BookingTypeID,
                cf.TUIDTraveler,
                coalesce(ta.TitleTxt, ltrim(rtrim(t.TITLE)),''), 
                coalesce(ta.FirstName, ltrim(rtrim(t.FIRST_NAME)),''),
                '' as MiddleName,
                coalesce(ta.LastName, ltrim(rtrim(t.FULL_NAME)),'')
  from dbo.InternalExtractEUCarFact cf
       left outer join
       dbo.TravelerAnonymous ta on ta.GroupAccountID = cf.GroupAccountID
       inner join 
       dbo.TRAVELLERS t on cf.MetaDossierID = t.MD_CODE 
           and cf.InternalExtractIDReserve = t.InternalExtractID
           and cf.TUIDAccount = t.PER_CODE and t.IS_MAIN = 1
 where cf.InternalExtractID = @pInternalExtractID

select @Error = @@Error  if (@Error <> 0) begin
    select @ErrorCode = @ERRUNEXPECTED,
           @MsgParm1 = cast(@Error as varchar(12)) + ' (insert #NamesTraveler)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


insert into #NamesApprover (InternalExtractID, CarCartID, CarCartLogID, BookingTypeID, TUID, TitleTxt, FirstName, MiddleName, LastName)
select distinct cf.InternalExtractID, 
                cf.CarCartID, 
                cf.CarCartLogID, 
                cf.BookingTypeID, 
                cf.TUIDApprover, 
                coalesce(ta.TitleTxt, ltrim(rtrim(t.TitleTxt)),''),
                coalesce(ta.FirstName, ltrim(rtrim(t.FirstName)),''),
                coalesce(ta.MiddleName, ltrim(rtrim(t.MiddleName)),''),
                coalesce(ta.LastName, ltrim(rtrim(t.LastName)),'')
  from dbo.InternalExtractEUCarFact cf
       left outer join
       dbo.TravelerAnonymous ta on ta.GroupAccountID = cf.GroupAccountID
       inner join 
       dbo.TravelerDim t on cf.TUIDApprover = t.TUID and t.CustomerSystemID = 2
 where cf.InternalExtractID = @pInternalExtractID

select @Error = @@Error  if (@Error <> 0) begin
    select @ErrorCode = @ERRUNEXPECTED,
           @MsgParm1 = cast(@Error as varchar(12)) + ' (insert #NamesApprover)'
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
         (select distinct a.TUID, 2 as CustomerSystemID, a.TitleTxt, a.FirstName, a.MiddleName, a.LastName, @Current_Timestamp as UpdateDate, 'IEEUCarFactLoadNames' as LastUpdatedBy
           from #NamesTraveler a 
                left join
                dbo.TravelerNameDim b on 
                    b.CustomerSystemID = 2 and
                    a.TUID = b.TUID and   
                    a.TitleTxt = b.TitleTxt and  
                    a.FirstName = b.FirstName and
                    a.MiddleName = b.MiddleName and
                    a.LastName = b.LastName
          where b.CustomerSystemID is null
         union 
         select distinct a.TUID, 2 as CustomerSystemID, a.TitleTxt, a.FirstName, a.MiddleName, a.LastName, @Current_Timestamp as UpdateDate, 'IEEUCarFactLoadNames' as LastUpdatedBy
           from #NamesApprover a 
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
    update dbo.InternalExtractEUCarFact
       set TravelerNameID = c.TravelerNameID 
      from dbo.InternalExtractEUCarFact a
           inner join    
           #NamesTraveler b on
               a.InternalExtractID = b.InternalExtractID and
               a.CarCartID = b.CarCartID and
               a.CarCartLogID = b.CarCartLogID and
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
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update InternalExtractEUCarFact TravelerNameID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- Update fact table with TravelerNameID
    update dbo.InternalExtractEUCarFact
       set TravelerNameIDApprover = c.TravelerNameID 
      from dbo.InternalExtractEUCarFact a
           inner join    
           #NamesApprover b on
               a.InternalExtractID = b.InternalExtractID and
               a.CarCartID = b.CarCartID and
               a.CarCartLogID = b.CarCartLogID and
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
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update InternalExtractEUCarFact TravelerNameIDApproval)'
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

exec dbo.InternalExtractEUCarFactLoadTravelerNames
    @pInternalExtractID = 16407 

*/

/*
-- Test La Rochelle (TravelerAnonymous)
declare @InternalExtractID int
select @InternalExtractID = max(ieef.InternalExtractID) 
from dbo.InternalExtractEUCarFact ieef 
where ieef.TravelerNameIDApprover is not null
group by InternalExtractID having count(*) > 10

begin tran
    insert dbo.TravelerAnonymous (TravelProductID,GroupAccountID,ComCode,TitleTxt,FirstName,MiddleName,LastName,EmailAddress,UpdateDate,LastUpdatedBy)
    output 'Test Anon Inserted --->', inserted.*
    select distinct top 2 ieef.TravelProductID, ieef.GroupAccountID, ieef.GroupAccountID, replicate('z',10), replicate('z',10), replicate('z',10), replicate('z',10), replicate('z',10), getdate(), 'test'
    from dbo.InternalExtractEUCarFact ieef 
    where ieef.InternalExtractID = @InternalExtractID
      and ieef.TravelerNameIDApprover is not null
      and not exists(select * from dbo.TravelerAnonymous ta where ta.GroupAccountID = ieef.GroupAccountID)

    select top 1000 ieef.GroupAccountID, ieef.TravelerNameID, ieef.TravelerNameIDApprover, tnd.FirstName, tnd.MiddleName, tnd.LastName, tnd1.FirstName, tnd1.MiddleName, tnd1.LastName, *
    from dbo.InternalExtractEUCarFact ieef
    join dbo.TravelerAnonymous ta on ta.TravelProductID = ieef.TravelProductID and ta.GroupAccountID = ieef.GroupAccountID
    join dbo.TravelerNameDim tnd on tnd.TravelerNameID = ieef.TravelerNameID
    left outer join dbo.TravelerNameDim tnd1 on ieef.TravelerNameIDApprover = tnd1.TravelerNameID
    where ieef.InternalExtractID = @InternalExtractID

    exec dbo.InternalExtractEUCarFactLoadTravelerNames @pInternalExtractID = @InternalExtractID


    select top 1000 ieef.GroupAccountID, ieef.TravelerNameID, ieef.TravelerNameIDApprover, tnd.FirstName, tnd.MiddleName, tnd.LastName, tnd1.FirstName, tnd1.MiddleName, tnd1.LastName, *
    from dbo.InternalExtractEUCarFact ieef
    join dbo.TravelerAnonymous ta on ta.TravelProductID = ieef.TravelProductID and ta.GroupAccountID = ieef.GroupAccountID
    join dbo.TravelerNameDim tnd on tnd.TravelerNameID = ieef.TravelerNameID
    left outer join dbo.TravelerNameDim tnd1 on ieef.TravelerNameIDApprover = tnd1.TravelerNameID
    where ieef.InternalExtractID = @InternalExtractID

rollback tran

*/