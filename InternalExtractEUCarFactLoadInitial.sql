if object_id('dbo.InternalExtractEUCarFactLoadInitial') is null begin
    print 'Creating stored procedure InternalExtractEUCarFactLoadInitial (placeholder)'
    execute('create procedure dbo.InternalExtractEUCarFactLoadInitial as return 0')
end
go

print 'Altering stored procedure InternalExtractEUCarFactLoadInitial'
go

alter procedure dbo.InternalExtractEUCarFactLoadInitial
    @pInternalExtractID int

as

/*
*********************************************************************
Copyright (C) 2013 Expedia, Inc. All rights reserved.

Description:
     Inserts index set of EU car fact records for a particular
     extract.  

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2013-05-29  VBoerner        Created.
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
    @BookingTypeIDReserve           tinyint,
    @BookingTypeIDCancel            tinyint,
    @BookingSystemID_ECTWeb         tinyint,
    @FactRecordStatusIDOK           tinyint,
    @FactRecordStatusIDGPID         tinyint,
    @FactRecordStatusIDLWD          tinyint,
    @FactRecordStatusIDTRAV         tinyint,
    @CarCartID                      int,
    @TravelProductID                int

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
    @FactRecordStatusIDOK           = 1,
    @FactRecordStatusIDGPID         = 7,
    @FactRecordStatusIDLWD          = 8,
    @FactRecordStatusIDTRAV         = 9,

    @BookingSystemID_ECTWeb         = 22

create table #Working (
    CAR_CART_ID int not null,
    CAR_CART_LOG_ID int not null,
    CART_STATUS_CODE varchar(10) not null,
    IS_PENDING_CONFIRMATION bit null,
    IS_OFFLINE bit null,
    PER_CODE_BOOKER int null,
    PER_CODE_MAIN int null,
    BOOKING_DATETIME datetime null,
    MODIFICATION_DATE datetime null )

create clustered index temp_ix1 on #Working (CAR_CART_ID, CAR_CART_LOG_ID)

---------------------------------------------------------------------
-- Validation
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Processing
---------------------------------------------------------------------

-- Get the index set of records to work on.  
insert into #Working (CAR_CART_ID, CAR_CART_LOG_ID, CART_STATUS_CODE, IS_PENDING_CONFIRMATION, IS_OFFLINE, 
    PER_CODE_BOOKER, PER_CODE_MAIN, BOOKING_DATETIME, MODIFICATION_DATE)
select a.CAR_CART_ID, a.CAR_CART_LOG_ID, a.CART_STATUS_CODE, a.IS_PENDING_CONFIRMATION, a.IS_OFFLINE, 
    a.PER_CODE_BOOKER, a.PER_CODE_MAIN, a.BOOKING_DATETIME, a.MODIFICATION_DATE
  from (  select a.*,
                 RankNbr = row_number() over (partition by a.BookingSystemID, 
                                                           a.CAR_CART_ID, 
                                                           case when a.CART_STATUS_CODE in ('V','Q') then 'V'
                                                                when a.CART_STATUS_CODE in ('C','J') then 'C' end, 
                                                           case when a.CART_STATUS_CODE in ('V','Q') then a.IS_OFFLINE end 
                                                  order by a.CAR_CART_LOG_ID desc)
            from dbo.Car_Cart_Log a
           where a.InternalExtractID = @pInternalExtractID
             and a.BookingSystemID = @BookingSystemID_ECTWeb ) a
  where a.RankNbr = 1
        and a.CART_STATUS_CODE in ('V','Q','C','J') 

select @Error = @@Error if (@Error <> 0) begin 
    select @ErrorCode = @ERRUNEXPECTED,
           @MsgParm1 = cast(@Error as varchar(12)) + ' (insert #Working)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

select @TrancountSave = @@Trancount
if (@TrancountSave = 0) begin transaction @SavePointName
else                    save  transaction @SavePointName
select @TranStartedBool = @TRUE


-- Insert online reserves 
insert into dbo.InternalExtractEUCarFact (InternalExtractID, CarCartID, CarCartLogID, BookingTypeID, BookingSystemID, FactRecordStatusID,
    InternalExtractIDReserve, CarCartLogIDCancel, CustomerSystemID, IssueDate, IssueDatePrior, UpdateDate, LastUpdatedBy)
select @pInternalExtractID, a.CAR_CART_ID, a.CAR_CART_LOG_ID, @BookingTypeIDReserve, @BookingSystemID_ECTWeb, @FactRecordStatusIDOK,
        @pInternalExtractID, null as CarCartLogIDCancel, 2 as CustomerSystemID, cast(a.MODIFICATION_DATE as smalldatetime), 
        null, @Current_TimeStamp, 'IEEUCarFactLoadInitial'
  from #Working a
       left join    
       dbo.InternalExtractEUCarFact b on a.CAR_CART_ID = b.CarCartID and b.BookingTypeID = @BookingTypeIDReserve 
 where a.CART_STATUS_CODE in ('V','Q') --Verified/Approval Requested
       and a.IS_OFFLINE = 0
       and b.CarCartID is null

select @Error = @@Error if (@Error <> 0) begin
    select @ErrorCode = @ERRUNEXPECTED,
           @MsgParm1 = cast(@Error as varchar(12)) + ' (insert #InternalExtractEUCarFact Online Reserves)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


-- Insert cancel for offline modifications
insert into dbo.InternalExtractEUCarFact (InternalExtractID, CarCartID, CarCartLogID, BookingTypeID, BookingSystemID, FactRecordStatusID, 
    InternalExtractIDReserve, CarCartLogIDCancel, CustomerSystemID, IssueDate, IssueDatePrior, UpdateDate, LastUpdatedBy)
select @pInternalExtractID, a.CAR_CART_ID, b.CarCartLogIDPrior, @BookingTypeIDCancel, @BookingSystemID_ECTWeb, @FactRecordStatusIDOK,
        b.InternalExtractIDPrior, a.CAR_CART_LOG_ID, 2 as CustomerSystemID, cast(a.MODIFICATION_DATE as smalldatetime), 
        b.IssueDatePrior, @Current_TimeStamp, 'IEEUCarFactLoadInitial'
  from #Working a
       inner join (  
            select 
                a.InternalExtractID as InternalExtractIDPrior,
                a.CarCartID, 
                a.CarCartLogID as CarCartLogIDPrior,
                a.IssueDate as IssueDatePrior,
                RankNbr = row_number() over (partition by a.CarCartID order by a.CarCartLogID desc) 
            from dbo.InternalExtractEUCarFact a
            join #Working b on a.CarCartID = b.Car_Cart_ID 
            where a.BookingTypeID = @BookingTypeIDReserve ) b on a.CAR_CART_ID = b.CarCartID and b.RankNbr = 1 
 where a.CART_STATUS_CODE in ('V','Q') --Verified/Approval Requested
       and a.IS_OFFLINE = 1

select @Error = @@Error if (@Error <> 0) begin
    select @ErrorCode = @ERRUNEXPECTED,
           @MsgParm1 = cast(@Error as varchar(12)) + ' (insert #InternalExtractEUCarFact Offline Cancels)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


-- Insert offline reserves
insert into dbo.InternalExtractEUCarFact (InternalExtractID, CarCartID, CarCartLogID, BookingTypeID, BookingSystemID, FactRecordStatusID,
    InternalExtractIDReserve, CarCartLogIDCancel, CustomerSystemID, IssueDate, IssueDatePrior, UpdateDate, LastUpdatedBy)
select @pInternalExtractID, a.CAR_CART_ID, a.CAR_CART_LOG_ID, @BookingTypeIDReserve, @BookingSystemID_ECTWeb, @FactRecordStatusIDOK,
        @pInternalExtractID, null as CarCartLogIDCancel, 2 as CustomerSystemID, cast(a.MODIFICATION_DATE as smalldatetime), 
        b.IssueDatePrior, @Current_TimeStamp, 'IEEUCarFactLoadInitial'
  from #Working a
       left join (  
            select 
                a.InternalExtractID as InternalExtractIDPrior,
                a.CarCartID, 
                a.CarCartLogID as CarCartLogIDPrior,
                a.IssueDate as IssueDatePrior,
                RankNbr = row_number() over (partition by a.CarCartID order by a.CarCartLogID desc) 
            from dbo.InternalExtractEUCarFact a
            join #Working b on a.CarCartID = b.Car_Cart_ID 
            where a.BookingTypeID = @BookingTypeIDReserve ) b on a.CAR_CART_ID = b.CarCartID and b.RankNbr = 1 
       left join    
       dbo.InternalExtractEUCarFact c on a.CAR_CART_ID = c.CarCartID and a.CAR_CART_LOG_ID = c.CarCartLogID and c.BookingTypeID = @BookingTypeIDReserve 
 where a.CART_STATUS_CODE in ('V','Q') --Verified/Approval Requested
       and a.IS_OFFLINE = 1
       and c.CarCartID is null 

select @Error = @@Error if (@Error <> 0) begin
    select @ErrorCode = @ERRUNEXPECTED,
           @MsgParm1 = cast(@Error as varchar(12)) + ' (insert #InternalExtractEUCarFact Offline Reserves)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


-- Insert cancels 
insert into dbo.InternalExtractEUCarFact (InternalExtractID, CarCartID, CarCartLogID, BookingTypeID, BookingSystemID, FactRecordStatusID, 
    InternalExtractIDReserve, CarCartLogIDCancel, CustomerSystemID, IssueDate, IssueDatePrior, UpdateDate, LastUpdatedBy)
select @pInternalExtractID, a.CAR_CART_ID, b.CarCartLogIDPrior, @BookingTypeIDCancel, @BookingSystemID_ECTWeb, @FactRecordStatusIDOK,
        b.InternalExtractIDPrior, a.CAR_CART_LOG_ID, 2 as CustomerSystemID, cast(a.MODIFICATION_DATE as smalldatetime), 
        b.IssueDatePrior, @Current_TimeStamp, 'IEEUCarFactLoadInitial'
  from #Working a
       inner join (  
            select 
                a.InternalExtractID as InternalExtractIDPrior,
                a.CarCartID, 
                a.CarCartLogID as CarCartLogIDPrior,
                a.IssueDate as IssueDatePrior,
                RankNbr = row_number() over (partition by a.CarCartID order by a.CarCartLogID desc) 
            from dbo.InternalExtractEUCarFact a
            join #Working b on a.CarCartID = b.Car_Cart_ID 
            where a.BookingTypeID = @BookingTypeIDReserve ) b on a.CAR_CART_ID = b.CarCartID and b.RankNbr = 1 
       left join    
       dbo.InternalExtractEUCarFact c on a.CAR_CART_ID = c.CarCartID and c.CarCartLogID >= b.CarCartLogIDPrior and c.BookingTypeID = @BookingTypeIDCancel 
 where a.CART_STATUS_CODE in ('C','J') --Canceled/Approval Rejected
       and c.CarCartID is null

select @Error = @@Error if (@Error <> 0) begin
    select @ErrorCode = @ERRUNEXPECTED,
           @MsgParm1 = cast(@Error as varchar(12)) + ' (insert #InternalExtractEUCarFact Online Cancels)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

----------------------------------------
-- Update FactRecordStatusID
----------------------------------------

--Missing LWD 
update a set
    a.FactRecordStatusID = @FactRecordStatusIDLWD
from dbo.InternalExtractEUCarFact a 
    inner join
    dbo.vCAR_CART_LOG b on a.CarCartID = b.CAR_CART_ID and a.CarCartLogID = b.CAR_CART_LOG_ID
    left join
    dbo.LIGHTWEIGHTDOSSIERS c on a.CarCartID = c.REF_DOSSIER_CODE and c.REF_DOSSIER_TYPE = 'CAR_TC' and a.InternalExtractIDReserve = c.InternalExtractID
where a.InternalExtractID = @pInternalExtractID
    and c.REF_DOSSIER_CODE is null

select @Error = @@Error if (@Error <> 0) begin
    select @ErrorCode = @ERRUNEXPECTED,
           @MsgParm1 = cast(@Error as varchar(12)) + ' (update FactRecordStatusID LWD)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

--Missing Travellers 
update a set
    a.FactRecordStatusID = @FactRecordStatusIDTRAV
from dbo.InternalExtractEUCarFact a 
    inner join
    dbo.vCAR_CART_LOG b on a.CarCartID = b.CAR_CART_ID and a.CarCartLogID = b.CAR_CART_LOG_ID
    inner join
    dbo.LIGHTWEIGHTDOSSIERS c on a.CarCartID = c.REF_DOSSIER_CODE and c.REF_DOSSIER_TYPE = 'CAR_TC' and a.InternalExtractIDReserve = c.InternalExtractID
    left join 
    dbo.TRAVELLERS d on c.MD_CODE = d.MD_CODE and b.PER_CODE_MAIN = d.PER_CODE and a.InternalExtractIDReserve = d.InternalExtractID
where a.InternalExtractID = @pInternalExtractID
    and d.PER_CODE is null

select @Error = @@Error if (@Error <> 0) begin
    select @ErrorCode = @ERRUNEXPECTED,
           @MsgParm1 = cast(@Error as varchar(12)) + ' (update FactRecordStatusID Traveller)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

--Missing GPID Mapping
update a set
    a.FactRecordStatusID = @FactRecordStatusIDGPID
from dbo.InternalExtractEUCarFact a 
    inner join
    dbo.vCAR_CART_LOG b on a.CarCartID = b.CAR_CART_ID and a.CarCartLogID = b.CAR_CART_LOG_ID
    inner join
    dbo.LIGHTWEIGHTDOSSIERS c on a.CarCartID = c.REF_DOSSIER_CODE and c.REF_DOSSIER_TYPE = 'CAR_TC' and a.InternalExtractIDReserve = c.InternalExtractID
    inner join 
    dbo.TRAVELLERS d on c.MD_CODE = d.MD_CODE and b.PER_CODE_MAIN = d.PER_CODE and a.InternalExtractIDReserve = d.InternalExtractID
    left join 
    dbo.GroupAccountDim e on d.Company_ID_ = e.ComCode 
where a.InternalExtractID = @pInternalExtractID
    and e.GroupAccountID is null

select @Error = @@Error if (@Error <> 0) begin
    select @ErrorCode = @ERRUNEXPECTED,
           @MsgParm1 = cast(@Error as varchar(12)) + ' (update FactRecordStatusID GPID)'
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

exec dbo.InternalExtractEUCarFactLoadInitial
    @pInternalExtractID = 17135

select *
from dbo.InternalExtractEUCarFact a
join dbo.Car_Cart_Log b on a.CarCartLogID = b.Car_Cart_Log_ID
where b.Modification_date > '5/17/2013'

*/