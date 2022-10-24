if object_id('dbo.InternalExtractEUCarSpecialEquipmentFactLoad') is null begin
    print 'Creating stored procedure InternalExtractEUCarSpecialEquipmentFactLoad (placeholder)'
    execute('create procedure dbo.InternalExtractEUCarSpecialEquipmentFactLoad as return 0')
end
go

print 'Altering stored procedure InternalExtractEUCarSpecialEquipmentFactLoad'
go

alter procedure dbo.InternalExtractEUCarSpecialEquipmentFactLoad
    @pInternalExtractID int

as

/*
*********************************************************************
Copyright (C) 2013- 2014 Expedia, Inc. All rights reserved.

Description:
    Insert eu car special equipment fact records.

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2013-05-24  VBoerner        Created.
    2013-07-22  VBoerner        Updated for logging changes (spec equip
                                only logged on reserve not book/cancel).
    2014-07-15  jappleberry     EGE-63901 Updated fact processing to eliminate 
                                rows from CAR_CART_EQUIPMENT_LOG when RATE_PLAN_CODE is null
                                using PRICE_AMOUNT_BASE_LOCAL to load PRICE_AMOUNT_TOTAL_LOCAL  
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
    @TravelProductID                      int,
    @ExchangeRateNull                     money

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
    @BookingSystemID_ECTWeb         = 22,
    @ExchangeRateNull               = 0.0

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


    insert into dbo.InternalExtractEUCarSpecialEquipmentFact (InternalExtractID, CarCartID, CarCartLogID, 
        BookingTypeID, BookingSystemID, CarSpecialEquipmentSeqNbr, CarSpecialEquipmentID, SpecialEquipmentCnt, 
        CarRatePeriodID, BookingAmtGross, BookingAmtBase)
    select 
        a.InternalExtractID,
        a.CarCartID,
        a.CarCartLogID,
        a.BookingTypeID,
        a.BookingSystemID,
        RankNbr = row_number() over (partition by a.InternalExtractID, a.CarCartID, a.CarCartLogID, a.BookingTypeID, a.BookingSystemID order by f.CarSpecialEquipmentID, e.CarRatePeriodID),
        f.CarSpecialEquipmentID,
        sum(coalesce(b.Car_Equipment_Count,1)) Car_Equipment_Count,
        e.CarRatePeriodID,
        -- always using Price_Amount_Base_Local instead of Price_Amount_Total_Local 
        sum(coalesce(b.Price_Amount_Base_Local,0) * coalesce(d.ExchangeRateUsed, @ExchangeRateNull)
             * (case when a.BookingTypeID = @BookingTypeIDReserve then 1 else -1 end)) Price_Amount_Total_Local,
        sum(coalesce(b.Price_Amount_Base_Local,0) * coalesce(d.ExchangeRateUsed, @ExchangeRateNull)
             * (case when a.BookingTypeID = @BookingTypeIDReserve then 1 else -1 end)) Price_Amount_Base_Local
    from dbo.InternalExtractEUCarFact a 
        inner join (
              select 
                  b.InternalExtractID,
                  b.CAR_CART_EQUIPMENT_LOG_ID,
                  b.CAR_CART_EQUIPMENT_ID,
                  b.CAR_CART_ID,
                  b.CAR_EQUIPMENT_CODE,
                  b.CAR_EQUIPMENT_COUNT,
                  b.RATE_PLAN_CODE,
                  b.CURRENCY_CODE_LOCAL,
                  b.PRICE_AMOUNT_TOTAL_LOCAL,
                  b.PRICE_AMOUNT_BASE_LOCAL,
                  b.IS_SELECTED,
                  RankNbr = row_number() over (partition by a.InternalExtractID, a.CarCartID, b.CAR_CART_EQUIPMENT_ID order by b.CAR_CART_EQUIPMENT_LOG_ID desc) 
                from dbo.InternalExtractEUCarFact a 
                inner join
                dbo.CAR_CART_EQUIPMENT_LOG b on a.InternalExtractIDReserve = b.InternalExtractID 
                    and a.CarCartID = b.CAR_CART_ID 
               where a.InternalExtractID = @pInternalExtractID
                and b.RATE_PLAN_CODE is not null) B on a.InternalExtractIDReserve = B.InternalExtractID 
            and a.CarCartID = B.CAR_CART_ID 
            and B.RankNbr = 1
        inner join 
        dbo.TravelProductDim c on a.TravelProductID = c.TravelProductID
        left join
        dbo.ExchangeRateDailyFull d on dbo.TimeIDFromDate(a.IssueDate) = d.TimeID and
            b.CURRENCY_CODE_LOCAL = d.FromCurrencyCode and 
            c.CurrencyCodeStorage = d.ToCurrencyCode
        left join
        dbo.CarRatePeriodBookingSource e on ltrim(rtrim(b.RATE_PLAN_CODE)) = e.CarRatePeriodCode and e.BookingSourceID = 39
        left join  
        dbo.CarSpecialEquipmentBookingSource f on ltrim(rtrim(b.CAR_EQUIPMENT_CODE)) = f.CarSpecialEquipmentCode and f.BookingSourceID = 39
    where a.InternalExtractID = @pInternalExtractID
        and b.IS_SELECTED = @TRUE
    group by a.InternalExtractID, a.CarCartID, a.CarCartLogID, a.BookingTypeID, a.BookingSystemID, f.CarSpecialEquipmentID, e.CarRatePeriodID

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Insert IEEUCarSpecialEquipmentFact)'
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

select max(InternalExtractID)
from dbo.InternalExtractEUCarSpecialEquipmentFact


exec dbo.InternalExtractEUCarSpecialEquipmentFactLoad
    @pInternalExtractID = 29559
    
exec dbo.InternalExtractEUCarSpecialEquipmentFactLoad
    @pInternalExtractID = 22726

exec dbo.InternalExtractEUCarSpecialEquipmentFactLoad
    @pInternalExtractID = 17293

select *
from dbo.InternalExtractEUCarSpecialEquipmentFact a
--join dbo.Car_Cart_Equipment_Log b on a.CarCartID = b.Car_Cart_ID
where a.InternalExtractID = 17293

select *
from dbo.Car_Cart_Equipment_Log
where InternalExtractID > 17293
    and Is_Selected = 1

*/