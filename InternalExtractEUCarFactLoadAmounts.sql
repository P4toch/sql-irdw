if object_id('dbo.InternalExtractEUCarFactLoadAmounts') is null begin
    print 'Creating stored procedure InternalExtractEUCarFactLoadAmounts (placeholder)'
    execute('create procedure dbo.InternalExtractEUCarFactLoadAmounts as return 0')
end
go

print 'Altering stored procedure InternalExtractEUCarFactLoadAmounts'
go

alter procedure dbo.InternalExtractEUCarFactLoadAmounts
    @pInternalExtractID int

as

/*
*********************************************************************
Copyright (C) 2013 Expedia, Inc. All rights reserved.

Description:
    Updates most amounts for eu car fact records.

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2013-05-22  VBoerner        Created.
    2013-06-11  VBoerner        Updated to pull from local Car_Cart amount fields.
    2013-12-30  a-jako          EGE-53865 - Updated to clear out fee and tax
                                   IF (Total <> Base + Fee + Tax):
                                     THEN Fee = 0, Tax = 0, Base = Total.
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

    update a set
        a.BookingAmtGross = coalesce(b.PRICE_AMOUNT_TOTAL_LOCAL,0) * coalesce(d.ExchangeRateUsed, @ExchangeRateNull) 
            * (case when a.BookingTypeID = @BookingTypeIDReserve then 1 else -1 end), 
        a.BookingAmtBase = case when (PRICE_AMOUNT_TOTAL_LOCAL = PRICE_AMOUNT_BASE_LOCAL + PRICE_AMOUNT_TAX_LOCAL + PRICE_AMOUNT_FEES_LOCAL)
                                then -- Keep Base
                                     coalesce(b.PRICE_AMOUNT_BASE_LOCAL,0) * coalesce(d.ExchangeRateUsed, @ExchangeRateNull) 
                                         * (case when a.BookingTypeID = @BookingTypeIDReserve then 1 else -1 end)
                                else -- BookingAmtGross
                                     coalesce(b.PRICE_AMOUNT_TOTAL_LOCAL,0) * coalesce(d.ExchangeRateUsed, @ExchangeRateNull) 
                                        * (case when a.BookingTypeID = @BookingTypeIDReserve then 1 else -1 end)
                                end,
        a.BookingAmtTax = coalesce(b.PRICE_AMOUNT_TAX_LOCAL,0) * coalesce(d.ExchangeRateUsed, @ExchangeRateNull) 
            * (case when a.BookingTypeID = @BookingTypeIDReserve then 1 else -1 end)
            * (case when PRICE_AMOUNT_TOTAL_LOCAL = PRICE_AMOUNT_BASE_LOCAL + PRICE_AMOUNT_TAX_LOCAL + PRICE_AMOUNT_FEES_LOCAL then 1 else 0 end),
        a.BookingAmtFee = coalesce(b.PRICE_AMOUNT_FEES_LOCAL,0) * coalesce(d.ExchangeRateUsed, @ExchangeRateNull) 
            * (case when a.BookingTypeID = @BookingTypeIDReserve then 1 else -1 end)
            * (case when PRICE_AMOUNT_TOTAL_LOCAL = PRICE_AMOUNT_BASE_LOCAL + PRICE_AMOUNT_TAX_LOCAL + PRICE_AMOUNT_FEES_LOCAL then 1 else 0 end)
    from dbo.InternalExtractEUCarFact a 
        inner join
        dbo.vCAR_CART_LOG b on a.CarCartID = b.CAR_CART_ID and a.CarCartLogID = b.CAR_CART_LOG_ID
        inner join 
        dbo.TravelProductDim c on a.TravelProductID = c.TravelProductID
        left join
        dbo.ExchangeRateDailyFull d on dbo.TimeIDFromDate(a.IssueDate) = d.TimeID and
            b.CURRENCY_CODE_LOCAL = d.FromCurrencyCode and 
            c.CurrencyCodeStorage = d.ToCurrencyCode
    where a.InternalExtractID = @pInternalExtractID

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Update InternalExtractEUCarFact amounts)'
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

begin tran

select BookingAmtGross, BookingAmtBase, BookingAmtTax, BookingAmtFee, 
       --PRICE_AMOUNT_TOTAL_LOCAL, PRICE_AMOUNT_BASE_LOCAL, PRICE_AMOUNT_TAX_LOCAL, PRICE_AMOUNT_FEES_LOCAL,
    (case when PRICE_AMOUNT_TOTAL_LOCAL = PRICE_AMOUNT_BASE_LOCAL + PRICE_AMOUNT_TAX_LOCAL + PRICE_AMOUNT_FEES_LOCAL 
        or PRICE_AMOUNT_TAX_LOCAL + PRICE_AMOUNT_FEES_LOCAL = 0 then 1 else 0 end) as TotalMatches
from dbo.InternalExtractEUCarFact a
join dbo.Car_Cart_Log b on a.CarCartLogID = b.Car_Cart_Log_ID
where a.InternalExtractID = 17135

exec dbo.InternalExtractEUCarFactLoadAmounts
    @pInternalExtractID = 17135

select BookingAmtGross, BookingAmtBase, BookingAmtTax, BookingAmtFee, 
    --PRICE_AMOUNT_TOTAL_LOCAL, PRICE_AMOUNT_BASE_LOCAL, PRICE_AMOUNT_TAX_LOCAL, PRICE_AMOUNT_FEES_LOCAL,
    (case when BookingAmtGross = BookingAmtBase + BookingAmtTax + BookingAmtFee
        or BookingAmtTax + BookingAmtFee = 0 then 1 else 0 end) as TotalMatches
from dbo.InternalExtractEUCarFact a
join dbo.Car_Cart_Log b on a.CarCartLogID = b.Car_Cart_Log_ID
where a.InternalExtractID = 17135

rollback tran 

*/