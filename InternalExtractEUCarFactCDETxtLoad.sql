if object_id('dbo.InternalExtractEUCarFactCDETxtLoad') is null begin
    print 'Creating stored procedure InternalExtractEUCarFactCDETxtLoad (placeholder)'
    execute('create procedure dbo.InternalExtractEUCarFactCDETxtLoad as return 0')
end
go

print 'Altering stored procedure InternalExtractEUCarFactCDETxtLoad'
go

alter procedure dbo.InternalExtractEUCarFactCDETxtLoad
    @pInternalExtractID int

as

/*
*********************************************************************
Copyright (C) 2013-2016 Expedia, Inc. All rights reserved.

Description:
    Inserts cdf data into InternalExtractEUCarFactCDETxt &
    InternalExtractEUCarFactCDFValue tables.

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
    2014-02-13  DMurugesan      EGE:60803 Duplicate CDF Value clean up process added
    2016-11-29  jinye           EGE-131326 update FreeText3 to source from an additional table in Navision 
    2017-09-12  jappleberry     Added left(ltrim(rtrim(c.CC5_LABEL_)), 60) to handle update of CC5_LABEL_ to varchar(70)  
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


    --insert default records
    insert into dbo.InternalExtractEUCarFactCDETxt (InternalExtractID, CarCartID, CarCartLogID, 
        BookingTypeID, BookingSystemID, CustomDataElementTxt)
    select 
        a.InternalExtractID,
        a.CarCartID,
        a.CarCartLogID,
        a.BookingTypeID,
        a.BookingSystemID,
        '' as CustomDataElementTxt
    from dbo.InternalExtractEUCarFact a 
    where a.InternalExtractID = @pInternalExtractID

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Insert IEEUCarFactCDETxt)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    --update CC fields
    update a set 
        a.CustomDataElementTxt = a.CustomDataElementTxt + 
            coalesce( 'ID2' + '&&' + ltrim(rtrim(c2.CustomDataFieldName)) + '&&' + ltrim(rtrim(c.CC2_LABEL_)) + '<', '' ) + 
            coalesce( 'ID3' + '&&' + ltrim(rtrim(c3.CustomDataFieldName)) + '&&' + ltrim(rtrim(c.CC3_LABEL_)) + '<', '' ) + 
            coalesce( 'ID4' + '&&' + ltrim(rtrim(c4.CustomDataFieldName)) + '&&' + ltrim(rtrim(c.CC4_LABEL_)) + '<', '' ) + 
            coalesce( 'ID5' + '&&' + ltrim(rtrim(c5.CustomDataFieldName)) + '&&' + ltrim(rtrim(c.CC5_LABEL_)) + '<', '' ) 
    from dbo.InternalExtractEUCarFactCDETxt a 
        inner join 
        dbo.InternalExtractEUCarFact b on a.InternalExtractID = b.InternalExtractID 
            and a.CarCartID = b.CarCartID
            and a.CarCartLogID = b.CarCartLogID
            and a.BookingTypeID = b.BookingTypeID 
            and a.BookingSystemID = b.BookingSystemID
        inner join 
        dbo.TRAVELLERS c on b.MetaDossierID = c.MD_CODE and b.TUIDAccount = c.PER_CODE and b.InternalExtractIDReserve = c.InternalExtractID
        left join 
        dbo.CustomDataFieldDimCurrent c2 on b.GroupAccountID = c2.GroupAccountID and c2.CustomDataFieldID = 2
        left join 
        dbo.CustomDataFieldDimCurrent c3 on b.GroupAccountID = c3.GroupAccountID and c3.CustomDataFieldID = 3
        left join 
        dbo.CustomDataFieldDimCurrent c4 on b.GroupAccountID = c4.GroupAccountID and c4.CustomDataFieldID = 4
        left join 
        dbo.CustomDataFieldDimCurrent c5 on b.GroupAccountID = c5.GroupAccountID and c5.CustomDataFieldID = 5
    where a.InternalExtractID = @pInternalExtractID

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Update CC fields IEEUCarFactCDETxt)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    --FT1/15 FT2/1456 FT3/1324
    --update FT1 field
    update a set 
        a.CustomDataElementTxt = a.CustomDataElementTxt + 
            coalesce( 'FT1' + '&&' + ltrim(rtrim(f.CustomDataFieldName)) + '&&' + ltrim(rtrim(d.FLV_VARCHAR)) + '<', '' ) 
    from dbo.InternalExtractEUCarFactCDETxt a 
        inner join 
        dbo.InternalExtractEUCarFact b on a.InternalExtractID = b.InternalExtractID 
            and a.CarCartID = b.CarCartID
            and a.CarCartLogID = b.CarCartLogID
            and a.BookingTypeID = b.BookingTypeID 
            and a.BookingSystemID = b.BookingSystemID
        inner join 
        dbo.TRAVELLERS c on b.MetaDossierID = c.MD_CODE and b.TUIDAccount = c.PER_CODE and b.InternalExtractIDReserve = c.InternalExtractID
        inner join 
        dbo.FIELD_VALUES d on c.MD_CODE = d.TARGET_CODE and b.InternalExtractIDReserve = d.InternalExtractID 
            and d.APPLIES_TO = 'MID' and d.FLD_CODE = 15 
        inner join 
        dbo.CustomDataFieldDimCurrent f on b.GroupAccountID = f.GroupAccountID and f.CustomDataFieldID = 11
    where a.InternalExtractID = @pInternalExtractID

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Update FT1 fields IEEUCarFactCDETxt)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    --update FT2 field
    update a set 
        a.CustomDataElementTxt = a.CustomDataElementTxt + 
            coalesce( 'FT2' + '&&' + ltrim(rtrim(f.CustomDataFieldName)) + '&&' + ltrim(rtrim(d.FLV_VARCHAR)) + '<', '' ) 
    from dbo.InternalExtractEUCarFactCDETxt a 
        inner join 
        dbo.InternalExtractEUCarFact b on a.InternalExtractID = b.InternalExtractID 
            and a.CarCartID = b.CarCartID
            and a.CarCartLogID = b.CarCartLogID
            and a.BookingTypeID = b.BookingTypeID 
            and a.BookingSystemID = b.BookingSystemID
        inner join 
        dbo.TRAVELLERS c on b.MetaDossierID = c.MD_CODE and b.TUIDAccount = c.PER_CODE and b.InternalExtractIDReserve = c.InternalExtractID
        inner join 
        dbo.FIELD_VALUES d on c.MD_CODE = d.TARGET_CODE and b.InternalExtractIDReserve = d.InternalExtractID 
            and d.APPLIES_TO = 'MID' and d.FLD_CODE = 1456
        inner join 
        dbo.CustomDataFieldDimCurrent f on b.GroupAccountID = f.GroupAccountID and f.CustomDataFieldID = 12
    where a.InternalExtractID = @pInternalExtractID

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Update FT2 fields IEEUCarFactCDETxt)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    --update FT3 field
    update a set 
        a.CustomDataElementTxt = a.CustomDataElementTxt + 
            coalesce( 'FT3' + '&&' + ltrim(rtrim(f.CustomDataFieldName)) + '&&' + ltrim(rtrim(coalesce(d.FLV_VARCHAR, e.MISSION_NUMBER))) + '<', '' ) 
    from dbo.InternalExtractEUCarFactCDETxt a 
        inner join 
        dbo.InternalExtractEUCarFact b on a.InternalExtractID = b.InternalExtractID 
            and a.CarCartID = b.CarCartID
            and a.CarCartLogID = b.CarCartLogID
            and a.BookingTypeID = b.BookingTypeID 
            and a.BookingSystemID = b.BookingSystemID
        inner join 
        dbo.TRAVELLERS c on b.MetaDossierID = c.MD_CODE and b.TUIDAccount = c.PER_CODE and b.InternalExtractIDReserve = c.InternalExtractID
        left join 
        dbo.FIELD_VALUES d on c.MD_CODE = d.TARGET_CODE and b.InternalExtractIDReserve = d.InternalExtractID 
            and d.APPLIES_TO = 'MID' and d.FLD_CODE = 1324
        left join 
        dbo.METADOSSIERS e on e.MD_CODE = c.MD_CODE and b.InternalExtractIDReserve = e.InternalExtractID 
        inner join 
        dbo.CustomDataFieldDimCurrent f on b.GroupAccountID = f.GroupAccountID and f.CustomDataFieldID = 13
    where a.InternalExtractID = @pInternalExtractID

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Update FT3 fields IEEUCarFactCDETxt)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    --Insert Free text fields
    insert into dbo.InternalExtractEUCarFactCDFValue (InternalExtractID, CarCartID, CarCartLogID, 
        BookingTypeID, BookingSystemID, CustomDataFieldID, CustomDataFieldValue)
    select
        a.InternalExtractID,
        a.CarCartID,
        a.CarCartLogID,
        a.BookingTypeID,
        a.BookingSystemID,
        f.CustomDataFieldID,
        left(ltrim(rtrim(d.FLV_VARCHAR)),60)
    from dbo.InternalExtractEUCarFactCDETxt a 
        inner join 
        dbo.InternalExtractEUCarFact b on a.InternalExtractID = b.InternalExtractID 
            and a.CarCartID = b.CarCartID
            and a.CarCartLogID = b.CarCartLogID
            and a.BookingTypeID = b.BookingTypeID 
            and a.BookingSystemID = b.BookingSystemID
        inner join 
        dbo.TRAVELLERS c on b.MetaDossierID = c.MD_CODE and b.TUIDAccount = c.PER_CODE and b.InternalExtractIDReserve = c.InternalExtractID
        inner join 
        (select distinct
            a.TARGET_CODE,
            a.InternalExtractID,
            a.APPLIES_TO,
            a.FLD_CODE,
            a.FLV_VARCHAR 
         from             
            (select TARGET_CODE,
                    InternalExtractID,
                    APPLIES_TO,
                    FLD_CODE,
                    FLV_VARCHAR 
             from dbo.FIELD_VALUES) a 
          inner join 
            (select ROW_NUMBER() OVER ( PARTITION BY TARGET_CODE,
                                                      InternalExtractID,
                                                      APPLIES_TO,
                                                      FLD_CODE
                    ORDER BY FLV_VARCHAR ASC ) as rn,
                    TARGET_CODE,
                    InternalExtractID,
                    APPLIES_TO,
                    FLD_CODE,
                    FLV_VARCHAR 
               from dbo.FIELD_VALUES) b on
            a.TARGET_CODE = b.TARGET_CODE and 
            a.InternalExtractID = b.InternalExtractID and
            a.APPLIES_TO = b.APPLIES_TO and
            a.FLD_CODE = b.FLD_CODE and 
            a.FLV_VARCHAR = b.FLV_VARCHAR and
            b.rn = 1) as d 
        on c.MD_CODE = d.TARGET_CODE and b.InternalExtractIDReserve = d.InternalExtractID 
            and d.APPLIES_TO = 'MID' and d.FLD_CODE in (15,1456,1324)
        inner join 
        dbo.CustomDataFieldDimCurrent f on b.GroupAccountID = f.GroupAccountID and f.CustomDataFieldID =
            case d.FLD_CODE 
                when 15 then 11
                when 1456 then 12
                when 1324 then 13 end
    where a.InternalExtractID = @pInternalExtractID

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Insert FT fields IEEUCarFactCDFValue)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    --Insert from additional source for "Free text 3" field. If FT3 is null in FIELD_VALUES table, go to METADOSSIERS table. 
    insert into dbo.InternalExtractEUCarFactCDFValue (InternalExtractID, CarCartID, CarCartLogID, 
        BookingTypeID, BookingSystemID, CustomDataFieldID, CustomDataFieldValue)
    select 
        a.InternalExtractID,
        a.CarCartID,
        a.CarCartLogID,
        a.BookingTypeID,
        a.BookingSystemID,
        f.CustomDataFieldID,
        left(ltrim(rtrim(e.MISSION_NUMBER)),60)
    from dbo.InternalExtractEUCarFactCDETxt a 
        inner join 
        dbo.InternalExtractEUCarFact b on a.InternalExtractID = b.InternalExtractID 
            and a.CarCartID = b.CarCartID
            and a.CarCartLogID = b.CarCartLogID
            and a.BookingTypeID = b.BookingTypeID 
            and a.BookingSystemID = b.BookingSystemID
        inner join 
        dbo.TRAVELLERS c on b.MetaDossierID = c.MD_CODE and b.TUIDAccount = c.PER_CODE and b.InternalExtractIDReserve = c.InternalExtractID
        inner join
        dbo.METADOSSIERS e on e.MD_CODE = c.MD_CODE and b.InternalExtractIDReserve = e.InternalExtractID 
        inner join 
        dbo.CustomDataFieldDimCurrent f on b.GroupAccountID = f.GroupAccountID and f.CustomDataFieldID = 13
        left join
        dbo.InternalExtractEUCarFactCDFValue g 
        on g.CarCartID=a.CarCartID and g.CarCartLogID=a.CarCartLogID and g.InternalExtractID=a.InternalExtractID and g.CustomDataFieldID=13
    where a.InternalExtractID = @pInternalExtractID and g.CustomDataFieldValue is null and (left(ltrim(rtrim(e.MISSION_NUMBER)),60) is not null)

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Insert FT fields IEEUCarFactCDFValue)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    --Insert Dept field
    insert into dbo.InternalExtractEUCarFactCDFValue (InternalExtractID, CarCartID, CarCartLogID, 
        BookingTypeID, BookingSystemID, CustomDataFieldID, CustomDataFieldValue)
    select
        a.InternalExtractID,
        a.CarCartID,
        a.CarCartLogID,
        a.BookingTypeID,
        a.BookingSystemID,
        0 as CustomDataFieldID,
        left(ltrim(rtrim(c.CC1_LABEL_)),60)
    from dbo.InternalExtractEUCarFactCDETxt a 
        inner join 
        dbo.InternalExtractEUCarFact b on a.InternalExtractID = b.InternalExtractID 
            and a.CarCartID = b.CarCartID
            and a.CarCartLogID = b.CarCartLogID
            and a.BookingTypeID = b.BookingTypeID 
            and a.BookingSystemID = b.BookingSystemID
        inner join 
        dbo.TRAVELLERS c on b.MetaDossierID = c.MD_CODE and b.TUIDAccount = c.PER_CODE and b.InternalExtractIDReserve = c.InternalExtractID
    where a.InternalExtractID = @pInternalExtractID
        and coalesce(c.CC1_LABEL_,'') <> ''

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Insert Dept field IEEUCarFactCDFValue)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    --Insert CC2 field
    insert into dbo.InternalExtractEUCarFactCDFValue (InternalExtractID, CarCartID, CarCartLogID, 
        BookingTypeID, BookingSystemID, CustomDataFieldID, CustomDataFieldValue)
    select
        a.InternalExtractID,
        a.CarCartID,
        a.CarCartLogID,
        a.BookingTypeID,
        a.BookingSystemID,
        f.CustomDataFieldID,
        left(ltrim(rtrim(c.CC2_LABEL_)),60)
    from dbo.InternalExtractEUCarFactCDETxt a 
        inner join 
        dbo.InternalExtractEUCarFact b on a.InternalExtractID = b.InternalExtractID 
            and a.CarCartID = b.CarCartID
            and a.CarCartLogID = b.CarCartLogID
            and a.BookingTypeID = b.BookingTypeID 
            and a.BookingSystemID = b.BookingSystemID
        inner join 
        dbo.TRAVELLERS c on b.MetaDossierID = c.MD_CODE and b.TUIDAccount = c.PER_CODE and b.InternalExtractIDReserve = c.InternalExtractID
        inner join 
        dbo.CustomDataFieldDimCurrent f on b.GroupAccountID = f.GroupAccountID and f.CustomDataFieldID = 2
    where a.InternalExtractID = @pInternalExtractID
        and coalesce(c.CC2_LABEL_,'') <> ''

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Insert CC2 field IEEUCarFactCDFValue)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    --Insert CC3 field
    insert into dbo.InternalExtractEUCarFactCDFValue (InternalExtractID, CarCartID, CarCartLogID, 
        BookingTypeID, BookingSystemID, CustomDataFieldID, CustomDataFieldValue)
    select
        a.InternalExtractID,
        a.CarCartID,
        a.CarCartLogID,
        a.BookingTypeID,
        a.BookingSystemID,
        f.CustomDataFieldID,
        ltrim(rtrim(c.CC3_LABEL_))
    from dbo.InternalExtractEUCarFactCDETxt a 
        inner join 
        dbo.InternalExtractEUCarFact b on a.InternalExtractID = b.InternalExtractID 
            and a.CarCartID = b.CarCartID
            and a.CarCartLogID = b.CarCartLogID
            and a.BookingTypeID = b.BookingTypeID 
            and a.BookingSystemID = b.BookingSystemID
        inner join 
        dbo.TRAVELLERS c on b.MetaDossierID = c.MD_CODE and b.TUIDAccount = c.PER_CODE and b.InternalExtractIDReserve = c.InternalExtractID
        inner join 
        dbo.CustomDataFieldDimCurrent f on b.GroupAccountID = f.GroupAccountID and f.CustomDataFieldID = 3
    where a.InternalExtractID = @pInternalExtractID
        and coalesce(c.CC3_LABEL_,'') <> ''

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Insert CC3 field IEEUCarFactCDFValue)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    --Insert CC4 field
    insert into dbo.InternalExtractEUCarFactCDFValue (InternalExtractID, CarCartID, CarCartLogID, 
        BookingTypeID, BookingSystemID, CustomDataFieldID, CustomDataFieldValue)
    select
        a.InternalExtractID,
        a.CarCartID,
        a.CarCartLogID,
        a.BookingTypeID,
        a.BookingSystemID,
        f.CustomDataFieldID,
        ltrim(rtrim(c.CC4_LABEL_))
    from dbo.InternalExtractEUCarFactCDETxt a 
        inner join 
        dbo.InternalExtractEUCarFact b on a.InternalExtractID = b.InternalExtractID 
            and a.CarCartID = b.CarCartID
            and a.CarCartLogID = b.CarCartLogID
            and a.BookingTypeID = b.BookingTypeID 
            and a.BookingSystemID = b.BookingSystemID
        inner join 
        dbo.TRAVELLERS c on b.MetaDossierID = c.MD_CODE and b.TUIDAccount = c.PER_CODE and b.InternalExtractIDReserve = c.InternalExtractID
        inner join 
        dbo.CustomDataFieldDimCurrent f on b.GroupAccountID = f.GroupAccountID and f.CustomDataFieldID = 4
    where a.InternalExtractID = @pInternalExtractID
        and coalesce(c.CC4_LABEL_,'') <> ''

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Insert CC4 field IEEUCarFactCDFValue)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    --Insert CC5 field
    insert into dbo.InternalExtractEUCarFactCDFValue (InternalExtractID, CarCartID, CarCartLogID, 
        BookingTypeID, BookingSystemID, CustomDataFieldID, CustomDataFieldValue)
    select
        a.InternalExtractID,
        a.CarCartID,
        a.CarCartLogID,
        a.BookingTypeID,
        a.BookingSystemID,
        f.CustomDataFieldID,
        left(ltrim(rtrim(c.CC5_LABEL_)), 60)
    from dbo.InternalExtractEUCarFactCDETxt a 
        inner join 
        dbo.InternalExtractEUCarFact b on a.InternalExtractID = b.InternalExtractID 
            and a.CarCartID = b.CarCartID
            and a.CarCartLogID = b.CarCartLogID
            and a.BookingTypeID = b.BookingTypeID 
            and a.BookingSystemID = b.BookingSystemID
        inner join 
        dbo.TRAVELLERS c on b.MetaDossierID = c.MD_CODE and b.TUIDAccount = c.PER_CODE and b.InternalExtractIDReserve = c.InternalExtractID
        inner join 
        dbo.CustomDataFieldDimCurrent f on b.GroupAccountID = f.GroupAccountID and f.CustomDataFieldID = 5
    where a.InternalExtractID = @pInternalExtractID
        and coalesce(c.CC5_LABEL_,'') <> ''

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Insert CC5 field IEEUCarFactCDFValue)'
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

exec dbo.InternalExtractEUCarFactCDETxtLoad
    @pInternalExtractID = 16407 

select *
from CustomDataFieldDimCurrent

select *
from dbo.InternalExtractEUCarFactCDETxt

select *
from dbo.InternalExtractEUCarFactCDFValue

*/