if object_id('dbo.InternalExtractEUAirFactCDFValueLoad') is null begin
    print 'Creating stored procedure InternalExtractEUAirFactCDFValueLoad (placeholder)'
    execute('create procedure dbo.InternalExtractEUAirFactCDFValueLoad as return 0')
end
go

print 'Altering stored procedure InternalExtractEUAirFactCDFValueLoad'
go

alter procedure dbo.InternalExtractEUAirFactCDFValueLoad 
    @pInternalExtractID int
with recompile
as

/*
*********************************************************************
Copyright (C) 2014-2016 Expedia, Inc. All rights reserved.

Description:
    Inserts/Updates InternalExtractEUAirFactCDFValue with new and modified
        data

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2014-07-23  JaredKo         Created.
    2015-04-22  JaredKo         EGE-83933 - Lookup replacement freetext
                                value on 'Ordre de Mission'
	2016-05-21  jappleberry     Added max timestamp to selection for load of #Nav_Customer
                                to assure there are no duplicate key volations
*********************************************************************
*/

set nocount on
set ansi_warnings off
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

-- SP specific constants and variables
declare @CcDefLabel varchar(30), -- Default label when lookup is null
        @FtDefLabel varchar(30)  -- Default label when lookup is null

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

select   -- SP specific constants and variables
    @CcDefLabel = 'Cost Center', -- for CustomDataElementTxt
    @FtDefLabel = 'Free Text'   -- for CustomDataElementTxt

---------------------------------------------------------------------
-- Validation
---------------------------------------------------------------------
-- None, done by caller           
---------------------------------------------------------------------
-- Processing
---------------------------------------------------------------------

-- ToDo: Code Review Commment. No need for transactions as this is re-runnable without error.
--       The first statement (UPDATE) will just replace the original CustomDataElementTxt value.
--       The second statement (INSERT) would fail the proc and, therefore, have nothing to roll back.

--select @TrancountSave = @@Trancount
--if (@TrancountSave = 0) begin transaction @SavePointName
--else                    save  transaction @SavePointName
--select @TranStartedBool = @TRUE


-- Duplicate Customer in Navision. Need a single customer based on RowVersion instead of ExtractID
-- select cast(efc.No_ as varbinary(max)), *
-- from [dbo].[EGENCIA FRANCE$Customer] efc
-- where efc.No_ like '%821861%'

create table #Nav_Customer (No_ varchar(20),
                            TravelProductID int, 
                            InternalExtractID int, 
                            Label_for_Analytical_Code_2 varchar(30), 
                            Label_for_Analytical_Code_3 varchar(30), 
                            Label_for_Analytical_Code_4 varchar(30), 
                            Label_for_Analytical_Code_5 varchar(30),
                            primary key (No_, TravelProductID)
                            )

insert #Nav_Customer
    select
        nc.No_,
        nc.TravelProductID,
        nc.InternalExtractID,
        nc.Label_for_Analytical_Code_2,
        nc.Label_for_Analytical_Code_3,
        nc.Label_for_Analytical_Code_4,
        nc.Label_for_Analytical_Code_5
        from dbo.Nav_Customer nc
        join (select
                nc2.No_,
                nc2.TravelProductID,
                max(nc2.InternalExtractID)  as InternalExtractID,
                max(nc2.Last_Date_Modified) as Last_Date_Modified,
				max(nc2.timestamp) as timestamp
                from dbo.Nav_Customer nc2
                where exists (select
                            * 
                            from dbo.Nav_Sales_Invoice_Line l2
                            where nc2.TravelProductID = l2.TravelProductID and
                                  nc2.No_ = l2.Sell_to_Customer_No_ and
                                  l2.InternalExtractID = @pInternalExtractID
                                ) or
                    exists (select
                            * 
                            from dbo.Nav_Sales_Cr_Memo_Line l2
                            where nc2.TravelProductID = l2.TravelProductID and
                                  nc2.No_ = l2.Sell_to_Customer_No_ and
                                  l2.InternalExtractID = @pInternalExtractID
                                )
                group by nc2.No_,
                         nc2.TravelProductID) ncSubQ on
            nc.No_ = ncSubQ.No_ and
            nc.TravelProductID = ncSubQ.TravelProductID and
            nc.InternalExtractID = ncSubQ.InternalExtractID and
            nc.Last_Date_Modified = ncSubQ.Last_Date_Modified and
			nc.timestamp = ncSubQ.timestamp

-- Find freetext fields for 'BDC_DATA', 'BDC2_DATA', 'BDCLIST_DATA'. If QUESTION_NAVISION = 'Ordre de Mission',
-- try other sources.
;with ft as(
    select a.RecordKey,
           a.TravelProductID,
           a.MetaDossierID,
           g.Comcode,
           ft.FLD_KEY, 
           ft.QUESTION_NAVISION, 
           ft.FLD_VALUE
      from dbo.InternalExtractEUAirFact a
           join dbo.GroupAccountDim g on 
                g.GroupAccountID = a.GroupAccountID and 
                g.TravelProductID = a.TravelProductID
          cross apply
                (select top 1 m.QUESTION_NAVISION, m.FLD_VALUE, m.FLD_KEY
                   from dbo.Nav_IMPORT_METAID_FIELD_VALUE m
                  where m.METAID = a.MetaDossierID and
                        m.TravelProductID = a.TravelProductID and
                        m.FLD_KEY = 'BDC_DATA'
                        order by m.InternalExtractID desc
                 union all
                 select top 1 m.QUESTION_NAVISION, m.FLD_VALUE, m.FLD_KEY
                   from dbo.Nav_IMPORT_METAID_FIELD_VALUE m
                  where m.METAID = a.MetaDossierID and
                        m.TravelProductID = a.TravelProductID and
                        m.FLD_KEY = 'BDC2_DATA'
                        order by m.InternalExtractID desc
                 union all
                 select top 1 m.QUESTION_NAVISION, m.FLD_VALUE, m.FLD_KEY
                   from dbo.Nav_IMPORT_METAID_FIELD_VALUE m
                  where m.METAID = a.MetaDossierID and
                        m.TravelProductID = a.TravelProductID and
                        m.FLD_KEY = 'BDCLIST_DATA'
                        order by m.InternalExtractID desc
                        ) ft
        where a.InternalExtractID = @pInternalExtractID
    )
select *
into #FreeText
from ft

-- For Rows where QUESTION_NAVISION = 'Ordre de Mission', 
-- look for earlier entries in the same table
update a
   set a.QUESTION_NAVISION = b.QUESTION_NAVISION,
       a.FLD_VALUE = b.FLD_VALUE
  from #FreeText a
       cross apply (select top 1 m.QUESTION_NAVISION, m.FLD_VALUE, m.FLD_KEY
                      from dbo.Nav_IMPORT_METAID_FIELD_VALUE m
                     where m.METAID = a.MetaDossierID and
                           m.TravelProductID = a.TravelProductID and
                           m.FLD_KEY = a.FLD_KEY and
                           m.QUESTION_NAVISION <> 'Ordre de Mission'
                           order by m.InternalExtractID desc
                           ) b
 where a.QUESTION_NAVISION = 'Ordre de Mission'

-- For Rows where (still) QUESTION_NAVISION = 'Ordre de Mission', 
-- look try to replace the question with a value in dbo.Nav_FIELD_METAID
update a
   set a.QUESTION_NAVISION = b.QUESTION_NAVISION
  from #FreeText a
       cross apply (select top 1 m.QUESTION_NAVISION, m.FLD_KEY
                      from dbo.Nav_FIELD_METAID m
                     where m.TravelProductID = a.TravelProductID and
                           m.COM_CODE = cast(a.Comcode as varchar(20)) and
                           m.FLD_KEY = a.FLD_KEY and
                           m.QUESTION_NAVISION <> 'Ordre de Mission'
                           order by m.InternalExtractID desc
                           ) b
 where a.QUESTION_NAVISION = 'Ordre de Mission'

 -- Get the most-recent entries from Nav_Customer


 -- Update dbo.InternalExtractEUAirFact.CustomDataElementTxt
 -- CountryLabel CTE: Default value is 'Cost Center' except for Germany and France.
;with CountryLabel as(
    select 'FR[CI][0-9]%' as CountryPrefix, 'Centre de Coût' as CcDefLabel union all
    select 'GE[CI][0-9]%' as CountryPrefix, 'Kostenstelle' as CcDefLabel
    -- else 'Cost Center'
    ),
/* with */ cte as (select a.RecordKey,
                          a.CustomDataElementTxt,
                          -- If Label_for_Analytical_Code is blank/null but Analytical_Code is not, insert a default label such as 'Cost Center 2'
                          '<ID2&&' + isnull(nullif(replace(replace(c.Label_for_Analytical_Code_2, '<', ''), '&&', ''), ''), isnull(d.CcDefLabel, @CcDefLabel) + ' 2') + '&&' + nullif(replace(replace(b.Analytical_Code_2, '<', ''), '&&', ''), '') ID2,
                          '<ID3&&' + isnull(nullif(replace(replace(c.Label_for_Analytical_Code_3, '<', ''), '&&', ''), ''), isnull(d.CcDefLabel, @CcDefLabel) + ' 3') + '&&' + nullif(replace(replace(b.Analytical_Code_3, '<', ''), '&&', ''), '') ID3,
                          '<ID4&&' + isnull(nullif(replace(replace(c.Label_for_Analytical_Code_4, '<', ''), '&&', ''), ''), isnull(d.CcDefLabel, @CcDefLabel) + ' 4') + '&&' + nullif(replace(replace(b.Analytical_Code_4, '<', ''), '&&', ''), '') ID4,
                          '<ID5&&' + isnull(nullif(replace(replace(c.Label_for_Analytical_Code_5, '<', ''), '&&', ''), ''), isnull(d.CcDefLabel, @CcDefLabel) + ' 5') + '&&' + nullif(replace(replace(b.Analytical_Code_5, '<', ''), '&&', ''), '') ID5,
                          (select top 1 '<FT1&&' + isnull(nullif(replace(replace(m.QUESTION_NAVISION, '<', ''), '&&', ''), ''), @FtDefLabel + ' 1') + '&&' + nullif(replace(replace(m.FLD_VALUE, '<', ''), '&&', ''), '')
                                  from dbo.#FreeText m
                              where m.MetaDossierID = b.Meta_ID and
                                  m.TravelProductID = a.TravelProductID and
                                  m.FLD_KEY = 'BDC_DATA') FT1,
                          (select top 1 '<FT2&&' + isnull(nullif(replace(replace(m.QUESTION_NAVISION, '<', ''), '&&', ''), ''), @FtDefLabel + ' 2') + '&&' + nullif(replace(replace(m.FLD_VALUE, '<', ''), '&&', ''), '')
                                  from dbo.#FreeText m
                              where m.MetaDossierID = b.Meta_ID and
                                  m.TravelProductID = a.TravelProductID and
                                  m.FLD_KEY = 'BDC2_DATA') FT2,
                          (select top 1 '<FT3&&' + isnull(nullif(replace(replace(m.QUESTION_NAVISION, '<', ''), '&&', ''), ''), @FtDefLabel + ' 3') + '&&' + nullif(replace(replace(m.FLD_VALUE, '<', ''), '&&', ''), '')
                                  from dbo.#FreeText m
                              where m.MetaDossierID = b.Meta_ID and
                                  m.TravelProductID = a.TravelProductID and
                                  m.FLD_KEY = 'BDCLIST_DATA') FT3
                          from dbo.InternalExtractEUAirFact a 
                              cross apply(
                                select l2.Sell_to_Customer_No_, l2.Document_No_, l2.Meta_ID, l2.Analytical_Code_1, l2.Analytical_Code_2, l2.Analytical_Code_3, l2.Analytical_Code_4, l2.Analytical_Code_5
                                  from dbo.Nav_Sales_Invoice_Line l2
                                 where l2.InternalExtractID = @pInternalExtractID and 
                                       l2.TravelProductID = a.TravelProductID and
                                       l2.Document_No_ = a.SalesDocumentCode and
                                       l2.Line_No_ = a.SalesDocumentLineNbr
                                 union all
                                select l2.Sell_to_Customer_No_, l2.Document_No_, l2.Meta_ID, l2.Analytical_Code_1, l2.Analytical_Code_2, l2.Analytical_Code_3, l2.Analytical_Code_4, l2.Analytical_Code_5
                                  from dbo.Nav_Sales_Cr_Memo_Line l2
                                 where l2.InternalExtractID = @pInternalExtractID and 
                                       l2.TravelProductID = a.TravelProductID and
                                       l2.Document_No_ = a.SalesDocumentCode and
                                       l2.Line_No_ = a.SalesDocumentLineNbr
                              ) b
                              inner join
                              dbo.#Nav_Customer c on 
                                    c.No_ = b.Sell_to_Customer_No_ and
                                    c.TravelProductID = a.TravelProductID
                              left join
                              CountryLabel d on b.Document_No_ like d.CountryPrefix
                              where a.InternalExtractID = @pInternalExtractID
                       )
    -- Use STUFF to remove the first '<'
    -- Remove Commas
    -- Any NULLs should be empty string
    update cte
    set CustomDataElementTxt = isnull(replace(isnull(stuff(isnull(ID2, '') + isnull(ID3, '') + isnull(ID4, '') + isnull(ID5, '') + isnull(FT1, '') + isnull(FT2, '') + isnull(FT3, ''), 1, 1, ''),''),',',' '),'')

-- ToDo: Several parts added while coding the Unpivot section. Remove after thorough data compare.
-- Insert InternalExtractEUAirFactCDFValue (Unpivot)
;with Facts as(
    select f.RecordKey, f.MetaDossierID, f.SalesDocumentCode, f.SalesDocumentLineNbr, f.TravelProductID, f.BookingTypeID
      from dbo.InternalExtractEUAirFact f
     where f.InternalExtractID = @pInternalExtractID
    ),
IDRows as(
    select a.RecordKey,
           a.SalesDocumentCode,
           a.SalesDocumentLineNbr,
           a.BookingTypeID,
           d.CustomDataFieldID,
           d.CustomDataFieldValue
      from Facts a
           inner join
           dbo.Nav_Sales_Invoice_Line b on
               a.SalesDocumentCode = b.Document_No_ and
               a.SalesDocumentLineNbr = b.Line_No_ and
               a.TravelProductID = b.TravelProductID
           inner join
           dbo.#Nav_Customer c on 
               c.No_ = b.Sell_to_Customer_No_ and
               c.TravelProductID = b.TravelProductID
           cross apply(
               select -1 as CustomDataFieldID, '' as CustomDataFieldValue where 0 = 1 union all -- Column Names
               select 2, b.Analytical_Code_2 where nullif(b.Analytical_Code_2,'') is not null union all
               select 3, b.Analytical_Code_3 where nullif(b.Analytical_Code_3,'') is not null union all
               select 4, b.Analytical_Code_4 where nullif(b.Analytical_Code_4,'') is not null union all
               select 5, b.Analytical_Code_5 where nullif(b.Analytical_Code_5,'') is not null
                       ) d
     where b.InternalExtractID = @pInternalExtractID
     union all
    select a.RecordKey,
           a.SalesDocumentCode,
           a.SalesDocumentLineNbr,
           a.BookingTypeID,
           d.CustomDataFieldID,
           --d.CustomDataFieldIDTxt, 
           --d.CustomDataFieldName,
           d.CustomDataFieldValue
      from Facts a
           inner join
           dbo.Nav_Sales_Cr_Memo_Line b on
               a.SalesDocumentCode = b.Document_No_ and
               a.SalesDocumentLineNbr = b.Line_No_ and
               a.TravelProductID = b.TravelProductID
           inner join
           dbo.#Nav_Customer c on 
               c.No_ = b.Sell_to_Customer_No_ and
               c.TravelProductID = b.TravelProductID
           cross apply(
               select -1 as CustomDataFieldID, '' as CustomDataFieldValue where 0 = 1 union all -- Column Names
               select 2, b.Analytical_Code_2 where nullif(b.Analytical_Code_2,'') is not null union all
               select 3, b.Analytical_Code_3 where nullif(b.Analytical_Code_3,'') is not null union all
               select 4, b.Analytical_Code_4 where nullif(b.Analytical_Code_4,'') is not null union all
               select 5, b.Analytical_Code_5 where nullif(b.Analytical_Code_5,'') is not null
                    ) d
     where b.InternalExtractID = @pInternalExtractID
        ),

-- FTs Pulls CDF rows from Nav_IMPORT_METAID_FIELD_VALUE - including entries from previous extracts
-- FTRows filters out the entries from FTs so that only the most-recent entry is inserted.
FTs as (
    select f.RecordKey, f.SalesDocumentCode, f.SalesDocumentLineNbr, f.BookingTypeID,
           v.FLD_KEY, 
           v.QUESTION_NAVISION, 
           v.FLD_VALUE 
           from Facts f
                inner join 
                #FreeText v on
                    v.MetaDossierID = f.MetaDossierID and
                    v.TravelProductID = f.TravelProductID
            where v.FLD_KEY in ('BDC_DATA', 'BDC2_DATA', 'BDCLIST_DATA')
    ),
FTRows as(
    select RecordKey, SalesDocumentCode, SalesDocumentLineNbr, BookingTypeID,
           case FLD_KEY 
                when 'BDC_DATA' then 11
                when 'BDC2_DATA' then 12
                when 'BDCLIST_DATA' then 13
            end as CustomDataFieldID,
            replace(FLD_VALUE,',',' ') as CustomDataFieldValue
      from FTs 
    )

-- CTEs done. Now insert:
insert into dbo.InternalExtractEUAirFactCDFValue( InternalExtractID, RecordKey, BookingTypeID, CustomDataFieldID, CustomDataFieldValue ) 
-- Nav_Sales_Invoice_Line
select @pInternalExtractID,
       RecordKey,
       BookingTypeID,
       CustomDataFieldID,
       CustomDataFieldValue
from IDRows
union
-- Nav_IMPORT_METAID_FIELD_VALUE
select @pInternalExtractID,
       RecordKey,
       BookingTypeID,
       CustomDataFieldID,
       CustomDataFieldValue
from FTRows
union
-- Include GroupAccountDepartmentName as CustomDataFieldID = 0
select 
    @pInternalExtractID,
    RecordKey,
    BookingTypeID,
    0 as CustomDataFieldID,
    d.GroupAccountDepartmentName        
from dbo.InternalExtractEUAirFact f 
join dbo.GroupAccountDepartmentDim d
    on f.GroupAccountDepartmentID = d.GroupAccountDepartmentID
where f.InternalExtractID = @pInternalExtractID
    and d.CustomerSystemID = 2


select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (insert InternalExtractEUAirFactCDFValue)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

--if ((@TrancountSave = 0) and (@TranStartedBool = @TRUE))
--    commit transaction @SavePointName

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
        select RecordKey, CustomDataElementTxt from InternalExtractEUAirFact  where InternalExtractID = 30622
        select * from InternalExtractEUAirFactCDFValue where InternalExtractID = 30622
        delete dbo.InternalExtractEUAirFactCDFValue where InternalExtractID = 30622
        exec InternalExtractEUAirFactCDFValueLoad         @pInternalExtractID = 30622
        select RecordKey, CustomDataElementTxt from InternalExtractEUAirFact  where InternalExtractID = 30622
        select * from InternalExtractEUAirFactCDFValue where InternalExtractID = 30622
    rollback tran
*/
/*
begin tran
	declare @InternalExtractID int = 37072
    select RecordKey, CustomDataElementTxt from InternalExtractEUAirFact  where InternalExtractID = @InternalExtractID
    select * from InternalExtractEUAirFactCDFValue where InternalExtractID = @InternalExtractID
    delete dbo.InternalExtractEUAirFactCDFValue where InternalExtractID = @InternalExtractID
    exec InternalExtractEUAirFactCDFValueLoad         @pInternalExtractID = @InternalExtractID
    select RecordKey, CustomDataElementTxt from InternalExtractEUAirFact  where InternalExtractID = @InternalExtractID
    select * from InternalExtractEUAirFactCDFValue where InternalExtractID = @InternalExtractID
rollback tran
*/