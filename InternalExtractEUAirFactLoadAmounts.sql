if object_id('dbo.InternalExtractEUAirFactLoadAmounts') is null
begin
    print 'Creating stored procedure InternalExtractEUAirFactLoadAmounts (placeholder)'
    execute ('create procedure dbo.InternalExtractEUAirFactLoadAmounts as return 0')
end
go

print 'Altering stored procedure InternalExtractEUAirFactLoadAmounts'
go

alter procedure dbo.InternalExtractEUAirFactLoadAmounts @pInternalExtractID int as

/*
*********************************************************************
Copyright (C) 2014-2018 Expedia, Inc. All rights reserved.

Description:
     Updates InternalExtractEUAirFact with amount information

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2014-06-16  JaredKo         Created.
    2014-10-13  JaredKo         jira.EGE-73190 - EU Air: SegmentValue and FlightValue 
                                    null in Segment and Flight for some billback air
    2015-03-23  JaredKo         jira.EGE-80645 - Eu Air: Exchange tickets are not 
                                    being processed. Neither is the exchange penalty.
    2015-06-22  rkanrati        EGE-87758 - Exchange Penalty linakge for Q1 2014 where TicketCode 
                                    and TicketPrior was NOT available in Navision 
    2015-10-16  JaredKo         EGE-91086 - Performance improvements after additional Navision data
                                    added and/or PK column changes
    2015-10-20  JaredKo         EGE-97686 - Add Business_Category 7 for Exchange Penalties
	2015-11-13  pbressan        EGE-98897 - EU Air Savings: Added OB_Fees_Amount
    2015-11-30  pbressan        EU Air Savings Logic
    2015-12-03  JaredKo         EGE-97690 - Performance Optimizations
    2016-01-25  jappleberry     Added order by segment_rank to for xml to guarantee order
    2017-07-27  pbressan        EGE-160162 Update of the matching logic for checkout fusion
    2017-08-31  manzeno         EGE-163250 TPC_FIX: Change the RS2 Air Processing Source Code to not use Initial Booking Price
    2017-09-08  pbressan        EGE-155376 Nullify PolicyReasonCodeID when negating missed savings
    2017-10-05  pbressan        Rolling back EGE-155376 Nullify PolicyReasonCodeID when negating missed savings
    2018-05-22  japplberry      To fix '-' at end of MetaDossierID added replace(a.MetaDossierID,'-','') MetaDossierID,
    2018-05-28  pbressan        Jira.EGE-195421 Add logic for TicketAmtTaxFee
    2018-06-05  pbressan        Jira.EGE-199218 Add logic for markup/FSF related columns
*********************************************************************
*/

set nocount on

---------------------------------------------------------------------
-- Declarations
---------------------------------------------------------------------
declare     -- Standard constants and variables
        @FALSE tinyint,
        @TRUE tinyint,
        @RC_FAILURE int,
        @RC_SUCCESS int,
        @Current_Timestamp datetime,
        @Error int,
        @ErrorCode int,
        @ExitCode int,
        @ProcedureName sysname,
        @RC int,            -- Return code from called SP
        @Rowcount int,
        @SavePointName varchar(32),
        @TranStartedBool tinyint,
        @TrancountSave int,
        @NonNumericString varchar(10),
        @savings_activation_date datetime

declare   -- Error message constants
        @ERRUNEXPECTED int,
        @ERRPARAMETER int,
        @MsgParm1 varchar(100),
        @MsgParm2 varchar(100),
        @MsgParm3 varchar(100)

declare @BookingTypeIDPurchase tinyint,
        @BookingTypeIDReserve tinyint,
        @BookingTypeIDVoid tinyint,
        @BookingTypeIDExchange tinyint,
        @BookingTypeIDRefund tinyint,
        @BookingTypeIDPRefund tinyint,
        @BookingTypeIDUTT tinyint,
        @ExchangeRateNull money

declare @FactRecordStatusID_AEP tinyint
        
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
    @TranStartedBool                = @FALSE,
    -- @NonNumericString is used to find integer values through exclusion
    -- Example: SELECT @ID = ID FROM tbl WHERE ID not like @NonNumericString
    -- Translated: Rows WHERE ID does NOT include values that are NOT 0-9.
    @NonNumericString               = '%[^0-9-]%',
    @savings_activation_date        = '20160101'

select   -- Error message constants
    @ERRUNEXPECTED                  = 200104,
    @ERRPARAMETER                   = 200110


select @BookingTypeIDPurchase  = 1,
       @BookingTypeIDReserve   = 3,
       @BookingTypeIDVoid      = 4,
       @BookingTypeIDExchange  = 5,
       @BookingTypeIDRefund    = 7,
       @BookingTypeIDPRefund   = 8,
       @BookingTypeIDUTT       = 9,
       @ExchangeRateNull       = 0.0
       
select @FactRecordStatusID_AEP = 11    -- Air Exchange Penalty
---------------------------------------------------------------------
-- Validation
---------------------------------------------------------------------
-- None, done by caller
---------------------------------------------------------------------
-- Processing
---------------------------------------------------------------------

select @TrancountSave = @@Trancount
if (@TrancountSave = 0)
    begin transaction @SavePointName
    else
        save transaction @SavePointName
    select @TranStartedBool = @TRUE

select f.SalesDocumentCode,
       f.SalesDocumentLineNbr,
       f.BookingTypeID,
       f.RecordKey,
       f.TravelProductID,
       f.FactRecordStatusID,
       f.TUIDTraveler,
       f.MetaDossierID,
       f.TicketCodePrior,
       f.InvoiceDate,
       f.CurrencyCode,
       f.TicketAmt,
       f.TicketAmtBase,
       f.TicketAmtTax,
       f.TicketAmtTaxVAT,
       f.TicketAmtTaxFee,
       f.TicketAmtExchangePenalty,
       f.TicketAmtCommission,
       f.TicketAmtMarkup,
       f.TicketAmtMarkupGds,
       f.TicketAmtMarkupPub,
       f.TicketAmtGross,
       f.TicketAmtOBFees,
       f.LowFareChosenBool,
       f.SavingsReasonCode,
       f.FareAmtLowestInPolicy,
       f.AirlineCodeLowestInPolicy,
       f.ClassOfServiceCodeLowestInPolicy,
       f.CabinClassIDLowestInPolicy,
       f.AirFareTypeIDLowestInPolicy,
       f.AirFareBasisCodeLowestInPolicy,
       f.FareAmtPublished,
       f.AirFareBasisCodePublished,
       f.ClassOfServiceCodePublished,
       f.CabinClassIDPublished,
       f.FareAmtNegotiated,
       f.AirFareBasisCodeNegotiated,
       f.CabinClassIDNegotiated,
       f.PNRCode,
       f.TravelDateStart,
       f.TravelDateEnd,
       f.SegmentCnt,
       f.AirLineCode,
       f.AirFareBasisCode,
       f.CabinClassID,
       f.AirFareTypeID,
       f.ClassOfServiceCode,
       f.FSFAmtSavings
       into #InternalExtractEUAirFact
       from dbo.InternalExtractEUAirFact f
            where f.InternalExtractID = @pInternalExtractID and 
                  f.BookingTypeID <> 99

    -- Amounts CTE uses logic from the "Populate_Second_Facts" Kettle transformation.
    ;with Amounts as (select l.Document_No_,
                             l.Line_No_,
                             l.TravelProductID,
                             convert(money, l.Commission_Amount_Without_VAT) as Comissionamount,
                             convert(money, l.Airport_Tax) as Fullothertaxamount,
                             l.OB_Fees_Amount,
                             case
                                 when l.TravelProductID in (60010, 60083, 60035,60013) and
                                     l.Business_sub_category = 12 then convert(money, l.PRIX_PUBLIC)
                                 else convert(money, l.Amount_Including_VAT)
                             end TicketAmt, -- Also TicketAmtGross and TransactionAmount (Kettle)
                             case
                                 when l.TravelProductID in (60010, 60083, 60035,60013) and
                                     l.Business_sub_category = 12 then convert(money, l.PRIX_PUBLIC - l.Airport_Tax)
                                 else convert(money, l.[Amount] - l.Airport_Tax)
                             end TicketAmtBase,
                             case -- ToDo: If BookingTypeID = 5 then NULL
                                 when l.TravelProductID in (60010, 60083, 60035,60013) and
                                     l.Business_sub_category = 12 then convert(money, 0)
                                 else convert(money, l.Amount_Including_VAT - l.[Amount])
                             end TicketAmtTaxVAT,
                             /**Note:The below 3 fileds: Fullmarkupamount,Differentialbasefare & DifferentialVATamount are not used any where in RS2 **/
                             case -- ToDo: If BookingTypeID = 5 then NULL
                                 when l.TravelProductID in (60010, 60083, 60035,60013) and
                                     l.Business_sub_category = 12 and 
                                     PRIX_PUBLIC = 0 then null                                                                           
                                 else case
                                     when PRIX_PUBLIC = 0  then null
                                        else abs(Amount) - abs(PRIX_PUBLIC)
                                 end
                             end as Fullmarkupamount, -- and DifferentialMarkupAmount
                             case -- ToDo: If BookingTypeID = 5 then NULL
                                 when l.TravelProductID in (60010, 60083, 60035,60013) and
                                     l.Business_sub_category = 12 then convert(money, l.PRIX_PUBLIC - l.Airport_Tax)
                                 else convert(money, l.Amount_Including_VAT - l.Airport_Tax)
                             end Differentialbasefare,
                             case -- ToDo: If BookingTypeID = 5 then NULL
                                 when l.TravelProductID in (60010, 60083, 60035,60013) and
                                     l.Business_sub_category = 12 then convert(money, l.Amount_Including_VAT - l.PRIX_PUBLIC)
                                 else convert(money, l.Amount_Including_VAT - l.[Amount])
                             end as DifferentialVATamount,
                             convert(money, null) as TicketAmtExchangePenalty,
                             abs(l.Markup_Total) as Markup,/**This was wrong, it is updated based on Navision ETL**/
                             l.[ID_1] as [FileName],
                             l.[C_R_S],
                             l.[Business_category],
                             l.[Business_sub_category],
                             abs(l.Markup_amadeus) as MarkupGds,
                             abs(l.Markup_ECTE) as MarkupPub
                        from dbo.nav_Sales_Line(@pInternalExtractID) l)
    -- in Populate_Second_Facts, the following columns should be NULL when BookingTypeID = 5 (Exchange):
    -- FullMarkupAmount
    -- DifferentialTicketedFare
    -- DifferentialBaseFare
    -- DifferentialVATAmount
    -- DifferentialOtherTaxAmount
    -- DifferentialMarkupAmount
    select
        *
    into #amounts
    from Amounts

    ------------------------------------------------------------
    -- Anticipating Exchanges Section / SLE (Service Ledger Entry)
    ------------------------------------------------------------
    select
        a.TravelProductID,
        a.SalesDocumentCode,
        a.SalesDocumentLineNbr,
        c.Document_No_ as [SLE_SalesDocumentCode],
        c.Line_No_ as [SLE_SalesDocumentLineNbr],
        c.[Amount_(LCY)_including_VAT] as OriginalAmount,
        c.Gross_Commission as OriginalCommission,
        c.Net_Amount - c.Tax_Amount as OriginalFullBaseFare,
        c.[Amount_(LCY)_including_VAT] - c.Net_Amount as OriginalFullVAT,
        c.Tax_Amount as OriginalTax,
        c.[ID_1] as [FileName],
        c.[C_R_S],
        c.[Business_category],
        c.[Business_sub_category]--,
--        c.[Markup_Total] as OriginalMarkupTotal,
--        c.[Markup_amadeus] as OriginalMarkupGds,
--        c.[Markup_ECTE] as OriginalMarkupPub
    into #sle
    from dbo.#InternalExtractEUAirFact a
        join dbo.Nav_Sales_Invoice_Line l on
            a.TravelProductID = l.TravelProductID and 
            a.SalesDocumentCode = l.Document_No_ and
            a.SalesDocumentLineNbr = l.Line_No_
        cross apply(select top 1 b.* 
                        from dbo.Nav_Service_Ledger_Entry b
--                    outer apply (
--                        select top 1 [Markup_Total], [Markup_amadeus], [Markup_ECTE]
--                        from [dbo].[vNav_Sales_Line]
--                        where TravelProductID = b.TravelProductID
--                        and Document_No_ = b.Document_No_
--                        and Line_No_ = b.Line_No_
--                        order by InternalExtractID desc
--                    ) sil
                    where b.TravelProductID = a.TravelProductID and
                            b.Ticket_No = a.TicketCodePrior and
                            b.Traveller_Code = cast(a.TUIDTraveler as varchar(20)) and
                            b.Service_Group in (1,6) and -- Air / Low Cost
                            b.Entry_type = 3 and
                            b.[Sell_to_Customer_No_] = l.[Sell_to_Customer_No_] and
                            b.Resource_Type = 0 -- Primary
                            order by b.InternalExtractID desc, b.Entry_No_ desc) c
    where a.BookingTypeID = 5 -- Exchange

    ------------------------------------------------------------
    -- TicketAmtTaxFee
    ------------------------------------------------------------

    select a.*
    into #import_air_keys
    from (
        select
            distinct TravelProductID, [FileName], 'Nav_Sales_Line' as [Source]
        from #amounts
        where [C_R_S] <> 'LOWCOST'
        and [FileName] <> ''
        /*
        union all
        select
            distinct TravelProductID, [FileName], 'Nav_Service_Ledger_Entry' as [Source]
        from #sle
        where [C_R_S] <> 'LOWCOST'
        and [FileName] <> ''
        */
    ) a

    select a.*
    into #surcharge
    from (
	    select
            f.TravelProductID
        ,   i.InternalExtractID
	    ,	i.[FileName] as [FileName]
        ,   i.[Tag]
	    ,	convert(money, replace(substring(i.[Text1], charindex('YQ', i.[Text1]) - 9, 8), 'EXEMPT', '')) as [YQ]
	    ,	convert(money, replace(substring(i.[Text1], charindex('YR', i.[Text1]) - 9, 8), 'EXEMPT', '')) as [YR]
        ,   convert(int, 0) as [Count]
        ,   convert(int, 0) as [max_InternalExtractID]
        ,   convert(varchar(50), newid()) as [unique_identifier]
        ,   convert(varchar(50), null) as [max_unique_identifier]
        ,   f.[Source]
	    from [dbo].[Nav_Import_Air] i
	    inner join #import_air_keys f
		    on f.[FileName] = i.[FileName]
            and f.[TravelProductID] = i.[TravelProductID]
	    where i.[Tag] in ('TAX', 'KRF', 'KST')
	    and (i.[Text1] like '%YQ%' or i.[Text1] like '%YR%')
	    and (isnumeric(substring(i.[Text1], charindex('YQ', i.[Text1]) - 9, 8)) = 1
	      or isnumeric(substring(i.[Text1], charindex('YR', i.[Text1]) - 9, 8)) = 1)

        union all

	    select
            f.TravelProductID
        ,   i.InternalExtractID
	    ,	i.[FileName] as [FileName]
        ,   i.[Tag]
	    ,	convert(money, replace(substring(i.[Text1], charindex('YQ', i.[Text1], charindex('YQ', i.[Text1]) + 1) - 9, 8), 'EXEMPT', '')) as [YQ]
	    ,	convert(money, replace(substring(i.[Text1], charindex('YR', i.[Text1], charindex('YR', i.[Text1]) + 1) - 9, 8), 'EXEMPT', '')) as [YR]
        ,   convert(int, 0) as [Count]
        ,   convert(int, 0) as [max_InternalExtractID]
        ,   convert(varchar(50), newid()) as [unique_identifier]
        ,   convert(varchar(50), null) as [max_unique_identifier]
        ,   f.[Source]
	    from [dbo].[Nav_Import_Air] i
	    inner join #import_air_keys f
		    on f.[FileName] = i.[FileName]
            and f.[TravelProductID] = i.[TravelProductID]
	    where i.[Tag] in ('TAX', 'KRF', 'KST')
	    and (i.[Text1] like '%YQ%' or i.[Text1] like '%YR%')
	    and (isnumeric(substring(i.[Text1], charindex('YQ', i.[Text1], charindex('YQ', i.[Text1]) + 1) - 9, 8)) = 1
	      or isnumeric(substring(i.[Text1], charindex('YR', i.[Text1], charindex('YR', i.[Text1]) + 1) - 9, 8)) = 1)
    ) a

    create index ix1 on #surcharge ([FileName], [TravelProductID], [Tag], [InternalExtractID], [Source]) include ([YQ], [YR])

    -- dedup #surcharge - part 1
    update s set
        s.max_InternalExtractID = dup.max_InternalExtractID
    ,   s.[Count] = dup.[Count]
    from #surcharge s
    inner join (
        select
            [TravelProductID]
        ,   [FileName]
        ,   [Tag]
        ,   [Source]
        ,   [YQ]
        ,   [YR]
        ,   count(1) as [Count]
        ,   max(InternalExtractID) as max_InternalExtractID
        from #surcharge
        group by [TravelProductID], [FileName], [Source], [Tag], [YQ], [YR]
        having count(1) > 1
        ) dup
        on dup.[TravelProductID] = s.[TravelProductID]
        and dup.[FileName] = s.[FileName]
        and dup.[Tag] = s.[Tag]
        and dup.[YQ] = s.[YQ]
        and dup.[YR] = s.[YR]
        and dup.[Source] = s.[Source]

    delete from #surcharge where [Count] <> 0 and InternalExtractID <> max_InternalExtractID

    -- dedup #surcharge - part 2
    update s set
        s.[YQ] = dup.[YQ]
    ,   s.[YR] = dup.[YR]
    ,   s.[Count] = -1
    ,   s.[max_unique_identifier] = dup.[max_unique_identifier]
    from #surcharge s
    inner join (
        select
            [TravelProductID], [FileName], [Tag], [InternalExtractID], [Source], count(1) as [Count]
        ,   sum([YQ]) as [YQ]
        ,   sum([YR]) as [YR]
        ,   max([unique_identifier]) as [max_unique_identifier]
        from #surcharge
        group by [TravelProductID], [FileName], [Tag], [InternalExtractID], [Source]
        having count(1) > 1
        ) dup 
        on dup.[TravelProductID] = s.[TravelProductID]
        and dup.[FileName] = s.[FileName]
        and dup.[Tag] = s.[Tag]
        and dup.[InternalExtractID] = s.[InternalExtractID]
        and dup.[Source] = s.[Source]

    delete from #surcharge where [Count] = -1 and [unique_identifier] <> [max_unique_identifier]

    ------------------------------------------------------------
    -- FSFAmtSavings
    ------------------------------------------------------------

    -- #employees
    select distinct
        otc.ID * -1 as travel_consultant_id,
        alm.LAST_NAME + ' ' + alm.FIRST_NAME as fld_value,
        alm.[LOGIN]
    into #employees
    from dbo.[OPST_TRAVEL_CONSULTANT] otc
    inner join dbo.[AGENT_LOGIN_MAPPING] alm
        on alm.ID = otc.ECT_USER_ID

    select
        B.*
    ,   coalesce(e.[travel_consultant_id], -500000) as [travel_consultant_id]
    into #fsfamtsavings
    from (
	    select
		    A.*
	       ,row_number() over(partition by A.[FileName] order by A.[rank] asc) 'RowNum'
	    from (
		    select
			    pnr.[FileName] as [FileName]
               ,pnr.[TravelProductID]
		       ,case when (ltrim(rtrim(pnr.[Text1])) like 'RM%*%IFD %')   then 1
		        --   when (ltrim(rtrim(pnr.[Text1])) like 'RM%*%TPM %')   then 2
				     when (ltrim(rtrim(pnr.[Text1])) like 'RM%*%AO %')    then 3
				     when (ltrim(rtrim(pnr.[Text1])) like 'RM%*%MICE %')  then 4
				     else 100
			    end 'rank'
		       ,case when (ltrim(rtrim(pnr.[Text1])) like 'RM%*%IFD %')   then 'ifd'
		        --   when (ltrim(rtrim(pnr.[Text1])) like 'RM%*%TPM %')   then 'tpm'
				     when (ltrim(rtrim(pnr.[Text1])) like 'RM%*%AO %')    then 'ao'
				     when (ltrim(rtrim(pnr.[Text1])) like 'RM%*%MICE %')  then 'mice'
				     else 'unknown'
			    end 'type'
		       ,replace(pnr.[Text1], 'RM*', '') as [Remark Text]
		       ,case when (ltrim(rtrim(pnr.[Text1])) like 'RM%*%IFD %')  then 'IFD:'  + case when patindex('%[0-9][0-9,.]%', pnr.[Text1]) > 0 then lower(reverse(left(reverse(rtrim(substring(pnr.[Text1], 1, patindex('%[1-9]%', pnr.[Text1]) - 1))), charindex(space(1), reverse(rtrim(substring(pnr.[Text1], 1, patindex('%[1-9]%', pnr.[Text1]) - 1)))) - 1))) else lower(reverse(left(reverse(rtrim(pnr.[Text1])), charindex(space(1), reverse(rtrim(pnr.[Text1]))) - 1))) end
                --   when (ltrim(rtrim(pnr.[Text1])) like 'RM%*%TPM %')  then 'TPM:'  + case when patindex('%[0-9][0-9,.]%', pnr.[Text1]) > 0 then lower(reverse(left(reverse(rtrim(substring(pnr.[Text1], 1, patindex('%[1-9]%', pnr.[Text1]) - 1))), charindex(space(1), reverse(rtrim(substring(pnr.[Text1], 1, patindex('%[1-9]%', pnr.[Text1]) - 1)))) - 1))) else lower(reverse(left(reverse(rtrim(pnr.[Text1])), charindex(space(1), reverse(rtrim(pnr.[Text1]))) - 1))) end
				     when (ltrim(rtrim(pnr.[Text1])) like 'RM%*%AO %')   then 'AO:'   + lower(reverse(left(reverse(rtrim(pnr.[Text1])), charindex(space(1), reverse(rtrim(pnr.[Text1]))) - 1)))
				     when (ltrim(rtrim(pnr.[Text1])) like 'RM%*%MICE %') then 'MICE:' + lower(reverse(left(reverse(rtrim(pnr.[Text1])), charindex(space(1), reverse(rtrim(pnr.[Text1]))) - 1)))
				     else null
			    end 'formated'
               /*  TopUpMarkup Feature of IR
		       ,case when (ltrim(rtrim(pnr.[Text1])) like 'RM%*%TPM %')
				    then
					    case isnumeric(case when patindex('%[0-9];%', pnr.[Text1]) > 0 then replace(substring(substring(pnr.[Text1], patindex('%[0-9]%', pnr.[Text1]), 100), 1, patindex('%[^0-9.,]%', substring(pnr.[Text1], patindex('%[0-9]%', pnr.[Text1]), 100)) - 1), ',', '.') else replace(ltrim(rtrim(lower(reverse(left(reverse(rtrim(pnr.[Text1])), charindex(space(1), reverse(rtrim(pnr.[Text1]))) - 1))))), ',', '.') end)
						    when 1 then convert(money, case when patindex('%[0-9];%', pnr.[Text1]) > 0 then replace(substring(substring(pnr.[Text1], patindex('%[0-9]%', pnr.[Text1]), 100), 1, patindex('%[^0-9.,]%', substring(pnr.[Text1], patindex('%[0-9]%', pnr.[Text1]), 100)) - 1), ',', '.') else replace(ltrim(rtrim(lower(reverse(left(reverse(rtrim(pnr.[Text1])), charindex(space(1), reverse(rtrim(pnr.[Text1]))) - 1))))), ',', '.') end)
						    else 0
					    end
				    else 0
			    end as 'topupmamt'
               */
		       ,case when (ltrim(rtrim(pnr.[Text1])) like 'RM%*%IFD %')
				    then
					    case isnumeric(case when patindex('%[0-9];%', pnr.[Text1]) > 0 then replace(substring(substring(pnr.[Text1], patindex('%[0-9]%', pnr.[Text1]), 100), 1, patindex('%[^0-9.,]%', substring(pnr.[Text1], patindex('%[0-9]%', pnr.[Text1]), 100)) - 1), ',', '.') else replace(ltrim(rtrim(lower(reverse(left(reverse(rtrim(pnr.[Text1])), charindex(space(1), reverse(rtrim(pnr.[Text1]))) - 1))))), ',', '.') end)
						    when 1 then convert(money, case when patindex('%[0-9];%', pnr.[Text1]) > 0 then replace(substring(substring(pnr.[Text1], patindex('%[0-9]%', pnr.[Text1]), 100), 1, patindex('%[^0-9.,]%', substring(pnr.[Text1], patindex('%[0-9]%', pnr.[Text1]), 100)) - 1), ',', '.') else replace(ltrim(rtrim(lower(reverse(left(reverse(rtrim(pnr.[Text1])), charindex(space(1), reverse(rtrim(pnr.[Text1]))) - 1))))), ',', '.') end)
						    else 0
					    end
				    else 0
			    end as 'fsfamt'
	        from [dbo].[Nav_Import_Air] pnr
	        inner join #import_air_keys f
		        on f.[FileName] = pnr.[FileName]
                and f.[TravelProductID] = pnr.[TravelProductID]
		    where (ltrim(rtrim(pnr.[Text1])) like 'RM*IFD %'  or ltrim(rtrim(pnr.[Text1])) like 'RM* IFD %'	 or ltrim(rtrim(pnr.[Text1])) like 'RM*  IFD %'
		    --  or ltrim(rtrim(pnr.[Text1])) like 'RM*TPM %'  or ltrim(rtrim(pnr.[Text1])) like 'RM* TPM %'	 or ltrim(rtrim(pnr.[Text1])) like 'RM*  TPM %'
		        or ltrim(rtrim(pnr.[Text1])) like 'RM*AO %'   or ltrim(rtrim(pnr.[Text1])) like 'RM* AO %'	 or ltrim(rtrim(pnr.[Text1])) like 'RM*  AO %'
		        or ltrim(rtrim(pnr.[Text1])) like 'RM*MICE %' or ltrim(rtrim(pnr.[Text1])) like 'RM* MICE %' or ltrim(rtrim(pnr.[Text1])) like 'RM*  MICE %')
		    and pnr.[Tag]  = 'RM'
		    group by pnr.[FileName], pnr.[Text1], pnr.[TravelProductID]
	    ) as A
    ) as B
    outer apply (
        select top 1
            [travel_consultant_id]
        from #employees
        where [LOGIN] = substring(B.formated, charindex(':', B.formated) + 1, 100)
        order by [travel_consultant_id] asc
    ) e
    where B.RowNum = 1

    create index ix1 on #fsfamtsavings ([FileName], [TravelProductID], [type]) include ([fsfamt], [travel_consultant_id])

    ------------------------------------------------------------
    -- Update Amounts
    ------------------------------------------------------------

    update f
       set f.TicketAmt = a.TicketAmt,
           f.TicketAmtTaxVAT = a.TicketAmtTaxVAT,
           f.TicketAmtTax = a.Fullothertaxamount + a.TicketAmtTaxVAT,
           f.TicketAmtBase = a.TicketAmtBase,
           f.TicketAmtExchangePenalty = a.TicketAmtExchangePenalty,
           f.TicketAmtGross = a.TicketAmt,
           f.TicketAmtCommission = a.Comissionamount,
           f.TicketAmtMarkup = a.Markup,
           f.TicketAmtOBFees = a.OB_Fees_Amount,
           f.TicketAmtTaxFee = coalesce(abs(s1.YQ), abs(s2.YQ), abs(s3.YQ), 0) +
                                coalesce(abs(s1.YR), abs(s2.YR), abs(s3.YR), 0),
           f.TicketAmtMarkupGds = a.MarkupGds,
           f.TicketAmtMarkupPub = a.MarkupPub,
           f.FSFAmtSavings = coalesce(fsf.[fsfamt], 0)
      from dbo.#InternalExtractEUAirFact f
           inner join
           #amounts a on f.TravelProductID = a.TravelProductID and
                         f.SalesDocumentCode = a.Document_No_ and
                         f.SalesDocumentLineNbr = a.Line_No_
      left outer join #surcharge s1
           on s1.Tag = 'TAX'
           and s1.[FileName] = a.[FileName]
           and f.BookingTypeID in (1, 4, 5)
           and s1.TravelProductID = a.TravelProductID
           and a.Business_category in (1, 3, 10)
           and f.AirFareTypeID not in (101, 102, 103, 104, 105) -- Excluding TPC
           and s1.[Source] = 'Nav_Sales_Line'
      left outer join #surcharge s2
           on s2.Tag = 'KRF'
           and s2.[FileName] = a.[FileName]
           and f.BookingTypeID in (7, 8)
           and s2.TravelProductID = a.TravelProductID
           and a.Business_category in (1, 3, 10)
           and f.AirFareTypeID not in (101, 102, 103, 104, 105) -- Excluding TPC
           and s2.[Source] = 'Nav_Sales_Line'
      left outer join #surcharge s3
           on s3.Tag = 'KST'
           and s3.[FileName] = a.[FileName]
           and f.BookingTypeID in (1, 4, 5)
           and s3.TravelProductID = a.TravelProductID
           and a.Business_category in (1, 3, 10)
           and f.AirFareTypeID not in (101, 102, 103, 104, 105) -- Excluding TPC
           and s3.[Source] = 'Nav_Sales_Line'
      left outer join #fsfamtsavings fsf
            on fsf.[FileName] = a.[FileName]
            and fsf.[TravelProductID] = a.[TravelProductID]

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update #InternalExtractEUAirFact amounts)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    ------------------------------------------------------------
    -- Exchanges
    ------------------------------------------------------------

    -- Exchanges need to include amounts from prior ticket
    -- Logic obtained from: Navision.dbo.GR_Get_Real_Exchange_Amount(b.OriginalTicket, b.[ID 2], b.[Traveller Code], '${PAYS}') 'OriginalAmountString'
    ;with OriginalTicket as (
        select
            sle.*
        ,   i.TicketAmt
        ,   i.TicketAmtCommission
        ,   i.TicketAmtBase
        ,   i.TicketAmtTaxVAT
        ,   i.TicketAmtTax
        ,   i.TicketAmtTaxFee
        ,   i.TicketAmtMarkup
        from #sle sle
        inner join #InternalExtractEUAirFact i
            on i.SalesDocumentCode = sle.SalesDocumentCode
            and i.SalesDocumentLineNbr = sle.SalesDocumentLineNbr
            and i.TravelProductID = sle.TravelProductID
    )
    update OriginalTicket
    set TicketAmt = coalesce(TicketAmt,0) + OriginalAmount,
        TicketAmtCommission = coalesce(TicketAmtCommission, 0) + OriginalCommission,
        TicketAmtBase = coalesce(TicketAmtBase,0) + OriginalFullBaseFare,
        TicketAmtTaxVAT = coalesce(TicketAmtTaxVAT,0) + OriginalFullVAT,
        TicketAmtTax = coalesce(TicketAmtTax,0) + OriginalTax + OriginalFullVAT
        
    -- dbo.GR_Get_Markup(SalesInvoiceLine.[Amount], SalesInvoiceLine.[PRIX_PUBLIC]) 'Fullmarkupamount', 

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update #InternalExtractEUAirFact Exchanges)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    ------------------------------------------------------------
    -- Exchange Penalties
    ------------------------------------------------------------

    -- Linking exchange penalty amounts with exchange itself - EGE-64393
    update a 
        set 
         a.TicketAmtGross = a.TicketAmtGross + e.TicketAmtGross, 
         a.TicketAmtExchangePenalty = e.TicketAmtGross
    from dbo.#InternalExtractEUAirFact a 
        inner join 
        dbo.nav_Sales_Line(@pInternalExtractID) b on a.TravelProductID = b.TravelProductID and 
                                                     a.SalesDocumentCode = b.Document_No_ and
                                                     a.SalesDocumentLineNbr = b.Line_No_
        inner join (select a2.MetaDossierID, 
                           a2.TUIDTraveler, 
                           a2.TicketCodePrior, 
                           coalesce(a2.TicketAmtGross, 0) * coalesce(d.ExchangeRateUsed, @ExchangeRateNull) TicketAmtGross, 
                           row_number() over(partition by a2.MetaDossierID, a2.TUIDTraveler, a2.TicketCodePrior order by a2.SalesDocumentLineNbr) PenaltyRank
                      from dbo.#InternalExtractEUAirFact a2 
                           inner join 
                           dbo.nav_Sales_Line(@pInternalExtractID) b2 on a2.TravelProductID = b2.TravelProductID and 
                                                                         a2.SalesDocumentCode = b2.Document_No_ and
                                                                         a2.SalesDocumentLineNbr = b2.Line_No_
                           inner join 
                           dbo.TravelProductDim c on a2.TravelProductID = c.TravelProductID 
                           left join
                           dbo.ExchangeRateDailyFull d on dbo.TimeIDFromDate(a2.InvoiceDate) = d.TimeID and a2.CurrencyCode = d.FromCurrencyCode 
                               and c.CurrencyCodeStorage = d.ToCurrencyCode                
                       where 
                               b2.Business_category in (6, 7)   -- Ancillary Service
                           and a2.FactRecordStatusID = 11       -- Air Exchange Penalty (based on BusinessSubCategoryID in the sproc ExternalFileDataVldEUAir_) 
                           and a2.BookingTypeID = 1             -- Ancillary Service Purchase
                           and isnull(a2.TicketAmtGross, 0) > 0 -- Exchange Penalty Gross > 0 
                       ) e on b.Meta_ID = e.MetaDossierID and b.Traveller_Code = e.TUIDTraveler and b.Ticket = e.TicketCodePrior
    where 
        b.InternalExtractID = @pInternalExtractID and
        a.InvoiceDate >= '04/01/2014' and   -- This is applicable for Exchange/Penalty invoiced on or after 04/01/2014 only
        a.BookingTypeID = 5 and -- Exchange 
        e.PenaltyRank = 1 

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (Linking exchange penalty amounts with exchange itself)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- Linking exchange penalty amounts with exchange itself for Q1 2014 transaction where TicketCode and TicketCodePrior linkage was not available in Nav - EGE-87758  
    -- ENSURE THAT THE QUERY ABOVE IS DATED MUTUALLY EXCLUSIVE FROM THE BELOW, OTHERWISE TicketAmtExchangePenalty WILL BE DOUBLED IN TicketAmtGross 

    update a 
        set 
         a.TicketAmtGross = a.TicketAmtGross + e.TicketAmtGross, 
         a.TicketAmtExchangePenalty = e.TicketAmtGross
    from dbo.#InternalExtractEUAirFact a 
        inner join 
        dbo.nav_Sales_Line(@pInternalExtractID) b on a.TravelProductID = b.TravelProductID and 
                                                     a.SalesDocumentCode = b.Document_No_ and
                                                     a.SalesDocumentLineNbr = b.Line_No_
        inner join 
        (select 
            a2.MetaDossierID, 
            a2.TUIDTraveler, 
            a2.PNRCode, max(b2.Posting_Date) Posting_Date, 
            sum(coalesce(a2.TicketAmtGross, 0) * coalesce(d.ExchangeRateUsed, @ExchangeRateNull)) TicketAmtGross
        from dbo.#InternalExtractEUAirFact a2 
            inner join 
            dbo.nav_Sales_Line(@pInternalExtractID) b2 on a2.TravelProductID = b2.TravelProductID and 
                                                          a2.SalesDocumentCode = b2.Document_No_ and
                                                          a2.SalesDocumentLineNbr = b2.Line_No_
            inner join 
            dbo.TravelProductDim c on a2.TravelProductID = c.TravelProductID 
            left join
            dbo.ExchangeRateDailyFull d on dbo.TimeIDFromDate(a2.InvoiceDate) = d.TimeID and a2.CurrencyCode = d.FromCurrencyCode 
                and c.CurrencyCodeStorage = d.ToCurrencyCode                
        where 
            b2.Business_category = 6                            -- Ancillary Service
            and isnull(b2.Business_sub_category, 0) in (1,2,3)      -- BusinessSubCategoryID: 1-Exchange without Payment/2-Exchange with Payment/3-Exchange Refund 
            and b2.Resource_Type = 0                            -- Primary Resource             
            and a2.FactRecordStatusID = @FactRecordStatusID_AEP          -- Air Exchange Penalty (based on BusinessSubCategoryID in the sproc ExternalFileDataVldEUAir_) 
            and a2.BookingTypeID = 1                            -- Ancillary Service Purchase
            and isnull(a2.TicketAmtGross, 0) > 0                -- Exchange Penalty Gross > 0 
            and b2.Posting_Date >= '01/01/2014' 
            and b2.Posting_Date < '04/01/2014' 
        group by 
            a2.MetaDossierID, a2.TUIDTraveler, a2.PNRCode
        having COUNT(1) = 1                                     -- Ignore multiple Ancillary Service entries that exists in same day/batch 
        ) e on b.Meta_ID = e.MetaDossierID and b.Traveller_Code = e.TUIDTraveler and ltrim(rtrim(b.ID_2)) = e.PNRCode 
    where 
        b.InternalExtractID = @pInternalExtractID and
        a.BookingTypeID = 5 and                                 -- Exchange 
        a.InvoiceDate >= '01/01/2014' and                       -- This is applicable for Exchange/Penalty invoiced on or after 01/01/2014 and 
        a.InvoiceDate < '04/01/2014' and                        -- before 04/01/2014 
        abs(datediff(mi, b.Posting_Date, e.Posting_Date)) < 2 and    -- Exchange and A/S as penalty issued within 2 minutes
        e.Posting_Date >= b.Posting_Date                        -- Ancillary Service date after Exchange to avoid multiple exchanges cartisioning each other 
  
    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (Linking exchange penalty amounts with exchange itself (Q1 2014))' 
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    ------------------------------------------------------------
    -- Refund / Partial Refund
    ------------------------------------------------------------

    -- Refund / Partial Refund amounts should be negative
    update dbo.#InternalExtractEUAirFact
       set TicketAmt = abs(TicketAmt) * -1,
           TicketAmtBase = abs(TicketAmtBase) * -1,
           TicketAmtTax = abs(TicketAmtTax) * -1,
           TicketAmtGross = abs(TicketAmtGross) * -1,
           TicketAmtMarkup = abs(TicketAmtMarkup) * -1,
           TicketAmtMarkupGds = abs(TicketAmtMarkupGds) * -1,
           TicketAmtMarkupPub = abs(TicketAmtMarkupPub) * -1,
           FareAmtLowestInPolicy = abs(FareAmtLowestInPolicy) * -1,
           FareAmtPublished = abs(FareAmtPublished) * -1,
           FareAmtNegotiated = abs(FareAmtNegotiated) * -1,
           TicketAmtOBFees = abs(TicketAmtOBFees) * -1,
           TicketAmtTaxFee = abs(TicketAmtTaxFee) * -1,
           FSFAmtSavings = abs(FSFAmtSavings) * -1
     where BookingTypeID in (4, 7, 8)

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update #InternalExtractEUAirFact  Refund / Partial Refund)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end
  
    ------------------------------------------------------------
    -- Savings Logic Begin
    ------------------------------------------------------------

    alter table #InternalExtractEUAirFact add [PRODUCT_COMPOSITE_KEY]       varchar(500)
    alter table #InternalExtractEUAirFact add [MD_CODE]                     int

    alter table #InternalExtractEUAirFact add [savings_match]               bit
    alter table #InternalExtractEUAirFact add [savings_InternalExtractID]   int
    alter table #InternalExtractEUAirFact add [savings_message_unique_id]   uniqueidentifier
    alter table #InternalExtractEUAirFact add [savings_booked_fare_number]  tinyint
    alter table #InternalExtractEUAirFact add [savings_fare_number]         tinyint
    alter table #InternalExtractEUAirFact add [savings_amt_percentage_diff] float
    alter table #InternalExtractEUAirFact add [savings_total_flight_amount] money
    alter table #InternalExtractEUAirFact add [savings_rule]                varchar(30)
    alter table #InternalExtractEUAirFact add [savings_mdcode]              varchar(40) -- join on transaction_id

    create nonclustered index ix_ieeaf_1 on #InternalExtractEUAirFact (RecordKey, BookingTypeID)
    create nonclustered index ix_ieeaf_2 on #InternalExtractEUAirFact (MetaDossierID, BookingTypeID, savings_match)
 -- create nonclustered index ix_ieeaf_3 on #InternalExtractEUAirFact (SalesDocumentCode, SalesDocumentLineNbr, BookingTypeID)
 
    
    -- Modified logic from Kettle: Transformations\Global Reporting\Misc\GetMetaDossierInfosGR\[Build CompositeKey]
    -- Each end of a segment joined by dash/minus (-)
    -- Each segment row joined by a forward slash (/)
    -- Example: SEA to NRT; NRT to CKS; CKS to NRT; NRT to SEA = 'SEA-NRT/NRT-CKS/CKS-NRT/NRT-SEA'
    select
         s.RecordKey
        ,SegmentNbrAdj
        ,s.AirportCodeFrom + '-' + s.AirportCodeTo as AirportCodes 
    into #s
    from dbo.InternalExtractEUAirSegmentFact s 
    where s.InternalExtractID = @pInternalExtractID
    and s.BookingTypeID <> 99
    order by RecordKey, SegmentNbrAdj
    
    -- create nonclustered index ix_s on #s (RecordKey, SegmentNbrAdj) include (AirportCodes)
    create clustered index ix_s on #s (RecordKey, SegmentNbrAdj)

    update f
    set f.PRODUCT_COMPOSITE_KEY = stuff((select '/' + s.AirportCodes
                                         from #s s 
                                         where s.RecordKey = f.RecordKey
                                         order by s.SegmentNbrAdj
                                         for xml path('')
                                  ), 1, 1, '')
       ,f.TicketAmtOBFees = isnull(f.TicketAmtOBFees, 0)
       ,f.[MD_CODE] = convert(int, replace(f.MetaDossierID,'-',''))
       ,f.[savings_mdcode] = convert(int, replace(f.MetaDossierID,'-',''))
    from #InternalExtractEUAirFact f
    where f.BookingTypeID = 1
    and f.MetaDossierID not like @NonNumericString
    and (len(f.MetaDossierID) < 10 or
        (len(f.MetaDossierID) = 10 and cast(replace(f.MetaDossierID,'-','') as bigint) <= 2147483647)
    )

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update #InternalExtractEUAirFact PRODUCT_COMPOSITE_KEY)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    ------------------------------------------------------------
    -- Get Savings. Old Logic.
    ------------------------------------------------------------

    -- FareInfos CTE duplicates code from GetMetaDossierInfosGR kettle transformation. Step: "Get Savings"
    -- ToDo: This is a copy of "Get Savings" wrapped into a CTE. Determine what's actually needed.
    ;with FareInfos as(
        select m.NB_TRAVELLER,
               a.SalesDocumentCode,
               a.SalesDocumentLineNbr,
               a.FareAmtLowestInPolicy,
               a.AirlineCodeLowestInPolicy,
               a.ClassOfServiceCodeLowestInPolicy,
               a.CabinClassIDLowestInPolicy,
               a.AirFareTypeIDLowestInPolicy,
               a.AirFareBasisCodeLowestInPolicy,
               a.SavingsReasonCode,
               a.FareAmtPublished,
               a.AirFareBasisCodePublished,
               a.ClassOfServiceCodePublished,
               a.CabinClassIDPublished,
               a.FareAmtNegotiated,
               a.AirFareBasisCodeNegotiated,
               a.CabinClassIDNegotiated,
               a.LowFareChosenBool,
               substring(isnull(REFUSAL_CODE, ACCEPTED_CODE), 1, 3) as SAVING_REASON_CODE,
               
               case isnull(LOW_FAREBASIS, 'EMPTY')
                   when 'EMPTY' then null
                   else case
                        when (BOOK_PRICE      = LOW_PRICE) and
                             (BOOK_AIRLINE    = LOW_AIRLINE) and
                             (BOOK_FAREBASIS  = LOW_FAREBASIS) and
                             (BOOK_FARETYPE   = LOW_FARETYPE) and
                             (BOOK_FAREBASIS  = LOW_FAREBASIS) and
                             (BOOK_CABINCLASS = LOW_CABINCLASS) then (case ACCEPTED_CODE
                               when 'A01' then 1
                               else 2
                           end)
                       else 0
                   end
               end as LOW_FARE_CHOOSEN,
               
               LOW_PRICE / m.NB_TRAVELLER as LOW_PRICE,
               substring(LOW_AIRLINE, 1, 3) as LOW_AIRLINE,
               substring(upper(LOW_FAREBASIS), 1, 15) as LOW_FAREBASIS,
               substring(upper(LOW_FAREBASIS), 1, 1) as LOW_CLASSOFSERVICE,
               case isnull(LOW_FAREBASIS, 'EMPTY')
                   when 'EMPTY' then null
                   else case LOW_FARETYPE
                       when 1 then 1
                       when 2 then 9
                       when 3 then 10
                       when 4 then 0
                       when 5 then 2
                       when 6 then 1
                       when 7 then 1
                       when 8 then 1
                       when 9 then 1
                       else 0
                   end
               end as LOW_FARETYPE,
               case isnull(LOW_FAREBASIS, 'EMPTY')
                   when 'EMPTY' then null
                   else case substring(LOW_CABINCLASS, 1, 1)
                       when 'Y' then '3'
                       when 'C' then '2'
                       when 'F' then '1'
                       when 'W' then '5'
                       else '0'
                   end
               end as LOW_CABINCLASS,
               
               IATA_PRICE / m.NB_TRAVELLER as IATA_PRICE_PerTraveler,
               substring(upper(IATA_FAREBASIS), 1, 15) as IATA_FAREBASIS,
               substring(upper(IATA_FAREBASIS), 1, 1) as IATA_CLASSOFSERVICE,
               case isnull(IATA_FAREBASIS, 'EMPTY')
                   when 'EMPTY' then null
                   else case substring(IATA_CABINCLASS, 1, 1)
                       when 'Y' then '3'
                       when 'C' then '2'
                       when 'F' then '1'
                       when 'W' then '5'
                       else '0'
                   end
               end as IATA_CABINCLASS,
               
               case REF_FARETYPE
                   when 2 then REF_PRICE / m.NB_TRAVELLER
                   else null
               end as NEGO_PRICE,
               case REF_FARETYPE
                   when 2 then substring(upper(REF_FAREBASIS), 1, 15)
                   else null
               end as NEGO_FAREBASIS,
               case REF_FARETYPE
                   when 2 then case substring(upper(REF_CABINCLASS), 1, 1)
                           when 'Y' then '3'
                           when 'C' then '2'
                           when 'F' then '1'
                           when 'W' then '5'
                           else '0'
                       end
                   else null
               end as NEGO_CABINCLASS

              from dbo.#InternalExtractEUAirFact a
                   cross apply
                   (select fi1.*
                      from dbo.FARE_INFOS fi1
                     where a.MD_CODE = fi1.MD_CODE and
                           a.PRODUCT_COMPOSITE_KEY = fi1.PRODUCT_COMPOSITE_KEY and
                           fi1.InternalExtractID = (select max(fi2.InternalExtractID) 
                                                    from dbo.FARE_INFOS fi2
                                                    where a.MD_CODE = fi2.MD_CODE and
                                                          a.PRODUCT_COMPOSITE_KEY = fi2.PRODUCT_COMPOSITE_KEY)
                                                    ) b
                   outer apply 
                   (select top 1 cast(SubQ.FLD_VALUE as int) as NB_TRAVELLER
                    from dbo.Nav_IMPORT_METAID_FIELD_VALUE SubQ
                    where a.TravelProductID = SubQ.TravelProductID and
                        a.MetaDossierID = SubQ.METAID and
                        SubQ.FLD_KEY = 'NB_TRAVELLER' and
                        isnumeric(SubQ.FLD_VALUE) = 1
                        order by SubQ.InternalExtractId desc) m
             where isnumeric(a.MetaDossierID) = 1 -- ToDo: Do we need this anymore if we're creating a temporary INT?
                                          -- where a.MetaDossierID = cast(fi2.MD_CODE as varchar(20)))
                   )

    -- Update #InternalExtractEUAirFact (FareInfos CTE)
    update FareInfos
        set
        FareAmtLowestInPolicy = LOW_PRICE,
        AirlineCodeLowestInPolicy = LOW_AIRLINE,
        ClassOfServiceCodeLowestInPolicy = LOW_CLASSOFSERVICE,
        CabinClassIDLowestInPolicy = LOW_CABINCLASS,
        AirFareTypeIDLowestInPolicy = LOW_FARETYPE,
        AirFareBasisCodeLowestInPolicy = LOW_FAREBASIS,
        LowFareChosenBool = case LOW_FARE_CHOOSEN 
                        when 1 then 1
                        when 0 then 0
                        else null
                        end,
        SavingsReasonCode = SAVING_REASON_CODE,
        FareAmtPublished = IATA_PRICE_PerTraveler,
        AirFareBasisCodePublished = IATA_FAREBASIS,
        ClassOfServiceCodePublished = IATA_CLASSOFSERVICE,
        CabinClassIDPublished = IATA_CABINCLASS,
        FareAmtNegotiated = NEGO_PRICE,
        AirFareBasisCodeNegotiated = NEGO_FAREBASIS,
        CabinClassIDNegotiated = NEGO_CABINCLASS
    
    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update #InternalExtractEUAirFact FareInfos)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- alter table #InternalExtractEUAirFact drop column MD_CODE

    ------------------------------------------------------------
    -- Get Savings. New Logic.
    ------------------------------------------------------------

    -- Reduce the set of data to pick from air_savings_tbl
    select
             req.InternalExtractID
            ,req.[message_unique_id]
            ,asv.[xml_creation_date]
            ,asv.[create_date]
            ,asv.[transaction_id]
    into #air_saving_keys
    from (
        select
             max(a.[InternalExtractID]) as [InternalExtractID]
            ,a.[message_unique_id]
        from [dbo].[air_saving_fare] a
        inner join [dbo].[air_saving] b
            on b.[InternalExtractID] = a.[InternalExtractID]
            and b.[message_unique_id] = a.[message_unique_id]
        inner join #InternalExtractEUAirFact i
            on i.BookingTypeID = @BookingTypeIDPurchase
            and i.PNRCode = a.[PNR]
        where b.[xml_generated_at] = 'ORIGINAL_PURCHASE'
        group by a.[message_unique_id]
    ) req
    inner join [dbo].[air_saving] asv
        on asv.[InternalExtractID] = req.[InternalExtractID]
        and asv.[message_unique_id] = req.[message_unique_id]

    create nonclustered index ix_air_saving_keys on #air_saving_keys (message_unique_id, xml_creation_date, InternalExtractID)

    -- saving_booked_fare level
    select
         asv.[InternalExtractID]
        ,asf.[message_unique_id]
        ,asf.[booked_fare_number]
        ,asv.[Transaction_id]
        ,asfa.[total_amount]
        ,asfa.[fare_amount]
        ,asfa.[obfees]
        ,asfa.[ancillaries]
        ,anci.[has_ancillaries]
        ,geo.[geometry_type]
        ,seg.[is_lowcost]
        ,seg.[segment_cnt]
        ,from_.[from_date]
        ,to_.[to_date]
        ,convert(varchar(500), r.[route_txt]) as route_txt
        ,convert(varchar(500), r.[marketing_carrier_txt]) as marketing_carrier_txt
        -- better to build a flexi_txt
        ,case seg.[is_lowcost] when 1 then 'LOWCOST' else isnull(from_.[flexibility_level], 'UNKNOWN') end as flexibility_level
    into #saving_booked_fares
    from [dbo].[air_saving_booked_fare] asf
    inner join [dbo].[air_saving] asv
        on asv.[InternalExtractID] = asf.[InternalExtractID]
        and asv.[message_unique_id] = asf.[message_unique_id]
    left outer join (
        -- Amounts
        select   [InternalExtractID]
                ,[message_unique_id]
                ,[booked_fare_number]
                ,[total_amount]
                ,[fare_amount]
                ,[obfees]
                ,[ancillaries]
        from
        (
            select
                 a.[InternalExtractID]
                ,a.[message_unique_id]
                ,a.[booked_fare_number]
                ,a.[type]
                ,a.[amount]
            from [dbo].[air_saving_fare_amount] a
            inner join #air_saving_keys ask
                on ask.[InternalExtractID] = a.[InternalExtractID]
                and ask.[message_unique_id] = a.[message_unique_id]
        ) q
        pivot
        (
            sum(q.[amount])
            for q.[type] in ([total_amount], [fare_amount], [obfees], [ancillaries])
        ) piv
        ) asfa
        on asfa.[InternalExtractID] = asf.[InternalExtractID]
        and asfa.[message_unique_id] = asf.[message_unique_id]
        and asfa.[booked_fare_number] = asf.[booked_fare_number]
    left outer join (
        -- Geometry
        select
             a.[InternalExtractID]
            ,a.[message_unique_id]
            ,a.[booked_fare_number]
            ,case count(a.[segment_number])
                when 0 then 'NA'
                when 1 then 'OW'
                when 2 then 'RT'
                else        'MD'
            end as [geometry_type]
        from [dbo].[air_saving_segment] a
        inner join #air_saving_keys ask
            on ask.[InternalExtractID] = a.[InternalExtractID]
            and ask.[message_unique_id] = a.[message_unique_id]
        and a.[is_connection] = 0
        group by a.[InternalExtractID], a.[message_unique_id], a.[booked_fare_number]
        ) geo
        on geo.[InternalExtractID] = asf.[InternalExtractID]
        and geo.[message_unique_id] = asf.[message_unique_id]
        and geo.[booked_fare_number] = asf.[booked_fare_number]
    left outer join (
        select
             a.[InternalExtractID]
            ,a.[message_unique_id]
            ,a.[booked_fare_number]
            ,max(convert(int, a.[marketing_carrier_is_lowcost])) as [is_lowcost]
            ,count(a.[segment_number]) as [segment_cnt]
            ,min(a.[segment_rank]) as [min_segment_rank]
            ,max(a.[segment_rank]) as [max_segment_rank]
        from [dbo].[air_saving_segment] a
        inner join #air_saving_keys ask
            on ask.[InternalExtractID] = a.[InternalExtractID]
            and ask.[message_unique_id] = a.[message_unique_id]
        group by a.[InternalExtractID], a.[message_unique_id], a.[booked_fare_number]
        ) seg
        on seg.[InternalExtractID] = asf.[InternalExtractID]
        and seg.[message_unique_id] = asf.[message_unique_id]
        and seg.[booked_fare_number] = asf.[booked_fare_number]
    left outer join [dbo].[air_saving_segment] from_
        on from_.[InternalExtractID] = asf.[InternalExtractID]
        and from_.[message_unique_id] = asf.[message_unique_id]
        and from_.[booked_fare_number] = asf.[booked_fare_number]
        and from_.[segment_rank] = seg.min_segment_rank
    left outer join [dbo].[air_saving_segment] to_
        on to_.[InternalExtractID] = asf.[InternalExtractID]
        and to_.[message_unique_id] = asf.[message_unique_id]
        and to_.[booked_fare_number] = asf.[booked_fare_number]
        and to_.[segment_rank] = seg.max_segment_rank
    left outer join (
        select
             s.[InternalExtractID]
            ,s.[message_unique_id]
            ,s.[booked_fare_number]
            ,stuff((
                select
                    '/' + coalesce([from_airport_code], '') + '-' + coalesce([to_airport_code], '')
                from [air_saving_segment]
                where [InternalExtractID] = s.[InternalExtractID]
                and [message_unique_id] = s.[message_unique_id]
                and [booked_fare_number] = s.[booked_fare_number]
                order by [segment_rank]
                for xml path('') 
                ), 1,1, ''
            ) as [route_txt]
            ,stuff((
                select
                    '/' + coalesce([marketing_carrier_code], '')
                from [air_saving_segment]
                where [InternalExtractID] = s.[InternalExtractID]
                and [message_unique_id] = s.[message_unique_id]
                and [booked_fare_number] = s.[booked_fare_number]
                order by [segment_rank]
                for xml path('') 
                ), 1,1, ''
            ) as [marketing_carrier_txt]
        from [air_saving_segment] s
        inner join #air_saving_keys ask
            on ask.[InternalExtractID] = s.[InternalExtractID]
            and ask.[message_unique_id] = s.[message_unique_id]
        group by s.[InternalExtractID], s.[message_unique_id], s.[booked_fare_number]
        ) r
        on r.[InternalExtractID] = asf.[InternalExtractID]
        and r.[message_unique_id] = asf.[message_unique_id]
        and r.[booked_fare_number] = asf.[booked_fare_number]
    left outer join (
        select
             a.[InternalExtractID]
            ,a.[message_unique_id]
            ,a.[booked_fare_number]
            ,case isnull(sum(a.[amount]), 0) when 0 then 0 else 1 end as [has_ancillaries]
        from [dbo].[air_saving_fare_amount] a
        inner join #air_saving_keys ask
            on ask.[InternalExtractID] = a.[InternalExtractID]
            and ask.[message_unique_id] = a.[message_unique_id]
            and a.[type] = 'ancillaries'
        group by a.[InternalExtractID], a.[message_unique_id], a.[booked_fare_number]
        ) anci
        on anci.[InternalExtractID] = asf.[InternalExtractID]
        and anci.[message_unique_id] = asf.[message_unique_id]
        and anci.[booked_fare_number] = asf.[booked_fare_number]
    inner join #air_saving_keys ask
        on ask.[InternalExtractID] = asv.[InternalExtractID]
        and ask.[message_unique_id] = asv.[message_unique_id]

    create clustered index pk_saving_fares on #saving_booked_fares ([InternalExtractID], [message_unique_id], [booked_fare_number])

    -- saving_fare level
    select
         asv.[InternalExtractID]
        ,asf.[message_unique_id]
        ,asf.[booked_fare_number]
        ,asf.[fare_number]
        ,asf.[PNR]
        ,asv.[Transaction_id]
        ,asbf.[percode]
        ,asf.[fare_type]
        ,asfa.[total_amount]
        ,((convert(decimal(8,2), 97)  * asfa.[total_amount]) / convert(decimal(8,2), 100)) as total_amount_down
        ,((convert(decimal(8,2), 103) * asfa.[total_amount]) / convert(decimal(8,2), 100)) as total_amount_up
        ,asfa.[fare_amount]
        ,asfa.[obfees]
        ,asfa.[ancillaries]
        ,anci.[has_ancillaries]
        ,geo.[geometry_type]
        ,case seg.[is_lowcost] when 1 then 'LOWCOST' else isnull(from_.[flexibility_level], 'UNKNOWN') end as flexibility_level
        ,seg.[is_lowcost]
        ,seg.[segment_cnt]
        ,from_.[from_date]
        ,to_.[to_date]
        ,convert(varchar(500), r.[route_txt]) as route_txt
        ,convert(varchar(500), r.[marketing_carrier_txt]) as marketing_carrier_txt
        ,asv.[xml_creation_date]
        ,asv.[com_code]
        ,asv.[UpdateDate]
    into #saving_fares
    from [dbo].[air_saving_fare] asf
    inner join [dbo].[air_saving] asv
        on asv.[InternalExtractID] = asf.[InternalExtractID]
        and asv.[message_unique_id] = asf.[message_unique_id]
    inner join [dbo].[air_saving_booked_fare] asbf
        on asbf.[InternalExtractID] = asf.[InternalExtractID]
        and asbf.[message_unique_id] = asf.[message_unique_id]
        and asbf.[booked_fare_number] = asf.[booked_fare_number]
    left outer join (
        -- Amounts
        select   [InternalExtractID]
                ,[message_unique_id]
                ,[booked_fare_number]
                ,[fare_number]
                ,[total_amount]
                ,[fare_amount]
                ,[obfees]
                ,[ancillaries]
        from
        (
            select
                 a.[InternalExtractID]
                ,a.[message_unique_id]
                ,a.[booked_fare_number]
                ,a.[fare_number]
                ,a.[type]
                ,a.[amount]
            from [dbo].[air_saving_fare_amount] a
            inner join #air_saving_keys ask
                on ask.[InternalExtractID] = a.[InternalExtractID]
                and ask.[message_unique_id] = a.[message_unique_id]
        ) q
        pivot
        (
            sum(q.[amount])
            for q.[type] in ([total_amount], [fare_amount], [obfees], [ancillaries])
        ) piv
        ) asfa
        on asfa.[InternalExtractID] = asf.[InternalExtractID]
        and asfa.[message_unique_id] = asf.[message_unique_id]
        and asfa.[booked_fare_number] = asf.[booked_fare_number]
        and asfa.[fare_number] = asf.[fare_number]
    left outer join (
        -- Geometry
        select
            a.[InternalExtractID]
            ,a.[message_unique_id]
            ,a.[booked_fare_number]
            ,a.[fare_number]
            ,case count(a.[segment_number])
                when 0 then 'NA'
                when 1 then 'OW'
                when 2 then 'RT'
                else        'MD'
            end as [geometry_type]
        from [dbo].[air_saving_segment] a
        inner join #air_saving_keys ask
            on ask.[InternalExtractID] = a.[InternalExtractID]
            and ask.[message_unique_id] = a.[message_unique_id]
        and a.[is_connection] = 0
        group by a.[InternalExtractID], a.[message_unique_id], a.[booked_fare_number], a.[fare_number]
        ) geo
        on geo.[InternalExtractID] = asf.[InternalExtractID]
        and geo.[message_unique_id] = asf.[message_unique_id]
        and geo.[booked_fare_number] = asf.[booked_fare_number]
        and geo.[fare_number] = asf.[fare_number]
    left outer join (
        select
             a.[InternalExtractID]
            ,a.[message_unique_id]
            ,a.[booked_fare_number]
            ,a.[fare_number]
            ,max(convert(int, a.[marketing_carrier_is_lowcost])) as [is_lowcost]
            ,count(a.[segment_number]) as [segment_cnt]
            ,min(a.[segment_rank]) as [min_segment_rank]
            ,max(a.[segment_rank]) as [max_segment_rank]
        from [dbo].[air_saving_segment] a
        inner join #air_saving_keys ask
            on ask.[InternalExtractID] = a.[InternalExtractID]
            and ask.[message_unique_id] = a.[message_unique_id]
        group by a.[InternalExtractID], a.[message_unique_id], a.[booked_fare_number], a.[fare_number]
        ) seg
        on seg.[InternalExtractID] = asf.[InternalExtractID]
        and seg.[message_unique_id] = asf.[message_unique_id]
        and seg.[booked_fare_number] = asf.[booked_fare_number]
        and seg.[fare_number] = asf.[fare_number]
    left outer join [dbo].[air_saving_segment] from_
        on from_.[InternalExtractID] = asf.[InternalExtractID]
        and from_.[message_unique_id] = asf.[message_unique_id]
        and from_.[booked_fare_number] = asf.[booked_fare_number]
        and from_.[fare_number] = asf.[fare_number]
        and from_.[segment_rank] = seg.min_segment_rank
    left outer join [dbo].[air_saving_segment] to_
        on to_.[InternalExtractID] = asf.[InternalExtractID]
        and to_.[message_unique_id] = asf.[message_unique_id]
        and to_.[booked_fare_number] = asf.[booked_fare_number]
        and to_.[fare_number] = asf.[fare_number]
        and to_.[segment_rank] = seg.max_segment_rank
    left outer join (
        select
             s.[InternalExtractID]
            ,s.[message_unique_id]
            ,s.[booked_fare_number]
            ,s.[fare_number]
            ,stuff((
                select
                    '/' + coalesce([from_airport_code], '') + '-' + coalesce([to_airport_code], '')
                from [air_saving_segment]
                where [InternalExtractID] = s.[InternalExtractID]
                and [message_unique_id] = s.[message_unique_id]
                and [booked_fare_number] = s.[booked_fare_number]
                and [fare_number] = s.[fare_number]
                order by [segment_rank]
                for xml path('') 
                ), 1,1, ''
            ) as [route_txt]
            ,stuff((
                select
                    '/' + coalesce([marketing_carrier_code], '')
                from [air_saving_segment]
                where [InternalExtractID] = s.[InternalExtractID]
                and [message_unique_id] = s.[message_unique_id]
                and [booked_fare_number] = s.[booked_fare_number]
                and [fare_number] = s.[fare_number]
                order by [segment_rank]
                for xml path('') 
                ), 1,1, ''
            ) as [marketing_carrier_txt]
        from [air_saving_segment] s
        inner join #air_saving_keys ask
            on ask.[InternalExtractID] = s.[InternalExtractID]
            and ask.[message_unique_id] = s.[message_unique_id]
        group by s.[InternalExtractID], s.[message_unique_id], s.[booked_fare_number], s.[fare_number]
        ) r
        on r.[InternalExtractID] = asf.[InternalExtractID]
        and r.[message_unique_id] = asf.[message_unique_id]
        and r.[booked_fare_number] = asf.[booked_fare_number]
        and r.[fare_number] = asf.[fare_number]
    left outer join (
        select
             a.[InternalExtractID]
            ,a.[message_unique_id]
            ,a.[booked_fare_number]
            ,case isnull(sum(a.[amount]), 0) when 0 then 0 else 1 end as [has_ancillaries]
        from [dbo].[air_saving_fare_amount] a
        inner join #air_saving_keys ask
            on ask.[InternalExtractID] = a.[InternalExtractID]
            and ask.[message_unique_id] = a.[message_unique_id]
            and a.[type] = 'ancillaries'
        group by a.[InternalExtractID], a.[message_unique_id], a.[booked_fare_number]
        ) anci
        on anci.[InternalExtractID] = asf.[InternalExtractID]
        and anci.[message_unique_id] = asf.[message_unique_id]
        and anci.[booked_fare_number] = asf.[booked_fare_number]
    inner join #air_saving_keys ask
        on ask.[InternalExtractID] = asv.[InternalExtractID]
        and ask.[message_unique_id] = asv.[message_unique_id]

    create clustered index pk_saving_fares on #saving_fares ([InternalExtractID], [message_unique_id], [booked_fare_number], [fare_number])
    create nonclustered index ix_saving_fares on #saving_fares (transaction_id, route_txt)

    -- Matching Logic
    update i
         set i.savings_match = 1
            ,i.[savings_InternalExtractID]   = s.[InternalExtractID]
            ,i.[savings_message_unique_id]   = s.[message_unique_id]
            ,i.[savings_booked_fare_number]  = s.[booked_fare_number]
            ,i.[savings_fare_number]         = s.[fare_number]
            ,i.[savings_amt_percentage_diff] = 
                case i.TicketAmt - i.TicketAmtOBFees
                    when s.total_amount then convert(float, 100.00)
                    else case s.total_amount when 0
                        then convert(float, 100.00)
                        else (convert(float, i.TicketAmt - i.TicketAmtOBFees) * convert(float, 100.00)) / convert(float, s.total_amount)
                    end
                end
    from #InternalExtractEUAirFact i
    inner join #saving_fares s
        on  i.PRODUCT_COMPOSITE_KEY = s.route_txt             -- same route
        and i.SegmentCnt = s.segment_cnt                      -- same number of segments
        and i.TicketAmt - i.TicketAmtOBFees                   
            between s.total_amount_down and s.total_amount_up -- same price +/- 3%
        and i.TravelDateStart = s.[from_date]                 -- same departure
        and i.TravelDateEnd = s.[to_date]                     -- same arrival
        and i.TUIDTraveler = s.[percode]                      -- same traveler (issue when the backoffice change it)
        and i.PNRCode = s.[PNR]                               -- same PNR        
    where i.BookingTypeID = @BookingTypeIDPurchase            -- purchase
    and i.MetaDossierID not like @NonNumericString            -- mdcode numeric
    and s.[has_ancillaries] = 0                               -- no ancillaries

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update #InternalExtractEUAirFact savings debugging indicators)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    create nonclustered index ix_InternalExtractEUAirFact on #InternalExtractEUAirFact (savings_message_unique_id, savings_InternalExtractID, savings_booked_fare_number, savings_fare_number)

    -- We need the [savings_total_flight_amount] in some cases OW vs RT comparison
    update ieeuaf
        set ieeuaf.[savings_total_flight_amount] = q.[savings_total_flight_amount]
    from #InternalExtractEUAirFact ieeuaf
    inner join (
        select
             i.savings_InternalExtractID
            ,i.savings_message_unique_id
            ,i.savings_booked_fare_number
            ,sum(i.TicketAmt) - sum(i.TicketAmtOBFees) as [savings_total_flight_amount]
        from #InternalExtractEUAirFact i
        where i.savings_match = 1
        and i.savings_message_unique_id is not null
        group by
             i.savings_InternalExtractID
            ,i.savings_message_unique_id
            ,i.savings_booked_fare_number
    ) q
    on q.savings_InternalExtractID = ieeuaf.savings_InternalExtractID 
    and q.savings_message_unique_id = ieeuaf.savings_message_unique_id 
    and q.savings_booked_fare_number = ieeuaf.savings_booked_fare_number

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update #InternalExtractEUAirFact [savings_total_flight_amount])'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- Reduce the set of data to pick from air_savings_tbl
    select
         keys.[savings_InternalExtractID]
        ,keys.[savings_message_unique_id]
    into #ieeuaf_keys
    from (
        select
             max([savings_InternalExtractID]) as [savings_InternalExtractID]
            ,[savings_message_unique_id]
        from #InternalExtractEUAirFact
        where [Savings_match] = 1
        group by [savings_message_unique_id]
    ) keys

    create clustered index ix_ieeuaf_keys on #ieeuaf_keys ([savings_message_unique_id], [savings_InternalExtractID])
    
    -- comparison_option level
    select
         asco.[InternalExtractID]
        ,asco.[message_unique_id]
        ,asco.[booked_fare_number]
        ,asco.[comparison_option_number]
        ,ascf.[fare_type]
        ,ascfa.[total_amount]
        ,ascfa.[fare_amount]
        ,ascfa.[obfees]
        ,ascfa.[ancillaries]
        ,geo.[geometry_type]
        ,seg.[is_lowcost]
        ,seg.[segment_cnt]
        ,from_.[from_date]
        ,to_.[to_date]
        ,convert(varchar(500), r.[route_txt]) as route_txt
        ,convert(varchar(500), r.[marketing_carrier_txt]) as marketing_carrier_txt
        ,asco.[is_lowest_recommended_fare]
        ,case when ((ascfa.[fare_amount] = sbf.[fare_amount]) and
                    (from_.[from_date] = sbf.[from_date]) and 
                    (to_.[to_date] = sbf.[to_date]) and
                    (convert(varchar(500), r.[route_txt]) = sbf.[route_txt])
                   )
         then 1 else 0 end as [is_booked_fare]
        ,from_.[marketing_carrier_code] as [1st_seg_marketing_carrier]
        ,from_.[cabin_class] as [1st_seg_cabin_class]
        ,case isnull(from_.[cabin_class], '')
            when ''                then 0 
            when 'ECONOMY'         then 3
            when 'PREMIUM_ECO'     then 5
            when 'BUSINESS'        then 2
            when 'FIRST'           then 1
            else 0
        end as [1st_seg_cabin_class_id]
        ,case seg.[is_lowcost]
            when 0 then left(from_.[booking_class], 1)
            else null
        end as [1st_seg_booking_class]
        ,case seg.[is_lowcost]
            when 0 then left(from_.[farebasis], 15)
            else null
        end as [1st_seg_farebasis]
       ,case isnull(ascf.[fare_type], 'UNKNOWN')
            when 'PUBLISHED'      then 1
            when 'EGENCIA'        then 2
            when 'CORPORATE'      then 9
            when 'SUBSCRIPTION'   then 2
            when 'DISCOUNT'       then 2
            when 'YOUNG'          then 2
            when 'ELDER'          then 2
            when 'RESIDENT'       then 2
            when 'TPC'            then 101
            when 'TPU'            then 104
            when 'TMU'            then 105
            when 'TMP'            then 103
            when 'TPP'            then 102
            when 'UNKNOWN'        then 0
            else 0
        end as [fare_type_id]
        -- better to build a flexi_txt
       ,case seg.[is_lowcost] when 1 then 'LOWCOST' else isnull(from_.[flexibility_level], 'UNKNOWN') end as flexibility_level
    into #saving_comparison_options
    from [dbo].[air_saving_comparison_option] asco
    left outer join (
        select [InternalExtractID]
              ,[message_unique_id]
              ,[booked_fare_number]
              ,[comparison_option_number]
              -- Make something better here
              ,min([fare_type]) as [fare_type]
        from [dbo].[air_saving_comparison_fare] a
        inner join #ieeuaf_keys ik
            on ik.[savings_InternalExtractID] = a.[InternalExtractID]
            and ik.[savings_message_unique_id] = a.[message_unique_id]
        group by a.[InternalExtractID], a.[message_unique_id], a.[booked_fare_number], a.[comparison_option_number]
        ) ascf
        on ascf.[InternalExtractID] = asco.[InternalExtractID]
        and ascf.[message_unique_id] = asco.[message_unique_id]
        and ascf.[booked_fare_number] = asco.[booked_fare_number]
        and ascf.[comparison_option_number] = asco.[comparison_option_number]
    left outer join (
        -- Amounts
        select   [InternalExtractID]
                ,[message_unique_id]
                ,[booked_fare_number]
                ,[comparison_option_number]
                ,[total_amount]
                ,[fare_amount]
                ,[obfees]
                ,[ancillaries]
        from
        (
            select
                 a.[InternalExtractID]
                ,a.[message_unique_id]
                ,a.[booked_fare_number]
                ,a.[comparison_option_number]
                ,a.[type]
                ,sum(a.[amount]) as [amount]
            from [dbo].[air_saving_comparison_fare_amount] a
            inner join #ieeuaf_keys ik
                on ik.[savings_InternalExtractID] = a.[InternalExtractID]
                and ik.[savings_message_unique_id] = a.[message_unique_id]
            group by a.[InternalExtractID], a.[message_unique_id], a.[booked_fare_number], a.[comparison_option_number], a.[type]
        ) q
        pivot
        (
            sum(q.[amount])
            for q.[type] in ([total_amount], [fare_amount], [obfees], [ancillaries])
        ) piv
        ) ascfa
        on ascfa.[InternalExtractID] = ascf.[InternalExtractID]
        and ascfa.[message_unique_id] = ascf.[message_unique_id]
        and ascfa.[booked_fare_number] = ascf.[booked_fare_number]
        and ascfa.[comparison_option_number] = ascf.[comparison_option_number]
    left outer join (
        -- Geometry
        select
             a.[InternalExtractID]
            ,a.[message_unique_id]
            ,a.[booked_fare_number]
            ,a.[comparison_option_number]
            ,case count(a.[comparison_segment_number])
                when 0 then 'NA'
                when 1 then 'OW'
                when 2 then case count(distinct(a.[comparison_fare_number])) when 1 then 'RT' else '2OW' end
                else        'MD'
            end as [geometry_type]
        from [dbo].[air_saving_comparison_segment] a
        inner join #ieeuaf_keys ik
            on ik.[savings_InternalExtractID] = a.[InternalExtractID]
            and ik.[savings_message_unique_id] = a.[message_unique_id]
        and a.[is_connection] = 0
        group by a.[InternalExtractID], a.[message_unique_id], a.[booked_fare_number], a.[comparison_option_number]
        ) geo
        on geo.[InternalExtractID] = ascf.[InternalExtractID]
        and geo.[message_unique_id] = ascf.[message_unique_id]
        and geo.[booked_fare_number] = ascf.[booked_fare_number]
        and geo.[comparison_option_number] = ascf.[comparison_option_number]
    left outer join (
        select
             a.[InternalExtractID]
            ,a.[message_unique_id]
            ,a.[booked_fare_number]
            ,a.[comparison_option_number]
            ,count(a.[comparison_segment_number]) as [segment_cnt]
            ,min(a.[segment_rank]) as [min_segment_rank]
            ,max(a.[segment_rank]) as [max_segment_rank]
            ,max(convert(int, a.[marketing_carrier_is_lowcost])) as [is_lowcost]
        from [dbo].[air_saving_comparison_segment] a
        inner join #ieeuaf_keys ik
            on ik.[savings_InternalExtractID] = a.[InternalExtractID]
            and ik.[savings_message_unique_id] = a.[message_unique_id]
        group by a.[InternalExtractID], a.[message_unique_id], a.[booked_fare_number], a.[comparison_option_number]
        ) seg
        on seg.[InternalExtractID] = ascf.[InternalExtractID]
        and seg.[message_unique_id] = ascf.[message_unique_id]
        and seg.[booked_fare_number] = ascf.[booked_fare_number]
        and seg.[comparison_option_number] = ascf.[comparison_option_number]
    left outer join [dbo].[air_saving_comparison_segment] from_
        on from_.[InternalExtractID] = ascf.[InternalExtractID]
        and from_.[message_unique_id] = ascf.[message_unique_id]
        and from_.[booked_fare_number] = ascf.[booked_fare_number]
        and from_.[comparison_option_number] = ascf.[comparison_option_number]
        and from_.[segment_rank] = seg.min_segment_rank
    left outer join [dbo].[air_saving_comparison_segment] to_
        on to_.[InternalExtractID] = ascf.[InternalExtractID]
        and to_.[message_unique_id] = ascf.[message_unique_id]
        and to_.[booked_fare_number] = ascf.[booked_fare_number]
        and to_.[comparison_option_number] = ascf.[comparison_option_number]
        and to_.[segment_rank] = seg.max_segment_rank
    left outer join (
        select
             s.[InternalExtractID]
            ,s.[message_unique_id]
            ,s.[booked_fare_number]
            ,s.[comparison_option_number]
            ,stuff((
                select
                    '/' + coalesce([from_airport_code], '') + '-' + coalesce([to_airport_code], '')
                from [air_saving_comparison_segment]
                where [InternalExtractID] = s.[InternalExtractID]
                and [message_unique_id] = s.[message_unique_id]
                and [booked_fare_number] = s.[booked_fare_number]
                and [comparison_option_number] = s.[comparison_option_number]
                order by [segment_rank]
                for xml path('') 
                ), 1,1, ''
            ) as [route_txt]
            ,stuff((
                select
                    '/' + coalesce([marketing_carrier_code], '')
                from [air_saving_comparison_segment]
                where [InternalExtractID] = s.[InternalExtractID]
                and [message_unique_id] = s.[message_unique_id]
                and [booked_fare_number] = s.[booked_fare_number]
                and [comparison_option_number] = s.[comparison_option_number]
                order by [segment_rank]
                for xml path('') 
                ), 1,1, ''
            ) as [marketing_carrier_txt]
        from [air_saving_comparison_segment] s
        inner join #ieeuaf_keys ik
            on ik.[savings_InternalExtractID] = s.[InternalExtractID]
            and ik.[savings_message_unique_id] = s.[message_unique_id]
        group by s.[InternalExtractID], s.[message_unique_id], s.[booked_fare_number], s.[comparison_option_number]
        ) r
        on r.[InternalExtractID] = ascf.[InternalExtractID]
        and r.[message_unique_id] = ascf.[message_unique_id]
        and r.[booked_fare_number] = ascf.[booked_fare_number]
        and r.[comparison_option_number] = ascf.[comparison_option_number]
    left outer join #saving_booked_fares sbf
        on sbf.[InternalExtractID] = ascf.[InternalExtractID]
        and sbf.[message_unique_id] = ascf.[message_unique_id]
        and sbf.[booked_fare_number] = ascf.[booked_fare_number]
    inner join #ieeuaf_keys ik
        on ik.[savings_InternalExtractID] = ascf.[InternalExtractID]
        and ik.[savings_message_unique_id] = ascf.[message_unique_id]

    create nonclustered index ix_saving_comparison_options on #saving_comparison_options (message_unique_id, InternalExtractID, booked_fare_number, comparison_option_number)
    
    -- Create a view close to the comparison_fare level
    select
         ascf.[InternalExtractID]
        ,ascf.[message_unique_id]
        ,ascf.[booked_fare_number]
        ,ascf.[comparison_option_number]
        ,ascf.[comparison_fare_number]
        ,ascf.[fare_type]
        ,ascfa.[total_amount]
        ,ascfa.[fare_amount]
        ,ascfa.[obfees]
        ,ascfa.[ancillaries]
        ,geo.[geometry_type]
        ,seg.[is_lowcost]
        ,case seg.[is_lowcost] when 1 then 'LOWCOST' else isnull(from_.[flexibility_level], 'UNKNOWN') end as flexibility_level
        ,seg.[segment_cnt]
        ,from_.[from_date]
        ,to_.[to_date]
        ,convert(varchar(500), r.[route_txt]) as route_txt
        ,convert(varchar(500), r.[marketing_carrier_txt]) as marketing_carrier_txt
        ,asco.[is_lowest_recommended_fare]
        ,from_.[marketing_carrier_code] as [1st_seg_marketing_carrier]
        ,from_.[cabin_class] as [1st_seg_cabin_class]
        ,case isnull(from_.[cabin_class], '')
            when ''                then 0 
            when 'ECONOMY'         then 3
            when 'PREMIUM_ECO'     then 5
            when 'BUSINESS'        then 2
            when 'FIRST'           then 1
            else 0
        end as [1st_seg_cabin_class_id]
        ,case seg.[is_lowcost]
            when 0 then left(from_.[booking_class], 1)
            else null
        end as [1st_seg_booking_class]
        ,case seg.[is_lowcost]
            when 0 then left(from_.[farebasis], 15)
            else null
        end as [1st_seg_farebasis]
       ,case isnull(ascf.[fare_type], 'UNKNOWN')
            when 'PUBLISHED'      then 1
            when 'EGENCIA'        then 2
            when 'CORPORATE'      then 9
            when 'SUBSCRIPTION'   then 2
            when 'DISCOUNT'       then 2
            when 'YOUNG'          then 2
            when 'ELDER'          then 2
            when 'RESIDENT'       then 2
            when 'TPC'            then 101
            when 'TPU'            then 104
            when 'TMU'            then 105
            when 'TMP'            then 103
            when 'TPP'            then 102
            when 'UNKNOWN'        then 0
            else 0
        end as [fare_type_id]
    into #saving_comparison_fares
    from [dbo].[air_saving_comparison_fare] ascf
    left outer join [dbo].[air_saving_comparison_option] asco
        on asco.[InternalExtractID] = ascf.[InternalExtractID]
        and asco.[message_unique_id] = ascf.[message_unique_id]
        and asco.[booked_fare_number] = ascf.[booked_fare_number]
        and asco.[comparison_option_number] = ascf.[comparison_option_number]
    left outer join (
        -- Amounts
        select   [InternalExtractID]
                ,[message_unique_id]
                ,[booked_fare_number]
                ,[comparison_option_number]
                ,[comparison_fare_number]
                ,[total_amount]
                ,[fare_amount]
                ,[obfees]
                ,[ancillaries]
        from
        (
            select
                 a.[InternalExtractID]
                ,a.[message_unique_id]
                ,a.[booked_fare_number]
                ,a.[comparison_option_number]
                ,a.[comparison_fare_number]
                ,a.[type]
                ,a.[amount]
            from [dbo].[air_saving_comparison_fare_amount] a
            inner join #ieeuaf_keys ik
                on ik.[savings_InternalExtractID] = a.[InternalExtractID]
                and ik.[savings_message_unique_id] = a.[message_unique_id]
        ) q
        pivot
        (
            sum(q.[amount])
            for q.[type] in ([total_amount], [fare_amount], [obfees], [ancillaries])
        ) piv
        ) ascfa
        on ascfa.[InternalExtractID] = ascf.[InternalExtractID]
        and ascfa.[message_unique_id] = ascf.[message_unique_id]
        and ascfa.[booked_fare_number] = ascf.[booked_fare_number]
        and ascfa.[comparison_option_number] = ascf.[comparison_option_number]
        and ascfa.[comparison_fare_number] = ascf.[comparison_fare_number]
    left outer join (
        -- Geometry
        select
             a.[InternalExtractID]
            ,a.[message_unique_id]
            ,a.[booked_fare_number]
            ,a.[comparison_option_number]
            ,a.[comparison_fare_number]
            ,case count(a.[comparison_segment_number])
                when 0 then 'NA'
                when 1 then 'OW'
                when 2 then 'RT'
                else        'MD'
            end as [geometry_type]
        from [dbo].[air_saving_comparison_segment] a
        inner join #ieeuaf_keys ik
            on ik.[savings_InternalExtractID] = a.[InternalExtractID]
            and ik.[savings_message_unique_id] = a.[message_unique_id]
        and a.[is_connection] = 0
        group by a.[InternalExtractID], a.[message_unique_id], a.[booked_fare_number], a.[comparison_option_number], a.[comparison_fare_number]
        ) geo
        on geo.[InternalExtractID] = ascf.[InternalExtractID]
        and geo.[message_unique_id] = ascf.[message_unique_id]
        and geo.[booked_fare_number] = ascf.[booked_fare_number]
        and geo.[comparison_option_number] = ascf.[comparison_option_number]
        and geo.[comparison_fare_number] = ascf.[comparison_fare_number]
    left outer join (
        select
             a.[InternalExtractID]
            ,a.[message_unique_id]
            ,a.[booked_fare_number]
            ,a.[comparison_option_number]
            ,a.[comparison_fare_number]
            ,count(a.[comparison_segment_number]) as [segment_cnt]
            ,min(a.[segment_rank]) as [min_segment_rank]
            ,max(a.[segment_rank]) as [max_segment_rank]
            ,max(convert(int, a.[marketing_carrier_is_lowcost])) as [is_lowcost]
        from [dbo].[air_saving_comparison_segment] a
        inner join #ieeuaf_keys ik
            on ik.[savings_InternalExtractID] = a.[InternalExtractID]
            and ik.[savings_message_unique_id] = a.[message_unique_id]
        group by a.[InternalExtractID], a.[message_unique_id], a.[booked_fare_number], a.[comparison_option_number], a.[comparison_fare_number]
        ) seg
        on seg.[message_unique_id] = ascf.[message_unique_id]
        and seg.[booked_fare_number] = ascf.[booked_fare_number]
        and seg.[comparison_option_number] = ascf.[comparison_option_number]
        and seg.[comparison_fare_number] = ascf.[comparison_fare_number]
    left outer join [dbo].[air_saving_comparison_segment] from_
        on from_.[InternalExtractID] = ascf.[InternalExtractID]
        and from_.[message_unique_id] = ascf.[message_unique_id]
        and from_.[booked_fare_number] = ascf.[booked_fare_number]
        and from_.[comparison_option_number] = ascf.[comparison_option_number]
        and from_.[comparison_fare_number] = ascf.[comparison_fare_number]
        and from_.[segment_rank] = seg.min_segment_rank
    left outer join [dbo].[air_saving_comparison_segment] to_
        on to_.[InternalExtractID] = ascf.[InternalExtractID]
        and to_.[message_unique_id] = ascf.[message_unique_id]
        and to_.[booked_fare_number] = ascf.[booked_fare_number]
        and to_.[comparison_option_number] = ascf.[comparison_option_number]
        and to_.[comparison_fare_number] = ascf.[comparison_fare_number]
        and to_.[segment_rank] = seg.max_segment_rank
    left outer join (
        select
             s.[InternalExtractID]
            ,s.[message_unique_id]
            ,s.[booked_fare_number]
            ,s.[comparison_option_number]
            ,s.[comparison_fare_number]
            ,stuff((
                select
                    '/' + coalesce([from_airport_code], '') + '-' + coalesce([to_airport_code], '')
                from [air_saving_comparison_segment]
                where [InternalExtractID] = s.[InternalExtractID]
                and [message_unique_id] = s.[message_unique_id]
                and [booked_fare_number] = s.[booked_fare_number]
                and [comparison_option_number] = s.[comparison_option_number]
                and [comparison_fare_number] = s.[comparison_fare_number]
                order by [segment_rank]
                for xml path('') 
                ), 1,1, ''
            ) as [route_txt]
            ,stuff((
                select
                    '/' + coalesce([marketing_carrier_code], '')
                from [air_saving_comparison_segment]
                where [InternalExtractID] = s.[InternalExtractID]
                and [message_unique_id] = s.[message_unique_id]
                and [booked_fare_number] = s.[booked_fare_number]
                and [comparison_option_number] = s.[comparison_option_number]
                and [comparison_fare_number] = s.[comparison_fare_number]
                order by [segment_rank]
                for xml path('') 
                ), 1,1, ''
            ) as [marketing_carrier_txt]
        from [air_saving_comparison_segment] s
        inner join #ieeuaf_keys ik
            on ik.[savings_InternalExtractID] = s.[InternalExtractID]
            and ik.[savings_message_unique_id] = s.[message_unique_id]
        group by s.[InternalExtractID], s.[message_unique_id], s.[booked_fare_number], s.[comparison_option_number], s.[comparison_fare_number]
        ) r
        on r.[InternalExtractID] = ascf.[InternalExtractID]
        and r.[message_unique_id] = ascf.[message_unique_id]
        and r.[booked_fare_number] = ascf.[booked_fare_number]
        and r.[comparison_option_number] = ascf.[comparison_option_number]
        and r.[comparison_fare_number] = ascf.[comparison_fare_number]
    inner join #ieeuaf_keys ik
        on ik.[savings_InternalExtractID] = ascf.[InternalExtractID]
        and ik.[savings_message_unique_id] = ascf.[message_unique_id]

    create nonclustered index ix_saving_comparison_fares on #saving_comparison_fares (message_unique_id, InternalExtractID, booked_fare_number, comparison_option_number, comparison_fare_number)
    
    -- _____________________________________________________________________________________________________________
    -- LRF Logic BEGIN
    -- Create a #tmp_table to know in which situation we are between booked fare and comparison fares (OW vs RT ...)
    -- _____________________________________________________________________________________________________________
    select
         i.RecordKey
        ,i.BookingTypeID
        ,lrf.[InternalExtractID]
        ,lrf.[message_unique_id]
        ,lrf.[booked_fare_number]
        ,s.[fare_number]
        ,s.[geometry_type] as [booked_geometry_type]
        ,lrf.[comparison_option_number]
        ,lrf.[geometry_type] as [comparison_geometry_type]
    into #lrf_cases
    from #InternalExtractEUAirFact i
    inner join #saving_fares s
        on s.[InternalExtractID] = i.[savings_InternalExtractID]
        and s.[message_unique_id] = i.[savings_message_unique_id]
        and s.[booked_fare_number] = i.[savings_booked_fare_number]
        and s.[fare_number] = i.[savings_fare_number]
    inner join (
        select
            sub.*
            from (
                select
                    row_number() over (partition by InternalExtractID, message_unique_id, booked_fare_number order by total_amount asc) as [ROWNUM]
                    ,*
                from #saving_comparison_options
                where is_lowest_recommended_fare = 1
            ) sub
            where sub.ROWNUM = 1
        ) lrf
        on lrf.InternalExtractID = i.[savings_InternalExtractID]
        and lrf.[message_unique_id] = i.[savings_message_unique_id]
        and lrf.[booked_fare_number] = i.[savings_booked_fare_number]   
    where i.savings_match = 1

    create nonclustered index ix_lrf_cases on #lrf_cases (RecordKey, BookingTypeID) include (booked_geometry_type, comparison_geometry_type)

    select
         RecordKey
        ,BookingTypeID
        ,savings_InternalExtractID  
        ,savings_message_unique_id  
        ,savings_booked_fare_number 
        ,savings_fare_number       
        ,savings_rule               
        ,savings_amt_percentage_diff
        ,savings_total_flight_amount
        ,LowFareChosenBool          
        ,SavingsReasonCode          
        ,FareAmtLowestInPolicy      
        ,AirlineCodeLowestInPolicy  
        ,ClassOfServiceCodeLowestInPolicy
        ,CabinClassIDLowestInPolicy 
        ,AirFareTypeIDLowestInPolicy
        ,AirFareBasisCodeLowestInPolicy
        ,FareAmtPublished           
        ,AirFareBasisCodePublished  
        ,ClassOfServiceCodePublished
        ,CabinClassIDPublished      
        ,FareAmtNegotiated          
        ,AirFareBasisCodeNegotiated 
        ,CabinClassIDNegotiated
    into #InternalExtractEUAirSavingsFact
    from #InternalExtractEUAirFact

    create clustered index ix_ieeuasf on #InternalExtractEUAirSavingsFact (RecordKey, BookingTypeID)

    update #InternalExtractEUAirSavingsFact
    set
       LowFareChosenBool                = null
      ,SavingsReasonCode                = null
      ,FareAmtLowestInPolicy            = null
      ,AirlineCodeLowestInPolicy        = null
      ,ClassOfServiceCodeLowestInPolicy = null
      ,CabinClassIDLowestInPolicy       = null
      ,AirFareTypeIDLowestInPolicy      = null
      ,AirFareBasisCodeLowestInPolicy   = null
      ,FareAmtPublished                 = null
      ,AirFareBasisCodePublished        = null
      ,ClassOfServiceCodePublished      = null
      ,CabinClassIDPublished            = null
      ,FareAmtNegotiated                = null
      ,AirFareBasisCodeNegotiated       = null
      ,CabinClassIDNegotiated           = null

    delete from dbo.InternalExtractEUAirSavingsFact where InternalExtractID = @pInternalExtractID 

    insert
    into dbo.InternalExtractEUAirSavingsFact
    select
         @pInternalExtractID
        ,RecordKey
        ,BookingTypeID
        ,savings_InternalExtractID  
        ,savings_message_unique_id  
        ,savings_booked_fare_number 
        ,savings_fare_number       
        ,savings_rule               
        ,savings_amt_percentage_diff
        ,savings_total_flight_amount
        ,LowFareChosenBool          
        ,SavingsReasonCode          
        ,FareAmtLowestInPolicy      
        ,AirlineCodeLowestInPolicy  
        ,ClassOfServiceCodeLowestInPolicy
        ,CabinClassIDLowestInPolicy 
        ,AirFareTypeIDLowestInPolicy
        ,AirFareBasisCodeLowestInPolicy
        ,FareAmtPublished           
        ,AirFareBasisCodePublished  
        ,ClassOfServiceCodePublished
        ,CabinClassIDPublished      
        ,FareAmtNegotiated          
        ,AirFareBasisCodeNegotiated 
        ,CabinClassIDNegotiated
        ,@Current_Timestamp
        ,'savings_engine'
    from #InternalExtractEUAirSavingsFact

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #InternalExtractEUAirSavingsFact)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- Case RT vs RT or OW vs OW or OW vs 2OW
    update j
    set
         j.savings_rule = convert(varchar, 1) + '. || ' + c.[booked_geometry_type] + ' vs ' + c.[comparison_geometry_type]
        ,j.LowFareChosenBool = case sco.[is_booked_fare] when 1 then 1 else 0 end
        ,j.SavingsReasonCode = null
        ,j.FareAmtLowestInPolicy =
            case sco.[is_booked_fare]
                -- When the LRF is the booked_fare
                when 1 then i.[TicketAmt]
                else case sf.[is_lowcost]
                    when 0 then
                        -- When booked and comparsion are regular airlines and the same airlines > apply ObFees
                        case when (scf.[is_lowcost] = 0 and sbf.[marketing_carrier_txt] = sco.[marketing_carrier_txt])
                            then scf.[total_amount] + i.[TicketAmtOBFees]
                            else scf.[total_amount]
                        end
                    else scf.[total_amount]
                end
            end
        ,j.AirlineCodeLowestInPolicy        = scf.[1st_seg_marketing_carrier]
        ,j.ClassOfServiceCodeLowestInPolicy = scf.[1st_seg_booking_class]
        ,j.CabinClassIDLowestInPolicy       = scf.[1st_seg_cabin_class_id]
        ,j.AirFareTypeIDLowestInPolicy      = scf.[fare_type_id]
        ,j.AirFareBasisCodeLowestInPolicy   = scf.[1st_seg_farebasis]
    from #InternalExtractEUAirFact i
    inner join #InternalExtractEUAirSavingsFact j
        on j.RecordKey = i.RecordKey
        and j.BookingTypeID = i.BookingTypeID
    inner join #lrf_cases c
        on i.RecordKey = c.RecordKey
        and i.BookingTypeID = c.BookingTypeID
    inner join #saving_comparison_fares scf
        on c.[InternalExtractID] = scf.[InternalExtractID]
        and c.[message_unique_id] = scf.[message_unique_id]
        and c.[booked_fare_number] = scf.[booked_fare_number]
        and c.[comparison_option_number] = scf.[comparison_option_number]
        -- We suppose that this is provided in the same order
        and c.[fare_number] = scf.[comparison_fare_number]
    inner join #saving_comparison_options sco
        on c.[InternalExtractID] = sco.[InternalExtractID]
        and c.[message_unique_id] = sco.[message_unique_id]
        and c.[booked_fare_number] = sco.[booked_fare_number]
        and c.[comparison_option_number] = sco.[comparison_option_number]
    inner join #saving_booked_fares sbf
        on c.[InternalExtractID] = sbf.[InternalExtractID]
        and c.[message_unique_id] = sbf.[message_unique_id]
        and c.[booked_fare_number] = sbf.[booked_fare_number]
    inner join #saving_fares sf
        on sf.[InternalExtractID] = c.[InternalExtractID]
        and sf.[message_unique_id] = c.[message_unique_id]
        and sf.[booked_fare_number] = c.[booked_fare_number]
        and sf.[fare_number] = c.[fare_number]
    where ((c.[booked_geometry_type] = 'OW' and c.[comparison_geometry_type] =  'OW') or 
           (c.[booked_geometry_type] = 'RT' and c.[comparison_geometry_type] =  'RT') or 
           (c.[booked_geometry_type] = 'OW' and c.[comparison_geometry_type] = '2OW'))

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update #InternalExtractEUAirSavingsFact savings lrf 1.)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- Case OW vs RT. Prorate method.
    update j
    set
         j.savings_rule = convert(varchar, 2) + '. || ' + c.[booked_geometry_type] + ' vs ' + c.[comparison_geometry_type]
        ,j.LowFareChosenBool = case sco.[is_booked_fare] when 1 then 1 else 0 end
        ,j.SavingsReasonCode = null
        ,j.FareAmtLowestInPolicy =
            case sco.[is_booked_fare]
                -- When the LRF is the booked_fare
                when 1 then i.[TicketAmt]
                else case sf.[is_lowcost]
                    when 0 then
                        -- When booked and comparison are regular airlines and the same airlines > apply ObFees
                        case when (scf.[is_lowcost] = 0 and sbf.[marketing_carrier_txt] = sco.[marketing_carrier_txt])
                            then ((sf.[total_amount] / i.[savings_total_flight_amount]) * scf.[total_amount]) + i.[TicketAmtOBFees]
                            else ((sf.[total_amount] / i.[savings_total_flight_amount]) * scf.[total_amount])
                        end
                    else ((sf.[total_amount] / i.[savings_total_flight_amount]) * scf.[total_amount])
                end
            end
        ,j.AirlineCodeLowestInPolicy        = scf.[1st_seg_marketing_carrier]
        ,j.ClassOfServiceCodeLowestInPolicy = scf.[1st_seg_booking_class]
        ,j.CabinClassIDLowestInPolicy       = scf.[1st_seg_cabin_class_id]
        ,j.AirFareTypeIDLowestInPolicy      = scf.[fare_type_id]
        ,j.AirFareBasisCodeLowestInPolicy   = scf.[1st_seg_farebasis]
    from #InternalExtractEUAirFact i
    inner join #InternalExtractEUAirSavingsFact j
        on j.RecordKey = i.RecordKey
        and j.BookingTypeID = i.BookingTypeID
    inner join #lrf_cases c
        on i.RecordKey = c.RecordKey
        and i.BookingTypeID = c.BookingTypeID
    inner join #saving_comparison_fares scf
        on c.[InternalExtractID] = scf.[InternalExtractID]
        and c.[message_unique_id] = scf.[message_unique_id]
        and c.[booked_fare_number] = scf.[booked_fare_number]
        and c.[comparison_option_number] = scf.[comparison_option_number]
        -- Not needed in that case OW vs RT
        -- and c.[fare_number] = scf.[comparison_fare_number]
    inner join #saving_comparison_options sco
        on c.[InternalExtractID] = sco.[InternalExtractID]
        and c.[message_unique_id] = sco.[message_unique_id]
        and c.[booked_fare_number] = sco.[booked_fare_number]
        and c.[comparison_option_number] = sco.[comparison_option_number]
    inner join #saving_booked_fares sbf
        on c.[InternalExtractID] = sbf.[InternalExtractID]
        and c.[message_unique_id] = sbf.[message_unique_id]
        and c.[booked_fare_number] = sbf.[booked_fare_number]
    inner join #saving_fares sf
        on sf.[InternalExtractID] = c.[InternalExtractID]
        and sf.[message_unique_id] = c.[message_unique_id]
        and sf.[booked_fare_number] = c.[booked_fare_number]
        and sf.[fare_number] = c.[fare_number]
    where c.[booked_geometry_type] = 'OW'
    and c.[comparison_geometry_type] = 'RT'

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update #InternalExtractEUAirSavingsFact savings lrf 2.)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- Case RT vs 2OW
    update j
    set
         j.savings_rule = convert(varchar, 3) + '. || ' + c.[booked_geometry_type] + ' vs ' + c.[comparison_geometry_type]
        ,j.LowFareChosenBool = case sco.[is_booked_fare] when 1 then 1 else 0 end
        ,j.SavingsReasonCode = null
        ,j.FareAmtLowestInPolicy =
            case sco.[is_booked_fare]
                -- When the LRF is the booked_fare
                when 1 then i.[TicketAmt]
                else case sf.[is_lowcost]
                    when 0 then
                        -- When booked and comparsion are regular airlines and the same airlines > apply ObFees
                        case when (sco.[is_lowcost] = 0 and sbf.[marketing_carrier_txt] = sco.[marketing_carrier_txt])
                            then sco.[total_amount] + i.[TicketAmtOBFees]
                            else sco.[total_amount]
                        end
                    else sco.[total_amount]
                end
            end
        ,j.AirlineCodeLowestInPolicy        = sco.[1st_seg_marketing_carrier]
        ,j.ClassOfServiceCodeLowestInPolicy = sco.[1st_seg_booking_class]
        ,j.CabinClassIDLowestInPolicy       = sco.[1st_seg_cabin_class_id]
        ,j.AirFareTypeIDLowestInPolicy      = sco.[fare_type_id]
        ,j.AirFareBasisCodeLowestInPolicy   = sco.[1st_seg_farebasis]
    from #InternalExtractEUAirFact i
    inner join #InternalExtractEUAirSavingsFact j
        on j.RecordKey = i.RecordKey
        and j.BookingTypeID = i.BookingTypeID
    inner join #lrf_cases c
        on i.RecordKey = c.RecordKey
        and i.BookingTypeID = c.BookingTypeID
    inner join #saving_comparison_options sco
        on sco.[InternalExtractID] = c.[InternalExtractID]
        and sco.[message_unique_id] = c.[message_unique_id]
        and sco.[booked_fare_number] = c.[booked_fare_number]
        and sco.[comparison_option_number] = c.[comparison_option_number]
    inner join #saving_booked_fares sbf
        on c.[InternalExtractID] = sbf.[InternalExtractID]
        and c.[message_unique_id] = sbf.[message_unique_id]
        and c.[booked_fare_number] = sbf.[booked_fare_number]
    inner join #saving_fares sf
        on sf.[InternalExtractID] = c.[InternalExtractID]
        and sf.[message_unique_id] = c.[message_unique_id]
        and sf.[booked_fare_number] = c.[booked_fare_number]
        and sf.[fare_number] = c.[fare_number]
    where c.[booked_geometry_type] = 'RT'
    and c.[comparison_geometry_type] = '2OW'

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update #InternalExtractEUAirSavingsFact savings lrf 3.)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- _____________________________________________________________________________________________________________
    -- PUBLISHED Logic BEGIN
    -- Dedup Lowest Price by Flexibility Level
    -- _____________________________________________________________________________________________________________
    select
         i.RecordKey
        ,i.BookingTypeID
        ,pub.[InternalExtractID]
        ,pub.[message_unique_id]
        ,pub.[booked_fare_number]
        ,s.[fare_number]
        ,s.[geometry_type] as [booked_geometry_type]
        ,pub.[comparison_option_number]
        ,pub.[geometry_type] as [comparison_geometry_type]
        ,pub.[flexibility_level]
        ,pub.total_amount
    into #pub_cases
    from #InternalExtractEUAirFact i
    inner join #saving_fares s
        on s.[InternalExtractID] = i.[savings_InternalExtractID]
        and s.[message_unique_id] = i.[savings_message_unique_id]
        and s.[booked_fare_number] = i.[savings_booked_fare_number]
        and s.[fare_number] = i.[savings_fare_number]
    inner join (
        select
        sub.*
        from (
            select
                row_number() over (partition by InternalExtractID, message_unique_id, booked_fare_number, flexibility_level order by total_amount asc) as [ROWNUM]
                ,*
            from #saving_comparison_options
            where is_lowest_recommended_fare = 0
            and isnull(fare_type, 'UNKNOWN') = 'PUBLISHED'
            ) sub
            where sub.ROWNUM = 1
        ) pub
        on pub.InternalExtractID = i.[savings_InternalExtractID]
        and pub.[message_unique_id] = i.[savings_message_unique_id]
        and pub.[booked_fare_number] = i.[savings_booked_fare_number]   
    where i.savings_match = 1

    create nonclustered index ix_pub_cases on #pub_cases (RecordKey, BookingTypeID) include (booked_geometry_type, comparison_geometry_type)

    -- Published Fare: Case RT vs RT or OW vs OW or OW vs 2OW
    update j
    set
         j.FareAmtPublished =
             case sf.[is_lowcost]
                 when 1 then i.TicketAmt
                 else convert(money, scf.[total_amount]) * convert(money, i.[savings_amt_percentage_diff]) / convert(money, 100) + i.[TicketAmtOBFees]
             end
        ,j.AirFareBasisCodePublished =
             case sf.[is_lowcost]
                 when 1 then null
                 else scf.[1st_seg_farebasis]
             end
        ,j.ClassOfServiceCodePublished = 
             case sf.[is_lowcost]
                 when 1 then null
                 else scf.[1st_seg_booking_class]
             end
        ,j.CabinClassIDPublished = scf.[1st_seg_cabin_class_id]
    from #InternalExtractEUAirFact i
    inner join #InternalExtractEUAirSavingsFact j
        on j.RecordKey = i.RecordKey
        and j.BookingTypeID = i.BookingTypeID
    inner join #pub_cases c
        on i.RecordKey = c.RecordKey
        and i.BookingTypeID = c.BookingTypeID
    inner join #saving_comparison_fares scf
        on c.[InternalExtractID] = scf.[InternalExtractID]
        and c.[message_unique_id] = scf.[message_unique_id]
        and c.[booked_fare_number] = scf.[booked_fare_number]
        and c.[comparison_option_number] = scf.[comparison_option_number]
        -- We suppose that this is provided in the same order
        and c.[fare_number] = scf.[comparison_fare_number]
    inner join #saving_comparison_options sco
        on c.[InternalExtractID] = sco.[InternalExtractID]
        and c.[message_unique_id] = sco.[message_unique_id]
        and c.[booked_fare_number] = sco.[booked_fare_number]
        and c.[comparison_option_number] = sco.[comparison_option_number]
    inner join #saving_booked_fares sbf
        on c.[InternalExtractID] = sbf.[InternalExtractID]
        and c.[message_unique_id] = sbf.[message_unique_id]
        and c.[booked_fare_number] = sbf.[booked_fare_number]
    inner join #saving_fares sf
        on sf.[InternalExtractID] = c.[InternalExtractID]
        and sf.[message_unique_id] = c.[message_unique_id]
        and sf.[booked_fare_number] = c.[booked_fare_number]
        and sf.[fare_number] = c.[fare_number]
    where ((c.[booked_geometry_type] = 'OW' and c.[comparison_geometry_type] =  'OW') or 
           (c.[booked_geometry_type] = 'RT' and c.[comparison_geometry_type] =  'RT') or 
           (c.[booked_geometry_type] = 'OW' and c.[comparison_geometry_type] = '2OW'))
    and sf.[flexibility_level] = scf.[flexibility_level]
    and sf.[flexibility_level] <> 'UNKNONW'
    and scf.[flexibility_level] <> 'UNKNONW'

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update #InternalExtractEUAirSavingsFact savings pub 1.)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- Case OW vs RT. Prorate method.
    update j
    set
         j.FareAmtPublished =
            case sco.[is_booked_fare]
                -- When the LRF is the booked_fare
                when 1 then i.[TicketAmt]
                else case sf.[is_lowcost]
                    when 0 then
                        -- When booked and comparison are regular airlines and the same airlines > apply ObFees
                        case when (scf.[is_lowcost] = 0 and sbf.[marketing_carrier_txt] = sco.[marketing_carrier_txt])
                            then ((sf.[total_amount] / i.[savings_total_flight_amount]) * (convert(money, scf.[total_amount]) * convert(money, i.[savings_amt_percentage_diff]) / convert(money, 100))) + i.[TicketAmtOBFees]
                            else ((sf.[total_amount] / i.[savings_total_flight_amount]) * (convert(money, scf.[total_amount]) * convert(money, i.[savings_amt_percentage_diff]) / convert(money, 100)))
                        end
                    else ((sf.[total_amount] / i.[savings_total_flight_amount]) * (convert(money, scf.[total_amount]) * convert(money, i.[savings_amt_percentage_diff]) / convert(money, 100)))
                end
            end
        ,j.AirFareBasisCodePublished =
             case sf.[is_lowcost]
                 when 1 then null
                 else scf.[1st_seg_farebasis]
             end
        ,j.ClassOfServiceCodePublished = 
             case sf.[is_lowcost]
                 when 1 then null
                 else scf.[1st_seg_booking_class]
             end
        ,j.CabinClassIDPublished = scf.[1st_seg_cabin_class_id]
    from #InternalExtractEUAirFact i
    inner join #InternalExtractEUAirSavingsFact j
        on j.RecordKey = i.RecordKey
        and j.BookingTypeID = i.BookingTypeID
    inner join #pub_cases c
        on i.RecordKey = c.RecordKey
        and i.BookingTypeID = c.BookingTypeID
    inner join #saving_comparison_fares scf
        on c.[InternalExtractID] = scf.[InternalExtractID]
        and c.[message_unique_id] = scf.[message_unique_id]
        and c.[booked_fare_number] = scf.[booked_fare_number]
        and c.[comparison_option_number] = scf.[comparison_option_number]
        -- Not needed in that case OW vs RT
        -- and c.[fare_number] = scf.[comparison_fare_number]
    inner join #saving_comparison_options sco
        on c.[InternalExtractID] = sco.[InternalExtractID]
        and c.[message_unique_id] = sco.[message_unique_id]
        and c.[booked_fare_number] = sco.[booked_fare_number]
        and c.[comparison_option_number] = sco.[comparison_option_number]
    inner join #saving_booked_fares sbf
        on c.[InternalExtractID] = sbf.[InternalExtractID]
        and c.[message_unique_id] = sbf.[message_unique_id]
        and c.[booked_fare_number] = sbf.[booked_fare_number]
    inner join #saving_fares sf
        on sf.[InternalExtractID] = c.[InternalExtractID]
        and sf.[message_unique_id] = c.[message_unique_id]
        and sf.[booked_fare_number] = c.[booked_fare_number]
        and sf.[fare_number] = c.[fare_number]
    where c.[booked_geometry_type] = 'OW'
    and c.[comparison_geometry_type] = 'RT'
    and sf.[flexibility_level] = scf.[flexibility_level]
    and sf.[flexibility_level] <> 'UNKNONW'
    and scf.[flexibility_level] <> 'UNKNONW'

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update #InternalExtractEUAirSavingsFact savings pub 2.)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- Published Fare: Case RT vs 2OW
    update j
    set
         j.FareAmtPublished =
             case sf.[is_lowcost]
                 when 1 then i.TicketAmt
                 else convert(money, sco.[total_amount]) * convert(money, i.[savings_amt_percentage_diff]) / convert(money, 100) + i.[TicketAmtOBFees]
             end
        ,j.AirFareBasisCodePublished =
             case sf.[is_lowcost]
                 when 1 then null
                 else sco.[1st_seg_farebasis]
             end
        ,j.ClassOfServiceCodePublished = 
             case sf.[is_lowcost]
                 when 1 then null
                 else sco.[1st_seg_booking_class]
             end
        ,j.CabinClassIDPublished = sco.[1st_seg_cabin_class_id]
    from #InternalExtractEUAirFact i
    inner join #InternalExtractEUAirSavingsFact j
        on j.RecordKey = i.RecordKey
        and j.BookingTypeID = i.BookingTypeID
    inner join #pub_cases c
        on i.RecordKey = c.RecordKey
        and i.BookingTypeID = c.BookingTypeID
    inner join #saving_comparison_options sco
        on sco.[InternalExtractID] = c.[InternalExtractID]
        and sco.[message_unique_id] = c.[message_unique_id]
        and sco.[booked_fare_number] = c.[booked_fare_number]
        and sco.[comparison_option_number] = c.[comparison_option_number]
    inner join #saving_booked_fares sbf
        on c.[InternalExtractID] = sbf.[InternalExtractID]
        and c.[message_unique_id] = sbf.[message_unique_id]
        and c.[booked_fare_number] = sbf.[booked_fare_number]
    inner join #saving_fares sf
        on sf.[InternalExtractID] = c.[InternalExtractID]
        and sf.[message_unique_id] = c.[message_unique_id]
        and sf.[booked_fare_number] = c.[booked_fare_number]
        and sf.[fare_number] = c.[fare_number]
    where c.[booked_geometry_type] = 'RT'
    and c.[comparison_geometry_type] = '2OW'
    and sbf.[flexibility_level] = sco.[flexibility_level]
    and sbf.[flexibility_level] <> 'UNKNONW'
    and sco.[flexibility_level] <> 'UNKNONW'

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update #InternalExtractEUAirSavingsFact savings pub 3.)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- Negates when NoLRF or LRF > TicketAmt
    update j
    set
        j.LowFareChosenBool = null
       ,j.SavingsReasonCode = null
       ,j.FareAmtLowestInPolicy = i.TicketAmt
       ,j.AirlineCodeLowestInPolicy = i.AirLineCode
       ,j.ClassOfServiceCodeLowestInPolicy = i.ClassOfServiceCode
       ,j.CabinClassIDLowestInPolicy = i.CabinClassID
       ,j.AirFareTypeIDLowestInPolicy = i.AirFareTypeID
       ,j.AirFareBasisCodeLowestInPolicy = i.AirFareBasisCode
    -- ,j.FareAmtPublished = null
    -- ,j.AirFareBasisCodePublished = null
    -- ,j.ClassOfServiceCodePublished = null
    -- ,j.CabinClassIDPublished = null
    -- ,j.FareAmtNegotiated = null
    -- ,j.AirFareBasisCodeNegotiated = null
    -- ,j.CabinClassIDNegotiated = null
       ,j.savings_rule =
            case when isnull(j.FareAmtLowestInPolicy, 0) = 0 then '4. || Negates: Empty'
            else '4. || Negates: LRF > Amt'
            end
    from #InternalExtractEUAirFact i
    inner join #InternalExtractEUAirSavingsFact j
        on j.RecordKey = i.RecordKey
        and j.BookingTypeID = i.BookingTypeID
    where isnull(j.FareAmtLowestInPolicy, 0) = 0
    or isnull(j.FareAmtLowestInPolicy, -1) > i.TicketAmt

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update #InternalExtractEUAirSavingsFact negates)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    ------------------------------------------------------------
    -- Savings Logic End
    ------------------------------------------------------------

    update a
    set a.SalesDocumentCode = b.SalesDocumentCode,
        a.SalesDocumentLineNbr = b.SalesDocumentLineNbr,
        a.BookingTypeID = b.BookingTypeID,
        a.RecordKey= b.RecordKey,
        a.TravelProductID = b.TravelProductID,
        a.TUIDTraveler = b.TUIDTraveler,
        a.MetaDossierID = b.MetaDossierID,
        a.TicketCodePrior = b.TicketCodePrior,
        a.InvoiceDate = b.InvoiceDate,
        a.CurrencyCode = b.CurrencyCode,
        a.TicketAmt = b.TicketAmt,
        a.TicketAmtBase = b.TicketAmtBase,
        a.TicketAmtTax = b.TicketAmtTax,
        a.TicketAmtTaxVAT = b.TicketAmtTaxVAT,
        a.TicketAmtTaxFee = b.TicketAmtTaxFee,
        a.TicketAmtExchangePenalty = coalesce(b.TicketAmtExchangePenalty,0),
        a.TicketAmtCommission = b.TicketAmtCommission,
        a.TicketAmtMarkup = b.TicketAmtMarkup,
        a.TicketAmtMarkupGds = b.TicketAmtMarkupGds,
        a.TicketAmtMarkupPub = b.TicketAmtMarkupPub,
        a.TicketAmtGross = b.TicketAmtGross,
        a.TicketAmtOBFees = b.TicketAmtOBFees,
        a.FSFAmtSavings = b.FSFAmtSavings,
        a.LowFareChosenBool = case when (a.InvoiceDate < @savings_activation_date) then b.LowFareChosenBool else c.LowFareChosenBool end,
        a.SavingsReasonCode = case when (a.InvoiceDate < @savings_activation_date) then b.SavingsReasonCode else c.SavingsReasonCode end,
        a.FareAmtLowestInPolicy = case when (a.InvoiceDate < @savings_activation_date) then b.FareAmtLowestInPolicy else c.FareAmtLowestInPolicy end,
        a.AirlineCodeLowestInPolicy = case when (a.InvoiceDate < @savings_activation_date) then b.AirlineCodeLowestInPolicy else c.AirlineCodeLowestInPolicy end,
        a.ClassOfServiceCodeLowestInPolicy = case when (a.InvoiceDate < @savings_activation_date) then b.ClassOfServiceCodeLowestInPolicy else c.ClassOfServiceCodeLowestInPolicy end,
        a.CabinClassIDLowestInPolicy = case when (a.InvoiceDate < @savings_activation_date) then b.CabinClassIDLowestInPolicy else c.CabinClassIDLowestInPolicy end,
        a.AirFareTypeIDLowestInPolicy = case when (a.InvoiceDate < @savings_activation_date) then b.AirFareTypeIDLowestInPolicy else c.AirFareTypeIDLowestInPolicy end,
        a.AirFareBasisCodeLowestInPolicy = case when (a.InvoiceDate < @savings_activation_date) then b.AirFareBasisCodeLowestInPolicy else c.AirFareBasisCodeLowestInPolicy end,
        a.FareAmtPublished = case when (a.InvoiceDate < @savings_activation_date) then b.FareAmtPublished else c.FareAmtPublished end,
        a.AirFareBasisCodePublished = case when (a.InvoiceDate < @savings_activation_date) then b.AirFareBasisCodePublished else c.AirFareBasisCodePublished end,
        a.ClassOfServiceCodePublished = case when (a.InvoiceDate < @savings_activation_date) then b.ClassOfServiceCodePublished else c.ClassOfServiceCodePublished end,
        a.CabinClassIDPublished = case when (a.InvoiceDate < @savings_activation_date) then b.CabinClassIDPublished else c.CabinClassIDPublished end,
        a.FareAmtNegotiated = case when (a.InvoiceDate < @savings_activation_date) then b.FareAmtNegotiated else c.FareAmtNegotiated end,
        a.AirFareBasisCodeNegotiated = case when (a.InvoiceDate < @savings_activation_date) then b.AirFareBasisCodeNegotiated else c.AirFareBasisCodeNegotiated end,
        a.CabinClassIDNegotiated = case when (a.InvoiceDate < @savings_activation_date) then b.CabinClassIDNegotiated else c.CabinClassIDNegotiated end
    from dbo.InternalExtractEUAirFact a
    inner join #InternalExtractEUAirFact b
        on a.RecordKey = b.RecordKey
        and a.BookingTypeID = b.BookingTypeID
    left outer join #InternalExtractEUAirSavingsFact c
        on a.RecordKey = c.RecordKey
        and a.BookingTypeID = c.BookingTypeID
     where a.InternalExtractID = @pInternalExtractID

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update dbo.InternalExtractEUAirFact final)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    update a
       set a.LowFareChosenBool = b.LowFareChosenBool,
           a.SavingsReasonCode = b.SavingsReasonCode,
           a.FareAmtLowestInPolicy = b.FareAmtLowestInPolicy,
           a.AirlineCodeLowestInPolicy = b.AirlineCodeLowestInPolicy,
           a.ClassOfServiceCodeLowestInPolicy = b.ClassOfServiceCodeLowestInPolicy,
           a.CabinClassIDLowestInPolicy = b.CabinClassIDLowestInPolicy,
           a.AirFareTypeIDLowestInPolicy = b.AirFareTypeIDLowestInPolicy,
           a.AirFareBasisCodeLowestInPolicy = b.AirFareBasisCodeLowestInPolicy,
           a.FareAmtPublished = b.FareAmtPublished,
           a.AirFareBasisCodePublished = b.AirFareBasisCodePublished,
           a.ClassOfServiceCodePublished = b.ClassOfServiceCodePublished,
           a.CabinClassIDPublished = b.CabinClassIDPublished,
           a.FareAmtNegotiated = b.FareAmtNegotiated,
           a.AirFareBasisCodeNegotiated = b.AirFareBasisCodeNegotiated,
           a.CabinClassIDNegotiated = b.CabinClassIDNegotiated,
           a.savings_rule = b.savings_rule,
           a.savings_InternalExtractID = b.savings_InternalExtractID,
           a.savings_message_unique_id = b.savings_message_unique_id,
           a.savings_booked_fare_number = b.savings_booked_fare_number,
           a.savings_fare_number = b.savings_fare_number
    from dbo.InternalExtractEUAirSavingsFact a
    inner join #InternalExtractEUAirSavingsFact b
        on a.RecordKey = b.RecordKey
        and a.BookingTypeID = b.BookingTypeID
     where a.InternalExtractID = @pInternalExtractID

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update dbo.InternalExtractEUAirSavingsFact final)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    ----------------------------------------------------------------------
    --  SegmentValue
    ----------------------------------------------------------------------
    update dbo.InternalExtractEUAirSegmentFact
       set SegmentValue = case when coalesce(C.MileageCntTotal,0.0) <> 0 then (a.MileageCnt/C.MileageCntTotal) * b.TicketAmtBase else null end
      from dbo.InternalExtractEUAirSegmentFact a
           inner join
           dbo.InternalExtractEUAirFact b
               on a.InternalExtractID = b.InternalExtractID and
                  a.RecordKey = b.RecordKey and
                  a.BookingTypeID = b.BookingTypeID
           inner join (select SubQ.InternalExtractID,
                              SubQ.RecordKey,
                              SubQ.BookingTypeID,
                              case
                                  when count(1) = count(SubQ.MileageCnt) then sum(SubQ.MileageCnt)
                                  else null
                              end as MileageCntTotal
                         from dbo.InternalExtractEUAirSegmentFact SubQ
                         where SubQ.InternalExtractID = @pInternalExtractID
                         group by InternalExtractID,
                                  RecordKey,
                                  BookingTypeID) as C
               on a.InternalExtractID = C.InternalExtractID and
                  a.RecordKey = C.RecordKey and
                  a.BookingTypeID = C.BookingTypeID
    where a.InternalExtractID = @pInternalExtractID
    
    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update InternalExtractEUAirSegmentFact SegmentValue)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    ----------------------------------------------------------------------
    --  FlightValue
    ----------------------------------------------------------------------
    update dbo.InternalExtractEUAirFlightFact
       set FlightValue = B.FlightValue
       from dbo.InternalExtractEUAirFlightFact a
            inner join 
            (select a.InternalExtractID, a.RecordKey, a.BookingTypeID, a.TripNbR,
                    case when count(1) = count(b.SegmentValue) then sum(b.SegmentValue) else null end as FlightValue    
              from dbo.InternalExtractEUAirFlightFact a 
                   inner join 
                   dbo.InternalExtractEUAirSegmentFact b on
                       a.InternalExtractID = b.InternalExtractID and
                       a.RecordKey = b.RecordKey and
                       a.BookingTypeID = b.BookingTypeID and
                       a.TripNbR = b.TripNbr
             where a.InternalExtractID = @pInternalExtractID
             group by a.InternalExtractID, a.RecordKey, a.BookingTypeID, a.TripNbR) as B on
                   a.InternalExtractID = B.InternalExtractID and
                   a.RecordKey = B.RecordKey and     
                   a.BookingTypeID = B.BookingTypeID and
                   a.TripNbR = B.TripNbr 
    where a.InternalExtractId = @pInternalExtractID 

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update InternalExtractEUAirFlightFact FlightValue)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    if ((@TrancountSave = 0)
        and (@TranStartedBool = @TRUE))
    commit transaction @SavePointName

goto ExitProc

---------------------------------------------------------------------
-- Error Handler
---------------------------------------------------------------------
ErrorHandler:
if (@TranStartedBool = @TRUE)
    rollback transaction @SavePointName
select @ExitCode = @RC_FAILURE
goto ExitProc

---------------------------------------------------------------------
-- Exit Procedure
---------------------------------------------------------------------
ExitProc:
return (@ExitCode)
go
