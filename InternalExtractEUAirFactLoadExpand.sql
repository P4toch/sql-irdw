if object_id('dbo.InternalExtractEUAirFactLoadExpand') is null
begin
    print 'Creating stored procedure InternalExtractEUAirFactLoadExpand (placeholder)'
    execute ('create procedure dbo.InternalExtractEUAirFactLoadExpand as return 0')
end
go

print 'Altering stored procedure InternalExtractEUAirFactLoadExpand'
go

alter procedure dbo.InternalExtractEUAirFactLoadExpand @pInternalExtractID int
as

/*
*********************************************************************
Copyright (C) 2014-2018 Expedia, Inc. All rights reserved.

Description:
     Updates InternalExtractEUAirFact with most attributes
     from the expanded/flattened BookingItem

Notes:

Result Set:
    None

Return values:
    0     Success
    -100  Failure

Error codes:

Change History:
    Date        Author          Description
    ----------  --------------- ------------------------------------
    2014-05-06  Jared Ko        Created
    2014-09-30  rakarnati       EGE-72755 - Removed the attribute reference that is not populated here
    2015-03-23  JaredKo         EGE-82560 - EU Air bookings have offlinebookingTypeID of 5 while
                                    offlineBookingTypeDim table only has definition for 0, 1 and 2
    2015-04-13  JaredKo         EGE-83168 - EU Air Rewrite: Integrate AirFareType updates
    2015-06-04  JaredKo         Performance Optimizations
    2015-06-10  rkarnati        EGE-87758 - Exchange Penalty linkage for Q1 2014 where TicketCode and TicketPrior was NOT available in Navision.
                                        So setting AEP from 01/01/2014
    2015-06-29  JaredKo         Added code to lookup PolicyStatusID for invoices in 2014Q1 as these used a
                                    different algorithm. This allows the use of "Like/Match" calculations.
    2015-06-30  JaredKo         Removed (commented out) Credit Card 6/4 calculation pending validation of algorithm.
    2015-10-16  JaredKo         EGE-91086 - Performance improvements after additional Navision data
                                    added and/or PK column changes
    2015-10-16  JaredKo         EGE-95314 - Add support for new EMD exchange penalty data
    2015-11-11  jappleberry     EGE-100455 - Data fix for missing GroupAccountDepartmentID in RS2 AirFact and AllLOB tables.
                                 Clense Routine remove carriage return; remove line feed; replace double quote with single quote; 
                                 replace comma withn space, remove space ; remove tab
    2016-02-17  minieto         EGE-104136 - Investigate CreditCardTypeID 254 in EUAir
    2016-02-07  minieto         EGE-106772 - Fix NULL GroupAccountDepartmentID
    2016-03-04  GKoneru         EGE-98735 -  AIR + Hotel booking if hotel is OOP then AIR booking also displaying OOP Reason code in Air
    2016-08-25  JaredKo         EGE-124470 - Improve ETL Processing Time
    2017-01-25  JaredKo         EGE-124470 - Improve ETL Processing Time
    2017-01-31  jappleberry     EGE-138798 BackOffice Air Redesign: 
                                 Air Integration Process in Navision 
    2017-03-02  jappleberry     EGE-142883 - Set Exchange Policy to Unknown 
    2018-02-15  nrasmussen      EGE-181461 - Set IncrementCnt to zero for bookingtypeid 1 for amountincluding_vat + PRIX_PUBLIC = 0
    2018-02-26  pbressan        EGE-179499 - Credit Card Infos for EU transactions
    2018-04-10  nrasmussen      EGE-189992 IRD - Enrich an agent assisted transaction with Travel Consultant TUID
    2018-05-16  nrasmussen      EGE-195465 IRD - replaced table event_log with event_log_eu_agentid 
    2018-05-21  pbressan        Jira.EGE-195410 Add column PricingEntry
    2018-06-07  nrasmussen      EGE-200489 IRD moving eu agentid section due to dataflow issues
	2018-07-31  gurprsingh      Updated for Bug #197356
    2018-08-09  manzeno         Credit Card Info / CentralBillBool CTE to tempTable. The performance of the sproc degraded
    2018-08-17  nrasmussen      Jira.EGE-204392 Add column LowCostBool
    2018-06-22  pbressan        Jira.EGE-199218 Add logic for FSF related columns
    2018-10-03  nrasmussen      Jira.EGE-217181 IRD EU AgentID fix MetaDossierID overflow for int datatype when joining to EctWeb data
*********************************************************************
*/

set nocount on

---------------------------------------------------------------------
-- Declarations
---------------------------------------------------------------------


declare @FALSE tinyint, -- Standard constants and variables
        @TRUE tinyint,
        @RC_FAILURE int,
        @RC_SUCCESS int,
        @Current_Timestamp datetime,
        @Error int,
        @ErrorCode int,
        @ErrorLine int,
        @ExitCode int,
        @ProcedureName sysname,
        @RC int,            -- Return code from called SP
        @Rowcount int,
        @SavePointName varchar(32),
        @TranStartedBool tinyint,
        @TrancountSave int,
        @NonNumericString varchar(10)


declare @ERRUNEXPECTED int, -- Error message constants
        @ERRPARAMETER int,
        @MsgParm1 varchar(100),
        @MsgParm2 varchar(100),
        @MsgParm3 varchar(100)


declare @BSIAir tinyint, -- SP specific constants and variables
        @BookingTypeIDPurchase tinyint,
        @BookingTypeIDReserve tinyint,
        @BookingTypeIDVoid tinyint,
        @BookingTypeIDExchange tinyint,
        @BookingTypeIDRefund tinyint,
        @BookingTypeIDPartialRefund tinyint,
        @BookingTypeIDUTT tinyint,
        @TimeZoneIDFrom int,
        @CcDefLabel varchar(30), -- for CustomDataElementTxt
        @FtDefLabel varchar(30), -- for CustomDataElementTxt
        @AirFareTypeRuleGroupID smallint = 0


declare @FactRecordStatusID_SUB tinyint,
        @FactRecordStatusID_AEP tinyint,
        @FactRecordStatusID_ANC tinyint


---------------------------------------------------------------------
-- Initializations
---------------------------------------------------------------------

select   -- Standard constants
    @FALSE                          = 0,
    @TRUE                           = 1,
    @RC_FAILURE                     = -100,
    @RC_SUCCESS                     = 0,
    -- @NonNumericString is used to find integer values through exclusion
    -- Example: SELECT @ID = ID FROM tbl WHERE ID not like @NonNumericString
    -- Translated: Rows WHERE ID does NOT include values that are NOT 0-9.
    @NonNumericString               = '%[^0-9]%'

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
    @BSIAir = 7,
    @BookingTypeIDPurchase                = 1,
    @BookingTypeIDReserve                 = 3,
    @BookingTypeIDVoid                    = 4,
    @BookingTypeIDExchange                = 5,
    @BookingTypeIDRefund                  = 7,
    @BookingTypeIDPartialRefund           = 8,
    @BookingTypeIDUTT                     = 9,
    @TimeZoneIDFrom                       = 51,
    @FactRecordStatusID_SUB               = 12, -- Duplicate Record
    @FactRecordStatusID_AEP               = 11, -- Air Exchange Penalty
    @FactRecordStatusID_ANC               = 13  -- Air/Train Ancillary Service

select    -- for CustomDataElementTxt
    @CcDefLabel = 'Cost Center',
    @FtDefLabel = 'Free Text'

-- @Rules_Single used for looping to process AirFareTypeID
declare @Rules_Single table (ColumnName varchar(30), comparison varchar(10), Value varchar(100))

-- Mapping Navision Credit Card Type to RS2 Credit Card Type
-- All non-matches are 254  -- Other
declare @CreditCardMap table(Nav_CreditCardTypeID tinyint primary key, CreditCardTypeID tinyint)
    insert @CreditCardMap
              select           0 , 0      -- Visa
    union all select 1 , 1      -- Mastercard
    union all select 2 , 11     -- Carte Bleue
    union all select 3 , 2      -- American Express
    union all select 4 , 4      -- Diners
    union all select 5 , 3      -- Discover
    union all select 6 , 8      -- JCB
    union all select 7 , 8      -- JCB
    union all select 8 , 254    -- Other
    union all select 9 , 254    -- Other
    union all select 10, 254    -- Other
    union all select 11, 254    -- Other
    union all select 12, 5      -- Airplus
    union all select 13, 12     -- Maestro
    union all select 14, 100    -- China Union Pay

---------------------------------------------------------------------
-- Validation
---------------------------------------------------------------------
-- Make sure we've coded for all comparison operators
if exists(select * from dbo.AirFareTypeRuleGroupDetails d
          where d.Comparison not in ('LIKE', '=', 'NOT LIKE', '<>', 'IN', 'MATCH'))
begin
    select @ErrorCode   = @ERRUNEXPECTED,
            @MsgParm1    = '(Checking AirFareTypeRuleGroupDetails for unknown comparison operators)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end


---------------------------------------------------------------------
-- None, done by caller
---------------------------------------------------------------------
-- Processing
---------------------------------------------------------------------

    -- create table
    select *
    into dbo.#InternalExtractEUAirFact
    from dbo.InternalExtractEUAirFact
    where 1 = 2

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (create dbo.#InternalExtractEUAirFact)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- ensure datatype consistency for metadossierid
    alter table dbo.#InternalExtractEUAirFact alter column metadossierid varchar(20)

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (alter dbo.#InternalExtractEUAirFact)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    insert into dbo.#InternalExtractEUAirFact
    select *
    from dbo.InternalExtractEUAirFact
    where InternalExtractID = @pInternalExtractID

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (select into dbo.#InternalExtractEUAirFact)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    alter table dbo.#InternalExtractEUAirFact add CostCenterIDMain int

    create clustered index temp_ix1 on dbo.#InternalExtractEUAirFact (SalesDocumentCode, SalesDocumentLineNbr, BookingTypeID, InternalExtractID, MetaDossierID)

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (index create on dbo.#InternalExtractEUAirFact)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    create index temp_ix2 on dbo.#InternalExtractEUAirFact (RecordKey)

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (index create on dbo.#InternalExtractEUAirFact)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    --------------
    -- Flatten
    --------------

    update dbo.#InternalExtractEUAirFact
       set CustomerSystemID = 2, -- ECT EU
           TUIDAccount = l.Traveller_Code, -- ToDo: Need to verify - Is the Booking TUID?
           TUIDTraveler = l.Traveller_Code,
           PNRCode = ltrim(rtrim(l.ID_2)),
           AirlineCode = case when len(l.Corporation) > 2 then 'YY'
                              else coalesce(l.Corporation,'YY')
                         end,
           TicketCode = ltrim(rtrim(l.Ticket)),
           TicketCodePrior = l.Originalticket,
           TicketTypeID = case
                            when l.E_ticket = 1 then 2 -- E-Ticket
                                                else 1 -- Paper Ticket
                            end,
           ETicketRefusedBool = case
                                    when l.E_ticket <> 1 and -- Paper Ticket
                                         l.E_Ticket_Refused = 1 then 1
                                                                else 0
                                    end,
           GroupTicketBool = case
                               when l.[group] = 1 then 1
                                                  else 0
                             end,
           IssueDate = dateadd(second, l.IncrementOrder,
                           case
                               when l.Issued_Date is null or
                               year(l.Issued_Date) in ('1753', '1900')
                                   then l.Posting_Date
                               else l.Issued_Date
                           end
                           ),
            MetaDossierID = l.Meta_ID,
            AgentErrorBool = l.ECTERROR,
            DirectPaymentBool = isnull(l.RTU, 0),

            --InvoiceDate = dateadd(hour, 12, h.Posting_Date),
            -- ToDo: Determine BookingSourceID. Shouldn't be Service_Group
            BookingSourceID = case l.Service_Group
                                    when 1 then 3
                                    when 6 then 4
                                    else 3
                              end,
            TransactionCnt = 1,
            IncrementCnt =
                    case
                        when f.BookingTypeID  = @BookingTypeIDPurchase
                            then
                                case 
                                    when (l.Amount_Including_VAT + l.PRIX_PUBLIC) = 0
                                        then 0
                                        else 1
                                end
                        when f.BookingTypeID in (@BookingTypeIDExchange, @BookingTypeIDPartialRefund)
                            then 0
                        when f.BookingTypeID in (@BookingTypeIDVoid, @BookingTypeIDRefund)
                            then -1
                    end,
            -- Table dbo.Nav_Tour_Code___FT no longer available
            --TourCode = (select top 1 left(Tour_Code, 50)
            --            from dbo.Nav_Tour_Code___FT tc
            --            where
            --                  tc.File_Name = l.ID_1 and
            --                  tc.Code_PNR = l.ID_2 and
            --                  tc.TravelProductID = l.TravelProductID
            --            order by tc.InternalExtractID desc
            --            )
            --Now Use dbo.Nav_Import_Air Tag = 'FT'
            TourCode = (select top 1 substring(substring((a.Text1), 1, 
                          case charindex(';', (a.Text1)) when 0 then 150 else charindex(';', (a.Text1)) end - 
                          case charindex(';', (a.Text1)) when 0 then 0 else 1 end), 3, 150) 
                          from dbo.Nav_Import_Air a
                         where
						      a.Tag = 'FT' and
                              a.FileName = l.ID_1 and
                              a.Code_PNR = l.ID_2 and
                              a.TravelProductID = l.TravelProductID
                         order by a.InternalExtractID desc
                        ),
            PricingEntry = (
                        select top 1 substring(replace(replace(reverse(substring(reverse(a.[Text1]), 1,
                            case charindex(';', reverse(a.[Text1])) when 0 then 150
                            else charindex(';', reverse(a.[Text1])) - 1 end)), ' ', ''), 'UU', 'U'), 1, 100)
                          from dbo.Nav_Import_Air a
                         where
                              a.Tag = 'Q-' and
                              a.FileName = l.ID_1 and
                              a.Code_PNR = l.ID_2 and
                              a.TravelProductID = l.TravelProductID
                         order by a.InternalExtractID desc
                         ),
            LowCostBool = case l.Service_Group
                              when 6 then 1 
                              else 0
                          end
        from dbo.#InternalExtractEUAirFact f
             inner join
             dbo.nav_Sales_Line(@pInternalExtractID) l
                 on l.InternalExtractID = f.InternalExtractID and
                    l.TravelProductID = f.TravelProductID and
                    l.Document_No_ = f.SalesDocumentCode and
                    l.Line_No_ = f.SalesDocumentLineNbr
             left join
             dbo.Nav_Travel_Ledger_Entry tle
                 on tle.InternalExtractID = l.InternalExtractID and
                    tle.TravelProductID = l.TravelProductID and
                    tle.Document_No_ = l.Document_No_ and
                    tle.Ticket_No = ltrim(rtrim(l.Ticket))
       where f.InternalExtractID = @pInternalExtractID

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update dbo.#InternalExtractEUAirFact initial expansion)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    ------------------------------------------------------------
    -- OnlineBool, OfflineBookingTypeID, AgentAssistedBool
    ------------------------------------------------------------
    -- Note: OnlineBool is the inverse of AgentAssistedBool. If one is 0 then the other is 1.
    update dbo.#InternalExtractEUAirFact
    set OnlineBool = case when f.SalesDocumentTypeID = 2 then 0 -- Credits are always offline / agent-assisted
                          when l.ExchangeTicket = 1 then 0
                          when l.Reservation_Mode = 2 then 1
                          else 0
                     end,
        AgentAssistedBool = case when f.SalesDocumentTypeID = 2 then 1 -- Credits are always offline / agent-assisted
                                 when l.ExchangeTicket = 1 then 1
                                 when l.Reservation_Mode = 2 then 0
                            else 1
                            end,
        OfflineBookingTypeID = case when l.Reservation_Mode = 2 then null
                                    when exists(select *
                                                  from dbo.Nav_IMPORT_METAID_FIELD_VALUE v
                                                 where v.METAID = f.MetaDossierID and
                                                       v.TravelProductID = f.TravelProductID and
                                                       v.FLD_KEY = 'TRAVEL_CONSULTANT')
                                                                then 1
                                    when exists(select *
                                                  from dbo.Nav_IMPORT_METAID_FIELD_VALUE v
                                                 where v.METAID = f.MetaDossierID and
                                                       v.TravelProductID = f.TravelProductID)
                                                                then 0
                                    else 2
                                    end
        from dbo.#InternalExtractEUAirFact f
             inner join
             dbo.nav_Sales_Line(@pInternalExtractID) l
                 on l.InternalExtractID = f.InternalExtractID and
                    l.TravelProductID = f.TravelProductID and
                    l.Document_No_ = f.SalesDocumentCode and
                    l.Line_No_ = f.SalesDocumentLineNbr


    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update dbo.#InternalExtractEUAirFact Online/Offline/Agent Info)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    ------------------------------------------------------------
    -- Arranger
    ------------------------------------------------------------
    update dbo.#InternalExtractEUAirFact
    set TUIDArranger = (select top 1 v.FLD_VALUE
                          from dbo.Nav_IMPORT_METAID_FIELD_VALUE v
                         where v.METAID = f.MetaDossierID and
                               v.TravelProductID = f.TravelProductID and
                               v.FLD_KEY = 'BOOKER_PERCODE' and
                               v.FLD_VALUE not like @NonNumericString -- isnumeric(v.FLD_VALUE) = 1
                         order by v.InternalExtractID desc
                         )
        from dbo.#InternalExtractEUAirFact f


    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update dbo.#InternalExtractEUAirFact TUIDArranger)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    ------------------------------------------------------------
    -- TUIDLogon
    ------------------------------------------------------------
    update dbo.#InternalExtractEUAirFact
    set TUIDLogon = TUIDArranger
    from dbo.#InternalExtractEUAirFact f
    where exists(select * from dbo.Nav_Traveller t
                 where t.No_ = cast(f.TUIDArranger as varchar)
                   and t.TravelProductID = f.TravelProductID)
       or exists(select * from  dbo.TravelerAccountDim t
                 where t.TUID = f.TUIDArranger
                   and t.CustomerSystemID = 2)
 
    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update dbo.#InternalExtractEUAirFact TUIDLogon)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    ------------------------------------------------------------
    -- Approval Info
    ------------------------------------------------------------
    update dbo.#InternalExtractEUAirFact
       set ApprovalDate = cast(v.FLD_VALUE as datetime)
      from dbo.#InternalExtractEUAirFact f
           -- ToDo: consider revising. dbo.Nav_IMPORT_METAID_FIELD_VALUE may have repeating rows
           inner join dbo.Nav_IMPORT_METAID_FIELD_VALUE v
           on f.MetaDossierID = v.METAID and
              f.TravelProductID = v.TravelProductID and
              isdate(v.FLD_VALUE) = 1 and
              v.FLD_KEY = 'APPROVAL_DATE'


    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update dbo.#InternalExtractEUAirFact ApprovalDate)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    update dbo.#InternalExtractEUAirFact
       set TUIDApprover = cast(v.FLD_VALUE as int)
      from dbo.#InternalExtractEUAirFact f
           -- ToDo: consider revising. dbo.Nav_IMPORT_METAID_FIELD_VALUE may have repeating rows
             inner join dbo.Nav_IMPORT_METAID_FIELD_VALUE v
             on f.MetaDossierID = v.METAID and
                f.TravelProductID = v.TravelProductID and
                v.FLD_VALUE not like @NonNumericString and -- isnumeric(v.FLD_VALUE) = 1
                v.FLD_KEY = 'APPROVER_PERCODE'

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update dbo.#InternalExtractEUAirFact TUIDApprover)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    update dbo.#InternalExtractEUAirFact
       set ApprovalTxt = v.FLD_VALUE
      from dbo.#InternalExtractEUAirFact f
           -- ToDo: consider revising. dbo.Nav_IMPORT_METAID_FIELD_VALUE may have repeating rows
             inner join dbo.Nav_IMPORT_METAID_FIELD_VALUE v
             on f.MetaDossierID = v.METAID and
                f.TravelProductID = v.TravelProductID and
                v.FLD_VALUE <> '' and
                v.FLD_KEY = 'APPROVER_COMMENT'

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update dbo.#InternalExtractEUAirFact ApprovalTxt)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    ------------------------------------------------------------
    -- BookingDate
    ------------------------------------------------------------

    update dbo.#InternalExtractEUAirFact
       set BookingDate =
           case
               when BookingTypeID = 2 -- Cancel
                   then f.IssueDate
               when BookingTypeID = 5 -- Exchange
                   then null
               when BookingTypeID = 10 -- Cancel Refund
                   then null

               else coalesce(cast((select top 1 v.FLD_VALUE
                                   from dbo.Nav_IMPORT_METAID_FIELD_VALUE v
                                   where f.MetaDossierID = v.METAID and
                                         f.TravelProductID = v.TravelProductID and
                                         v.FLD_KEY = 'BOOK_DATE' and
                                         v.SYSTEM <> 'Y' and
                                         isdate(fld_value) = 1
                                   order by v.InternalExtractID desc
                                      ) as datetime), h.Posting_Date)
           end,
           InvoiceDate = dateadd(hour, 12, h.Posting_Date)
       from dbo.#InternalExtractEUAirFact f
            inner join
             dbo.nav_Sales_Header(@pInternalExtractID) h
                on f.InternalExtractID = h.InternalExtractID and
                   f.SalesDocumentCode = h.No_ and
                   f.TravelProductID = h.TravelProductID
      where f.InternalExtractID = @pInternalExtractID

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (Get Booking Date)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end



    ------------------------------------------------------------
    -- GroupAccountID, GroupAccountDepartmentID
    ------------------------------------------------------------
    -- This has dependencies on ExternalFile Data Processing
    -- in Kettle step "Populate Dimension"

    update f
       set f.GroupAccountID = g.GroupAccountID,
           f.GroupAccountDepartmentID = d.GroupAccountDepartmentID
      from dbo.#InternalExtractEUAirFact f
           inner join
             dbo.nav_Sales_Header(@pInternalExtractID) h
               on  h.TravelProductID = f.TravelProductID and
                   h.No_ = f.SalesDocumentCode
           inner join
             dbo.nav_Sales_Line(@pInternalExtractID) l
               on  l.TravelProductID = f.TravelProductID and
                   l.Document_No_ = f.SalesDocumentCode and
                   l.Line_No_ = f.SalesDocumentLineNbr
           inner join
           dbo.GroupAccountDim g
               on h.Sell_to_Customer_No_ = g.Comcode and
                   h.TravelProductID = g.TravelProductID
           left outer join
           dbo.GroupAccountDepartmentDim d
               on g.GroupAccountID = d.GroupAccountID and
                  d.GroupAccountDepartmentName = dbo.CleanGroupAccountDepartmentName(l.Analytical_Code_1) and
                  d.CustomerSystemID = 2
    where f.InternalExtractID = @pInternalExtractID and
          h.Sell_to_Customer_No_ not like @NonNumericString -- isnumeric(h.Sell_to_Customer_No_) = 1

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update dbo.#InternalExtractEUAirFact GroupAccountID, GroupAccountDepartmentID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- CurrencyCode
    update f
       set f.CurrencyCode = t.CurrencyCodeStorage
      from dbo.#InternalExtractEUAirFact f
           inner join
           dbo.TravelProductDim t
               on f.TravelProductID = t.TravelProductID

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update dbo.#InternalExtractEUAirFact CurrencyCode)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    ------------------------------------------------------------
    -- PolicyReasonCodeID, PolicyStatusID
    ------------------------------------------------------------
    -- Exchanges should have Policy set to Unknown 
    update dbo.#InternalExtractEUAirFact
       set PolicyReasonCodeID = 0,
           PolicyStatusID = 0
      from dbo.#InternalExtractEUAirFact f
     where f.InternalExtractID = @pInternalExtractID
       and f.BookingTypeID = @BookingTypeIDExchange 

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update dbo.#InternalExtractEUAirFact PolicyReasonCodeID for Exchanges)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- ToDo: PolicyStatusID and PolicyReasonCodeID should be based on the same InternalExtractID
    update dbo.#InternalExtractEUAirFact
        set PolicyReasonCodeID = (select top 1 cast(SubQ.ReasonsTxt as int) as PolicyReasonCodeID
                                    from dbo.Nav_workflow_out_of_policy_reasons SubQ
                                   where SubQ.ReasonType = 'WHY_CHOSEN_AS_OOP_ID' and
                                         SubQ.MdCode = f.MetaDossierID and
										 charindex(f.pnrcode,SubQ.pnr) <> 0 and
                                         SubQ.ServiceTypeID in (1,6) and
                                         -- isnumeric(SubQ.ReasonsTxt) = 1
                                         SubQ.ReasonsTxt not like @NonNumericString
                                   order by SubQ.InternalExtractID desc),
            PolicyStatusID =
                case (select top 1 v.fld_value
                        from dbo.Nav_IMPORT_METAID_FIELD_VALUE v
                       where v.METAID = f.MetaDossierID and
                             v.TravelProductID = f.TravelProductID and
                             v.FLD_KEY = 'ISCOMPLIANT' and (
-- ToDo: Validate PolicyStatusID switchover in Navision
                                (v.DATE_INSERT >= '2014-03-25' and v.REF = f.PNRCode) or
                                (v.DATE_INSERT < '2014-03-25')
                                )
                        order by v.InternalExtractID desc
                     )
                    when 'True'  then 1 -- Yes
                    when 'False' then 2 -- No
                                 else 0 -- Unknown
                end
        from dbo.#InternalExtractEUAirFact f
        where f.InternalExtractID = @pInternalExtractID
          and f.BookingTypeID <> @BookingTypeIDExchange    

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update dbo.#InternalExtractEUAirFact PolicyReasonCodeID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    ------------------------------------------------------------
    -- Credit Card Info / CentralBillBool
    ------------------------------------------------------------

if object_id('tempdb..#tmp_cc1') is not null drop table #tmp_cc1;
select
            l.InternalExtractID
        ,   l.[TravelProductID]
        ,   l.[Document_No_]
        ,   l.[Line_No_]
        ,   h.[Sell_to_Customer_No_]
        ,   l.[RTU]
        ,   l.[Card_Number]
        ,   l.[Card_Reference]
        into #tmp_cc1
        from dbo.#InternalExtractEUAirFact sil
        inner join dbo.nav_Sales_Header(@pInternalExtractID) h
            on h.InternalExtractID = sil.InternalExtractID
           and h.TravelProductID = sil.TravelProductID
           and h.No_ = sil.SalesDocumentCode
        inner join dbo.nav_Sales_Line(@pInternalExtractID) l
            on l.InternalExtractID = sil.InternalExtractID
           and l.TravelProductID = sil.TravelProductID
           and l.Document_No_ = sil.SalesDocumentCode
           and l.Line_No_ = sil.SalesDocumentLineNbr
    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (#tmp_cc1 table create)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end
    	
 if object_id('tempdb..#tmp_cc2') is not null drop table #tmp_cc2;
select
            cc.*
        ,   case cc.[RTU]
            when 1 then
                case isnull(pc.[Card_No_1], '')
                when '' then pch.[Card_No_1] + pch.[Card_No_2] + pch.[Card_No_3] + pch.[Card_No_4]
                else          pc.[Card_No_1] +  pc.[Card_No_2] +  pc.[Card_No_3] +  pc.[Card_No_4]
                end
            when 0 then      pdh.[Card_No_1] + pdh.[Card_No_2] + pdh.[Card_No_3] + pdh.[Card_No_4]
            end as 'cc_num'
        ,   case cc.[RTU]
            when 1 then
                case isnull(pc.[Card_Type], -1)
                    when -1 then pch.[Card_Type]
                    else pc.[Card_Type]
                end
            when 0 then pdh.[Card_Type]
            end as 'cc_type'
        ,   case cc.[RTU]
            when 1 then
                case isnull(pc.[Sharing_Type], -1)
                    when -1 then pch.[Sharing_Type]
                    else pc.[Sharing_Type]
                end
            when 0 then pdh.[Sharing_Type]
            end as 'sharing_type'
        into #tmp_cc2
        from #tmp_cc1 cc
        outer apply (
            select top 1 pc.*
            from [Nav_Payment_Card] pc
            where pc.[TravelProductID] = cc.[TravelProductID]
            and pc.[Token] = cc.[Card_Number]
            and pc.[Customer_No_] = cc.[Sell_to_Customer_No_]
            order by pc.[InternalExtractID] desc
        ) pc
        outer apply (
            select top 1 pch.*
            from [Nav_Payment_Card_History] pch
            where pch.[TravelProductID] = cc.[TravelProductID]
            and pch.[Customer_No_] = cc.[Sell_to_Customer_No_]
            and pch.[Token] = cc.[Card_Number]
            order by pch.[InternalExtractID] desc
        ) pch
        outer apply (
            select top 1 pdh.*
            from [Nav_Payment_Data_History] pdh
            where pdh.[TravelProductID] = cc.[TravelProductID]
            and pdh.[Card_Reference] = cc.[Card_Reference]
            and pdh.[Customer_No_] = cc.[Sell_to_Customer_No_]
            and pdh.[No_] = cc.[Document_No_]
            order by pdh.[InternalExtractID] desc
        ) pdh
    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (#tmp_cc2 table create)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end  
  if object_id('tempdb..#tmp_cc3') is not null drop table #tmp_cc3;
        select
            cc.*
        ,   isnull(m.CreditCardTypeID, 254) as CreditCardTypeID
        ,   case isnull(m.CreditCardTypeID, 254) when 254 then null else left(cc.cc_num, 6) end as CreditCardNbrBegin
        ,   case isnull(m.CreditCardTypeID, 254) when 254 then null else right(cc.cc_num, 4) end as CreditCardNbrEnd
        ,   case isnull(m.CreditCardTypeID, 254)
                when 254 then null
                else case isnull(cc.sharing_type, -1) when 1 then 1 when 2 then 0 else null end
            end as CentralBillBool
        into #tmp_cc3
        from #tmp_cc2 cc
        left join @CreditCardMap m
            on m.Nav_CreditCardTypeID = cc.cc_type
    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (#tmp_cc3 table create)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    update af set
        af.CreditCardTypeID = cc.CreditCardTypeID
    ,   af.CreditCardNbrBegin = cc.CreditCardNbrBegin
    ,   af.CreditCardNbrEnd = cc.CreditCardNbrEnd
    ,   af.CentralBillBool = cc.CentralBillBool
    from dbo.#InternalExtractEUAirFact af
    inner join #tmp_cc3 cc
        on cc.InternalExtractID = af.InternalExtractID
       and cc.TravelProductID = af.TravelProductID
       and cc.Document_No_ = af.SalesDocumentCode
       and cc.Line_No_ = af.SalesDocumentLineNbr
    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update dbo.#InternalExtractEUAirFact Credit Card Infos)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end
/*
    ;with tmp_cc1 as (
        select
            l.InternalExtractID
        ,   l.[TravelProductID]
        ,   l.[Document_No_]
        ,   l.[Line_No_]
        ,   h.[Sell_to_Customer_No_]
        ,   l.[RTU]
        ,   l.[Card_Number]
        ,   l.[Card_Reference]
        from dbo.#InternalExtractEUAirFact sil
        inner join dbo.nav_Sales_Header(@pInternalExtractID) h
            on h.InternalExtractID = sil.InternalExtractID
           and h.TravelProductID = sil.TravelProductID
           and h.No_ = sil.SalesDocumentCode
        inner join dbo.nav_Sales_Line(@pInternalExtractID) l
            on l.InternalExtractID = sil.InternalExtractID
           and l.TravelProductID = sil.TravelProductID
           and l.Document_No_ = sil.SalesDocumentCode
           and l.Line_No_ = sil.SalesDocumentLineNbr
    )
    , tmp_cc2 as (
        select
            cc.*
        ,   case cc.[RTU]
            when 1 then
                case isnull(pc.[Card_No_1], '')
                when '' then pch.[Card_No_1] + pch.[Card_No_2] + pch.[Card_No_3] + pch.[Card_No_4]
                else          pc.[Card_No_1] +  pc.[Card_No_2] +  pc.[Card_No_3] +  pc.[Card_No_4]
                end
            when 0 then      pdh.[Card_No_1] + pdh.[Card_No_2] + pdh.[Card_No_3] + pdh.[Card_No_4]
            end as 'cc_num'
        ,   case cc.[RTU]
            when 1 then
                case isnull(pc.[Card_Type], -1)
                    when -1 then pch.[Card_Type]
                    else pc.[Card_Type]
                end
            when 0 then pdh.[Card_Type]
            end as 'cc_type'
        ,   case cc.[RTU]
            when 1 then
                case isnull(pc.[Sharing_Type], -1)
                    when -1 then pch.[Sharing_Type]
                    else pc.[Sharing_Type]
                end
            when 0 then pdh.[Sharing_Type]
            end as 'sharing_type'
        from tmp_cc1 cc
        outer apply (
            select top 1 pc.*
            from [Nav_Payment_Card] pc
            where pc.[TravelProductID] = cc.[TravelProductID]
            and pc.[Token] = cc.[Card_Number]
            and pc.[Customer_No_] = cc.[Sell_to_Customer_No_]
            order by pc.[InternalExtractID] desc
        ) pc
        outer apply (
            select top 1 pch.*
            from [Nav_Payment_Card_History] pch
            where pch.[TravelProductID] = cc.[TravelProductID]
            and pch.[Customer_No_] = cc.[Sell_to_Customer_No_]
            and pch.[Token] = cc.[Card_Number]
            order by pch.[InternalExtractID] desc
        ) pch
        outer apply (
            select top 1 pdh.*
            from [Nav_Payment_Data_History] pdh
            where pdh.[TravelProductID] = cc.[TravelProductID]
            and pdh.[Card_Reference] = cc.[Card_Reference]
            and pdh.[Customer_No_] = cc.[Sell_to_Customer_No_]
            and pdh.[No_] = cc.[Document_No_]
            order by pdh.[InternalExtractID] desc
        ) pdh
    )
    , tmp_cc3 as (
        select
            cc.*
        ,   isnull(m.CreditCardTypeID, 254) as CreditCardTypeID
        ,   case isnull(m.CreditCardTypeID, 254) when 254 then null else left(cc.cc_num, 6) end as CreditCardNbrBegin
        ,   case isnull(m.CreditCardTypeID, 254) when 254 then null else right(cc.cc_num, 4) end as CreditCardNbrEnd
        ,   case isnull(m.CreditCardTypeID, 254)
                when 254 then null
                else case isnull(cc.sharing_type, -1) when 1 then 1 when 2 then 0 else null end
            end as CentralBillBool
        from tmp_cc2 cc
        left join @CreditCardMap m
            on m.Nav_CreditCardTypeID = cc.cc_type
    )
    update af set
        af.CreditCardTypeID = cc.CreditCardTypeID
    ,   af.CreditCardNbrBegin = cc.CreditCardNbrBegin
    ,   af.CreditCardNbrEnd = cc.CreditCardNbrEnd
    ,   af.CentralBillBool = cc.CentralBillBool
    from dbo.#InternalExtractEUAirFact af
    inner join tmp_cc3 cc
        on cc.InternalExtractID = af.InternalExtractID
       and cc.TravelProductID = af.TravelProductID
       and cc.Document_No_ = af.SalesDocumentCode
       and cc.Line_No_ = af.SalesDocumentLineNbr

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update dbo.#InternalExtractEUAirFact Credit Card Infos)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end */


    ------------------------------------------------------------
    -- EUServiceLevelID
    ------------------------------------------------------------
    update f
       set EUServiceLevelID = case when t.IsVIP = 1 then 2 else null end
      from dbo.#InternalExtractEUAirFact f
           inner join
           dbo.Nav_Sales_Invoice_Line l
               on l.InternalExtractID = f.InternalExtractID and
                  l.TravelProductID = f.TravelProductID and
                  l.Document_No_ = f.SalesDocumentCode and
                  l.Line_No_ = f.SalesDocumentLineNbr
           join dbo.Nav_Traveller t
                on l.Traveller_Code = t.No_
                     and t.TravelProductID = f.TravelProductID
           where f.InternalExtractID = @pInternalExtractID

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update dbo.#InternalExtractEUAirFact.TUIDArranger Invoice Line)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    update f
    set EUServiceLevelID = case when t.IsVIP = 1 then 2 else null end
    from dbo.#InternalExtractEUAirFact f
         inner join
         dbo.Nav_Sales_Cr_Memo_Line l
             on l.InternalExtractID = f.InternalExtractID and
                l.TravelProductID = f.TravelProductID and
                l.Document_No_ = f.SalesDocumentCode and
                l.Line_No_ = f.SalesDocumentLineNbr
         join dbo.Nav_Traveller t
              on l.Traveller_Code = t.No_
                   and t.TravelProductID = f.TravelProductID
    where l.InternalExtractID = @pInternalExtractID

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update dbo.#InternalExtractEUAirFact.TUIDArranger Credit Memo Line)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -- TravelerGroupPolicyID
    -- Note: This has a dependency on TravelerAccountDim being populated first (successful execution of PopulateDimensions)
    -- FactSourceProcessing won't execute for this until Dimension tables are complete

    update a
       set a.TravelerGroupPolicyID = d.TravelerGroupPolicyID
      from #InternalExtractEUAirFact a
           inner join
           TravelerAccountDim b on
                b.TUID = a.TUIDTraveler and
                b.CustomerSystemID = 2
           inner join
           dbo.GroupAccountDim c on
                b.GroupAccountID = c.GroupAccountID
           inner join
           dbo.TravelerGroupPolicyDim d on
                d.GroupAccountID = a.GroupAccountID and
                d.TravelerGroupPolicyID = b.TravelerGroupPolicyID and
                d.CustomerSystemID = 2

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update #InternalExtractEUAirFact.TravelerGroupPolicyID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    ------------------------------------------------------------
    -- AirFareTypeID
    ------------------------------------------------------------

    -- Non-Kettle AirFareTypeID conditions
    update f
    set AirFareTypeID = case
                            when l.Business_sub_category = 12
                                then 101 --TPC
                            when l.Business_sub_category = 14
                                then 102 --TPP
                            when l.Business_sub_category = 16
                                then 103 --TMP
                            when l.Business_sub_category = 13
                                then 104 --TPU
                            when l.Business_sub_category = 15
                                then 105 --TMU
                            else AirFareTypeID
                        end
    from #InternalExtractEUAirFact f
    join dbo.nav_Sales_Line(@pInternalExtractID) l
         on f.SalesDocumentCode = l.Document_No_ and
            f.SalesDocumentLineNbr = l.Line_No_ and
            f.TravelProductID = l.TravelProductID
    where l.Business_sub_category between 12 and 16
      and f.AirFareTypeID is null

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update #InternalExtractEUAirFact.AirfareTypeID WHERE Business_sub_category between 12 and 16)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    update f
    set AirFareTypeRuleGroupID = -1
    from #InternalExtractEUAirFact f
    join dbo.nav_Sales_Line(@pInternalExtractID) l
         on f.SalesDocumentCode = l.Document_No_ and
            f.SalesDocumentLineNbr = l.Line_No_ and
            f.TravelProductID = l.TravelProductID
    where l.Business_sub_category between 12 and 16
      and f.AirFareTypeID > 0

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update #InternalExtractEUAirFact.AirFareTypeRuleGroupID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    -----------------------------------------------------------------------
    -- start EU agentID
    -----------------------------------------------------------------------
    alter table #InternalExtractEUAirFact add IsNumericBool int default 0

    update #InternalExtractEUAirFact
    set IsNumericBool = case
                            when isnumeric(MetaDossierID) = 0 then 0
                            when MetaDossierID like @NonNumericString then 0
                            when cast(MetaDossierID as numeric(38, 0)) not between -2147483648. and 2147483647. then 0
                            else 1
                        end

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update IsNumericBool #InternalExtractEUAirFact)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    create index temp_ix on  #InternalExtractEUAirFact (MetaDossierID,IsNumericBool)

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (index create on  #InternalExtractEUAirFact)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    --EUTravelConsultant prepare
    if object_id('tempdb..#EUTravelConsultant') is not null begin
        drop table #EUTravelConsultant
    end

    create table #EUTravelConsultant (
        dossier_id varchar(20),
        event_date smalldatetime,
        travel_consultant_id int,
        event_count int)

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (create table #EUTravelConsultant)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    --getting EUTravelConsultantID from event_log
    insert into #EUTravelConsultant
        select 
            e.dossier_id,
            convert(smalldatetime,convert(varchar,e.event_date,23),120) as event_date,
            e.travel_consultant_id * -1 as travel_consultant_id,
            count(*) as event_count
        from dbo.event_log_eu_agentid e
        inner join dbo.event_action a
            on (e.action_id = a.id)
        where
            e.travel_consultant_id is not null and
            a.LABEL_CODE in ('BOOK',
                             'CANCEL',
                             'TRAINBOOK',
                             'TRAINCANCEL',
                             'TCCHANGED',
                             'WAVEMODIF',
                             'TCAIRMODIFIED',
                             'TCCARMODIFIED',
                             'TCHOTELMODIFIED',
                             'TCAIRDELETED',
                             'TCAIRADDED',
                             'TCCARADDED',
                             'TCHOTELADDED',
                             'TCAIRPRICEMODIFIED',
                             'HCBOOK',
                             'IANHOTELCANCEL',
                             'IANHOTELDATECHANGE',
                             'IANHOTELPRICECHANGE',
                             'AIRINSERTION',
                             'TRAININSERTION',
                             'CARINSERTION',
                             'CHARGEMODIF',
                             'CHARGEMODIFCANCEL',
                             'TRAINUPDATE',
                             'TRAINDELETE',
                             'TRAINREFUND',
                             'TRAINEXCHANGE',
                             'TRAVREMOVED',
                             'HCADD',
                             'TRAINADD',
                             'RCARADD',
                             'WAVEADD',
                             'BOOKING_NEW',
                             'BOOKING_NEW_TC_CHECK',
                             'BOOKING_UPDATED',
                             'BOOKING_CANCELLED',
                             'BOOKING_EXCHANGED',
                             'CARBOOK',
                             'CARCANCEL',
                             'CHECKOUT_INSERTION',
                             'GROUNDADD',
                             'GROUNDUPDATE',
                             'GROUNDCANCEL') and
            -- exclude website automation
			e.travel_consultant_id <> 504141 and  
            exists (select * 
                    from #InternalExtractEUAirFact 
                    where 
                        MetaDossierID = e.dossier_id and 
                        IsNumericBool = 1)
        group by 
            e.dossier_id,
            convert(smalldatetime,convert(varchar,e.event_date,23),120),
            e.travel_consultant_id

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #EUTravelConsultant)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    create index temp_ix2 on #EUTravelConsultant (dossier_id, event_date)

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (index create on #EUTravelConsultant)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    --#NavTC
    if object_id('tempdb..#NavTC') is not null begin
        drop table #NavTC
    end

    select distinct
        otc.ID * -1 as travel_consultant_id
    ,   alm.LAST_NAME + ' ' + alm.FIRST_NAME as fld_value
    ,   alm.[LOGIN]
    into #NavTC
    from dbo.[OPST_TRAVEL_CONSULTANT] otc
    inner join dbo.[AGENT_LOGIN_MAPPING] alm
        on alm.ID = otc.ECT_USER_ID


    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (insert #NavTC)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler 
    end

    create index temp_ix3 on #NavTC (fld_value)

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (index create on #NavTC)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end
    --end EU travel consultant prepare

    --updating EU travel consultant
    update f
        set TUIDLogon = case f.AgentAssistedBool
                            when 1 then
                                case f.IsNumericBool
                                    when 1 then
                                        coalesce(c.travel_consultant_id,coalesce(d.nav_tc,-500000)) 
                                    else
                                        case
                                            when left(f.metadossierid,3) = 'NMD' then -500000
                                            when left(f.metadossierid,1) = 'M' then coalesce(d.nav_tc,-500000)
                                            else -500000
                                        end
                                end
                            else
                                f.TUIDLogon
                        end
        from #InternalExtractEUAirFact f 
            outer apply (select top 1 travel_consultant_id
                         from #EUTravelConsultant
                         where 
                             dossier_id = f.MetaDossierID and
                             event_date <= f.IssueDate
                         order by event_date desc, event_count desc) c
            outer apply (select top 1 
                             a.fld_value, 
                             b.travel_consultant_id as nav_tc
                         from dbo.Nav_IMPORT_METAID_FIELD_VALUE a
                         inner join #NavTC b
                             on (a.fld_value = b.fld_value)
                         where 
                             a.METAID = f.metadossierid and 
                             a.travelproductid = f.travelproductid and 
                             a.fld_key = 'TRAVEL_CONSULTANT') d


    select @Error = @@Error
    if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update #InternalExtractEUAirFact look up TravelConsultantID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end
    -- end EU AgentID


    ------------------------------------------------------------
    -- FSFBool / TUIDFSF
    ------------------------------------------------------------

    -- if object_id('tempdb..#id_1')            is not null drop table #id_1
    -- if object_id('tempdb..#import_air_keys') is not null drop table #import_air_keys
    -- if object_id('tempdb..#fsfamtsavings')   is not null drop table #fsfamtsavings

    select
        [TravelProductID]
    ,   [Document_No_]
    ,   [Line_No_]
    ,   [ID_1]
    into #id_1
    from dbo.nav_Sales_Line(@pInternalExtractID)
    where [Service_Group] = 1
    and [C_R_S] <> 'LOWCOST'
    and [Resource_Type] = 0
    and [ID_1] <> ''
    and [Type] = 3

    create index ix1 on #id_1 ([TravelProductID], [Document_No_], [Line_No_])

    select distinct [TravelProductID], [ID_1] as [FileName] into #import_air_keys from #id_1

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
        from #NavTC
        where [LOGIN] = substring(B.formated, charindex(':', B.formated) + 1, 100)
        order by [travel_consultant_id] asc
    ) e
    where B.RowNum = 1

    create index ix1 on #fsfamtsavings ([FileName], [TravelProductID], [type]) include ([fsfamt], [travel_consultant_id])

    update f set
        f.[FSFBool] = isnull(case when isnull(fsf.[type], '') = 'ifd' then 1 else 0 end, 0)
    ,   f.[TUIDFSF] = case when coalesce(fsf.[travel_consultant_id], -1) <> -1 then fsf.[travel_consultant_id] else null end
    from dbo.#InternalExtractEUAirFact f
    left outer join #id_1 a
        on f.[TravelProductID] = a.[TravelProductID]
        and f.[SalesDocumentCode] = a.[Document_No_]
        and f.[SalesDocumentLineNbr] = a.[Line_No_]
    left outer join #fsfamtsavings fsf
        on fsf.[TravelProductID] = a.[TravelProductID]
        and fsf.[FileName] = a.[ID_1]

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update dbo.#InternalExtractEUAirFact FSFBool/TUIDFSF)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    -- Kettle-based AirFareTypeID conditions
    -- The following section unpivots all comparison data for rules processing against rules in dbo.AirFareTypeRuleGroup/dbo.AirFareTypeRuleGroupDetails
    -- ForEach AirFareTypeRuleGroupID:
    --     ForEach Row in #InternalExtractEUAirFact (#InternalExtractEUAirFact_Unpivot)
    --         IF InternalExtractEUAirFact matches all conditions set in dbo.AirFareTypeRuleGroupDetails
    --             Choose this rule; Do not try to match this row against other rules
    --         ELSE
    --             Process next rule
    --
    --
    -- See dbo.AirFareTypeRuleGroup and dbo.AirFareTypeRuleGroupDetails for examples on rule formatting
    -- For rules using the "IN" or "MATCH" criteria, the Value column MUST follow a format of "<v>Value1</v><v>Value2</v>".
    -- Any deviation from this format may fail validation.
    --
    -- Since some columns are only used for a single rule, the unpivot can be prohibitively large. For this reason, some rules are process
    -- prior to unpivoting.

    -- ToDo: Need a way to test rules

    begin try

        if exists(select * from AirFareTypeRuleGroupDetails d
                  where d.ColumnName = 'Meta_ID' and
                        d.Comparison = 'LIKE' and
                        d.Value = 'MA%' and
                        getdate() between d.EffectiveDateStart and d.EffectiveDateEnd)
        begin
            -- Run this rule directly for performance reasons
            set @AirFareTypeRuleGroupID = 100
            update f set f.AirFareTypeRuleGroupID = @AirFareTypeRuleGroupID
            from #InternalExtractEUAirFact f
                  join dbo.nav_Sales_Line(@pInternalExtractID) l
                    on --f.InternalExtractID = l.InternalExtractID and
                       f.SalesDocumentCode = l.Document_No_ and
                       f.SalesDocumentLineNbr = l.Line_No_ and
                       f.TravelProductID = l.TravelProductID
            where left(l.Meta_ID,2) = 'MA' and -- Billback Prefix
                  f.InternalExtractID = @pInternalExtractID and
                  f.AirFareTypeRuleGroupID is null
        end

        if exists(select * from AirFareTypeRuleGroupDetails d
                  where d.ColumnName = 'Service_Group' and
                        d.Comparison = '=' and
                        d.Value = '6' and
                        getdate() between d.EffectiveDateStart and d.EffectiveDateEnd)
        begin
            -- Run this rule directly for performance reasons
            set @AirFareTypeRuleGroupID = 200
            update f set f.AirFareTypeRuleGroupID = @AirFareTypeRuleGroupID
            from #InternalExtractEUAirFact f
                  join dbo.nav_Sales_Line(@pInternalExtractID) l
                    on --f.InternalExtractID = l.InternalExtractID and
                       f.SalesDocumentCode = l.Document_No_ and
                       f.SalesDocumentLineNbr = l.Line_No_ and
                       f.TravelProductID = l.TravelProductID
            where l.Service_Group = 6 and
                  f.InternalExtractID = @pInternalExtractID
        end

        if exists(select * from AirFareTypeRuleGroupDetails d
                  where d.ColumnName = 'Service_Group' and
                        d.Comparison = '<>' and
                        d.Value = '1' and
                        getdate() between d.EffectiveDateStart and d.EffectiveDateEnd) and
           exists(select * from AirFareTypeRuleGroupDetails d
                  where d.ColumnName = 'Resource_Type' and
                        d.Comparison = '<>' and
                        d.Value = '0' and
                        getdate() between d.EffectiveDateStart and d.EffectiveDateEnd)
        begin        -- Run this rule directly for performance reasons
            update f set f.AirFareTypeRuleGroupID = -2
            from #InternalExtractEUAirFact f
                  join dbo.nav_Sales_Line(@pInternalExtractID) l
                    on --f.InternalExtractID = l.InternalExtractID and
                       f.SalesDocumentCode = l.Document_No_ and
                       f.SalesDocumentLineNbr = l.Line_No_ and
                       f.TravelProductID = l.TravelProductID
            where (l.Resource_Type <> 0 or l.Service_Group <> 1) and
                  f.AirFareTypeRuleGroupID is null
        end


        -- Place all necessary calculation columns into an unpivoted table for compare

        if object_id('tempdb.dbo.#InternalExtractEUAirFact_Unpivot') is not null drop table #InternalExtractEUAirFact_Unpivot
        create table #InternalExtractEUAirFact_Unpivot (SalesDocumentCode varchar(20), SalesDocumentLineNbr int, ColumnName varchar(200), Value varchar(200) )

        create clustered index Unpvt_temp_ix1 on dbo.#InternalExtractEUAirFact_Unpivot (SalesDocumentCode, SalesDocumentLineNbr, ColumnName)

        insert #InternalExtractEUAirFact_Unpivot
        select  SalesDocumentCode,
                SalesDocumentLineNbr,
                ColumnName,
                Value
        from (
        select distinct
                     f.SalesDocumentCode,
                     f.SalesDocumentLineNbr,
                cast(f.TravelProductID as varchar(200)) as TravelProductID,
                cast(l.code_group as varchar(200)) as code_group,
                cast(l.Service_Group as varchar(200)) as Service_Group,
                --cast(l.Markup_amadeus as varchar(200)) as Markup_amadeus,
                cast(case when l.Markup_amadeus <> 0 then 1 else 0 end as varchar(200)) as IsMarkupAmadeus,
                cast(l.Corporation as varchar(200)) as Corporation,
                --cast(REPLACE(REPLACE(ISNULL(fc.Fare_Calculation2, ''), ' ', ''), 'UU', 'U') as varchar(200)) as Fare_Calculation2,
                cast(REPLACE(REPLACE(ISNULL(reverse(substring(reverse(fc2.Text1), 1, case charindex(';', reverse(fc2.Text1)) when 0 then 150 else charindex(';', reverse(fc2.Text1)) - 1 end)), ''), ' ', ''), 'UU', 'U') as varchar(200)) as Fare_Calculation2, 
               -- Table dbo.Nav_Tour_Code___FT no longer available                
				--cast(isnull(
                --      (select top 1 Tour_Code -- ToDo: We may not need this lookup after adding TourCode to InternalExtractEUAirFact
                --        from dbo.Nav_Tour_Code___FT tc
                --        where
                --        tc.File_Name = l.ID_1 and
                --        tc.Code_PNR = l.ID_2 and
                --        tc.TravelProductID = l.TravelProductID
                --        order by tc.InternalExtractID desc
                --        ),'')
                --        as varchar(200)) as Tour_Code,
                --Now Use dbo.Nav_Import_Air Tag = 'FT'
				cast(isnull(
				       -- ToDo: We may not need this lookup after adding TourCode to InternalExtractEUAirFact
				      (select top 1 substring(substring((tc2.Text1), 1, 
                                    case charindex(';', (tc2.Text1)) when 0 then 150 else charindex(';', (tc2.Text1)) end - 
                                    case charindex(';', (tc2.Text1)) when 0 then 0 else 1 end), 3, 150) 
                          from dbo.Nav_Import_Air tc2
                         where
						      tc2.Tag = 'FT' and
                              tc2.FileName = l.ID_1 and
                              tc2.Code_PNR = l.ID_2 and
                              tc2.TravelProductID = l.TravelProductID
                        order by tc2.InternalExtractID desc
                        ),'')
                        as varchar(200)) as Tour_Code,	
                 cast(l.Business_sub_category as varchar(200)) as Business_sub_category,
                -- Table dbo.Nav_Servicing_Carrier___C_ no longer available
                --cast(isnull(
                --      (select top 1 ltrim(rtrim(Pricing_Code))
                --        from dbo.Nav_Servicing_Carrier___C_ a
                --        where
                --        a.File_Name = l.ID_1 and
                --        a.Code_PNR = l.ID_2 and
                --        a.TravelProductID = l.TravelProductID
                --        order by a.InternalExtractID desc
                --        ),'')
                --        as varchar(200)) as Pricing_Code,
                --Now Use dbo.Nav_Import_Air use tag 'C-' to get  Pricing_Code 
                cast(isnull(
                      (select top 1 substring(p2.Text1, 27, 1)
                         from dbo.Nav_Import_Air p2
                        where p2.Tag = 'C-' and
							  p2.FileName = l.ID_1 and
							  p2.Code_PNR = l.ID_2 and 
						      p2.TravelProductID = l.TravelProductID
                        order by p2.InternalExtractID desc
                        ),'')
                        as varchar(200)) as Pricing_Code,
                -- Table bo.Nav_PNR_Remarks___RM no longer available
                --cast(isnull(
                --      (select top 1 ltrim(rtrim(Remark_Text))
                --      from dbo.Nav_PNR_Remarks___RM r
                --      where f.TravelProductID = r.TravelProductID and
                --            f.PNRCode = r.Code_PNR and
                --            l.ID_1 = r.[File_Name] and
                --            r.Remark_Text like 'AIRFARE%'
                --            order by r.InternalExtractID desc
                --            ),'')
                --    as varchar(200)) as Remark_Text,
                --Now Use dbo.Nav_Import_Air use tag 'RM' and Text1 like 'RM*AIRFARE%' to get Remarks_Text
                cast(isnull(
                      (select top 1 replace(ltrim(rtrim(r2.Text1)), 'RM*', '')
                         from dbo.Nav_Import_Air r2
                        where r2.Tag = 'RM' and
						      r2.Text1 like 'RM*AIRFARE%%' and
							  r2.FileName = l.ID_1 and
							  r2.Code_PNR = l.ID_2 and 
						      r2.TravelProductID = l.TravelProductID  
                              order by r2.InternalExtractID desc
                              ),'')
                      as varchar(200)) as Remark_Text,
                -- Table bo.Nav_PNR_Remarks___RM no longer available
                --cast(case when exists
                --      (select *
                --      from dbo.Nav_PNR_Remarks___RM r
                --      where f.TravelProductID = r.TravelProductID and
                --            f.PNRCode = r.Code_PNR and
                --            l.ID_1 = r.[File_Name] and
                --            r.Remark_Text = 'MOE'
                --            )
                --    then 1 else 0 end
                --    as varchar(200)) as IsMarineFare,
                --Now Use dbo.Nav_Import_Air use tag 'RM' and Text1 like 'RM*MOE%' to set IsMarineFare 
                cast(case when exists
                      (select *
                         from dbo.Nav_Import_Air m2
                        where m2.Tag = 'RM' and
						      m2.Text1 like 'RM*MOE%' and
							  m2.FileName = l.ID_1 and
							  m2.Code_PNR = l.ID_2 and 
						      m2.TravelProductID = l.TravelProductID 
                       )
                           then 1 else 0 end
                       as varchar(200)) as IsMarineFare,
                cast(l.Markup_Total as varchar(200)) as Markup_Total,
                cast(ltrim(rtrim(l.Code_tarif)) as varchar(200)) as Code_tarif,
                cast(l.Meta_ID as varchar(200)) as Meta_ID,
                cast(l.No_ as varchar(200)) as ResourceCode
        from #InternalExtractEUAirFact f
              join dbo.nav_Sales_Line(@pInternalExtractID) l
                on f.SalesDocumentCode = l.Document_No_ and
                   f.SalesDocumentLineNbr = l.Line_No_ and
                   f.TravelProductID = l.TravelProductID
              -- Table dbo.Nav_Fare_Calculation___Q_ no longer available
              --left join
              --   dbo.Nav_Fare_Calculation___Q_ fc on
              --      fc.File_Name = l.ID_1 and
              --      fc.Code_PNR = l.ID_2 and
              --      fc.TravelProductID = l.TravelProductID
              --Now Use dbo.Nav_Import_Air tag 'Q-' to set Fare_Calculation2            
              left join
                 dbo.Nav_Import_Air fc2 on
				 	fc2.Tag = 'Q-' and
                    fc2.FileName = l.ID_1 and
                    fc2.Code_PNR = l.ID_2 and
                    fc2.TravelProductID = l.TravelProductID
        where f.AirFareTypeRuleGroupID is null and
              f.InternalExtractID = @pInternalExtractID
        ) t
        unpivot ( Value for ColumnName in (code_group,
                              Service_Group,
                              IsMarkupAmadeus,
                              Corporation,
                              Fare_Calculation2,
                              Tour_Code, 
                              Pricing_Code,
                              Remark_Text,
                              IsMarineFare,
                              Markup_Total,
                              Code_tarif,
                              ResourceCode,
                              TravelProductID) ) as Unpvt

        -- ToDo: Consider validation check to see if there are any unpivoted columns in AirFareTypeRuleGroupDetails
        --       that don't exist in #InternalExtractEUAirFact_Unpivot

        -- Process all comparisons one rule at a time.
        set @AirFareTypeRuleGroupID = 260
        if exists(select * from dbo.AirFareTypeRuleGroupDetails d
                  where AirFareTypeRuleGroupID > @AirFareTypeRuleGroupID
                    and getdate() between d.EffectiveDateStart and d.EffectiveDateEnd)
        begin
            select @AirFareTypeRuleGroupID = min(AirFareTypeRuleGroupID)
              from dbo.AirFareTypeRuleGroupDetails d
             where AirFareTypeRuleGroupID > @AirFareTypeRuleGroupID
               and getdate() between d.EffectiveDateStart and d.EffectiveDateEnd
        end
        else
        begin
            set @AirFareTypeRuleGroupID = null
        end

        while @AirFareTypeRuleGroupID is not null
        begin
            -- Move the current rule into a table variable. There are performance issues when
            --     all rules are in temp table (even though we filter on a single rule)
            delete @Rules_Single

            -- Insert rules that don't use XML table
            insert @Rules_Single(ColumnName, Comparison, Value)
            select ColumnName, Comparison, Value
              from AirFareTypeRuleGroupDetails d
             where AirFareTypeRuleGroupID = @AirFareTypeRuleGroupID
               and d.Comparison in ('=', '<>', 'NOT LIKE', 'LIKE')
               and getdate() between d.EffectiveDateStart and d.EffectiveDateEnd

            -- Insert rules that DO use XML table
            -- Value column uses a format of '<v>60010</v><v>60013</v><v>60083</v><v>60035</v>'
           ;with RulesXML as(
            select ColumnName, Comparison, cast(Value as xml) as Value
              from AirFareTypeRuleGroupDetails d
             where AirFareTypeRuleGroupID = @AirFareTypeRuleGroupID
               and d.Comparison in ('IN', 'MATCH')
               and getdate() between d.EffectiveDateStart and d.EffectiveDateEnd
            )
            insert @Rules_Single(ColumnName, Comparison, Value)
            select a.ColumnName, a.Comparison, b.v.value('.', 'varchar(100)') as Value
            from RulesXML a
            cross apply Value.nodes('/v') b(v)

            update a
            set a.AirFareTypeRuleGroupID = @AirFareTypeRuleGroupID
            from #InternalExtractEUAirFact a
            -- All Conditions Must Match (Direct Matches)
            where a.AirFareTypeRuleGroupID is null and
                   a.InternalExtractID = @pInternalExtractID
            and 1 = case when not exists(select * from @Rules_Single where comparison in ('=', '<>', 'NOT LIKE', 'LIKE')) then 1
                else case when exists (select *
                                         from #InternalExtractEUAirFact_Unpivot a2
                                         join @Rules_Single b2 on b2.ColumnName = a2.ColumnName
                                        where a.SalesDocumentCode = a2.SalesDocumentCode and
                                              a.SalesDocumentLineNbr = a2.SalesDocumentLineNbr and
                                              b2.ColumnName = a2.ColumnName and
                                                ((b2.comparison = '=' and a2.Value = b2.Value) or
                                                 (b2.comparison = '<>' and a2.Value <> b2.Value) or
                                                 (b2.comparison = 'NOT LIKE' and a2.Value not like b2.Value) or
                                                 (b2.comparison = 'LIKE' and a2.Value LIKE b2.Value))
                                        ) then 1
                     else 0
                     end
                end
            -- All Conditions Must Match (Subquery Matches)
            -- IN condition
            and 1 = case when not exists(select * from @Rules_Single where comparison = 'IN') then 1
                else case when exists(select *
                                     from #InternalExtractEUAirFact_Unpivot a2
                                     join @Rules_Single b2 on b2.ColumnName = a2.ColumnName
                                     where a.SalesDocumentCode = a2.SalesDocumentCode and
                                           a.SalesDocumentLineNbr = a2.SalesDocumentLineNbr and
                                           b2.ColumnName = a2.ColumnName and
                                           b2.comparison = 'IN' and a2.Value = b2.Value) then 1
                     else 0
                     end
                end
            -- MATCH conditiaon a combination of IN/LIKE logic
            and 1 = case when not exists(select * from @Rules_Single where comparison = 'MATCH') then 1
                else case when exists(select *
                                     from #InternalExtractEUAirFact_Unpivot a2
                                     join @Rules_Single b2 on b2.ColumnName = a2.ColumnName
                                     where a.SalesDocumentCode = a2.SalesDocumentCode and
                                           a.SalesDocumentLineNbr = a2.SalesDocumentLineNbr and
                                           b2.ColumnName = a2.ColumnName and
                                           b2.comparison = 'MATCH' and a2.Value LIKE b2.Value) then 1
                     else 0
                     end
                end
            -- There are no conditions that fail to match
            and 1 = case when not exists(select * from @Rules_Single where comparison in ('=', '<>', 'NOT LIKE', 'LIKE')) then 1
                else case when not exists (select *
                                             from #InternalExtractEUAirFact_Unpivot a2
                                             join @Rules_Single b2 on b2.ColumnName = a2.ColumnName
                                             where a.SalesDocumentCode = a2.SalesDocumentCode and
                                                   a.SalesDocumentLineNbr = a2.SalesDocumentLineNbr and
                                                   b2.ColumnName = a2.ColumnName and
                                                (    (b2.comparison = '=' and not a2.Value = b2.Value) or
                                                     (b2.comparison = '<>' and not a2.Value <> b2.Value) or
                                                     (b2.comparison = 'NOT LIKE' and not a2.Value not like b2.Value) or
                                                     (b2.comparison = 'LIKE' and not a2.Value LIKE b2.Value))
                                            ) then 1
                    else 0
                    end
                end
            -- ToDo: Implement "NOT IN" logic later
            -- ToDo: Implement "NOT MATCH" logic later

            delete a
            from #InternalExtractEUAirFact_Unpivot a
            where exists(select * from #InternalExtractEUAirFact b
                            where b.AirFareTypeRuleGroupID is not null and
                                   a.SalesDocumentCode = b.SalesDocumentCode and
                                   a.SalesDocumentLineNbr = b.SalesDocumentLineNbr
                                   )

        if exists(select * from AirFareTypeRuleGroupDetails d
                  where AirFareTypeRuleGroupID > @AirFareTypeRuleGroupID
                    and getdate() between d.EffectiveDateStart and d.EffectiveDateEnd)
        begin
            select @AirFareTypeRuleGroupID = min(AirFareTypeRuleGroupID)
              from dbo.AirFareTypeRuleGroupDetails d
             where AirFareTypeRuleGroupID > @AirFareTypeRuleGroupID
               and getdate() between d.EffectiveDateStart and d.EffectiveDateEnd
        end
            else
            begin
                set @AirFareTypeRuleGroupID = null
            end

        end


        update af
        set af.AirFareTypeID = rg.AirFareTypeID
        from #InternalExtractEUAirFact af
             join dbo.AirFareTypeRuleGroup rg on
                  af.AirFareTypeRuleGroupID = rg.AirFareTypeRuleGroupID
             where af.AirFareTypeID is null

    end try
    begin catch

        select @Error = ERROR_NUMBER(),
               @ErrorLine = ERROR_LINE(),
               @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) +
                              ' (Calculating AirfareTypeID; Line: ' + cast(@ErrorLine as varchar) +
                              '; AirFareTypeRuleGroupID: ' + cast(@AirFareTypeRuleGroupID as varchar) + ')'

        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler

    end catch


    ------------------------------------------------------------
    -- NetPricedFareBool
    ------------------------------------------------------------

    update f
    set NetPricedFareBool = case f.AirFareTypeID
            when 2 then 1
            when 3 then 1
            when 1 then 0
            when 9 then 1
            else null end
    from #InternalExtractEUAirFact f

    select @Error = @@Error
    if (@Error <> 0) begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update #InternalExtractEUAirFact.NetPricedFareBool)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    ------------------------------------------------------------
    -- Flag Subscriptions/Ancillary charges
    ------------------------------------------------------------
    -- Based on ExternalFileDataVldEUAir_ (EGE-64394)
    update f
       set FactRecordStatusID = case when l.Business_category = 4
                                            or l.SubscriptionBool = 1
                                        then @FactRecordStatusID_SUB
                                     when l.Business_category = 6
                                            and l.Business_sub_category in (1,2,3)
                                            and (f.TravelProductID in (60010,60013,60083,60035) 
                                                 or f.InvoiceDate >= '01/01/2014') -- Ancillary Services w/Exchange: for VIA TPIDs or from rest of TPIDs from 01/01/2014
                                        then @FactRecordStatusID_AEP
                                     when l.Business_category = 6
                                            and isnull(l.Business_sub_category, 0) not in (1,2,3)
                                            and (f.TravelProductID in (60010,60013,60083,60035) 
                                                or f.InvoiceDate >= '01/01/2014') -- not ex penalty
                                        then @FactRecordStatusID_ANC
                                     when l.Business_category = 7
                                            and l.Business_sub_category in (1,2)
                                        then @FactRecordStatusID_AEP
                                     else f.FactRecordStatusID
                                end
      from #InternalExtractEUAirFact f
           cross apply(
                   select l2.Business_category, l2.abonnement as SubscriptionBool, l2.Business_sub_category
                     from dbo.Nav_Sales_Invoice_Line l2
                    where l2.InternalExtractID = f.InternalExtractID and
                          l2.TravelProductID = f.TravelProductID and
                          l2.Document_No_ = f.SalesDocumentCode and
                          l2.Line_No_ = f.SalesDocumentLineNbr
                    union all
                   select l2.Business_category, l2.abonnement as SubscriptionBool, l2.Business_sub_category
                     from dbo.Nav_Sales_Cr_Memo_Line l2
                    where l2.InternalExtractID = f.InternalExtractID and
                          l2.TravelProductID = f.TravelProductID and
                          l2.Document_No_ = f.SalesDocumentCode and
                          l2.Line_No_ = f.SalesDocumentLineNbr) l



select @TrancountSave = @@Trancount
if (@TrancountSave = 0)
    begin transaction @SavePointName
    else
        save transaction @SavePointName
    select @TranStartedBool = @TRUE


    update dbo.InternalExtractEUAirFact
       set InternalExtractID = b.InternalExtractID,
           SalesDocumentCode = b.SalesDocumentCode,
           SalesDocumentLineNbr = b.SalesDocumentLineNbr,
           BookingTypeID = b.BookingTypeID,
           TravelProductID = b.TravelProductID,
           RecordKey = b.RecordKey,
           FactRecordStatusID = b.FactRecordStatusID,
           SalesDocumentCodePrior = b.SalesDocumentCodePrior,
           SalesDocumentTypeID = b.SalesDocumentTypeID,
           CustomerSystemID = b.CustomerSystemID,
           BookingSourceID = b.BookingSourceID,
           GroupAccountID = b.GroupAccountID,
           GroupAccountDepartmentID = b.GroupAccountDepartmentID,
           TUIDAccount = b.TUIDAccount,
           TUIDTraveler = b.TUIDTraveler,
           TUIDLogon = b.TUIDLogon,
           TUIDArranger = b.TUIDArranger,
           TravelerNameID = b.TravelerNameID,
           MetaDossierID = b.MetaDossierID,
           IATACode = b.IATACode,
           PNRCode = b.PNRCode,
           TicketCode = b.TicketCode,
           TicketCodePrior = b.TicketCodePrior,
           TicketCodeEnding = b.TicketCodeEnding,
           IssueDate = b.IssueDate,
           InvoiceDate = b.InvoiceDate,
           BillingSystemCreateDate = b.BillingSystemCreateDate,
           BookingDate = b.BookingDate,
           TravelDateStart = b.TravelDateStart,
           TravelDateEnd = b.TravelDateEnd,
           AirlineCode = b.AirlineCode,
           ConsolidatorCode = b.ConsolidatorCode,
           RouteID = b.RouteID,
           RouteTxt = b.RouteTxt,
           SegmentCnt = b.SegmentCnt,
           FlightCnt = b.FlightCnt,
           AirFareBasisCode = b.AirFareBasisCode,
           ClassOfServiceCode = b.ClassOfServiceCode,
           CabinClassID = b.CabinClassID,
           AirFareTypeID = b.AirFareTypeID,
           AirFareTypeRuleGroupID = b.AirFareTypeRuleGroupID,
           TripTypeID = b.TripTypeID,
           GeographyTypeID = b.GeographyTypeID,
           SaturdayNightStayBool = b.SaturdayNightStayBool,
           EUServiceLevelID = b.EUServiceLevelID,
           TravelerGroupPolicyID = b.TravelerGroupPolicyID,
           PolicyStatusID = b.PolicyStatusID,
           OnlineBool = b.OnlineBool,
           OfflineBookingTypeID = b.OfflineBookingTypeID,
           AgentAssistedBool = b.AgentAssistedBool,
           AdvancePurchaseDaysCnt = b.AdvancePurchaseDaysCnt,
           AdvancePurchaseID = b.AdvancePurchaseID,
           CurrencyCode = b.CurrencyCode,
           CreditCardTypeID = b.CreditCardTypeID,
           DirectPaymentBool = b.DirectPaymentBool,
           CentralBillBool = b.CentralBillBool,
           CreditCardNbrBegin = b.CreditCardNbrBegin,
           CreditCardNbrEnd = b.CreditCardNbrEnd,
           TransactionCnt = b.TransactionCnt,
           IncrementCnt = b.IncrementCnt,
           TicketAmt = b.TicketAmt,
           TicketAmtBase = b.TicketAmtBase,
           TicketAmtTax = b.TicketAmtTax,
           TicketAmtTaxVAT = b.TicketAmtTaxVAT,
           TicketAmtExchangePenalty = b.TicketAmtExchangePenalty,
           TicketAmtCommission = b.TicketAmtCommission,
           TicketAmtMarkup = b.TicketAmtMarkup,
           TicketAmtGross = b.TicketAmtGross,
           NetPricedFareBool = b.NetPricedFareBool,
           TicketTypeID = b.TicketTypeID,
           ETicketRefusedBool = b.ETicketRefusedBool,
           GroupTicketBool = b.GroupTicketBool,
           ApprovalDate = b.ApprovalDate,
           TUIDApprover = b.TUIDApprover,
           TravelerNameIDApproval = b.TravelerNameIDApproval,
           ApprovalTxt = b.ApprovalTxt,
           AgentErrorBool = b.AgentErrorBool,
           CustomDataElementTxt = b.CustomDataElementTxt,
           LowFareChosenBool = b.LowFareChosenBool,
           SavingsReasonCode = b.SavingsReasonCode,
           FareAmtLowestInPolicy = b.FareAmtLowestInPolicy,
           AirlineCodeLowestInPolicy = b.AirlineCodeLowestInPolicy,
           ClassOfServiceCodeLowestInPolicy = b.ClassOfServiceCodeLowestInPolicy,
           CabinClassIDLowestInPolicy = b.CabinClassIDLowestInPolicy,
           AirFareTypeIDLowestInPolicy = b.AirFareTypeIDLowestInPolicy,
           AirFareBasisCodeLowestInPolicy = b.AirFareBasisCodeLowestInPolicy,
           FareAmtPublished = b.FareAmtPublished,
           AirFareBasisCodePublished = b.AirFareBasisCodePublished,
           ClassOfServiceCodePublished = b.ClassOfServiceCodePublished,
           CabinClassIDPublished = b.CabinClassIDPublished,
           FareAmtNegotiated = b.FareAmtNegotiated,
           AirFareBasisCodeNegotiated = b.AirFareBasisCodeNegotiated,
           CabinClassIDNegotiated = b.CabinClassIDNegotiated,
           RecordKeyPrior = b.RecordKeyPrior,
           RecordKeyOriginal = b.RecordKeyOriginal,
           PolicyReasonCodeID = b.PolicyReasonCodeID,
           TourCode = b.TourCode,
           PricingEntry = b.PricingEntry,
           LowCostBool = b.LowCostBool,
           FSFBool = b.FSFBool,
           TUIDFSF = b.TUIDFSF,
           UpdateDate = b.UpdateDate,
           LastUpdatedBy = b.LastUpdatedBy

      from dbo.InternalExtractEUAirFact a
           inner join
           dbo.#InternalExtractEUAirFact b
               on a.SalesDocumentCode = b.SalesDocumentCode and
                  a.SalesDocumentLineNbr = b.SalesDocumentLineNbr and
                  a.RecordKey = b.RecordKey and
                  a.TravelProductID = b.TravelProductID and
                  a.BookingTypeID = b.BookingTypeID
    where a.InternalExtractID = @pInternalExtractID
    option(recompile)

    select @Error = @@Error
    if (@Error <> 0)
    begin
        select @ErrorCode   = @ERRUNEXPECTED,
               @MsgParm1    = cast(@Error as varchar(12)) + ' (update InternalExtractEUAirFact expansion)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

    if ((@TrancountSave = 0)
        and (@TranStartedBool = @TRUE))
    begin
        commit transaction @SavePointName
    end

goto ExitProc

---------------------------------------------------------------------
-- Error Handler
---------------------------------------------------------------------
ErrorHandler:
if (@TranStartedBool = @TRUE)
begin
    rollback transaction @SavePointName
end
select @ExitCode = @RC_FAILURE
goto ExitProc

---------------------------------------------------------------------
-- Exit Procedure
---------------------------------------------------------------------
ExitProc:
return (@ExitCode)
go
-- Conversion errors causing XACT_STATE issues. May need to clean up the transaction when coding.
-- while @@trancount > 0 begin print 'Rolling Back'; rollback tran; end
-- raiserror ('Executing InternalExtractEUAirFactLoadExpand', 10, 1)

-- exec [InternalExtractEUAirFactLoadExpand] @pInternalExtractID = 30566
-- select ieef.ApprovalDate, ieef.TUIDApprover, ieef.ApprovalTxt, ieef.TravelerNameIDApproval, * from dbo.InternalExtractEUAirFact ieef where ieef.InternalExtractID = 30566
-- exec [InternalExtractEUAirFactLoadExpand] @pInternalExtractID = 30566
-- exec [InternalExtractEUAirFactLoadExpand] @pInternalExtractID = 30568
-- exec [InternalExtractEUAirFactLoadExpand] @pInternalExtractID = 30569
-- exec [InternalExtractEUAirFactLoadExpand] @pInternalExtractID = 30571
-- exec [InternalExtractEUAirFactLoadExpand] @pInternalExtractID = 30572
-- select MetaDossierID, ApprovalDate, TUIDApprover, ApprovalTxt, TravelerNameIDApproval, * from dbo.InternalExtractEUAirFact where ApprovalTxt is not null or ApprovalDate is not null
-- select distinct InternalExtractID from InternalExtractEUAirFact order by 1

-- update InternalExtractEUAirFact set airfaretypeid=null where InternalExtractID = 31118
-- exec dbo.InternalExtractEUAirFactLoadExpand @pInternalExtractID = 31118

/*
    begin tran

        select RecordKey, tourcode from InternalExtractEUAirFact  where InternalExtractID = 37676 order by recordkey
        update InternalExtractEUAirFact set tourcode=null where InternalExtractID = 37676
        exec dbo.InternalExtractEUAirFactLoadExpand @pInternalExtractID = 37676
        select RecordKey, tourcode from InternalExtractEUAirFact  where InternalExtractID = 37676 order by recordkey

    rollback tran
*/

/*
    -- https://jira/jira/browse/EGE-82869
    -- EU Air: AirFareTypeID is not set (currently set as "unknown") for some of the Refunds.

    begin tran
        select RecordKey, BookingTypeID, AirFareTypeID, AirFareTypeRuleGroupID from InternalExtractEUAirFact where PNRCode in ( 'Y3T8Y6')
        update InternalExtractEUAirFact set AirFareTypeID = null where InternalExtractID in (25776, 25793)
        exec dbo.InternalExtractEUAirFactLoadExpand @pInternalExtractID = 25776
        exec dbo.InternalExtractEUAirFactLoadExpand @pInternalExtractID = 25793
        select RecordKey, BookingTypeID, AirFareTypeID, AirFareTypeRuleGroupID from InternalExtractEUAirFact where PNRCode in ( 'Y3T8Y6')
    rollback tran

*/

/*

-- Check Rule Parsing

select ColumnName, Comparison, Value
    from AirFareTypeRuleGroupDetails d
    where d.Comparison in ('=', '<>', 'NOT LIKE', 'LIKE')
    and getdate() between d.EffectiveDateStart and d.EffectiveDateEnd

-- Rules that DO use XML table
-- Value column uses a format of '<v>60010</v><v>60013</v><v>60083</v><v>60035</v>'
;with RulesXML as(
select AirFareTypeRuleGroupID, ColumnName, Comparison, cast(Value as xml) as Value
    from AirFareTypeRuleGroupDetails d
    where d.Comparison in ('IN', 'MATCH')
    and getdate() between d.EffectiveDateStart and d.EffectiveDateEnd
)
select AirFareTypeRuleGroupID, a.ColumnName, a.Comparison, b.v.value('.', 'varchar(100)') as Value
from RulesXML a
cross apply Value.nodes('/v') b(v)

select top 100 *   
select count(1) --67477 rows found
from AirFact
where GroupAccountDepartmentID is null
and CustomerSystemID = 2

*/