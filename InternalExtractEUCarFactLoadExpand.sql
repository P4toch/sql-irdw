if object_id('dbo.InternalExtractEUCarFactLoadExpand') is null begin
    print 'Creating stored procedure InternalExtractEUCarFactLoadExpand (placeholder)'
    execute('create procedure dbo.InternalExtractEUCarFactLoadExpand as return 0')
end
go

print 'Altering stored procedure InternalExtractEUCarFactLoadExpand'
go

alter procedure dbo.InternalExtractEUCarFactLoadExpand
    @pInternalExtractID int

as

/*
*********************************************************************
Copyright (C) 2013-2018 Expedia, Inc. All rights reserved.

Description:
    Updates most attributes for eu car fact records.

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
    2013-10-16  DMurugesan      GeographyTypeID override using GeographyTypeTravelProduct mapping table
    2014-01-30  VBoerner        EGE-53881 Incorporate IS_OFFLINE_INSERT
    2014-07-18  DMurugesan      EGE-68306   Fix GeographyTypeID
    2016-02-02  minieto         EGE-106772 - Fix NULL GroupAccountDepartmentID
    2016-12-14  jappleberry     EGE-19143 set null GroupAccountDepartmentID to 0
    2018-02-28  jappleberry     EGACP-1954 set AdvancePurchaseID = 99 when AdvancePurchaseDaysCnt < 0
    2018-04-10  nrasmussen      EGE-189992 IRD - Enrich an agent assisted transaction with Travel Consultant TUID
    2018-05-16  nrasmussen      EGE-195465 IRD - replaced table event_log with event_log_eu_agentid 
    2018-06-07  nrasmussen      EGE-200489 IRD moving eu agentid section due to dataflow issues
    2018-10-03  nrasmussen      Jira.EGE-217181 IRD EU AgentID fix MetaDossierID overflow for int datatype when joining to EctWeb data
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
    @TrancountSave                  int,
    @NonNumericString               varchar(10)

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
    @TimeZoneIDFrom                       int,
    @LineOfBusinessID                     tinyint

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
    @NonNumericString               = '%[^0-9]%'

select   -- Error message constants
    @ERRUNEXPECTED                  = 200104,
    @ERRPARAMETER                   = 200110

select
    @BookingTypeIDReserve           = 3,
    @BookingTypeIDCancel            = 2,
    @BookingSystemID_ECTWeb         = 22,
    @TimeZoneIDFrom                 = 52, --CET
    @LineOfBusinessID               = 3

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
        a.GroupAccountID = e.GroupAccountID, 
        a.TravelProductID = e.TravelProductID,
        a.TUIDAccount = b.PER_CODE_MAIN,
        a.TUIDLogon = coalesce(k.PER_CODE_BOOKER,b.PER_CODE_BOOKER),
        a.TUIDTraveler = b.PER_CODE_MAIN, 
        a.TUIDArranger = case when coalesce(k.PER_CODE_BOOKER,b.PER_CODE_BOOKER) <> b.PER_CODE_MAIN 
                                then coalesce(k.PER_CODE_BOOKER,b.PER_CODE_BOOKER) else null end,
        a.MetaDossierID = c.MD_CODE, 
        a.TravelDateStart = b.PICKUP_DATETIME, 
        a.TravelDateEnd = b.DROPOFF_DATETIME, 
        a.TotalDaysCnt = case when a.BookingTypeID = @BookingTypeIDReserve 
                                then b.RENTAL_DAY_COUNT else -1 * b.RENTAL_DAY_COUNT end, --(case when datediff(day, c.PickUpDate, c.DropOffDate) < 1 then 1
                             --else (datediff(minute, c.PickUpDate, c.DropOffDate)+ 1380)/1440 end) *
                       --(case when a.BookingTypeID = @BookingTypeIDCancel then -1 else 1 end ),
        a.CarVendorCode = g.CarVendorCode, 
        a.CarClassID = b.CAR_CATEGORY_ID, 
        a.CarTypeID = b.CAR_TYPE_ID,
        a.CarTransmissionDriveID = b.CAR_TRANSMISSION_DRIVE_TYPE_ID, 
        a.CarFuelAirConditionID = b.CAR_FUEL_AC_TYPE_ID,
        a.CarRateTypeID = h.CarRateTypeID,
        a.CarRatePeriodID = i.CarRatePeriodID,
        a.DeliveredBool = coalesce(b.IS_PICKUP_DELIVERY,0),
        a.CollectedBool = coalesce(b.IS_DROPOFF_COLLECTION,0),
        a.PNRCode = b.PNR_CODE, 
        a.ConfirmationNbr = ltrim(rtrim(nullif(b.SUPPLIER_RESERVATION_CODE,''))), 
        a.UserCarClubNbr = ltrim(rtrim(nullif(b.TRAVELER_LOYALTY_NUMBER,''))), 
        a.UserCorpDiscountCode = ltrim(rtrim(nullif(b.DISCOUNT_CODE,''))), 
        a.PolicyReasonCodeID = coalesce(c.POLICY_REASON_CODE_ID,0),
        a.PolicyStatusID = case when b.IS_POLICY_COMPLIANT = 1 then 1 else 2 end,
        a.TravelerGroupPolicyID = d.TRAVEL_GROUP_ID_,
        a.AgentAssistedBool = case when coalesce(k.IS_OFFLINE,b.IS_OFFLINE) = 1 then 1 end,
        a.OnlineBool = case when coalesce(k.IS_OFFLINE,b.IS_OFFLINE) = 1 then 0 else 1 end,
        a.OfflineBookingTypeID = case when b.IS_OFFLINE_INSERT = 1 then 1 else null end,
        a.CurrencyCode = f.CurrencyCodeStorage, 
        a.PaymentMethodID = j.PaymentMethodID,
        a.TransactionCnt = 1,
        a.IncrementCnt = case when a.BookingTypeID = @BookingTypeIDReserve then 1 else -1 end,
        a.BookingSourceID = 39,
        a.GDSAgencyCode = b.PROVIDER_OFFICE_NUMBER
    from dbo.InternalExtractEUCarFact a 
        inner join
        dbo.vCAR_CART_LOG b on a.CarCartID = b.CAR_CART_ID and a.CarCartLogID = b.CAR_CART_LOG_ID
        inner join
        dbo.LIGHTWEIGHTDOSSIERS c on b.CAR_CART_ID = c.REF_DOSSIER_CODE and c.REF_DOSSIER_TYPE = 'CAR_TC' and a.InternalExtractIDReserve = c.InternalExtractID
        inner join 
        dbo.TRAVELLERS d on c.MD_CODE = d.MD_CODE and b.PER_CODE_MAIN = d.PER_CODE and a.InternalExtractIDReserve = d.InternalExtractID
        inner join 
        dbo.GroupAccountDim e on d.Company_ID_ = e.ComCode 
        inner join
        dbo.TravelProductDim f on e.TravelProductID = f.TravelProductID
        inner join
        dbo.CarVendorDim g on b.CAR_SUPPLIER_ID = g.SupplierID and g.LangID = 1033
        left join
        dbo.CarRateTypeBookingSource h on ltrim(rtrim(b.RATE_TYPE_CODE)) = h.CarRateTypeCode and h.BookingSourceID = 39
        left join
        dbo.CarRatePeriodBookingSource i on ltrim(rtrim(b.RATE_PLAN_CODE)) = i.CarRatePeriodCode and i.BookingSourceID = 39
        left join
        dbo.PaymentMethodBookingSource j on ltrim(rtrim(b.PAYMENT_METHOD_CODE)) = j.PaymentMethodCode and j.BookingSourceID = 39
        left join         
        dbo.vCAR_CART_LOG k on a.CarCartID = k.CAR_CART_ID and a.CarCartLogIDCancel = k.CAR_CART_LOG_ID
    where a.InternalExtractID = @pInternalExtractID

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Expand InternalExtractEUCarFact attributes)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    --GroupAccountDepartmentID
    update a set
        a.GroupAccountDepartmentID = isnull(c.GroupAccountDepartmentID,0)
    from dbo.InternalExtractEUCarFact a 
        inner join 
        dbo.TRAVELLERS b on a.MetadossierID = b.MD_CODE and a.TUIDAccount = b.PER_CODE and a.InternalExtractIDReserve = b.InternalExtractID
        left outer join    
        dbo.GroupAccountDepartmentDim c on a.GroupAccountID = c.GroupAccountID 
                                       and c.GroupAccountDepartmentName = dbo.CleanGroupAccountDepartmentName(b.CC1_LABEL_) 
                                       and c.CustomerSystemID = 2
    where a.InternalExtractID = @pInternalExtractID

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Update InternalExtractEUCarFact GroupAccountDepartmentID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    --AgentAssistedBool
    --Note: Only consider events between prior and current transaction
    update a set
        a.AgentAssistedBool = case when b.DOSSIER_ID is not null then 1 else coalesce(a.AgentAssistedBool,0) end
    from dbo.InternalExtractEUCarFact a 
        left join (
            select DOSSIER_ID 
            from dbo.InternalExtractEUCarFact a
            join dbo.EVENT_LOG b on a.MetadossierID = b.DOSSIER_ID 
                and b.InternalExtractID <= a.InternalExtractID
                and b.EVENT_DATE <= a.IssueDate
                and b.EVENT_DATE > coalesce(a.IssueDatePrior,'1/1/1900')
            where a.InternalExtractID = @pInternalExtractID  
                and b.TRAVEL_CONSULTANT_ID is not null 
            group by DOSSIER_ID ) b on a.MetaDossierID = b.DOSSIER_ID
    where a.InternalExtractID = @pInternalExtractID

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Update InternalExtractEUCarFact AgentAssistedBool)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    --Approval Info 
    --Note: Consider most recent approval info for dossier
    update a set
        a.TUIDApprover = b.[USER_ID],
        a.ApprovalDate = case when a.PolicyStatusID = 1 then b.EVENT_DATE end,
        a.ApprovalCommentTxt = left(ltrim(rtrim(c.COMMENTS)),255)
    from dbo.InternalExtractEUCarFact a 
        left join (
            select b.*,
                RankNbr = row_number() over (partition by b.DOSSIER_ID order by b.EVENT_DATE desc)
            from dbo.InternalExtractEUCarFact a
            join dbo.EVENT_LOG b on a.MetadossierID = b.DOSSIER_ID 
                and b.InternalExtractID <= a.InternalExtractID
                and b.EVENT_DATE <= a.IssueDate
            where a.InternalExtractID = @pInternalExtractID 
                and b.ACTION_ID in (500008,500022,500034,500170,500607,500608) --Approval actions
                    ) b on a.MetaDossierID = b.DOSSIER_ID and b.RankNbr = 1  
        left join
        dbo.COMMENTS c on a.MetaDossierID = c.MD_CODE and a.InternalExtractID = c.InternalExtractID and c.TYPE_CODE = 'A'
    where a.InternalExtractID = @pInternalExtractID

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Update InternalExtractEUCarFact Approval)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    --IssueDate
    update a set 
        a.IssueDate = dateadd(mi, (d.UTCOffsetMinuteCnt + isnull(dd.DaylightSavingsOffset, 0)) - (c.UTCOffsetMinuteCnt + isnull(cc.DaylightSavingsOffset, 0)), a.IssueDate)
    from dbo.InternalExtractEUCarFact a
        inner join
        dbo.TravelProductDim b on a.TravelProductID = b.TravelProductID
        inner join
        dbo.vTimeZone c on c.TimeZoneID = @TimeZoneIDFrom
        left join
        dbo.vTimeZoneDaylightSavingsOffset cc on a.IssueDate between cc.StartDate and cc.EndDate and c.TimeZoneID = cc.TimeZoneID
        inner join
        dbo.vTimeZone d on b.TimeZoneID = d.TimeZoneID
        left join
        dbo.vTimeZoneDaylightSavingsOffset dd on a.IssueDate between dd.StartDate and dd.EndDate and d.TimeZoneID = dd.TimeZoneID
    where a.InternalExtractID = @pInternalExtractID

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Update InternalExtractEUCarFact IssueDate)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    --AdvancePurchaseDaysCnt
    update a set 
        a.AdvancePurchaseDaysCnt = datediff(dd, a.IssueDate, a.TravelDateStart)
    from dbo.InternalExtractEUCarFact a
    where a.InternalExtractID = @pInternalExtractID

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Update InternalExtractEUCarFact AdvancePurchaseDaysCnt)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    -- AdvancePurchaseID
    update a 
        set AdvancePurchaseID = case when a.AdvancePurchaseDaysCnt is null then -1
                                     when a.AdvancePurchaseDaysCnt < 0 then 99
                                     else coalesce(b.AdvancePurchaseID, -1)
                                 end 
    from dbo.InternalExtractEUCarFact a 
        left join
        dbo.AdvancePurchaseDim b on a.AdvancePurchaseDaysCnt between b.StartDay and b.EndDay and b.LangID = 1033
    where a.InternalExtractID = @pInternalExtractID

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Update InternalExtractEUCarFact AdvancePurchaseID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    --ACRISSCode
    update a set
        a.ACRISSCode = b.CarCategoryCode + c.CarTypeCOde + d.CarTransmissionDriveCode + e.CarFuelAirConditionCode
    from dbo.InternalExtractEUCarFact a 
        inner join 
        dbo.CarCategoryDim b on a.CarClassID = b.CarCategoryID and b.LangID = 1033
        inner join 
        dbo.CarTypeDim c on a.CarTypeID = c.CarTypeID and c.LangID = 1033
        inner join 
        dbo.CarTransmissionDriveDim d on a.CarTransmissionDriveID = d.CarTransmissionDriveID and d.LangID = 1033
        inner join 
        dbo.CarFuelAirConditionDim e on a.CarFuelAirConditionID = e.CarFuelAirConditionID and e.LangID = 1033
    where a.InternalExtractID = @pInternalExtractID

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Update InternalExtractEUCarFact ACRISSCode)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end


    --Geography
    update a set
        a.CarSupplierLocationIDFrom = d.CarSupplierLocationID,
        a.CarSupplierLocationIDTo = e.CarSupplierLocationID,
        a.AirportCodeFrom = coalesce(d.AirportCodeOverride, d.AirportCode, d.AirportCodeCalculated),
        a.AirportCodeTo = coalesce(e.AirportCodeOverride, e.AirportCode, e.AirportCodeCalculated),
        a.WithinCountryBool = case when d.CountryCode = e.CountryCode then 1 else 0 end
    from dbo.InternalExtractEUCarFact a 
        inner join
        dbo.vCAR_CART_LOG b on a.CarCartID = b.CAR_CART_ID and a.CarCartLogID = b.CAR_CART_LOG_ID
        inner join
        dbo.CarVendorDim c on b.CAR_SUPPLIER_ID = c.SupplierID and c.LangID = 1033
        left join 
        dbo.CarSupplierLocationBookingSource d on b.CAR_SUPPLIER_LOCATION_CODE_PICKUP = d.CarSupplierLocationCode 
            and c.CarVendorCode = d.CarVendorCode and d.BookingSourceID = 39
        left join 
        dbo.CarSupplierLocationBookingSource e on b.CAR_SUPPLIER_LOCATION_CODE_DROPOFF = e.CarSupplierLocationCode 
            and c.CarVendorCode = e.CarVendorCode and e.BookingSourceID = 39
        inner join
        dbo.TravelProductDim f on a.TravelProductID = f.TravelProductID
    where a.InternalExtractID = @pInternalExtractID

    select @Error = @@Error if (@Error <> 0) begin
        select @ErrorCode = @ERRUNEXPECTED,
               @MsgParm1 = cast(@Error as varchar(12)) + ' (Update InternalExtractEUCarFact Geography)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
        goto ErrorHandler
    end

--- GeographyTypeID
Update A 
set GeographyTypeID = case  when k.GeographyTypeID is not null then k.GeographyTypeID --- Highest priority for City Pair
                            else                                
                             case when f.PriorityOrderNbr  is not null and h.PriorityOrderNbr is not null then
                                    case when f.PriorityOrderNbr < h.PriorityOrderNbr then f.GeographyTypeID
                                         else h.GeographyTypeID
                                    end    
                                  when f.PriorityOrderNbr is null and h.PriorityOrderNbr is not null then
                                    case when j.PriorityOrderNbr < h.PriorityOrderNbr then j.GeographyTypeID
                                        else h.GeographyTypeID
                                     end
                                  when f.PriorityOrderNbr is not null and h.PriorityOrderNbr is null then
                                    case when j.PriorityOrderNbr < f.PriorityOrderNbr then j.GeographyTypeID
                                        else f.GeographyTypeID
                                     end
                               else j.GeographyTypeID
                             end                          
                      end
from dbo.InternalExtractEUCarFact a 
inner join  dbo.vCAR_CART_LOG v on 
    a.CarCartID = v.CAR_CART_ID and 
    a.CarCartLogID = v.CAR_CART_LOG_ID
inner join  dbo.CarVendorDim v1 on 
    v.CAR_SUPPLIER_ID = v1.SupplierID and 
    v1.LangID = 1033
left join dbo.CarSupplierLocationBookingSource c on 
    v.CAR_SUPPLIER_LOCATION_CODE_PICKUP = c.CarSupplierLocationCode and
    v1.CarVendorCode = c.CarVendorCode and 
    c.BookingSourceID = 39
left join dbo.CarSupplierLocationBookingSource d on 
    v.CAR_SUPPLIER_LOCATION_CODE_DROPOFF = d.CarSupplierLocationCode and
    v1.CarVendorCode = d.CarVendorCode and 
    d.BookingSourceID = 39
left join dbo.GeographyTypeTravelProductCountry e on
    e.TravelProductID = a.TravelProductID and 
    e.LineofBusinessID = @LineOfBusinessID and
    c.countrycode = e.Countrycode
left join dbo.GeographyTypeTravelProduct f on
    f.TravelProductID = e.TravelProductID and 
    f.GeographyTypeID = e.GeographyTypeID and
    f.LineofBusinessID = e.LineofBusinessID 
left join dbo.GeographyTypeTravelProductCountry g on
    g.TravelProductID = a.TravelProductID and 
    g.LineofBusinessID = @LineOfBusinessID and
    d.countrycode = g.Countrycode
left join dbo.GeographyTypeTravelProduct h on
    h.TravelProductID = g.TravelProductID and 
    h.GeographyTypeID = g.GeographyTypeID and
    h.LineofBusinessID = g.LineofBusinessID 
left join dbo.GeographyTypeTravelProductCountry i on
    i.TravelProductID = a.TravelProductID and
    i.CountryCode = '' and
    i.LineofBusinessID = @LineOfBusinessID 
left join dbo.GeographyTypeTravelProduct j on
    j.TravelProductID = i.TravelProductID and
    j.GeographyTypeID = i.GeographyTypeID and 
    j.LineOfBusinessID = i.LineOfBusinessID
left join dbo.GeographyTypeTravelProductCountry k on
    k.TravelProductID = a.TravelProductID and
    k.CountryCode = c.countrycode and
    k.CountryCodeDestination = d.countrycode and
    k.LineofBusinessID = @LineOfBusinessID
left join dbo.GeographyTypeTravelProduct l on
    l.TravelProductID = k.TravelProductID and
    l.GeographyTypeID = k.GeographyTypeID and 
    l.LineOfBusinessID = k.LineOfBusinessID  
where a.InternalExtractID = @pInternalExtractID

select @Error = @@Error if (@Error <> 0) begin
    select @ErrorCode = @ERRUNEXPECTED,
        @MsgParm1 = cast(@Error as varchar(12)) + ' (Update InternalExtractEUCarFact GeographyTypeID)'
        raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end

------------------------------------------------------------
-- IRD/EU AgentID
------------------------------------------------------------
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

--getting EUTravelConsultantID from event_log_eu_agentid
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
                -- metadossierid datatype int for dbo.InternalExtractEUCarFact 
                from dbo.InternalExtractEUCarFact 
                where  
                    InternalExtractID = @pInternalExtractID and
                    MetaDossierID = e.dossier_id)
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

--start updating EU travel consultant
update f
   set TUIDLogon = case f.AgentAssistedBool
                       when 1 then
                           case 
                               case
                                   when isnumeric(f.MetaDossierID) = 0 then 0
                                   when f.MetaDossierID like @NonNumericString then 0
                                   when cast(f.MetaDossierID as numeric(38, 0)) not between -2147483648. and 2147483647. then 0
                                   else 1
                               end
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
from dbo.InternalExtractEUCarFact f
outer apply (select top 1 travel_consultant_id
             from #EUTravelConsultant
             where 
                 -- for car metadossierid is by default int datatype - if not conversion error can happen
                 dossier_id = cast(f.MetaDossierID as varchar) and
                 event_date <= f.IssueDate
             order by 
                 event_date desc, 
                 event_count desc) c
outer apply (select top 1 
                  a.fld_value, 
                  b.travel_consultant_id as nav_tc
             from dbo.Nav_IMPORT_METAID_FIELD_VALUE a
             inner join #NavTC b
                 on (a.fld_value = b.fld_value)
             where 
                 -- for car metadossierid is by default int datatype - if not conversion error can happen
                 a.METAID = cast(f.metadossierid as varchar) and 
                 a.travelproductid = f.travelproductid and 
                 a.fld_key = 'TRAVEL_CONSULTANT') d
where f.InternalExtractID = @pInternalExtractID

select @Error = @@Error
if (@Error <> 0) begin
    select @ErrorCode   = @ERRUNEXPECTED,
           @MsgParm1    = cast(@Error as varchar(12)) + ' (update dbo.InternalExtractEUCarFact look up TravelConsultantID)'
    raiserror (@ErrorCode, 16, 1, @ProcedureName, @MsgParm1)
    goto ErrorHandler
end
--end EU agentID



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

exec dbo.InternalExtractEUCarFactLoadExpand
    @pInternalExtractID = 16407

select nullif(IS_OFFLINE,0), IS_OFFLINE, IS_OFFLINE_INSERT, *
from dbo.InternalExtractEUCarFact a
join dbo.Car_Cart_Log b on a.CarCartLogID = b.Car_Cart_Log_ID
left join dbo.CarSupplierLocationBookingSource c on a.CarSupplierLocationIDFrom = c.CarSupplierLocationID
where MetaDossierID is not null

*/