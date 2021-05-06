DECLARE

@newFocusCutoffDate as Date =  '2020-03-01', -- use this date as a start date of all types of engagement for this analysis
@cohort1 as date = '2020-03-01',
@cohort2 as date = '2020-07-01',
@cohort3 as date = '2020-10-01',
@snapshotStart as date = '2020-07-02',
@snapshotMid as date = '2020-10-02',
@snapshotEnd as date =  '2021-05-02',
@actionStartDate as int = -12 ;


SELECT org.[Organisation Key]
	, org.[Organisation Legal Name]
	, org.[Organisation Entry To Segment Date]
	, org.[Organisation Maori Customer]												as [Maori Customer]
	, org.[Organisation NZTE Sector]												as [Sector]
	, org.[Organisation Age in Focus (Months)]										as [Age in Focus (Month)]
    , org_snapshot.[Organisation Covid Traffic Light]								as [covid traffic light]
    , org_snapshot.[Organisation Covid Update]										as [covid update]
	, IIF(org.[Organisation Entry To Segment Date] >= @newFocusCutoffDate, 1, 0 )	as [Is New Focus]
    , case 
        when org.[Organisation Entry To Segment Date] < @cohort1    
            then 'Cohort 1'
        when org.[Organisation Entry To Segment Date] >= @cohort1 and org.[Organisation Entry To Segment Date] < @cohort2    
            then 'Cohort 2'
        when org.[Organisation Entry To Segment Date] >= @cohort2 and org.[Organisation Entry To Segment Date] < @cohort3   
            then 'Cohort 3'
        when org.[Organisation Entry To Segment Date] >= @cohort3
            then 'Cohort 4'
        else 'undefined'
        end as [cohort]
	, org_snapshot.[Organisation NZTE Segment]										as [prev_segment]	
	, org_snapshot.[SnapshotNZDate]													as [SnapshotNZDate]	
FROM [CRM].[Organisations] org
	left join [CRM_Snapshot].[Organisations] org_snapshot
		on org_snapshot.[Organisation Key] =  org.[Organisation Key]
        and org_snapshot.[SnapshotNZDate] in ( @snapshotStart, @snapshotEnd, @snapshotMid)
		--and (o.[SnapshotNZDate] >= '2019-07-01'     and o.SnapshotNZDate <> '2020-04-28') -- excluding mistaken snapshot.5
	
where 
	 org.[Organisation NZTE Segment] = 'Focus' 
	 --and org_snapshot.[SnapshotNZDate] = @snapshotEnd
     --and org.[Organisation Key] = '00B75B10-C9F5-DF11-A1F6-02BF0ADC02DB'	
order by  org.[Organisation Legal Name]
, org_snapshot.[SnapshotNZDate]



-- with cte_objectives  as (
-- 		select sum(obj.[Objective Count])                                                       as [objective_all]
--             , sum(iif(obj.[Objective Created On Date] > org.[Organisation Entry To Segment Date] 
--                     , obj.[objective count],0)   )                                               as [objective_aftr]
-- 		    , obj.[Objective Organisation key]
-- 			, obj.[SnapshotNZDate]
--         from [CRM_Snapshot].[objectives] obj
--                 left join [crm].[organisations] org
--                     on obj.[objective organisation key] = org.[organisation key]
--         where not [obj].[objective status] in  ( 'withdrawn', 'on hold')
-- 		and obj.[SnapshotNZDate] in ( @snapshotStart, @snapshotEnd)
--       	group by obj.[Objective Organisation Key], obj.[SnapshotNZDate]
        
-- ), cte_obj_cf_clinic  as (
-- 		select sum(obj.[Objective Count])                                                       as [cte_obj_cf_clinic_all]
--             , sum(iif(obj.[Objective Created On Date] > org.[Organisation Entry To Segment Date] 
--                     , obj.[objective count],0)   )                                               as [cte_obj_cf_clinic_aftr]
-- 		    , obj.[Objective Organisation key]
-- 			, obj.[SnapshotNZDate]
--         from [CRM_Snapshot].[objectives] obj
--                 left join [crm].[organisations] org
--                     on obj.[objective organisation key] = org.[organisation key]
--         where not [obj].[objective status] in  ( 'withdrawn', 'on hold')
-- 		and obj.[SnapshotNZDate] in ( @snapshotStart, @snapshotEnd)
-- 		and obj.[objective name] like '%cashflow clinic%'
--       	group by obj.[Objective Organisation Key], obj.[SnapshotNZDate]        
-- ), cte_investments as (
--         Select  count (distinct  invd.[Investment Opportunity GUID]) + 0													as [Inv Deals]
-- 				, [Customer GUID], inv.[SnapshotNZDate]            
--         From [INV_Snapshot].[INV_Investment_Fact_Header] inv
--             Left Join [INV].[INV_Investment_Deals_Header] invd
--                 On inv.[investment Opportunity GUID] = invd.[Investment Opportunity GUID]            		
--         Where invd.[Investment Status] in ('Engaged')
-- 		and inv.[SnapshotNZDate] in ( @snapshotStart, @snapshotEnd)
--         Group By [Customer GUID] , inv.[SnapshotNZDate]		
-- ), cte_iPlanLast12Months as (
-- 			select count(distinct(p.[Project ID]) )as [iplan_all]
-- 				,pr.[Project Registration Organisation Key] 
-- 				, pr.[SnapshotNZDate]
-- 			from [CRM_Snapshot].[ProjectRegistrations] pr
-- 				left join [CRM_Snapshot].[ProjectSessions] ps
-- 					on pr.[Project Session Key] = ps.[Project Session Key]
-- 					and ps.[SnapshotNZDate] = pr.[SnapshotNZDate]
-- 				inner join [CRM_Snapshot].[Projects] p
-- 					on p.[Project Key] = ps.[Project Key]
-- 					and p.[SnapshotNZDate] = ps.[SnapshotNZDate]
-- 			where p.[Project Budget Source] = 'iPlan'
-- 			and ps.[Project Session Start Date] >= @snapshotStart --DATEADD(dd,DATEDIFF(dd,0,ps.[SnapshotNZDate])-365,0)
-- 			and pr.[Project Registration Organisation Key]  is not null
-- 			and pr.[Project Registration Attended On Date] is not null
-- 			and pr.[snapshotnzDate] in ( @snapshotStart, @snapshotEnd)
--             group by pr.[Project Registration Organisation Key], pr.[SnapshotNZDate]
-- ), cte_IGF as (
-- 		select count(*) as [active IGFs], igf.[Fund Organisation Key]
-- 			, igf.[SnapshotNZDate]
-- 		from [IGF_Snapshot].[Funds] igf
-- 		where igf.[fund status] = 'Active'
-- 		and [Fund Type Name]  like 'IGF%' 
-- 		and  igf.[SnapshotNZDate]  in ( @snapshotStart, @snapshotEnd)
-- 		group BY igf.[Fund Organisation Key], igf.[SnapshotNZDate]
-- ), cte_actions_by_type as (
--      select  count (distinct(a.[Action ID]))                                                as [actions_all]
--                 ,count(distinct(iif(a.[action start date] > org.[organisation Entry to Segment Date]
--                         , a.[Action ID]
--                         , null) ))                                                          as [actions_aftr]
-- 		        , a.[action Organisation key]
--                 , a.[action service Type]
-- 				, a.[snapshotnzDate]
--         from [CRM_Snapshot].[Actions] a
--             left join [crm].[organisations] org
--                 on a.[Action Organisation Key] = org.[organisation key]
--         where not a.[Action Status] in ( 'withdrawn', 'on hold')
--         and (a.[action Start Date] >= dateadd(m,  @actionStartDate, a.snapshotnzDate)  
-- 			or a.[action End Date] >= dateadd(m,  @actionStartDate, a.[SnapshotNZDate]) )
-- 		and a.[SnapshotNZDate]  in ( @snapshotStart, @snapshotEnd)
--         group by a.[action organisation Key], a.[action service Type], a.[snapshotnzDate]
-- ), cte_service_actions as (
-- 	select sum(a.[actions_all]) 		as [actions_all]
-- 	, sum(a.[actions_aftr]) 			as [actions_aftr]
-- 	, a.[Action Organisation Key]
-- 	, a.[snapshotnzdate]
-- 	from cte_actions_by_type a	
-- 	where a.[Action Service Type] is not null
-- 	group by a.[Action Organisation Key], a.[snapshotnzDate]
-- ), cte_actions as (
-- 	select sum(a.[actions_all]) 		as [actions_all]
-- 	, sum(a.[actions_aftr]) 			as [actions_aftr]
-- 	, a.[Action Organisation Key]
-- 	, a.[snapshotnzDate]
-- 	from cte_actions_by_type a	
-- 	group by a.[Action Organisation Key], a.[snapshotnzDate]
-- )

