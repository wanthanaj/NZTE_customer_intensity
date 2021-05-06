DECLARE
@newFocusCutoffDate as Date =  '2020-03-01', -- use this date as a start date of all types of engagement for this analysis
@cohort1 as date = '2020-03-01',
@cohort2 as date = '2020-07-01',
@snapshotStart as date = '2020-07-02',
@snapshotEnd as date =  '2021-05-02',
@actionStartDate as int = -10;

with cte_actions_by_type as (
     select  count (distinct(a.[Action ID]))                                                as [actions_all]
                ,count(distinct(iif(a.[action start date] > org.[organisation Entry to Segment Date]
                        , a.[Action ID]
                        , null) ))                                                          as [actions_aftr]
		        , a.[Action Organisation Key]												as [Organisation Key]
                , a.[Action Service Type]
				, a.[SnapshotNZDate]
                , a.[action status]
        from [CRM_Snapshot].[Actions] a
            left join [crm].[organisations] org
                on a.[Action Organisation Key] = org.[organisation key]
        where not a.[Action Status] in ( 'withdrawn', 'on hold')
        -- all actions in the last 12 months before snapshotDate (both begin and end)
		and (a.[action Start Date] >= dateadd(m,  @actionStartDate, a.[SnapshotNZDate])   
			or a.[action End Date] >= dateadd(m,  @actionStartDate, a.[SnapshotNZDate]) )
		and a.[SnapshotNZDate]  in ( @snapshotStart, @snapshotEnd)
        group by a.[action organisation Key], a.[action service Type], a.[snapshotnzDate], a.[action status]
)
select * from cte_actions_by_type