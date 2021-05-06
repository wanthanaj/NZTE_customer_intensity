DECLARE

@newFocusCutoffDate as Date =  '2020-03-01', -- use this date as a start date of all types of engagement for this analysis
@cohort1 as date = '2020-03-01',
@cohort2 as date = '2020-07-01',
@cohort3 as date = '2020-10-01',
@snapshotStart as date = '2020-07-02',
@snapshotEnd as date =  '2021-05-02',
@cfCreatedDate as int = -15

select       sum(iif(obj.[Objective Created On Date] > org.[Organisation Entry To Segment Date] 
            , obj.[objective count],0)   )                                               as [obj_cf_clinic_aftr]
            , sum(obj.[objective count])                                                 as [obj_cf_clinic_all]
		    , obj.[Objective Organisation key]                                           as [Organisation Key]
			, obj.[SnapshotNZDate]
            
from [CRM_Snapshot].[objectives] obj
        left join [crm].[organisations] org
            on obj.[objective organisation key] = org.[organisation key]
where not [obj].[objective status] in  ( 'withdrawn', 'on hold')
and obj.[SnapshotNZDate] in ( @snapshotStart, @snapshotEnd)
and [Objective Objective Category] = 'investment services'
and (obj.[Objective Created On Date] >= dateadd(m,  @cfCreatedDate, obj.[SnapshotNZDate])   
	or obj.[Objective Created On Date] >= dateadd(m,  @cfCreatedDate, obj.[SnapshotNZDate]) )
--and obj.[objective organisation key] = 'F9A42CBC-C8F5-DF11-A1F6-02BF0ADC02DB'
group by obj.[Objective Organisation Key], obj.[SnapshotNZDate]      
order by obj.[objective organisation key]
