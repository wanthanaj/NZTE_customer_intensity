DECLARE

@newFocusCutoffDate as Date =  '2020-03-01', -- use this date as a start date of all types of engagement for this analysis
@cohort1 as date = '2020-03-01',
@cohort2 as date = '2020-07-01',
@cohort3 as date = '2020-10-01',
@snapshotStart as date = '2020-07-02',
@snapshotEnd as date =  '2021-05-02',
@monthsBeforeSnapshot as int = -10 ;

select count(*) as [active IGFs]
			, UPPER(igf.[Fund Organisation Key])			as [Organisation Key]
			,  igf.[Fund Type Name]
            , sum(igf.[Fund Amount Approved])       as [Total_Fund_Amount_Approved]
			, igf.[SnapshotNZDate]
		from [IGF_Snapshot].[Funds] igf
		where igf.[fund status] = 'Active'
		and [Fund Type Name]  like '%Market Validation%' 
		and  igf.[SnapshotNZDate]  in ( @snapshotStart, @snapshotEnd)
		group BY igf.[Fund Organisation Key], igf.[SnapshotNZDate], igf.[Fund Type Name]
        order by igf.[Fund Organisation Key], igf.[SnapshotNZDate]
