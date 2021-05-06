DECLARE
@newFocusCutoffDate as Date =  '2020-03-01', -- use this date as a start date of all types of engagement for this analysis
@cohort1 as date = '2020-03-01',
@cohort2 as date = '2020-07-01',
@snapshotStart as date = '2020-07-02',
@snapshotEnd as date =  '2021-04-02',
@monthsBeforeSnapshot as int =  -36 ;

select sum(oc.[outcome calculated amount (nzd)])                                                               	as [Deals$_all]
			, count(iif(oc.[Outcome Stage] = 'IGO', oc.[outcome id],  null ))                                 	as [IGOs_all] 
            , count(iif(oc.[Outcome Stage] = 'Deal', oc.[outcome id], null))                                   	as [Deals_all]
            , sum(oc.[outcome count])                                                                          	as [Outcome_all]
            , count(iif(oc.[Outcome Stage] = 'Introduction', oc.[outcome id], null))                           	as [Intro_all]
            , count(iif(oc.[Outcome Stage] = 'Lead', oc.[outcome id], null))                                   	as [Leads_all]
            , sum(IIF (oc.[outcome Approval Date]  > org.[Organisation Entry To Segment Date] 
                        , oc.[outcome calculated amount (nzd)], 0))                                                         as [Deals$_aftr] 
            , count(IIF (oc.[Outcome Stage] = 'Deal' and oc.[outcome Approval Date]  > org.[Organisation Entry To Segment Date] 
                        ,  oc.[outcome id] ,null))						                                                    as [Deals_aftr] 		
			, count(iif(  oc.[Outcome Stage] = 'IGO' and oc.[outcome Approval Date]  > org.[Organisation Entry To Segment Date]
                        , oc.[outcome id] ,null))                                                                           as [IGOs_aftr]   
			, sum(iif(  oc.[outcome Approval Date]  > org.[Organisation Entry To Segment Date]
                        , oc.[outcome count] ,0))     																		 as [Outcome_aftr]
			, count(iif(oc.[Outcome Stage] = 'Introduction' and oc.[outcome Approval Date]  > org.[Organisation Entry To Segment Date]
                        , oc.[outcome id] ,null))                                                                          	as [Intro_aftr]
            , count(iif(  oc.[Outcome Stage] = 'Lead' and oc.[outcome Approval Date]  > org.[Organisation Entry To Segment Date]
                        , oc.[outcome id] ,null))                                                                           as [Leads_aftr]
            , oc.[outcome organisation key]
            , oc.[SnapshotNZDate]
            from [CRM_Snapshot].[Outcomes] oc
                left join [crm].[organisations] org
                    on oc.[outcome organisation key] = org.[organisation key]
            where oc.[Outcome Status] = 'Complete'
			and oc.[Outcome Stage] in ('Deal', 'IGO')
			and (oc.[outcome approval Date] >= dateadd(m,  @monthsBeforeSnapshot, oc.snapshotnzDate)  
			    or oc.[Outcome Created Date] >= dateadd(m, @monthsBeforeSnapshot, oc.snapshotnzDate) )
            --and  oc.[outcome approval Date] >= @cohort1
			and oc.[SnapshotNZDate] in ( @snapshotStart, @snapshotEnd)
            group by oc.[outcome organisation key], oc.[SnapshotNZDate]