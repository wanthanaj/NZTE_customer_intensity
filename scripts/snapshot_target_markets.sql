-- purpose: to see numbers of TMs that  turned on or off overtime

select  cm.[SnapshotNZDate]
    , o.[Organisation Key]                              as [customer_key]
    , o.[Organisation Legal Name]                       as [customer_name]
    , [Customer Market Name]                            as [customer_market_name]
    ,[Customer Market Target Market Start Date]         as [tm_start_date]
    , [Customer Market Target Market End Date]          as [tm_end_date]
    , [Customer Market Target Market]          as [cm_or_tm]
from CRM_Snapshot.[CustomerMarkets] cm
left join CRM_Snapshot.[Organisations] o
        on o.[Organisation Key] = cm.[Customer Market Organisation Key]
        and o.SnapshotNZDate = cm.SnapshotNZDate
where [Customer Market Target Market Start Date] is not null
order by cm.SnapshotNZDate,o.[Organisation Legal Name]


-- select  o.[Organisation Legal Name]
-- , [Customer Market Name], count(distinct([Customer Market Target Market Start Date])), count(distinct([Customer Market Target Market End Date]))
-- from CRM_Snapshot.[CustomerMarkets] cm
--     left join CRM_Snapshot.[Organisations] o
--         on o.[Organisation Key] = cm.[Customer Market Organisation Key]
--         and o.SnapshotNZDate = cm.SnapshotNZDate
-- where [Customer Market Target Market Start Date] is not null
-- and [Customer Market Name] = 'Thailand'
-- group by o.[Organisation Legal Name]
-- , [Customer Market Name]
-- order by count(distinct([Customer Market Target Market Start Date])) desc, count(distinct([Customer Market Target Market End Date])) desc

