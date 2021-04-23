WITH contactlist AS ( 

                        SELECT 
                        o.[wave] 
                        , o.[organisation key] 
                        , o.[organisation nzbn]                                      AS [NZBN] 
                        , o.[organisation nzte number]                               AS [NZTE Number] 
                        , o.[organisation nzte segment]                              AS [Segment] 
                        , o.[Organisation Journey Stage]
                        , o.[Organisation Entry To Segment]                            as  [Entry to Segment]
                        , o.[organisation legal name]                                                  AS [Customer Name] 
                        , o.[organisation nzte sector]                                               AS [Sector] 
                        , o.[ir range] 
                        , o.[organisation manager]                                   AS [Customer Manager] 
                        , o.[organisation director]                                  AS [Customer Director] 
                        -- TM  
                        , tmSum.[#target markets] 
                       , a.[#actions]                                    -- Market Actions  
                        , agp.[#gp actions]                              -- Game Plan Actions
                        , Row_number() OVER (partition BY o.[organisation key] ORDER BY a.[#actions] DESC)         AS [Action Rank] 
                        , gpo.[#objectives] 
                        , tm.[customer market name]                     AS [Market Name] 
                        , tm.[customer market nzte region]              AS [Market Region] 
                        , tm.[customer market bdm]                      AS [BDM] 
                        , tm.[customer market stage name]               AS [Market Stage] 
                        , tm.[customer market target market start date] AS [TM start Date] 
                        -- game plans  
                                         , [game plan key]
                        , [game plan name] 
                        , [game plan status] 
                        , [game plan agreed sent on date] 
                        , [game plan category] 
                        , [game plan customer contact]                  AS [GP contact] 
                                         , [game plan person responsible email]                 AS [GP person responsible email]
                                         , [user active] 
                        -- Contact (Game Plan)  
                        , gc.[contact key]                              AS [GP contact Key] 
                        , gc.[contact full name]                        AS [GP contact Name] 
                        , gc.[contact email]                            AS [GP contact Email]
                        , o.[organisation primary contact]              AS [Primary Contact] 
                        -- Contact (Primary Contact)  
                        , pc.[contact key]                               AS [PC contact Key] 
                        , pc.[contact full name]                         AS [PC Name] 
                        , pc.[contact email]                             AS [PC contact Email] 
                        , Iif (gc.[contact email] = pc.[contact email] OR gp.[game plan customer contact key] IS NULL 
                                                , Cast(1 AS BIT)     --true  
                                                , Cast(0 AS BIT)     --false  
                        )                                                                              AS [Is Same As Primary Contact]

                        FROM   -- All surveyed customers with wave
                        (      SELECT  org.[organisation legal name] 
                                                       , org.[organisation key] 
                                                       , org.[organisation nzbn] 
                                                       , org.[organisation nzte number] 
                                                       , org.[organisation primary contact key] 
                                                       , org.[Organisation Entry To Segment]
                                                       , org.[organisation primary contact] 
                                                       , org.[organisation nzte sector] 
                                                       , org.[organisation id] 
                                                       , org.[organisation manager] 
                                                       , org.[Organisation Journey Stage]
                                                       , org.[organisation director] 
                                                       , org.[organisation nzte segment] 
                                                       , f.[financials international revenue range]                               AS 'IR Range' 
                                                       , (Row_number() OVER (partition BY org.[organisation nzte segment], org.[organisation nzte sector], f.[financials international revenue range] 
                                                       ORDER BY [organisation nzte sector])) %2 + 1  AS 'Wave' 

                                                       FROM   [CRM].[organisations] org 
                                                       -- International Revenue  
                                                       LEFT OUTER JOIN [CRM].[financials] f 
                                                                     ON org.[organisation key] = f.[financials organisation key] 
                                                              AND [financials valid financial] = 1 
                                                              AND [financials is latest] = 1 
                                                       WHERE org.[organisation nzte segment] IN (  'Focus Coalition', 'Focus', 'Foundation') 
                                         ) o 
                                         ----- count Target Markets------
                                         LEFT JOIN ( 
                                                              SELECT Count(DISTINCT(tm1.[customer market key])) AS [#Target Markets] 
                                                              , tm1.[customer market organisation key] 
                                                              FROM [CRM].[customermarkets] tm1 
                                                              WHERE tm1.[customer market target market] = 'Target Market' 
                                                              GROUP BY tm1.[customer market organisation key] 
                                                       ) tmSum 
                                                       ON o.[organisation key] = tmSum.[customer market organisation key] 
                                         ----- Target Markets------
                                         LEFT JOIN [CRM].[customermarkets] tm 
                                                       ON o.[organisation key] = tm.[customer market organisation key] 
                                                       AND tm.[customer market target market] = 'Target Market' 
                                         ----- Game Plans ---
                                         LEFT JOIN ( 
                                                                     SELECT  [game plan key] 
                                                                     , [game plan name] 
                                                                     , [game plan status] 
                                                                     , [game plan agreed sent on date] 
                                                                     , [game plan category] 
                                                                     , [game plan customer contact] 
                                                                     , [game plan customer contact key]
                                                                     , [user primary email]                   as [game plan person responsible email]
                                                                     , gu.[user active] 
                                                                     , lt.[organisation id] 
                                                                     , lt.[customer market id] 
                                                                     FROM [crm].[gameplans] g 
                                                                                  LEFT JOIN (
                                                                                         SELECT DISTINCT l.[organisation id], l.[customer market id], l.[gameplan id] 
                                                                                         FROM [crm].[link table] l 
                                                                                         ) lt 
                                                                                         ON lt.[gameplan id] = g.[game plan id] 
                                                                                  left join [crm].[users] gu
                                                                                         on gu.[user key] = g.[game plan person responsible key]
                                                                                         ---and gu.[user active] = 'Yes'
                                                                     WHERE g.[game plan status] IN ('Agreed', 'Proposed') 
                                                                           AND g.[game plan state] = 'Active' 
                                                                           AND  g.[game plan category] IN ('Market', 'Region') 
                                                                           ) gp 
                                                       ON gp.[customer market id] = tm.[customer market id] 
                                                       AND gp.[organisation id] = o.[organisation id] 
                                         ---- Game Plan Contacts----
                                         LEFT JOIN [CRM].[contacts] gc 
                                                       ON [game plan customer contact key] = gc.[contact key] 
                                         --- Primary Contacts----
                        LEFT JOIN [CRM].[contacts] pc 
                                                    ON o.[organisation primary contact key] = pc.[contact key] 
                        --- Counting related Actions in the Market  
                        LEFT JOIN ( 
                                                SELECT Count(DISTINCT (a1.[action key])) AS [#actions] 
                                                , a1.[action customer market key] 
                                                FROM [crm].[actions] a1 
                                                WHERE (a1.[action status] <> 'Withdrawn' AND a1.[action end date] > '2019-07-01') 
                                                GROUP BY  a1.[action customer market key] 
                                                ) a 
                                                ON a.[action customer market key] = tm.[customer market key] 
                        --- Counting Game Plan Actions  
                        LEFT JOIN ( 
                                                SELECT Count(DISTINCT (a1.[action key])) AS [#GP actions] 
                                                , a1.[action game plan key] 
                                                FROM [crm].[actions] a1 
                                                WHERE (a1.[action status] <> 'Withdrawn' AND a1.[action end date] > '2019-07-01') 
                                                GROUP BY  a1.[action game plan key] 
                                                ) agp 
                                                ON agp.[action game plan key] = gp.[game plan key] 
                        --- Counting related Objectives to the Game Plans  
                        LEFT JOIN ( 
                                                SELECT Count(DISTINCT([objective id])) AS [#objectives] 
                                                , [objective gameplan key] 
                                                FROM [crm].[objectives] o 
                                                WHERE [objective status] IN ('Agreed', 'On Hold', 'Proposed') 
                                                GROUP BY  [objective gameplan key] 
                                                ) AS gpo 
                                                ON gpo.[objective gameplan key] = gp.[game plan key] 


                        ) 
                        ------0. To ask ME  
                        -- results to coaltion_contacts sheet
                        SELECT  distinct
                        c.[Customer Name], cg.[Customer Group Name]
                        , cg.[Customer Group Member Organisation Name]
                        , c.*
                        
                        FROM contactlist c
                                left join [CRM].[CustomerGroup] cg
                                        on c.[Organisation Key] = cg.[Customer Group Primary Organisation Key]
                                        and cg.[Customer Group Member Status] = 'Coalition member'
                                        and cg.[Customer Group Status] = 'Active'
                        where c.Segment = 'Focus Coalition'
                        

                        -- --- results to coalition_members sheet
                        -- SELECT  distinct
                        -- c.[Customer Name], cg.[Customer Group Name]
                        -- , cg.[Customer Group Member Organisation Name]
                        -- , cg.[Customer Group Member Organisation Key]
                        -- , cg.[Customer Group Sector]
                        -- , c.[Primary Contact]
                        -- , m.[Organisation NZTE Segment]
                        -- , m.[Organisation Primary Contact]
                        -- , m.[Organisation NZTE Sector]
                        
                        -- FROM contactlist c
                        --         left join [CRM].[CustomerGroup] cg
                        --                 on c.[Organisation Key] = cg.[Customer Group Primary Organisation Key]
                        --                 and cg.[Customer Group Member Status] = 'Coalition member'
                        --                 and cg.[Customer Group Status] = 'Active'
                        --         left join crm.[Organisations] m
                        --                 on m.[Organisation Key] = cg.[Customer Group Member Organisation Key]
                        -- where c.Segment = 'Focus Coalition'
                      
