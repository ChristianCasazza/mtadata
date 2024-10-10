WITH rushing_stats AS (
    SELECT
        posteam AS Team,
        rusher_player_name AS Player_Name,
        rusher_position AS Position,
        
        COUNT(*) AS Total_Rush_Attempts,
        ROUND(SUM(yards_gained), 2) AS Total_Rushing_Yards,
        ROUND(SUM(CASE WHEN yards_gained >= 10 THEN 1 ELSE 0 END), 2) AS Explosive_Rushes,
        ROUND(SUM(CASE WHEN first_down_rush = 1 THEN 1 ELSE 0 END), 2) AS Total_First_Downs_Rush,
        ROUND(SUM(CASE WHEN touchdown = 1 THEN 1 ELSE 0 END), 2) AS Rushing_Touchdowns,
        ROUND(SUM(CASE WHEN third_down_converted = 1 THEN 1 ELSE 0 END), 2) AS Third_Down_Conversions_Rush,
        ROUND(SUM(CASE WHEN fourth_down_converted = 1 THEN 1 ELSE 0 END), 2) AS Fourth_Down_Conversions_Rush,
        ROUND(SUM(epa), 2) AS Total_Rushing_EPA
    FROM {{ source('main', 'nfl_pbp_2024') }}
    WHERE play_type = 'run'
    AND rusher_position IN ('RB', 'WR', 'TE')
    GROUP BY posteam, rusher_player_name, rusher_position
),
receiving_stats AS (
    SELECT
        posteam AS Team,
        receiver_display_name AS Player_Name,
        receiver_position AS Position,
        
        COUNT(*) AS Total_Pass_Targets,
        ROUND(SUM(CASE WHEN complete_pass = 1 THEN 1 ELSE 0 END), 2) AS Total_Catches,
        ROUND(SUM(yards_gained), 2) AS Total_Receiving_Yards,
        ROUND(SUM(air_yards), 2) AS Total_Air_Yards,
        ROUND(SUM(yards_after_catch), 2) AS Total_YAC,
        ROUND(SUM(CASE WHEN first_down_pass = 1 THEN 1 ELSE 0 END), 2) AS Total_First_Downs_Pass,
        ROUND(SUM(CASE WHEN touchdown = 1 THEN 1 ELSE 0 END), 2) AS Receiving_Touchdowns,
        ROUND(SUM(CASE WHEN third_down_converted = 1 THEN 1 ELSE 0 END), 2) AS Third_Down_Conversions_Pass,
        ROUND(SUM(CASE WHEN fourth_down_converted = 1 THEN 1 ELSE 0 END), 2) AS Fourth_Down_Conversions_Pass,
        ROUND(SUM(epa), 2) AS Total_Receiving_EPA
    FROM {{ source('main', 'nfl_pbp_2024') }}
    WHERE play_type = 'pass'
    AND receiver_position IN ('RB', 'WR', 'TE')
    GROUP BY posteam, receiver_display_name, receiver_position
)
SELECT
    COALESCE(rushing_stats.Team, receiving_stats.Team) AS Team,
    COALESCE(rushing_stats.Player_Name, receiving_stats.Player_Name) AS Player_Name,
    COALESCE(rushing_stats.Position, receiving_stats.Position) AS Position,
    
    COALESCE(Total_Rush_Attempts, 0) AS Total_Rush_Attempts,
    COALESCE(Total_Rushing_Yards, 0) AS Total_Rushing_Yards,
    
    COALESCE(Total_Pass_Targets, 0) AS Total_Pass_Targets,
    COALESCE(Total_Catches, 0) AS Total_Catches,
    COALESCE(Total_Receiving_Yards, 0) AS Total_Receiving_Yards,
    
    ROUND(COALESCE(Total_Rushing_Yards, 0) + COALESCE(Total_Receiving_Yards, 0), 2) AS Total_Yards,
    
    COALESCE(Total_First_Downs_Rush, 0) AS Total_First_Downs_Rush,
    COALESCE(Total_First_Downs_Pass, 0) AS Total_First_Downs_Pass,
    
    COALESCE(Third_Down_Conversions_Rush, 0) AS Third_Down_Conversions_Rush,
    COALESCE(Third_Down_Conversions_Pass, 0) AS Third_Down_Conversions_Pass,
    ROUND((COALESCE(Third_Down_Conversions_Rush, 0) + COALESCE(Third_Down_Conversions_Pass, 0)), 2) AS Total_Third_Down_Conversions,

    COALESCE(Fourth_Down_Conversions_Rush, 0) AS Fourth_Down_Conversions_Rush,
    COALESCE(Fourth_Down_Conversions_Pass, 0) AS Fourth_Down_Conversions_Pass,
    ROUND((COALESCE(Fourth_Down_Conversions_Rush, 0) + COALESCE(Fourth_Down_Conversions_Pass, 0)), 2) AS Total_Fourth_Down_Conversions,
    
    -- Conversion and success rate calculations
    ROUND(COALESCE(Total_First_Downs_Rush, 0) * 1.0 / COALESCE(NULLIF(Total_Rush_Attempts, 0), 1), 2) AS First_Down_Rush_Rate,
    ROUND(COALESCE(Total_First_Downs_Pass, 0) * 1.0 / COALESCE(NULLIF(Total_Pass_Targets, 0), 1), 2) AS First_Down_Pass_Rate,
    ROUND(COALESCE(Total_Catches, 0) * 1.0 / COALESCE(NULLIF(Total_Pass_Targets, 0), 1), 2) AS Catch_Rate,
    
    ROUND(COALESCE(Total_Rushing_Yards, 0) * 1.0 / COALESCE(NULLIF(Total_Rush_Attempts, 0), 1), 2) AS Average_Rushing_Yards_Per_Attempt,
    ROUND(COALESCE(Total_Receiving_Yards, 0) * 1.0 / COALESCE(NULLIF(Total_Pass_Targets, 0), 1), 2) AS Average_Receiving_Yards_Per_Target,
    
    ROUND((COALESCE(Total_Rushing_EPA, 0) + COALESCE(Total_Receiving_EPA, 0)), 2) AS Total_EPA  -- Moved EPA to the last column
FROM rushing_stats
FULL OUTER JOIN receiving_stats
ON rushing_stats.Player_Name = receiving_stats.Player_Name
AND rushing_stats.Team = receiving_stats.Team
ORDER BY Total_EPA DESC
