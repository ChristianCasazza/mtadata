
  
    
    

    create  table
      "database_raw"."main"."rushing__dbt_tmp"
  
    as (
      SELECT
    posteam AS Team,  
    rusher_player_name AS Rusher_Name,  
    rusher_position AS Rusher_Position,  

    COUNT(*) AS Rush_Attempts,  
    SUM(yards_gained) AS Total_Rushing_Yards,  
    SUM(CASE WHEN yards_gained >= 10 THEN 1 ELSE 0 END) AS Explosive_Rushes,  
    SUM(CASE WHEN first_down_rush = 1 THEN 1 ELSE 0 END) AS Total_First_Downs,  
    SUM(CASE WHEN touchdown = 1 THEN 1 ELSE 0 END) AS Rushing_Touchdowns,  
    
    SUM(CASE WHEN third_down_converted = 1 THEN 1 ELSE 0 END) AS Third_Down_Conversions,  
    SUM(CASE WHEN fourth_down_converted = 1 THEN 1 ELSE 0 END) AS Fourth_Down_Conversions,  
    (SUM(CASE WHEN third_down_converted = 1 THEN 1 ELSE 0 END) + SUM(CASE WHEN fourth_down_converted = 1 THEN 1 ELSE 0 END)) AS Total_Conversions,  
    
    (SUM(CASE WHEN down = 3 AND first_down_rush = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN down = 3 THEN 1 ELSE 0 END), 0)) AS Third_Down_Success_Rate,  
    (SUM(CASE WHEN down = 4 AND first_down_rush = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN down = 4 THEN 1 ELSE 0 END), 0)) AS Fourth_Down_Success_Rate,  
    (SUM(CASE WHEN down IN (3, 4) AND first_down_rush = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN down IN (3, 4) THEN 1 ELSE 0 END), 0)) AS Both_Down_Success_Rate,  

    SUM(epa) AS Total_EPA  
FROM "database_raw"."main"."nfl_pbp_2024"
WHERE play_type = 'run'
AND rusher_player_name IS NOT NULL  
AND rusher_position IN ('RB', 'WR')  
GROUP BY posteam, rusher_player_name, rusher_position  
ORDER BY Total_EPA DESC;
    );
  
  