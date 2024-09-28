
  
    
    

    create  table
      "database_raw"."main"."receiver_stats_per_team__dbt_tmp"
  
    as (
      SELECT
    posteam AS Team,  
    receiver_display_name AS Receiver_Name,  
    COUNT(*) AS Targets,  
    SUM(CASE WHEN complete_pass = 1 THEN 1 ELSE 0 END) AS Total_Catches,  
    SUM(yards_gained) AS Total_Yards,  
    SUM(air_yards) AS Total_Air_Yards,  
    SUM(CASE WHEN air_yards > 0 THEN air_yards ELSE 0 END) AS Adj_Air_Yards,  
    SUM(yards_after_catch) AS Total_YAC,  
    SUM(CASE WHEN touchdown = 1 THEN 1 ELSE 0 END) AS Total_Touchdowns,  
    SUM(CASE WHEN interception = 1 THEN 1 ELSE 0 END) AS Targeted_Interceptions,  
    (SUM(yards_after_catch) * 100.0 / NULLIF(SUM(yards_gained), 0)) AS YAC_Percentage,  
    
    SUM(CASE WHEN first_down_pass = 1 THEN 1 ELSE 0 END) AS Total_First_Down_Passes,  
    SUM(CASE WHEN third_down_converted = 1 THEN 1 ELSE 0 END) AS Third_Down_Conversions,  
    SUM(CASE WHEN fourth_down_converted = 1 THEN 1 ELSE 0 END) AS Fourth_Down_Conversions,  
    (SUM(CASE WHEN third_down_converted = 1 THEN 1 ELSE 0 END) + SUM(CASE WHEN fourth_down_converted = 1 THEN 1 ELSE 0 END)) AS Total_Conversions,  
    
    (SUM(CASE WHEN down = 3 AND first_down_pass = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN down = 3 THEN 1 ELSE 0 END), 0)) AS Third_Down_Success_Rate,  
    (SUM(CASE WHEN down = 4 AND first_down_pass = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN down = 4 THEN 1 ELSE 0 END), 0)) AS Fourth_Down_Success_Rate,  
    (SUM(CASE WHEN down IN (3, 4) AND first_down_pass = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN down IN (3, 4) THEN 1 ELSE 0 END), 0)) AS Both_Down_Success_Rate,  
    
    SUM(epa) AS Total_EPA  
FROM "database_raw"."main"."nfl_pbp_2024"
WHERE play_type = 'pass'
AND receiver_display_name IS NOT NULL  
GROUP BY posteam, receiver_display_name
ORDER BY Total_EPA DESC;
    );
  
  