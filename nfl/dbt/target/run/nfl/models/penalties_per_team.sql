
  
    
    

    create  table
      "database_output"."main"."penalties_per_team__dbt_tmp"
  
    as (
      SELECT
    penalty_team, 
    week,
    penalty_type,
    COUNT(*) AS total_penalties
FROM "database_output"."main"."nfl_pbp_2024"
WHERE penalty_type IS NOT NULL
GROUP BY 
    week,
    penalty_type,
    penalty_team
ORDER BY 
    week ASC
    );
  
  