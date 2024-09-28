
  
    
    

    create  table
      "database_raw"."main"."penalty_totals__dbt_tmp"
  
    as (
      SELECT 
    week,
    penalty_type,
    COUNT(*) AS total_penalties
FROM "database_raw"."main"."nfl_pbp_2024"
WHERE penalty_type IS NOT NULL
GROUP BY 
    week,
    penalty_type
ORDER BY 
    week ASC
    );
  
  