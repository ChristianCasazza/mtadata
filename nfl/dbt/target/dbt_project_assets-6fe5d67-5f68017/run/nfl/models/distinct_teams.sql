
  
    
    

    create  table
      "database_raw"."main"."distinct_teams__dbt_tmp"
  
    as (
      SELECT DISTINCT posteam 
FROM "database_raw"."main"."nfl_pbp_2024";
    );
  
  