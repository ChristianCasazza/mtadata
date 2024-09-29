
  
    
    

    create  table
      "database_raw"."main"."team_breakdown__dbt_tmp"
  
    as (
      WITH play_data AS (
    SELECT 
        posteam,  
        CASE 
            WHEN play_type_nfl = 'SACK' THEN 'Sack'
            WHEN play_type_nfl = 'INTERCEPTION' THEN 'Interception'
            WHEN play_type = 'run' AND qb_scramble = 1 THEN 'QB Scramble'
            WHEN play_type = 'pass' THEN 'Pass'
            ELSE 'Rush'
        END AS play_type_category,  
        yards_gained,
        epa,
        success,
        penalty,
        CASE 
            WHEN yards_gained >= ydstogo THEN 1 
            ELSE 0 
        END AS first_down_achieved
    FROM "database_raw"."main"."nfl_pbp_2024"
    WHERE play_type NOT IN ('no_play', 'qb_kneel', 'qb_spike', 'field_goal')
    AND penalty = 0  
)
SELECT 
    posteam,  
    play_type_category, 
    COUNT(*) AS total_plays,  
    (COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY posteam)) AS play_percentage,
    AVG(yards_gained) AS avg_yards_gained,
    MEDIAN(yards_gained) AS median_yards_gained,
    SUM(yards_gained) AS total_yards_gained,
    AVG(epa) AS avg_epa,
    MEDIAN(epa) AS median_epa,
    SUM(epa) AS total_epa,
    SUM(CASE WHEN epa > 0 THEN 1 ELSE 0 END) AS positive_epa_plays,
    (SUM(CASE WHEN epa > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS positive_epa_rate,
    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) AS successful_plays,
    (SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) AS success_rate,
    SUM(first_down_achieved) AS first_down_achievements,
    (SUM(first_down_achieved) * 100.0 / COUNT(*)) AS first_down_rate
FROM play_data
GROUP BY posteam, play_type_category
    );
  
  