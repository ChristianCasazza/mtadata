WITH pass_data AS (
    SELECT 
        CONCAT(pass_length, ' - ', pass_location) AS pass_type,  
        air_yards,
        yards_after_catch,
        shotgun,
        epa,
        yards_gained,
        success,
        CASE 
            WHEN yards_gained >= ydstogo THEN 1 
            ELSE 0 
        END AS first_down_achieved
    FROM "database_raw"."main"."nfl_pbp_2024"
    WHERE down = 1
    AND play_type = 'pass'
    AND posteam = 'MIA'
)
SELECT 
    pass_type, 
    AVG(air_yards) AS avg_air_yards,
    MEDIAN(air_yards) AS median_air_yards,
    AVG(yards_after_catch) AS avg_yac,
    MEDIAN(yards_after_catch) AS median_yac,
    AVG(yards_gained) AS avg_yards_gained,
    MEDIAN(yards_gained) AS median_yards_gained,
    SUM(yards_gained) AS total_yards_gained,
    AVG(epa) AS avg_epa,
    MEDIAN(epa) AS median_epa,
    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) AS successful_passes,
    (SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) AS success_rate,
    SUM(first_down_achieved) AS first_down_achievements,
    (SUM(first_down_achieved) * 1.0 / COUNT(*)) AS first_down_rate,
    SUM(CASE WHEN shotgun = 1 THEN 1 ELSE 0 END) AS shotgun_plays,
    (SUM(CASE WHEN shotgun = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) AS shotgun_rate
FROM pass_data
GROUP BY pass_type;