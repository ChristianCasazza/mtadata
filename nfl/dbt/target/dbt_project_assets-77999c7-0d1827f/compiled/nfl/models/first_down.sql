WITH play_data AS (
    SELECT 
        play_type,
        yards_gained,
        epa,
        success,
        CASE 
            WHEN yards_gained >= ydstogo THEN 1 
            ELSE 0 
        END AS first_down_achieved
    FROM pbp
    WHERE down = 1
    AND play_type NOT IN ('no_play', 'qb_kneel', 'qb_spike', 'field_goal')
    AND posteam = 'MIA'
)
SELECT 
    play_type, 
    (COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()) AS play_percentage,
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
GROUP BY play_type