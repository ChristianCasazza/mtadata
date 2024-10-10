WITH pass_data AS (
    SELECT 
        air_yards,
        pass_location,
        CASE 
            WHEN air_yards >= -10 AND air_yards < 0 THEN '[-10-0]'
            WHEN air_yards >= 0 AND air_yards < 10 THEN '[0-10]'
            WHEN air_yards >= 10 AND air_yards < 20 THEN '[10-20]'
            WHEN air_yards >= 20 AND air_yards < 30 THEN '[20-30]'
            WHEN air_yards >= 30 AND air_yards < 40 THEN '[30-40]'
            WHEN air_yards >= 40 AND air_yards < 50 THEN '[40-50]'
            WHEN air_yards >= 50 AND air_yards < 60 THEN '[50-60]'
            WHEN air_yards >= 60 AND air_yards <= 70 THEN '[60-70]'
        END AS air_yards_interval,  -- Separate column for air_yards interval
        CONCAT(
            CASE 
                WHEN air_yards >= -10 AND air_yards < 0 THEN '[-10-0]'
                WHEN air_yards >= 0 AND air_yards < 10 THEN '[0-10]'
                WHEN air_yards >= 10 AND air_yards < 20 THEN '[10-20]'
                WHEN air_yards >= 20 AND air_yards < 30 THEN '[20-30]'
                WHEN air_yards >= 30 AND air_yards < 40 THEN '[30-40]'
                WHEN air_yards >= 40 AND air_yards < 50 THEN '[40-50]'
                WHEN air_yards >= 50 AND air_yards < 60 THEN '[50-60]'
                WHEN air_yards >= 60 AND air_yards <= 70 THEN '[60-70]'
            END, ' - ', pass_location
        ) AS pass_type, -- Combined pass_type using air_yards interval and pass_location
        yards_after_catch,
        shotgun,
        epa,
        yards_gained,
        success,
        first_down,
        interception,
        qb_hit,
        down,
        third_down_converted,
        fourth_down_converted
    FROM {{ source('main', 'nfl_pbp_2024') }}
    WHERE play_type = 'pass'
    AND pass_location IS NOT NULL
    AND air_yards >= -10 AND air_yards <= 70 -- Filter to only include the specified air_yards intervals
)
SELECT 
    -- First column: Concatenated pass_type
    pass_type,

    -- Pass type percentage
    ROUND((COUNT(*) * 1.0 / SUM(COUNT(*)) OVER ()), 4) AS pass_type_percentage,

    -- Yardage metrics
    ROUND(AVG(yards_gained), 2) AS avg_yards_gained,
    SUM(air_yards) AS total_air_yards, -- Total air yards
    SUM(COALESCE(yards_after_catch, 0)) AS total_yac_yards, -- Total YAC with COALESCE to handle NULLs

    -- Rate metrics
    COALESCE(ROUND(SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4), 0) AS success_rate,
    COALESCE(ROUND(SUM(interception) * 1.0 / COUNT(*), 4), 0) AS interception_rate, -- Interception rate
    COALESCE(ROUND(SUM(CASE WHEN first_down = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4), 0) AS first_down_rate,
    COALESCE(ROUND(SUM(CASE WHEN down = 3 AND third_down_converted = 1 THEN 1 ELSE 0 END) * 1.0 / SUM(CASE WHEN down = 3 THEN 1 ELSE 0 END), 4), 0) AS third_down_conversion_rate,
    COALESCE(ROUND(SUM(CASE WHEN down = 4 AND fourth_down_converted = 1 THEN 1 ELSE 0 END) * 1.0 / SUM(CASE WHEN down = 4 THEN 1 ELSE 0 END), 4), 0) AS fourth_down_conversion_rate,
    COALESCE(ROUND(SUM(CASE WHEN down IN (3, 4) AND (third_down_converted = 1 OR fourth_down_converted = 1) THEN 1 ELSE 0 END) * 1.0 / SUM(CASE WHEN down IN (3, 4) THEN 1 ELSE 0 END), 4), 0) AS combined_third_fourth_down_conversion_rate,

    -- Counting stats
    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) AS successful_passes,
    SUM(CASE WHEN first_down = 1 THEN 1 ELSE 0 END) AS total_first_downs,
    SUM(interception) AS interceptions,
    SUM(shotgun) AS shotgun_plays,
    SUM(CASE WHEN qb_hit = 1 THEN 1 ELSE 0 END) AS total_qb_hits,

    -- EPA
    ROUND(AVG(epa), 2) AS avg_epa,
    ROUND(MEDIAN(epa), 2) AS median_epa,

    -- Last two columns: Air yards interval and pass location
    air_yards_interval, -- Air yards interval as a separate column
    pass_location -- Pass location as a separate column
FROM pass_data
GROUP BY pass_type, air_yards_interval, pass_location
ORDER BY
    CASE 
        WHEN pass_type LIKE '%left%' THEN 1
        WHEN pass_type LIKE '%middle%' THEN 2
        WHEN pass_type LIKE '%right%' THEN 3
        ELSE 4 -- For any other values that don't match
    END, pass_type
