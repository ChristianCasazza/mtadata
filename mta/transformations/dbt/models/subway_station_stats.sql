WITH ridership_by_day AS (
    SELECT 
        station_complex,
        CAST(transit_timestamp AS DATE) AS day,
        DAYOFWEEK(transit_timestamp) AS weekday,
        SUM(ridership) AS daily_ridership
    FROM 
        {{ source('main', 'mta_hourly_subway_socrata') }}
    WHERE 
        YEAR(transit_timestamp) = 2024
    GROUP BY 
        station_complex, 
        CAST(transit_timestamp AS DATE), 
        DAYOFWEEK(transit_timestamp)
),
weekday_weekend AS (
    SELECT 
        station_complex,
        CASE 
            WHEN weekday IN (2, 3, 4, 5, 6) THEN 'weekday'
            ELSE 'weekend'
        END AS day_type,
        AVG(daily_ridership) AS avg_ridership
    FROM 
        ridership_by_day
    GROUP BY 
        station_complex, 
        CASE 
            WHEN weekday IN (2, 3, 4, 5, 6) THEN 'weekday'
            ELSE 'weekend'
        END
),
single_day_stats AS (
    SELECT 
        station_complex,
        MAX(daily_ridership) AS highest_single_day_ridership,
        MIN(daily_ridership) AS lowest_single_day_ridership
    FROM 
        ridership_by_day
    GROUP BY 
        station_complex
),
highest_single_day AS (
    SELECT 
        station_complex,
        day AS highest_single_day_ridership_day
    FROM 
        ridership_by_day rbd
    WHERE 
        rbd.daily_ridership = (
            SELECT 
                MAX(daily_ridership)
            FROM 
                ridership_by_day
            WHERE 
                station_complex = rbd.station_complex
        )
),
lowest_single_day AS (
    SELECT 
        station_complex,
        day AS lowest_single_day_ridership_day
    FROM 
        ridership_by_day rbd
    WHERE 
        rbd.daily_ridership = (
            SELECT 
                MIN(daily_ridership)
            FROM 
                ridership_by_day
            WHERE 
                station_complex = rbd.station_complex
        )
)
SELECT 
    rbd.station_complex,
    MAX(CASE WHEN wwd.day_type = 'weekday' THEN wwd.avg_ridership END) AS avg_weekday_ridership,
    MAX(CASE WHEN wwd.day_type = 'weekend' THEN wwd.avg_ridership END) AS avg_weekend_ridership,
    sds.highest_single_day_ridership,
    hsd.highest_single_day_ridership_day,
    sds.lowest_single_day_ridership,
    lsd.lowest_single_day_ridership_day,
    (MAX(CASE WHEN wwd.day_type = 'weekend' THEN wwd.avg_ridership END) - MAX(CASE WHEN wwd.day_type = 'weekday' THEN wwd.avg_ridership END)) * 1.0 / MAX(CASE WHEN wwd.day_type = 'weekday' THEN wwd.avg_ridership END) AS weekend_percentage_change
FROM 
    ridership_by_day rbd
JOIN 
    weekday_weekend wwd ON rbd.station_complex = wwd.station_complex
JOIN 
    single_day_stats sds ON rbd.station_complex = sds.station_complex
JOIN 
    highest_single_day hsd ON rbd.station_complex = hsd.station_complex
JOIN 
    lowest_single_day lsd ON rbd.station_complex = lsd.station_complex
GROUP BY 
    rbd.station_complex, 
    sds.highest_single_day_ridership, 
    sds.lowest_single_day_ridership, 
    hsd.highest_single_day_ridership_day, 
    lsd.lowest_single_day_ridership_day
ORDER BY 
    rbd.station_complex
