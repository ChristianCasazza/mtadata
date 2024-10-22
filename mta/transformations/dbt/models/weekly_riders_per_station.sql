with weekly_ridership AS (
    SELECT 
        station_complex, 
        DATE_TRUNC('week', transit_timestamp) AS week_start,
        SUM(ridership) AS total_weekly_ridership
    FROM 
        {{ source('main', 'mta_hourly_subway_socrata') }}
    GROUP BY 
        station_complex, 
        DATE_TRUNC('week', transit_timestamp)
)
SELECT 
    station_complex, 
    week_start, 
    total_weekly_ridership
FROM 
    weekly_ridership
ORDER BY 
    station_complex, 
    week_start
