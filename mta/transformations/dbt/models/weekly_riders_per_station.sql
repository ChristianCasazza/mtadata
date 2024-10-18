WITH ridership_data AS (
    SELECT 
        station_complex_id, 
        station_complex, 
        DATE_TRUNC('week', transit_timestamp) AS week_date, 
        SUM(ridership) AS total_ridership,
        latitude,
        longitude
    FROM 
        {{ source('main', 'mta_hourly_subway_socrata') }}
    GROUP BY 
        station_complex_id, 
        station_complex, 
        week_date, 
        latitude, 
        longitude
),
weather_data AS (
    SELECT
        DATE_TRUNC('week', date) AS week_date, 
        SUM(precipitation_sum) AS total_precipitation -- Using the precipitation_sum column for total rain
    FROM 
           {{ source('main', 'daily_weather_asset') }}
    GROUP BY 
        week_date
)
SELECT 
    r.station_complex_id,
    r.station_complex,
    r.latitude,
    r.longitude,
    r.week_date,
    r.total_ridership,
    w.total_precipitation
FROM 
    ridership_data r
LEFT JOIN 
    weather_data w
ON 
    r.week_date = w.week_date
ORDER BY 
    r.week_date, r.total_ridership DESC
