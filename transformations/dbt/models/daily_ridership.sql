WITH ridership_data AS (
    SELECT  
        DATE_TRUNC('week', date) AS week_start,
        SUM(subways_total_ridership) AS ridership,
        'Subway' AS transport_type,
        AVG(subways_pct_pre_pandemic) AS avg_pct_pre_pandemic
    FROM {{ source('main', 'mta_daily_ridership') }}
    GROUP BY week_start, transport_type

    UNION ALL

    SELECT 
        DATE_TRUNC('week', date) AS week_start,
        SUM(buses_total_ridership) AS ridership,
        'Buses' AS transport_type,
        AVG(buses_pct_pre_pandemic) AS avg_pct_pre_pandemic
    FROM {{ source('main', 'mta_daily_ridership') }}
    GROUP BY week_start, transport_type

    UNION ALL

    SELECT 
        DATE_TRUNC('week', date) AS week_start,
        SUM(lirr_total_ridership) AS ridership,
        'LIRR' AS transport_type,
        AVG(lirr_pct_pre_pandemic) AS avg_pct_pre_pandemic
    FROM {{ source('main', 'mta_daily_ridership') }}
    GROUP BY week_start, transport_type

    UNION ALL

    SELECT 
        DATE_TRUNC('week', date) AS week_start,
        SUM(metro_north_total_ridership) AS ridership,
        'Metro North' AS transport_type,
        AVG(metro_north_pct_pre_pandemic) AS avg_pct_pre_pandemic
    FROM {{ source('main', 'mta_daily_ridership') }}
    GROUP BY week_start, transport_type

    UNION ALL

    SELECT 
        DATE_TRUNC('week', date) AS week_start,
        SUM(access_a_ride_total_trips) AS ridership,
        'Access-A-Ride' AS transport_type,
        AVG(access_a_ride_pct_pre_pandemic) AS avg_pct_pre_pandemic
    FROM {{ source('main', 'mta_daily_ridership') }}
    GROUP BY week_start, transport_type

    UNION ALL

    SELECT 
        DATE_TRUNC('week', date) AS week_start,
        SUM(bridges_tunnels_total_traffic) AS ridership,
        'Bridges and Tunnels' AS transport_type,
        AVG(bridges_tunnels_pct_pre_pandemic) AS avg_pct_pre_pandemic
    FROM {{ source('main', 'mta_daily_ridership') }}
    GROUP BY week_start, transport_type

    UNION ALL

    SELECT 
        DATE_TRUNC('week', date) AS week_start,
        SUM(staten_island_railway_total_ridership) AS ridership,
        'Staten Island Railway' AS transport_type,
        AVG(staten_island_railway_pct_pre_pandemic) AS avg_pct_pre_pandemic
    FROM {{ source('main', 'mta_daily_ridership') }}
    GROUP BY week_start, transport_type
),
weather_data AS (
    SELECT 
        DATE_TRUNC('week', date) AS week_start,
        AVG(temperature_mean) AS avg_weekly_temperature,
        SUM(precipitation_sum) AS total_weekly_precipitation
    FROM 
        daily_weather_asset
    GROUP BY 
        DATE_TRUNC('week', date)
)
SELECT 
    rd.week_start, 
    rd.transport_type,
    rd.ridership,
    rd.avg_pct_pre_pandemic,
    wd.avg_weekly_temperature,
    wd.total_weekly_precipitation
FROM 
    ridership_data rd
LEFT JOIN 
    weather_data wd
ON 
    rd.week_start = wd.week_start
WHERE 
    rd.week_start < '2024-10-15'
ORDER BY 
    rd.week_start, rd.transport_type
