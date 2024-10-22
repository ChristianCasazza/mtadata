SELECT 
    DATE_TRUNC('week', date) AS week_start,
    SUM(subways_total_ridership) AS ridership,
    'subway' AS transport_type,
    AVG(subways_pct_pre_pandemic) AS avg_pct_pre_pandemic
FROM {{ source('main', 'mta_daily_ridership') }}
GROUP BY week_start, transport_type

UNION ALL

SELECT 
    DATE_TRUNC('week', date) AS week_start,
    SUM(buses_total_ridership) AS ridership,
    'buses' AS transport_type,
    AVG(buses_pct_pre_pandemic) AS avg_pct_pre_pandemic
FROM {{ source('main', 'mta_daily_ridership') }}
GROUP BY week_start, transport_type

UNION ALL

SELECT 
    DATE_TRUNC('week', date) AS week_start,
    SUM(lirr_total_ridership) AS ridership,
    'lirr' AS transport_type,
    AVG(lirr_pct_pre_pandemic) AS avg_pct_pre_pandemic
FROM {{ source('main', 'mta_daily_ridership') }}
GROUP BY week_start, transport_type

UNION ALL

SELECT 
    DATE_TRUNC('week', date) AS week_start,
    SUM(metro_north_total_ridership) AS ridership,
    'metro_north' AS transport_type,
    AVG(metro_north_pct_pre_pandemic) AS avg_pct_pre_pandemic
FROM {{ source('main', 'mta_daily_ridership') }}
GROUP BY week_start, transport_type

UNION ALL

SELECT 
    DATE_TRUNC('week', date) AS week_start,
    SUM(access_a_ride_total_trips) AS ridership,
    'access_a_ride' AS transport_type,
    AVG(access_a_ride_pct_pre_pandemic) AS avg_pct_pre_pandemic
FROM {{ source('main', 'mta_daily_ridership') }}
GROUP BY week_start, transport_type

UNION ALL

SELECT 
    DATE_TRUNC('week', date) AS week_start,
    SUM(bridges_tunnels_total_traffic) AS ridership,
    'bridges_tunnels' AS transport_type,
    AVG(bridges_tunnels_pct_pre_pandemic) AS avg_pct_pre_pandemic
FROM {{ source('main', 'mta_daily_ridership') }}
GROUP BY week_start, transport_type

UNION ALL

SELECT 
    DATE_TRUNC('week', date) AS week_start,
    SUM(staten_island_railway_total_ridership) AS ridership,
    'staten_island_railway' AS transport_type,
    AVG(staten_island_railway_pct_pre_pandemic) AS avg_pct_pre_pandemic
FROM {{ source('main', 'mta_daily_ridership') }}
GROUP BY week_start, transport_type
