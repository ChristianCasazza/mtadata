
  
    
    

    create  table
      "data"."main"."subway_station_stats__dbt_tmp"
  
    as (
      WITH ridership_by_day AS (
    -- Calculate daily ridership for each station, weekday/weekend split
    SELECT 
        station_complex,
        CAST(transit_timestamp AS DATE) AS day,
        DAYOFWEEK(transit_timestamp) AS weekday,
        SUM(ridership) AS daily_ridership,
        SUM(transfers) AS daily_transfers,
        MIN(latitude) AS latitude,   -- Add latitude
        MIN(longitude) AS longitude  -- Add longitude
    FROM 
        "data"."main"."mta_hourly_subway_socrata"
    WHERE 
        YEAR(transit_timestamp) = 2024
    GROUP BY 
        station_complex, 
        CAST(transit_timestamp AS DATE), 
        DAYOFWEEK(transit_timestamp)
),
total_riders_ytd AS (
    -- Total riders in 2023 and 2024 YTD
    SELECT 
        station_complex,
        SUM(CASE WHEN YEAR(transit_timestamp) = 2023 THEN ridership ELSE 0 END) AS total_riders_2023,
        SUM(CASE WHEN YEAR(transit_timestamp) = 2024 THEN ridership ELSE 0 END) AS total_riders_2024,
        MIN(latitude) AS latitude,   -- Add latitude
        MIN(longitude) AS longitude  -- Add longitude
    FROM 
        "data"."main"."mta_hourly_subway_socrata"
    WHERE 
        YEAR(transit_timestamp) IN (2023, 2024)
    GROUP BY 
        station_complex
),
weekday_weekend AS (
    -- Calculate weekday vs. weekend ridership and percentage of transfers
    SELECT 
        station_complex,
        CASE 
            WHEN weekday IN (2, 3, 4, 5, 6) THEN 'weekday'
            ELSE 'weekend'
        END AS day_type,
        AVG(daily_ridership) AS avg_ridership,
        SUM(daily_transfers) * 1.0 / SUM(daily_ridership) AS transfer_percentage
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
    -- Highest and lowest single day ridership
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
    -- Day with highest single day ridership
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
    -- Day with lowest single day ridership
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
    MAX(ty.total_riders_2023) AS total_riders_2023,
    MAX(ty.total_riders_2024) AS total_riders_2024,
    MAX(CASE WHEN wwd.day_type = 'weekday' THEN wwd.transfer_percentage END) AS weekday_transfer_percentage,
    MAX(CASE WHEN wwd.day_type = 'weekend' THEN wwd.transfer_percentage END) AS weekend_transfer_percentage,
    (MAX(CASE WHEN wwd.day_type = 'weekend' THEN wwd.avg_ridership END) - MAX(CASE WHEN wwd.day_type = 'weekday' THEN wwd.avg_ridership END)) * 1.0 / MAX(CASE WHEN wwd.day_type = 'weekday' THEN wwd.avg_ridership END) AS weekend_ridership_percentage_change,
    (MAX(CASE WHEN wwd.day_type = 'weekend' THEN wwd.transfer_percentage END) - MAX(CASE WHEN wwd.day_type = 'weekday' THEN wwd.transfer_percentage END)) * 1.0 / MAX(CASE WHEN wwd.day_type = 'weekday' THEN wwd.transfer_percentage END) AS weekend_transfer_percentage_change,
    MAX(rbd.latitude) AS latitude,   -- Add latitude
    MAX(rbd.longitude) AS longitude  -- Add longitude
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
JOIN 
    total_riders_ytd ty ON rbd.station_complex = ty.station_complex
GROUP BY 
    rbd.station_complex, 
    sds.highest_single_day_ridership, 
    sds.lowest_single_day_ridership, 
    hsd.highest_single_day_ridership_day, 
    lsd.lowest_single_day_ridership_day
ORDER BY 
    rbd.station_complex
    );
  
  