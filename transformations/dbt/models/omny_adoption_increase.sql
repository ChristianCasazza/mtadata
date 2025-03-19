WITH omny_ridership_by_station_year AS (
    -- Calculate the OMNY ridership and total ridership for each station in 2023 and 2024, using the partitioned year column
    SELECT 
        station_complex_id, 
        station_complex, 
        latitude, 
        longitude, 
        year,  -- Using partition column directly
        SUM(CASE WHEN payment_method = 'omny' THEN ridership ELSE 0 END) AS omny_ridership,
        SUM(ridership) AS total_ridership
    FROM 
        {{ source('main', 'mta_subway_hourly_ridership') }}
    WHERE 
        year IN (2023, 2024)  -- Partition filter for performance
    GROUP BY 
        station_complex_id, station_complex, latitude, longitude, year
),
omny_percentage_by_station AS (
    -- Calculate the OMNY percentage for each station in 2023 and 2024
    SELECT 
        station_complex_id, 
        station_complex, 
        latitude, 
        longitude, 
        year, 
        (omny_ridership / NULLIF(total_ridership, 0)) AS omny_percentage
    FROM 
        omny_ridership_by_station_year
)
SELECT 
    s2023.station_complex_id AS station_id, 
    s2023.station_complex AS station_name,
    s2023.latitude,
    s2023.longitude,
    s2023.omny_percentage AS omny_percentage_2023,
    s2024.omny_percentage AS omny_percentage_2024,
    (s2024.omny_percentage - s2023.omny_percentage) AS omny_percentage_increase
FROM 
    omny_percentage_by_station s2023
LEFT JOIN 
    omny_percentage_by_station s2024 
    ON s2023.station_complex_id = s2024.station_complex_id
    AND s2023.latitude = s2024.latitude
    AND s2023.longitude = s2024.longitude
    AND s2023.year = 2023
    AND s2024.year = 2024
WHERE 
    s2024.omny_percentage > s2023.omny_percentage
ORDER BY 
    omny_percentage_increase DESC
