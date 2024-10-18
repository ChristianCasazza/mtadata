WITH total_ridership_by_station AS (
    -- Calculate total ridership by station, year, latitude, and longitude
    SELECT 
        station_complex_id, 
        station_complex, 
        latitude, 
        longitude, 
        EXTRACT(YEAR FROM transit_timestamp) AS year, 
        SUM(ridership) AS total_ridership
    FROM 
        {{ source('main', 'mta_hourly_subway_socrata') }}
    GROUP BY 
        station_complex_id, station_complex, latitude, longitude, year
),
omny_ridership_by_station AS (
    -- Calculate OMNY ridership by station, year, latitude, and longitude
    SELECT 
        station_complex_id, 
        latitude, 
        longitude, 
        EXTRACT(YEAR FROM transit_timestamp) AS year, 
        SUM(ridership) AS omny_ridership
    FROM 
        {{ source('main', 'mta_hourly_subway_socrata') }}
    WHERE 
        payment_method = 'omny'
    GROUP BY 
        station_complex_id, latitude, longitude, year
)
SELECT 
    t.station_complex_id AS station_id,
    t.station_complex AS station_name,
    t.latitude,
    t.longitude,
    t.year, 
    (o.omny_ridership / t.total_ridership) AS omny_percentage
FROM 
    total_ridership_by_station t
LEFT JOIN 
    omny_ridership_by_station o
    ON t.station_complex_id = o.station_complex_id 
    AND t.latitude = o.latitude
    AND t.longitude = o.longitude
    AND t.year = o.year
ORDER BY 
    t.year, omny_percentage DESC
