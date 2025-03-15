SELECT 
    station_complex_id, 
    station_complex, 
    latitude, 
    longitude, 
    SUM(ridership) AS total_ridership
FROM 
    "mtastats"."main"."mta_hourly_subway_socrata"
GROUP BY 
    station_complex_id, station_complex, latitude, longitude
ORDER BY 
    total_ridership DESC