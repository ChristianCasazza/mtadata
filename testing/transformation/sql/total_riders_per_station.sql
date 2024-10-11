SELECT 
    station_complex_id, 
    station_complex, 
    latitude, 
    longitude, 
    SUM(ridership) AS total_ridership
FROM 
    mta
GROUP BY 
    station_complex_id, station_complex, latitude, longitude
ORDER BY 
    total_ridership DESC;