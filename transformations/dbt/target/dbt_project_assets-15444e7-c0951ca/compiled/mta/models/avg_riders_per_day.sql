SELECT 
    station_complex_id, 
    station_complex, 
    latitude, 
    longitude, 
    EXTRACT(DAYOFWEEK FROM transit_timestamp) AS day_of_week, 
    AVG(ridership) AS average_ridership
FROM 
    "data"."main"."mta_hourly_subway_socrata"
GROUP BY 
    station_complex_id, 
    station_complex, 
    latitude, 
    longitude, 
    day_of_week
ORDER BY 
    average_ridership DESC