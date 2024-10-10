SELECT 
    station_complex_id, 
    station_complex, 
    DATE_TRUNC('week', transit_timestamp) AS date, 
    SUM(ridership) AS total_ridership
FROM 
    mta
GROUP BY 
    station_complex_id, 
    station_complex, 
    date
ORDER BY 
    date, total_ridership DESC;
