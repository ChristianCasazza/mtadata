SELECT 
    EXTRACT(HOUR FROM transit_timestamp) AS transit_hour, 
    AVG(ridership) AS average_ridership
FROM 
    mta
GROUP BY 
    transit_hour
ORDER BY 
    average_ridership DESC;
