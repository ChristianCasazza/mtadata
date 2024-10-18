SELECT 
    EXTRACT(HOUR FROM transit_timestamp) AS transit_hour, 
    AVG(ridership) AS average_ridership
FROM 
    {{ source('main', 'mta_hourly_subway_socrata') }}
GROUP BY 
    transit_hour
ORDER BY 
    average_ridership DESC
