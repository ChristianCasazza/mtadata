WITH total_ridership_per_borough_daytype AS (
    -- Calculate total ridership for each borough and day type (Weekday or Weekend)
    SELECT 
        borough, 
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM transit_timestamp) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type,
        SUM(ridership) AS total_ridership_borough_daytype
    FROM 
        "data"."main"."mta_hourly_subway_socrata"
    GROUP BY 
        borough, day_type
),
ridership_by_fare_class AS (
    -- Calculate total ridership by fare class category, borough, and day type (Weekday/Weekend)
    SELECT 
        borough, 
        fare_class_category, 
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM transit_timestamp) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type,
        SUM(ridership) AS total_ridership
    FROM 
        "data"."main"."mta_hourly_subway_socrata"
    GROUP BY 
        borough, fare_class_category, day_type
)
SELECT 
    r.borough, 
    r.fare_class_category, 
    r.day_type,
    r.total_ridership, 
    ROUND(r.total_ridership / t.total_ridership_borough_daytype, 4) AS ridership_percentage
FROM 
    ridership_by_fare_class r
JOIN 
    total_ridership_per_borough_daytype t
    ON r.borough = t.borough 
    AND r.day_type = t.day_type
ORDER BY 
    total_ridership DESC