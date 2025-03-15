
  
    
    

    create  table
      "data"."main"."omny_adoption_by_station__dbt_tmp"
  
    as (
      WITH yearly_data AS (
    SELECT 
        station_complex,
        YEAR(transit_timestamp) AS year,
        SUM(CASE WHEN payment_method = 'omny' THEN ridership ELSE 0 END) * 1.0 / SUM(ridership) AS omny_percentage
    FROM 
        "data"."main"."mta_hourly_subway_socrata"
    WHERE 
        YEAR(transit_timestamp) IN (2022, 2023, 2024)
    GROUP BY 
        station_complex, 
        YEAR(transit_timestamp)
),
pivoted_data AS (
    SELECT 
        station_complex,
        MAX(CASE WHEN year = 2022 THEN omny_percentage ELSE NULL END) AS omny_2022,
        MAX(CASE WHEN year = 2023 THEN omny_percentage ELSE NULL END) AS omny_2023,
        MAX(CASE WHEN year = 2024 THEN omny_percentage ELSE NULL END) AS omny_2024
    FROM 
        yearly_data
    GROUP BY 
        station_complex
)
SELECT 
    station_complex,
    omny_2022,
    omny_2023,
    omny_2024,
    CASE 
        WHEN omny_2022 IS NOT NULL AND omny_2023 IS NOT NULL THEN 
            (omny_2023 - omny_2022) / omny_2022
        ELSE NULL
    END AS omny_2023_growth,
    CASE 
        WHEN omny_2023 IS NOT NULL AND omny_2024 IS NOT NULL THEN 
            (omny_2024 - omny_2023) / omny_2023
        ELSE NULL
    END AS omny_2024_growth
FROM 
    pivoted_data
ORDER BY 
    station_complex
    );
  
  