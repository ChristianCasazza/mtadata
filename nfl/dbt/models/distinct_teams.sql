SELECT DISTINCT posteam 
FROM {{ source('main', 'nfl_pbp_2024') }}
