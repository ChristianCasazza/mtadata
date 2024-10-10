SELECT 
    passer_display_name AS player_name,           -- QB's name with alias as player_name
    SUM(CASE WHEN play_type_nfl IN ('PASS', 'INTERCEPTION') AND pass_attempt = 1 THEN 1 ELSE 0 END) AS pass_attempts, -- Total pass attempts based on play_type_nfl and pass_attempt=1
    SUM(CASE WHEN complete_pass = 1 THEN 1 ELSE 0 END) AS completions, -- Total completions
    SUM(passing_yards) AS total_passing_yards,   -- Total passing yards
    SUM(air_yards) AS total_air_yards,           -- Total air yards
    SUM(yards_after_catch) AS yac_yards,         -- Total Yards After Catch (YAC)
    SUM(CASE WHEN interception = 1 THEN 1 ELSE 0 END) AS interceptions, -- Total interceptions
    SUM(CASE WHEN sack = 1 THEN 1 ELSE 0 END) AS sacks, -- Total sacks
    SUM(CASE WHEN pass_touchdown = 1 THEN 1 ELSE 0 END) AS pass_touchdowns, -- Total passing touchdowns
    SUM(epa) AS total_epa,                       -- Total Expected Points Added (EPA)
    SUM(CASE WHEN qb_hit = 1 THEN 1 ELSE 0 END) AS total_hits,  -- Total times the QB was hit

    -- Calculated fields
    SUM(yards_after_catch) / SUM(passing_yards) AS percentage_from_yac, -- Percentage of yards from air (non-YAC)
    (SUM(CASE WHEN interception = 1 THEN 1 ELSE 0 END) * 1.0 / SUM(CASE WHEN play_type_nfl IN ('PASS', 'INTERCEPTION') AND pass_attempt = 1 THEN 1 ELSE 0 END)) AS interception_rate, -- Interception rate
    (SUM(CASE WHEN sack = 1 THEN 1 ELSE 0 END) * 1.0 / SUM(CASE WHEN play_type_nfl IN ('PASS', 'INTERCEPTION') AND pass_attempt = 1 THEN 1 ELSE 0 END)) AS sack_rate, -- Sack rate
    (SUM(CASE WHEN qb_hit = 1 THEN 1 ELSE 0 END) * 1.0 / SUM(CASE WHEN play_type_nfl IN ('PASS', 'INTERCEPTION') AND pass_attempt = 1 THEN 1 ELSE 0 END)) AS qb_hit_rate, -- QB hit rate
    (SUM(CASE WHEN complete_pass = 1 THEN 1 ELSE 0 END) * 1.0 / SUM(CASE WHEN play_type_nfl IN ('PASS', 'INTERCEPTION') AND pass_attempt = 1 THEN 1 ELSE 0 END)) AS completion_percentage -- Completion percentage
FROM {{ source('main', 'nfl_pbp_2024') }}
WHERE (play_type = 'pass' OR play_type = 'qb_spike')
AND passer_position = 'QB'
GROUP BY passer_display_name
ORDER BY pass_attempts DESC
