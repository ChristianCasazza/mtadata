WITH players AS (
    SELECT gsis_id, display_name, position
    FROM nfl_players
)
SELECT 
    pbp.*,
    p.display_name AS passer_display_name,
    p.position AS passer_position,
    r.display_name AS rusher_display_name,
    r.position AS rusher_position,
    rc.display_name AS receiver_display_name,
    rc.position AS receiver_position
FROM 
    pbp
LEFT JOIN players p ON pbp.passer_player_id = p.gsis_id
LEFT JOIN players r ON pbp.rusher_player_id = r.gsis_id
LEFT JOIN players rc ON pbp.receiver_player_id = rc.gsis_id
