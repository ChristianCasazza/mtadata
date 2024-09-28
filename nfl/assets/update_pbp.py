import os
import duckdb
from dagster import asset, get_dagster_logger, AssetIn

DATABASE_PATH = os.getenv("DATABASE_PATH", "data/database_raw.duckdb")

@asset(
    ins={
        "nfl_pbp_2024": AssetIn(),  # Define input from the nfl_pbp_2024 asset
        "nfl_players": AssetIn()     # Define input from the nfl_players asset
    },
    compute_kind="DuckDB",  # Specify the compute kind
)
def update_pbp_with_players(nfl_pbp_2024, nfl_players) -> None:  # Explicitly indicate no return
    """
    Joining Player Names and Positions to nfl_pbp_2024
    """
    logger = get_dagster_logger()
    
    logger.info("Connecting to DuckDB...")
    con = duckdb.connect(DATABASE_PATH)

    # Step 1: Add columns one by one (DuckDB limitations)
    columns_to_add = [
        "passer_display_name STRING",
        "passer_position STRING",
        "rusher_display_name STRING",
        "rusher_position STRING",
        "receiver_display_name STRING",
        "receiver_position STRING"
    ]

    for column in columns_to_add:
        try:
            con.execute(f"ALTER TABLE nfl_pbp_2024 ADD COLUMN {column}")
            logger.info(f"Column {column} added.")
        except Exception as e:
            logger.warning(f"Column {column} already exists or error: {e}")

    # Step 2: Perform updates for each new column
    logger.info("Updating player-related columns in nfl_pbp_2024...")

    # Update passer columns
    con.execute(f"""
        UPDATE nfl_pbp_2024
        SET passer_display_name = p.display_name, 
            passer_position = p.position
        FROM nfl_players p
        WHERE nfl_pbp_2024.passer_player_id = p.gsis_id
    """)

    # Update rusher columns
    con.execute(f"""
        UPDATE nfl_pbp_2024
        SET rusher_display_name = p.display_name, 
            rusher_position = p.position
        FROM nfl_players p
        WHERE nfl_pbp_2024.rusher_player_id = p.gsis_id
    """)

    # Update receiver columns
    con.execute(f"""
        UPDATE nfl_pbp_2024
        SET receiver_display_name = p.display_name, 
            receiver_position = p.position
        FROM nfl_players p
        WHERE nfl_pbp_2024.receiver_player_id = p.gsis_id
    """)

    # Step 3: Verify the results (Optional logging)
    result = con.execute("SELECT * FROM nfl_pbp_2024 LIMIT 10").fetchdf()
    logger.info(f"Updated PBP data: {result}")

    # Close the connection
    con.close()
