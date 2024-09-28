import os
from dagster import Definitions
from dagster_duckdb_polars import DuckDBPolarsIOManager
from .assets import nfl, update_pbp, bloombet_nfl
from .assets.bloombet_nfl import bloombet_nfl_data
from .assets.nfl import nfl_pbp_2024, nfl_players, nfl_weekly_rosters_2024, nfl_officials
from .assets.update_pbp import update_pbp_with_players 
from .assets.dbt import dbt_project_assets  # Import the dbt assets
from .assets.bi_export import analytics_cube  # Import the analytics_cube asset
from dagster_dbt import DbtCliResource
from .dbt_project import dbt_project

# Set the database path for raw data
DATABASE_PATH = os.getenv("DATABASE_PATH", "data/database_raw.duckdb")

# Define the assets (Parquet data and Bloombet API data)
assets = [
    nfl_pbp_2024,
    bloombet_nfl_data,
    nfl_players,
    nfl_weekly_rosters_2024, 
    nfl_officials,
    update_pbp_with_players,
    dbt_project_assets,  # Add the dbt assets here
    analytics_cube  # Add the analytics_cube asset here
]

# Resource definitions, including the I/O manager for DuckDB
resources = {
    "dbt": DbtCliResource(project_dir=dbt_project),
    "io_manager": DuckDBPolarsIOManager(database=DATABASE_PATH, schema="main"),
}

# Dagster Definitions for assets and resources
defs = Definitions(
    assets=assets,
    resources=resources
)
