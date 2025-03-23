# For filesystem operations
import os

# Import asset names from the respective groups
from pipeline.datasets import *




# Define the base path relative to the location where we will keep our data lake of parquet files.
LAKE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "opendata"))

# Base path to store our DuckDB. We store this DuckDB file in its own spot in data
WAREHOUSE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "app", "sources", "app", "data.duckdb"))


# Path to where we will store our Dagster logs
DAGSTER_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "logs"))

# Base path to store our SQLite file, powers our local data dictionary application
SQLITE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "metadata", "metadata.db"))



# Logic for creating a DuckDB warehouse from our data lake
# Dynamically create paths for MTA assets
SINGLE_PATH_ASSETS_PATHS = {
    asset_name: f"{LAKE_PATH}/{asset_name}"
    for asset_name in SINGLE_PATH_ASSETS_NAMES
}

# Dynamically create paths for Weather assets
PARTITIONED_ASSETS_PATHS = {
    asset_name: f"{LAKE_PATH}/{asset_name}"
    for asset_name in PARTITIONED_ASSETS_NAMES
}