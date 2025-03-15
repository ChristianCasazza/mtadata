#For filesystem operations
import os

# Import asset names from the respective groups
from pipeline.datasets import *

# Define the base path relative to the location where we will keep our data lake of parquet files. Our lake base path is one folder back, then data/opendata
LAKE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "opendata"))

# Base path to store our DuckDB We store this DuckDB file directly in our app folder, where it will be used to power our data application
WAREHOUSE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "app", "sources", "app", "data.duckdb")) 

# Path to where we will store our Dagster logs
DAGSTER_PATH=os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "logs"))

# Base path to store our SQLite file, powers our local data dictionary application
SQLITE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "metadata", "metadata.db")) #



# The path for the MTA hourly subway asset
HOURLY_PATH = os.path.join(LAKE_PATH, "mta_hourly_subway_socrata")




#Logic for creating a DuckDB warehouse from our data lake
# Dynamically create paths for MTA assets
MTA_ASSETS_PATHS = {
    asset_name: f"{LAKE_PATH}/{asset_name}"
    for asset_name in MTA_ASSETS_NAMES
}

# Dynamically create paths for other MTA assets
OTHER_MTA_ASSETS_PATHS = { 
    asset_name: f"{LAKE_PATH}/{asset_name}"
    for asset_name in OTHER_MTA_ASSETS_NAMES 
}

# Dynamically create paths for Weather assets
WEATHER_ASSETS_PATHS = {
    asset_name: f"{LAKE_PATH}/{asset_name}"
    for asset_name in WEATHER_ASSETS_NAMES
}