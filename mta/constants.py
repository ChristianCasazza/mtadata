import os

# Import asset names from the respective files
from mta.datasets import *

# Define the base path relative to the location of the current file
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "opendata"))

# LAKE_PATH for DuckDB
LAKE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "app", "sources", "app", "data.duckdb"))

SQLITE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "metadata", "metadata.db"))


# Dynamically create paths for MTA assets
MTA_ASSETS_PATHS = {
    asset_name: f"{BASE_PATH}/{asset_name}"
    for asset_name in MTA_ASSETS_NAMES
}

OTHER_MTA_ASSETS_PATHS = { 
    asset_name: f"{BASE_PATH}/{asset_name}"
    for asset_name in OTHER_MTA_ASSETS_NAMES 
}

# Dynamically create paths for Weather assets
WEATHER_ASSETS_PATHS = {
    asset_name: f"{BASE_PATH}/{asset_name}"
    for asset_name in WEATHER_ASSETS_NAMES
}

# Define the path for the hourly MTA data
HOURLY_PATH = os.path.join(BASE_PATH, "mta_hourly_subway_socrata")

