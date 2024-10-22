import os

# Import asset names from the respective files
from mta.assets.ingestion.mta_assets import MTA_ASSETS_NAMES
from mta.assets.ingestion.weather_assets import WEATHER_ASSETS_NAMES

# Define the base path relative to the location of the current file
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "opendata", "nyc", "mta"))

# Dynamically create paths for MTA assets
MTA_ASSETS_PATHS = {
    asset_name: f"{BASE_PATH}/{asset_name}"
    for asset_name in MTA_ASSETS_NAMES
}

# Dynamically create paths for Weather assets
WEATHER_ASSETS_PATHS = {
    asset_name: f"{BASE_PATH}/{asset_name}"
    for asset_name in WEATHER_ASSETS_NAMES
}




LAKE_PATH= '/home/christianocean/mta/mta/mtastats/sources/mta/mtastats.duckdb'


