# mta/definitions.py

from mta.assets.ingestion.mta_subway.mta_assets import MTA_ASSETS_NAMES
from mta.assets.ingestion.weather.weather_assets import WEATHER_ASSETS_NAMES
from mta.resources.io_manager_list import polars_parquet_io_managers
from dagster import Definitions

from mta.assets.ingestion.mta_subway.mta_assets import (
    mta_hourly_subway_socrata,
    mta_daily_subway_socrata,
    mta_bus_speeds,
    mta_bus_wait_time,
    mta_operations_statement,
    nyc_arrests_historical_raw
)
from mta.assets.ingestion.weather.weather_assets import (
    daily_weather_asset,
    hourly_weather_asset
)
from mta.resources.io_manager_list import polars_parquet_io_managers
from dagster import Definitions

# Define all assets
all_assets = [
    mta_hourly_subway_socrata,
    mta_daily_subway_socrata,
    mta_bus_speeds,
    mta_bus_wait_time,
    mta_operations_statement,
    nyc_arrests_historical_raw,
    daily_weather_asset,  # Add weather assets here
    hourly_weather_asset
]

# Create Definitions object with assets and IO managers
defs = Definitions(
    assets=all_assets,
    resources=polars_parquet_io_managers  # Dynamically created IO managers
)
