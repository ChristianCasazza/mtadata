import os
from dagster import Definitions
from mta.resources.io_manager_list import polars_parquet_io_managers  # Replace with MTA's IO managers
from dagster_dbt import DbtCliResource
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
from mta.assets.transformations.duckdb_assets import create_duckdb_and_views
from mta.assets.dbt_assets import dbt_project_assets  # Include the dbt assets
from mta.resources.dbt_project import dbt_project  # Reference the dbt project

# Define the assets (from Parquet data, subway, bus, and weather assets)
mta_assets = [
    mta_hourly_subway_socrata,
    mta_daily_subway_socrata,
    mta_bus_speeds,
    mta_bus_wait_time,
    mta_operations_statement,
    nyc_arrests_historical_raw,
    daily_weather_asset,
    hourly_weather_asset,
    create_duckdb_and_views
]

# Resource definitions, including the I/O manager for DuckDB and DBT
resources = {
    "dbt": DbtCliResource(project_dir=dbt_project.project_dir),  # Updated DBT resource reference
    **polars_parquet_io_managers,  # Existing MTA IO managers
}

# Dagster Definitions for assets, resources, and jobs
defs = Definitions(
    assets=mta_assets + [dbt_project_assets],  # Include all MTA and DBT assets
    resources=resources
)
