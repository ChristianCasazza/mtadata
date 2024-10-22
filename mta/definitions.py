

import os
from dagster import Definitions
from mta.resources.io_manager_list import polars_parquet_io_managers  
from mta.assets.ingestion.mta_assets import *
from mta.assets.ingestion.weather_assets import *
from mta.assets.transformations.duckdb_assets import create_duckdb_and_views
from mta.assets.ingestion.mta_assets import MTA_ASSETS_NAMES
from mta.assets.ingestion.weather_assets import WEATHER_ASSETS_NAMES

from dagster_dbt import DbtCliResource

from mta.assets.dbt_assets import dbt_project_assets  # Include the dbt assets
from mta.resources.dbt_project import dbt_project 

mta_assets = [globals()[asset_name] for asset_name in MTA_ASSETS_NAMES if asset_name in globals()]
weather_assets = [globals()[asset_name] for asset_name in WEATHER_ASSETS_NAMES if asset_name in globals()]

other_assets = [ create_duckdb_and_views]

# Resource definitions, including the I/O manager for DuckDB and DBT
resources = {
    "dbt": DbtCliResource(project_dir=dbt_project.project_dir),  # Updated DBT resource reference
    **polars_parquet_io_managers,  # IO_manager creator
}

# Dagster Definitions for assets, resources, and jobs
defs = Definitions(
    assets=mta_assets + weather_assets + other_assets + [dbt_project_assets],  # Include all MTA and DBT assets
    resources=resources
)