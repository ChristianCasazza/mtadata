import os
from dagster import Definitions, FilesystemIOManager, load_assets_from_modules
from mta.resources.io_manager_list import *
from mta.assets.ingestion import mta_assets
from mta.assets.ingestion.mta_assets import *
from mta.assets.ingestion import weather_assets
from mta.assets.ingestion.weather_assets import *
from mta.assets.transformations.duckdb_assets import duckdb_warehouse

from dagster_dbt import DbtCliResource
from mta.assets.dbt_assets import dbt_project_assets  # Include the dbt assets
from mta.resources.dbt_project import dbt_project 

# Load MTA and Weather assets
mta_assets = load_assets_from_modules([mta_assets])
weather_assets = load_assets_from_modules([weather_assets])

# Other assets like DuckDB
other_assets = [duckdb_warehouse]

# Resource definitions, including the I/O manager for DuckDB, DBT, and Filesystem
resources = {
    "dbt": DbtCliResource(project_dir=dbt_project.project_dir),  # Updated DBT resource reference
    **polars_parquet_io_managers,  # Polars Parquet IO managers
    **hourly_filesystem_io_manager,  # Hourly Filesystem IO manager
}


# Dagster Definitions for assets, resources, and jobs
defs = Definitions(
    assets=mta_assets + weather_assets + other_assets + [dbt_project_assets],  # Include all MTA and DBT assets
    resources=resources
)
