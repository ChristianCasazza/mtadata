import os
from dagster import Definitions, FilesystemIOManager, load_assets_from_modules
from mta.resources.io_manager_list import *
from mta.assets.ingestion import mta_assets, weather_assets, sf_assets
from mta.assets.ingestion.mta_assets import *
from mta.assets.ingestion.weather_assets import *
from mta.assets.transformations.duckdb_assets import duckdb_warehouse

from dagster_dbt import DbtCliResource
from mta.assets.dbt_assets import dbt_project_assets  # Include the dbt assets
from mta.resources.dbt_project import dbt_project 
from mta.constants import BASE_PATH
from mta.resources.io_managers.polars_parquet_io_manager import PolarsParquetIOManager
from mta.resources.io_managers.single_file_polars_parquet_io_manager import SingleFilePolarsParquetIOManager

from mta.resources.socrata_resource import SocrataResource

# Load MTA and Weather assets
mta_assets = load_assets_from_modules([mta_assets])
weather_assets = load_assets_from_modules([weather_assets])
sf_assets = load_assets_from_modules([sf_assets])

# Other assets like DuckDB
other_assets = [duckdb_warehouse]

io_manager = SingleFilePolarsParquetIOManager(base_dir=BASE_PATH)

# Step 3: Create the Socrata resource
socrata = SocrataResource()  # If using default env var for the token
hourly_filesystem_io_manager = {
    "hourly_mta_io_manager": FilesystemIOManager(base_dir=HOURLY_PATH)
}


# Resource definitions, including the I/O manager for DuckDB, DBT, and Filesystem
resources = {
    "dbt": DbtCliResource(project_dir=dbt_project.project_dir),  # Updated DBT resource reference
    "io_manager": io_manager,
    **hourly_filesystem_io_manager,  # Hourly Filesystem IO manager
    "socrata": socrata,
}


# Dagster Definitions for assets, resources, and jobs
defs = Definitions(
    assets=mta_assets + weather_assets + sf_assets + other_assets + [dbt_project_assets],  # Include all MTA and DBT assets
    resources=resources
)