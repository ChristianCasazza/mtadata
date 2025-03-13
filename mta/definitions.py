import dagster
import os
from dagster import Definitions, load_assets_from_modules
from mta.assets.ingestion import mta_assets, weather_assets #Our ingestion assets
from mta.assets.transformations import duckdb_assets #Our code for making a DuckDB warehouse from parquet files

# Our DBT imports
from dagster_dbt import DbtCliResource #Our Dagster-DBT Resource 
from mta.assets.dbt_assets import dbt_project_assets  # Our DBT assets
from mta.resources.dbt_project import dbt_project #Tells Dagster where it can find our DBT code relative to this file

from mta.constants import LAKE_PATH  #The base path for our data lake of parquet files
from mta.constants import HOURLY_PATH

from mta.resources.io_managers.single_file_polars_parquet_io_manager import SingleFilePolarsParquetIOManager #Our IO Manager for storing dagster dataframes as parquet files
from dagster import FilesystemIOManager


from mta.resources.socrata_resource import SocrataResource#Our Socrata resource for interacting with the Socrata API through a common format

# Load MTA and Weather assets
mta_assets = load_assets_from_modules([mta_assets])
weather_assets = load_assets_from_modules([weather_assets])


# Other assets like DuckDB
duckdb_assets = load_assets_from_modules([duckdb_assets])


#First, define our resources and io_managers

# Define our IO SingleFilePolarsParquetIOManager for storing the data from our API calls. io_manager is an abritrary name, it could be anything, like bob.
SingleFilePolarsParquetIOManager = SingleFilePolarsParquetIOManager(base_dir=LAKE_PATH)

# Create the Socrata resource
socrata = SocrataResource()  # Using default env var for the token

hourly_filesystem_io_manager = {
    "hourly_mta_io_manager": FilesystemIOManager(base_dir=HOURLY_PATH)
}


# Then, bundle all of them into resources
resources = {
    "dbt": DbtCliResource(project_dir=dbt_project.project_dir),  # Updated DBT resource reference
    "io_manager": SingleFilePolarsParquetIOManager, # The first io_manager matches the key assigned on all the assets in assets/ingestion. 
    **hourly_filesystem_io_manager,  
    "socrata": socrata,
}


# Define the Dagster assets taking part in our data platform, and the resources they can use
defs = Definitions(
    assets=mta_assets + weather_assets + duckdb_assets + [dbt_project_assets],  # Include all MTA and DBT assets
    resources=resources
)