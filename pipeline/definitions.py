import dagster
import os
from dagster import Definitions, load_assets_from_modules
from pipeline.assets.ingestion import mta_assets, weather_assets #Our ingestion assets
from pipeline.assets.transformations import duckdb_assets #Our code for making a DuckDB warehouse from parquet files

# Our DBT imports
from dagster_dbt import DbtCliResource #Our Dagster-DBT Resource 
from pipeline.assets.dbt_assets import dbt_project_assets  # Our DBT assets
from pipeline.resources.dbt_project import dbt_project #Tells Dagster where it can find our DBT code relative to this file

from pipeline.constants import LAKE_PATH  #The base path for our data lake of parquet files
from pipeline.constants import HOURLY_PATH

from pipeline.resources.io_managers.single_file_polars_parquet_io_manager import SingleFilePolarsParquetIOManager #Our IO Manager for storing dagster dataframes as parquet files
from pipeline.resources.io_managers.fastopendata_partitioned_parquet_io_manager import FastOpenDataPartitionedParquetIOManager


from pipeline.resources.socrata_resource import SocrataResource#Our Socrata resource for interacting with the Socrata API through a common format

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

# Create the existing partition-based IO manager
fastopendata_partitioned_parquet_io_manager = FastOpenDataPartitionedParquetIOManager(
    base_dir=LAKE_PATH  
)


# Then, bundle all of them into resources
resources = {
    "dbt": DbtCliResource(project_dir=dbt_project.project_dir),  # Updated DBT resource reference
    "io_manager": SingleFilePolarsParquetIOManager, # The first io_manager matches the key assigned on all the assets in assets/ingestion. 
    "fastopendata_partitioned_parquet_io_manager": fastopendata_partitioned_parquet_io_manager,
    "socrata": socrata,
}


# Define the Dagster assets taking part in our data platform, and the resources they can use
defs = Definitions(
    assets=mta_assets + weather_assets + duckdb_assets + [dbt_project_assets],  # Include all MTA and DBT assets
    resources=resources
)