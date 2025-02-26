# File: mta/definitions.py

import dagster
import os

from dagster import Definitions, FilesystemIOManager, load_assets_from_modules
from dagster_dbt import DbtCliResource

# --- Local imports ---
# IO Managers
from mta.resources.io_manager_list import *
from mta.resources.io_managers.polars_parquet_io_manager import PolarsParquetIOManager
from mta.resources.io_managers.single_file_polars_parquet_io_manager import SingleFilePolarsParquetIOManager
from mta.resources.io_managers.large_socrata_polars_parquet_io_manager import large_socrata_polars_parquet_io_manager


# Socrata resource
from mta.resources.socrata_resource import SocrataResource

# Asset modules
from mta.assets.ingestion import mta_assets, weather_assets, crime_assets
from mta.assets.ingestion.mta_assets import *
from mta.assets.ingestion.weather_assets import *
from mta.assets.ingestion.crime_assets import *
from mta.assets.transformations.duckdb_assets import duckdb_warehouse
from mta.assets.dbt_assets import dbt_project_assets
from mta.resources.dbt_project import dbt_project

# Constants
from mta.constants import BASE_PATH, HOURLY_PATH

# --- 1) Load ingestion assets ---
mta_assets = load_assets_from_modules([mta_assets])
weather_assets = load_assets_from_modules([weather_assets])
crime_assets = load_assets_from_modules([crime_assets])

# --- 2) Other transformation assets ---
other_assets = [duckdb_warehouse]

# --- 3) IO Managers ---
# Default single-file Polars Parquet IO Manager
io_manager = SingleFilePolarsParquetIOManager(base_dir=BASE_PATH)

# Specialized hourly filesystem IO manager
hourly_filesystem_io_manager = {
    "hourly_mta_io_manager": FilesystemIOManager(base_dir=HOURLY_PATH)
}

# Large Socrata IO Manager (for large batch-parquet ingestion)
large_socrata_io_mgr = large_socrata_polars_parquet_io_manager.configured(
    {"base_dir": BASE_PATH}
)


# --- 4) Resources ---
resources = {
    # DBT CLI Resource
    "dbt": DbtCliResource(project_dir=dbt_project.project_dir),

    # Default IO manager (single file per asset)
    "io_manager": io_manager,

    # Specialized hourly filesystem IO manager
    **hourly_filesystem_io_manager,

    # Socrata API Resource
    "socrata": SocrataResource(),

    # Large Socrata IO Manager (for large single-folder parquet batches)
    "large_socrata_io_manager": large_socrata_io_mgr,
}

# --- 5) Gather all assets ---
all_assets = mta_assets + weather_assets + crime_assets + other_assets + [dbt_project_assets]

# --- 6) Dagster Definitions ---
defs = Definitions(
    assets=all_assets,
    resources=resources,
)
