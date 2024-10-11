from mta.assets.ingestion.mta_subway.nyc_assets import fetch_and_save_weekly_data
from mta.resources.polars_parquet_io_manager import PolarsParquetIOManager

from dagster import Definitions

defs = Definitions(
    assets=[fetch_and_save_weekly_data],
    resources={
        "polars_parquet_io_manager": PolarsParquetIOManager(
            base_dir="/home/christianocean/mta/data/mta/nyc_subway_hourly_ridership"
        )
    }
)
