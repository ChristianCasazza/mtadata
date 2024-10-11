from mta.assets.ingestion.mta_subway.nyc_assets import mta_hourly_subway_socrata
from mta.resources.polars_parquet_io_manager import PolarsParquetIOManager

from dagster import Definitions

defs = Definitions(
    assets=[mta_hourly_subway_socrata],
    resources={
        "polars_parquet_io_manager": PolarsParquetIOManager(
            base_dir="/home/christianocean/mta/data/mta/new_subway_hourly_ridership"
        )
    }
)
