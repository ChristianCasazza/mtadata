# mta/definitions.py

from mta.assets.ingestion.mta_subway.nyc_assets import *
from mta.resources.resources import *
from dagster import Definitions

defs = Definitions(
    assets=[mta_hourly_subway_socrata, mta_daily_subway_socrata, mta_bus_speeds, mta_bus_wait_time, mta_operations_statement],
    resources={
        "hourly_subway_io_manager": hourly_subway_io_manager,
        "daily_subway_io_manager": daily_subway_io_manager,
        "bus_speeds_io_manager": bus_speeds_io_manager,
        "bus_wait_time_io_manager": bus_wait_time_io_manager,
        "operations_statement_io_manager": operations_statement_io_manager
    }
)