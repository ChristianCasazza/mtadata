# mta/assets/ingestion/mta_subway/mta_assets.py

from mta.assets.ingestion.mta_subway.asset_functions import *
from mta.utils.socrata_api import SocrataAPI
from mta.assets.ingestion.mta_subway.constants import *
from dagster import asset
import gc
import polars as pl


def get_io_manager(context):
    """Helper function to dynamically fetch the correct IO manager for the asset."""
    asset_name = context.asset_key.path[-1]  # Get the last part of the asset key, which is the asset name
    io_manager_key = f"{asset_name}_polars_parquet_io_manager"  # Construct the io_manager_key
    return getattr(context.resources, io_manager_key)


MTA_ASSETS_NAMES = [
    "mta_hourly_subway_socrata",
    "mta_daily_subway_socrata",
    "mta_bus_speeds",
    "mta_bus_wait_time",
    "mta_operations_statement",
    "nyc_arrests_historical_raw"
]

# Hourly subway data asset
@asset(io_manager_key="mta_hourly_subway_socrata_polars_parquet_io_manager")
def mta_hourly_subway_socrata(context):
    config = MTAHourlySubwayConfig()
    api_client = SocrataAPI(config)
    offset = config.offset
    batch_number = 1

    while True:
        context.log.info(f"Fetching data with offset: {offset}")
        updated_config = config.copy(update={"offset": offset})
        api_client.config = updated_config
        data = api_client.fetch_data()

        if not data:
            context.log.info(f"No more data to fetch at offset {offset}.")
            return

        raw_df = pl.DataFrame(data)
        processed_df = process_mta_hourly_subway_df(raw_df)

        if not processed_df.is_empty():
            io_manager = get_io_manager(context)
            io_manager.handle_output(context, processed_df, batch_number)

        del raw_df, processed_df, data
        gc.collect()

        offset += config.limit
        batch_number += 1

# Daily subway data asset
@asset(io_manager_key="mta_daily_subway_socrata_polars_parquet_io_manager")
def mta_daily_subway_socrata(context):
    config = MTADailySubwayConfig()  # Use the new config
    api_client = SocrataAPI(config)
    offset = config.offset
    batch_number = 1

    while True:
        context.log.info(f"Fetching daily data with offset: {offset}")
        updated_config = config.copy(update={"offset": offset})
        api_client.config = updated_config
        data = api_client.fetch_data()

        if not data:
            context.log.info(f"No more daily data to fetch at offset {offset}.")
            return

        raw_df = pl.DataFrame(data)
        processed_df = process_mta_daily_subway_df(raw_df)  # You may need a custom process function for daily data

        if not processed_df.is_empty():
            io_manager = get_io_manager(context)
            io_manager.handle_output(context, processed_df, batch_number)

        del raw_df, processed_df, data
        gc.collect()

        offset += config.limit
        batch_number += 1


# Bus speeds asset
@asset(io_manager_key="mta_bus_speeds_polars_parquet_io_manager")
def mta_bus_speeds(context, config: MTABusSpeedsConfig):
    api_client = SocrataAPI(config)  # Instantiate the SocrataAPI class
    offset = config.offset  # Start from the initial offset in config
    batch_number = 1

    while True:
        context.log.info(f"Fetching bus speeds data with offset: {offset}")
        updated_config = config.copy(update={"offset": offset})
        api_client.config = updated_config
        data = api_client.fetch_data()

        if not data:
            context.log.info(f"No more bus speeds data to fetch at offset {offset}.")
            context.log.info("All data has been fetched successfully.")
            return

        raw_df = pl.DataFrame(data)
        processed_df = process_mta_bus_speeds_df(raw_df)

        if not processed_df.is_empty():
            io_manager = get_io_manager(context)
            io_manager.handle_output(context, processed_df, batch_number)

        del raw_df, processed_df, data
        gc.collect()

        offset += config.limit
        batch_number += 1


# Bus wait time asset
@asset(io_manager_key="mta_bus_wait_time_polars_parquet_io_manager")
def mta_bus_wait_time(context, config: MTAWaitTimeBusConfig):
    api_client = SocrataAPI(config)
    offset = config.offset
    batch_number = 1

    while True:
        context.log.info(f"Fetching bus wait time data with offset: {offset}")
        updated_config = config.copy(update={"offset": offset})
        api_client.config = updated_config
        data = api_client.fetch_data()

        if not data:
            context.log.info(f"No more bus wait time data to fetch at offset {offset}.")
            context.log.info("All data has been fetched successfully.")
            return

        raw_df = pl.DataFrame(data)
        processed_df = process_mta_bus_wait_time_df(raw_df)

        if not processed_df.is_empty():
            io_manager = get_io_manager(context)
            io_manager.handle_output(context, processed_df, batch_number)

        del raw_df, processed_df, data
        gc.collect()

        offset += config.limit
        batch_number += 1


# Operations statement asset
@asset(io_manager_key="mta_operations_statement_polars_parquet_io_manager")
def mta_operations_statement(context, config: MTAOperationsStatementConfig):
    api_client = SocrataAPI(config)
    offset = config.offset
    batch_number = 1

    while True:
        context.log.info(f"Fetching operations statement data with offset: {offset}")
        updated_config = config.copy(update={"offset": offset})
        api_client.config = updated_config
        data = api_client.fetch_data()

        if not data:
            context.log.info(f"No more operations statement data to fetch at offset {offset}.")
            context.log.info("All data has been fetched successfully.")
            return

        raw_df = pl.DataFrame(data)
        processed_df = process_mta_operations_statement_df(raw_df)

        if not processed_df.is_empty():
            io_manager = get_io_manager(context)
            io_manager.handle_output(context, processed_df, batch_number)

        del raw_df, processed_df, data
        gc.collect()

        offset += config.limit
        batch_number += 1


# NYC arrests historical asset
@asset(io_manager_key="nyc_arrests_historical_raw_polars_parquet_io_manager")
def nyc_arrests_historical_raw(context, config: NYCArrestsHistoricalConfig):
    api_client = SocrataAPI(config)
    offset = config.offset
    batch_number = 1

    while True:
        context.log.info(f"Fetching NYC arrests data with offset: {offset}")
        updated_config = config.copy(update={"offset": offset})
        api_client.config = updated_config
        data = api_client.fetch_data()

        if not data:
            context.log.info(f"No more NYC arrests data to fetch at offset {offset}.")
            context.log.info("All data has been fetched successfully.")
            return

        raw_df = pl.DataFrame(data)
        processed_df = process_nyc_arrests_df(raw_df)

        if not processed_df.is_empty():
            io_manager = get_io_manager(context)
            io_manager.handle_output(context, processed_df, batch_number)

        del raw_df, processed_df, data
        gc.collect()

        offset += config.limit
        batch_number += 1
