# nyc_assets.py

from mta.assets.ingestion.mta_subway.asset_functions import *
from mta.utils.data_fetching import SocrataAPI
from mta.assets.ingestion.mta_subway.constants import *
from dagster import asset
import gc
import polars as pl

# Hourly subway data asset
@asset(io_manager_key="hourly_subway_io_manager")  # Specify the custom IO manager for hourly subway
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
            context.resources.hourly_subway_io_manager.handle_output(context, processed_df, batch_number)

        del raw_df, processed_df, data
        gc.collect()

        offset += config.limit
        batch_number += 1

# Daily subway data asset
@asset(io_manager_key="daily_subway_io_manager")  # Specify the custom IO manager for daily subway
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
            context.resources.daily_subway_io_manager.handle_output(context, processed_df, batch_number)

        del raw_df, processed_df, data
        gc.collect()

        offset += config.limit
        batch_number += 1


@asset(io_manager_key="bus_speeds_io_manager")
def mta_bus_speeds(context, config: MTABusSpeedsConfig):
    api_client = SocrataAPI(config)  # Instantiate the SocrataAPI class
    offset = config.offset  # Start from the initial offset in config
    batch_number = 1

    while True:
        # Log the current offset before each API call
        context.log.info(f"Fetching bus speeds data with offset: {offset}")

        # Create a new API client with the updated offset
        updated_config = config.copy(update={"offset": offset})
        api_client.config = updated_config

        # Fetch data using the SocrataAPI class method
        data = api_client.fetch_data()

        if not data:
            context.log.info(f"No more bus speeds data to fetch at offset {offset}.")
            context.log.info("All data has been fetched successfully.")
            return

        raw_df = pl.DataFrame(data)
        processed_df = process_mta_bus_speeds_df(raw_df)

        if not processed_df.is_empty():
            context.resources.bus_speeds_io_manager.handle_output(context, processed_df, batch_number)

        # Cleanup memory
        del raw_df, processed_df, data
        gc.collect()

        # Increment the offset for the next batch
        offset += config.limit
        batch_number += 1


@asset(io_manager_key="bus_wait_time_io_manager")
def mta_bus_wait_time(context, config: MTAWaitTimeBusConfig):
    api_client = SocrataAPI(config)
    offset = config.offset
    batch_number = 1

    while True:
        # Log the current offset before each API call
        context.log.info(f"Fetching bus wait time data with offset: {offset}")

        # Create a new API client with the updated offset
        updated_config = config.copy(update={"offset": offset})
        api_client.config = updated_config

        # Fetch data using the SocrataAPI class method
        data = api_client.fetch_data()

        if not data:
            context.log.info(f"No more bus wait time data to fetch at offset {offset}.")
            context.log.info("All data has been fetched successfully.")
            return

        raw_df = pl.DataFrame(data)
        processed_df = process_mta_bus_wait_time_df(raw_df)

        if not processed_df.is_empty():
            context.resources.bus_wait_time_io_manager.handle_output(context, processed_df, batch_number)

        # Cleanup memory
        del raw_df, processed_df, data
        gc.collect()

        # Increment the offset for the next batch
        offset += config.limit
        batch_number += 1


@asset(io_manager_key="operations_statement_io_manager")
def mta_operations_statement(context, config: MTAOperationsStatementConfig):
    api_client = SocrataAPI(config)
    offset = config.offset
    batch_number = 1

    while True:
        # Log the current offset before each API call
        context.log.info(f"Fetching operations statement data with offset: {offset}")

        # Create a new API client with the updated offset
        updated_config = config.copy(update={"offset": offset})
        api_client.config = updated_config

        # Fetch data using the SocrataAPI class method
        data = api_client.fetch_data()

        if not data:
            context.log.info(f"No more operations statement data to fetch at offset {offset}.")
            context.log.info("All data has been fetched successfully.")
            return

        raw_df = pl.DataFrame(data)
        processed_df = process_mta_operations_statement_df(raw_df)

        if not processed_df.is_empty():
            context.resources.operations_statement_io_manager.handle_output(context, processed_df, batch_number)

        # Cleanup memory
        del raw_df, processed_df, data
        gc.collect()

        # Increment the offset for the next batch
        offset += config.limit
        batch_number += 1