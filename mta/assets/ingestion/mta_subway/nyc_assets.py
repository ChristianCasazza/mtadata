from mta.assets.ingestion.mta_subway.asset_functions import process_mta_hourly_subway_df
from mta.utils.data_fetching import SocrataAPI, SocrataAPIConfig
from dagster import asset
from .constants import MTA_HOURLY_SUBWAY_ENDPOINT, MTA_HOURLY_SUBWAY_ORDER, MTA_HOURLY_SUBWAY_WHERE 
import gc
import polars as pl


class MTAHourlySubwayConfig(SocrataAPIConfig):
    SOCRATA_ENDPOINT: str = MTA_HOURLY_SUBWAY_ENDPOINT
    order: str = MTA_HOURLY_SUBWAY_ORDER
    limit: int = 500000
    where: str = MTA_HOURLY_SUBWAY_WHERE
    offset: int = 0


@asset(io_manager_key="polars_parquet_io_manager")
def mta_hourly_subway_socrata(context, config: MTAHourlySubwayConfig):
    api_client = SocrataAPI(config)  # Instantiate the SocrataAPI class
    offset = config.offset  # Start from the initial offset in config
    batch_number = 1

    while True:
        # Log the current offset before each API call
        context.log.info(f"Fetching data with offset: {offset}")

        # Create a new API client with the updated offset
        updated_config = config.copy(update={"offset": offset})
        api_client.config = updated_config

        # Fetch data using the SocrataAPI class method
        data = api_client.fetch_data()

        if not data:
            context.log.info(f"No more data to fetch at offset {offset}.")
            context.log.info("All data has been fetched successfully.")
            return

        raw_df = pl.DataFrame(data)
        processed_df = process_mta_hourly_subway_df(raw_df)

        if not processed_df.is_empty():
            context.resources.polars_parquet_io_manager.handle_output(context, processed_df, batch_number)

        # Cleanup memory
        del raw_df, processed_df, data
        gc.collect()

        # Increment the offset for the next batch
        offset += config.limit
        batch_number += 1
