import polars as pl
import gc
from dagster import asset
from datetime import datetime
from mta.utils.data_fetching import fetch_nyc_data_geojson, process_and_cast_df

# Define constants for the start date
start_date = datetime(2022, 2, 1)

@asset(io_manager_key="polars_parquet_io_manager")
def fetch_and_save_weekly_data(context):
    offset = 0
    page_size = 500000
    batch_number = 1  # Track batch numbers for file naming

    while True:
        # Fetch data for the current batch
        data = fetch_nyc_data_geojson(start_date.isoformat(), offset, page_size)

        if not data:
            print(f"No more data to fetch at offset {offset}.")
            context.log.info("All data has been fetched successfully.")
            return  # Exit the function without attempting further output

        # Convert to Polars DataFrame
        raw_df = pl.DataFrame(data)

        # Process and cast the data
        processed_df = process_and_cast_df(raw_df)

        # Save the DataFrame to Parquet file if there is data
        if not processed_df.is_empty():
            context.resources.polars_parquet_io_manager.handle_output(context, processed_df, batch_number)

        # Free up memory
        del raw_df, processed_df, data
        gc.collect()  # Trigger garbage collection

        # Increment the offset and batch number for the next batch
        offset += page_size
        batch_number += 1
