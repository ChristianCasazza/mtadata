import polars as pl
import gc
from dagster import asset
from datetime import datetime, timedelta
from mta.utils.data_fetching import fetch_nyc_data_geojson, last_day_of_month, process_and_cast_df

# Define constants for the start and end dates
start_date = datetime(2023, 9, 20)
end_date = datetime.now()

@asset(io_manager_key="polars_parquet_io_manager")
def fetch_and_save_weekly_data(context):
    current_date = start_date
    while current_date <= end_date:
        month_end = last_day_of_month(current_date)
        week_number = 1  # Track the week within the month

        # Loop through weeks within the month
        while current_date <= month_end:
            week_end = current_date + timedelta(days=6)

            # Make sure we don't go past the month
            if week_end > month_end:
                week_end = month_end

            # Fetch data for the current week
            data = fetch_nyc_data_geojson(current_date.isoformat(), week_end.isoformat())

            if data:
                # Convert to Polars DataFrame
                raw_df = pl.DataFrame(data)

                # Process and cast the data
                processed_df = process_and_cast_df(raw_df)

                # Save the DataFrame to Parquet partitioned by week within the month
                context.metadata = {'year': current_date.year, 'month': current_date.month, 'week': week_number}
                context.resources.polars_parquet_io_manager.handle_output(context, processed_df)

                # Free up memory
                del raw_df, processed_df, data
                gc.collect()  # Trigger garbage collection

            # Move to the next week
            current_date = week_end + timedelta(days=1)
            week_number += 1

        # Move to the next month
        current_date = month_end + timedelta(days=1)
