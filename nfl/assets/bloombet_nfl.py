import os
from datetime import datetime, timedelta
import polars as pl
from dotenv import load_dotenv
from dlt.sources.helpers import requests
from dagster import asset

# Load environment variables from .env file
load_dotenv()

@asset
def bloombet_nfl_data() -> pl.DataFrame:
    """
    Fetches and aggregates historical NFL data from the Bloombet API starting from
    September 24th, 2024, with 1-hour intervals.
    """
    api_key = os.getenv("BLOOMBET_API_KEY")
    sport = "nfl"
    start_time = datetime(2024, 9, 24, 0, 0, 0)
    time_interval = timedelta(hours=1)

    current_time = start_time
    aggregated_df = pl.DataFrame()

    while current_time <= datetime.now():
        print(f"Starting API call for {sport} at {current_time.strftime('%Y-%m-%d %H:%M:%S')}")

        response = requests.get(
            f'https://getbloombet.com/api/historical',
            params={
                'api_key': api_key,
                'sport': sport,
                'date': current_time.strftime('%Y-%m-%d %H:%M:%S')
            }
        )
        
        response.raise_for_status()
        print(f"API call successful for {sport} at {current_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Convert the JSON object to a Polars DataFrame
        data = response.json()
        df = pl.DataFrame(data)

        # Append the individual DataFrame to the aggregated DataFrame
        aggregated_df = pl.concat([aggregated_df, df], rechunk=True)

        # Drop the individual DataFrame to conserve memory
        del df

        print(f"Data for {current_time.strftime('%Y-%m-%d %H:%M:%S')} added to aggregated dataframe")

        # Move to the next interval
        current_time += time_interval

    return aggregated_df
