import dlt
from dlt.sources.helpers import requests
from datetime import datetime, timedelta
import polars as pl
from dagster import ConfigurableResource
import duckdb
import os
from dagster import resource, Field, String

class BloombetAPI(ConfigurableResource):
    api_key: str
    sport: str
    start_time: datetime
    time_interval: timedelta

    def fetch_bloombet_data(self, date: datetime):
        """Fetch historical data from Bloombet API for a specific sport and date."""
        print(f"Starting API call for {self.sport} at {date.strftime('%Y-%m-%d %H:%M:%S')}")
        
        response = requests.get(
            f'https://getbloombet.com/api/historical',
            params={
                'api_key': self.api_key,
                'sport': self.sport,
                'date': date.strftime('%Y-%m-%d %H:%M:%S')
            }
        )
        
        response.raise_for_status()
        print(f"API call successful for {self.sport} at {date.strftime('%Y-%m-%d %H:%M:%S')}")
        
        return response.json()

    def aggregate_data(self):
        """Run the process and aggregate all results into one Polars DataFrame."""
        current_time = self.start_time
        aggregated_df = pl.DataFrame()  # Initialize an empty Polars DataFrame for aggregation

        while current_time <= datetime.now():
            print(f"Fetching data for {current_time.strftime('%Y-%m-%d %H:%M:%S')}")

            try:
                data = self.fetch_bloombet_data(current_time)
                
                # Convert the JSON object to a Polars DataFrame
                df = pl.DataFrame(data)
                
                # Append the individual DataFrame to the aggregated DataFrame
                aggregated_df = pl.concat([aggregated_df, df], rechunk=True)
                
                # Drop the individual DataFrame to conserve memory
                del df
                
                print(f"Data for {current_time.strftime('%Y-%m-%d %H:%M:%S')} added to aggregated dataframe")

            except Exception as e:
                print(f"Failed to fetch data for {current_time.strftime('%Y-%m-%d %H:%M:%S')} due to {e}")
            
            # Move to the next interval
            current_time += self.time_interval

        # Return the aggregated DataFrame
        return aggregated_df


class DuckDBBIExport:
    def __init__(self, db_raw_path: str, sql_folder: str, new_duckdb_path: str):
        self.db_raw_path = db_raw_path
        self.sql_folder = sql_folder
        self.new_duckdb_path = new_duckdb_path

    def export_tables(self):
        # Your logic for exporting tables
        pass  # Implement the existing logic here

@resource(config_schema={
    "db_raw_path": Field(String, is_required=True),
    "sql_folder": Field(String, is_required=True),
    "new_duckdb_path": Field(String, is_required=True),
})
def duckdb_bi_export(context):
    return DuckDBBIExport(
        db_raw_path=context.resource_config["db_raw_path"],
        sql_folder=context.resource_config["sql_folder"],
        new_duckdb_path=context.resource_config["new_duckdb_path"]
    )