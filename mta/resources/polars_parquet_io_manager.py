import os
import polars as pl
from dagster import ConfigurableIOManager, OutputContext, InputContext

class PolarsParquetIOManager(ConfigurableIOManager):
    base_dir: str

    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        # Define the path where the Parquet file will be stored
        year, month, week = context.metadata['year'], context.metadata['month'], context.metadata['week']
        month_dir = f"{self.base_dir}/{year}_{month:02}"
        os.makedirs(month_dir, exist_ok=True)
        file_name = f"{month_dir}/week_{week}.parquet"
        print(f"Saving data to {file_name}...")
        obj.write_parquet(file_name)
        print(f"Data successfully saved to {file_name}")

    def load_input(self, context: InputContext) -> pl.DataFrame:
        # Define the path where the Parquet file is stored
        year, month, week = context.metadata['year'], context.metadata['month'], context.metadata['week']
        file_name = f"{self.base_dir}/{year}_{month:02}/week_{week}.parquet"
        return pl.read_parquet(file_name)
