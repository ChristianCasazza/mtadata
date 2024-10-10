import os
import polars as pl
from dagster import ConfigurableIOManager, OutputContext, InputContext

class PolarsParquetIOManager(ConfigurableIOManager):
    base_dir: str

    def handle_output(self, context: OutputContext, obj: pl.DataFrame, batch_number: int = None):
        # If obj is None or empty, log and return
        if obj is None or obj.is_empty():
            context.log.info("No data to save.")
            return

        # If batch_number is provided, use it; otherwise, assign a default name
        if batch_number is not None:
            file_name = f"{self.base_dir}/file_{batch_number}.parquet"
        else:
            # In the final output phase where batch_number is not provided, save with a generic name
            file_name = f"{self.base_dir}/final_output.parquet"

        # Define the path where the Parquet file will be stored
        os.makedirs(self.base_dir, exist_ok=True)
        print(f"Saving data to {file_name}...")
        obj.write_parquet(file_name)
        print(f"Data successfully saved to {file_name}")

    def load_input(self, context: InputContext) -> pl.DataFrame:
        # Use a generic or specific batch number to determine the file path
        batch_number = context.upstream_output.metadata.get('batch_number', 1)
        file_name = f"{self.base_dir}/file_{batch_number}.parquet"
        return pl.read_parquet(file_name)
