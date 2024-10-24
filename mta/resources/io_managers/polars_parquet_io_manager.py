import os
import polars as pl
from dagster import ConfigurableIOManager, OutputContext, InputContext

class PolarsParquetIOManager(ConfigurableIOManager):
    base_dir: str

    def handle_output(self, context: OutputContext, obj: pl.DataFrame, batch_number: int = None):
        # Get the absolute path relative to this file's directory
        script_dir = os.path.dirname(os.path.realpath(__file__))
        abs_base_dir = os.path.join(script_dir, self.base_dir)

        # If obj is None or empty, log and return
        if obj is None or obj.is_empty():
            context.log.info("No data to save.")
            return

        # If batch_number is not provided, default to 1
        if batch_number is None:
            batch_number = 1

        # Use the batch_number to create the file name (file_1.parquet, file_2.parquet, etc.)
        file_name = os.path.join(abs_base_dir, f"file_{batch_number}.parquet")

        # Create the base directory if it doesn't exist
        os.makedirs(abs_base_dir, exist_ok=True)
        print(f"Saving data to {file_name}...")
        obj.write_parquet(file_name)
        print(f"Data successfully saved to {file_name}")


    def load_input(self, context: InputContext) -> pl.DataFrame:
        # Get the absolute path relative to this file's directory
        script_dir = os.path.dirname(os.path.realpath(__file__))
        abs_base_dir = os.path.join(script_dir, self.base_dir)

        # Use a generic or specific batch number to determine the file path
        batch_number = context.upstream_output.metadata.get('batch_number', 1)
        file_name = os.path.join(abs_base_dir, f"file_{batch_number}.parquet")
        return pl.read_parquet(file_name)