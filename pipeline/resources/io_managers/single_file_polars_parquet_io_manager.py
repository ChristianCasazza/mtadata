# File: mta/resources/io_managers/single_file_polars_parquet_io_manager.py

import os
import polars as pl
from dagster import ConfigurableIOManager, OutputContext, InputContext

class SingleFilePolarsParquetIOManager(ConfigurableIOManager):
    """
    A single I/O manager that stores exactly one Polars DataFrame per asset
    as a .parquet file, named after the asset itself, in a folder also named
    after the asset.

    For example, if the asset_key is "my_asset", we store:
        {base_dir}/my_asset/my_asset.parquet
    """

    base_dir: str  # The base directory in which to store subfolders

    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        """
        Writes a single Polars DataFrame to a single .parquet file.
        The file is named {asset_name}.parquet in the subdirectory
        {base_dir}/{asset_name}.
        """
        if obj is None:
            context.log.info("No data to write (None).")
            return

        if not isinstance(obj, pl.DataFrame):
            raise ValueError(
                f"Expected a Polars DataFrame, got {type(obj)}. "
                "This I/O manager only supports single-file Polars DataFrames."
            )

        asset_name = context.asset_key.to_python_identifier()
        dir_path = os.path.join(self.base_dir, asset_name)
        os.makedirs(dir_path, exist_ok=True)

        file_name = f"{asset_name}.parquet"  # e.g. "daily_weather_asset.parquet"
        file_path = os.path.join(dir_path, file_name)

        obj.write_parquet(file_path)
        context.log.info(
            f"[SingleFilePolarsParquetIOManager] Wrote {len(obj)} rows "
            f"to {file_path}"
        )

    def load_input(self, context: InputContext) -> pl.DataFrame:
        """
        Reads the single .parquet file from base_dir/asset_name/asset_name.parquet
        """
        asset_name = context.asset_key.to_python_identifier()
        file_name = f"{asset_name}.parquet"
        file_path = os.path.join(self.base_dir, asset_name, file_name)

        if not os.path.exists(file_path):
            raise FileNotFoundError(
                f"No file found for asset '{asset_name}' at {file_path}"
            )

        df = pl.read_parquet(file_path)
        context.log.info(
            f"[SingleFilePolarsParquetIOManager] Loaded {len(df)} rows "
            f"from {file_path}"
        )
        return df
