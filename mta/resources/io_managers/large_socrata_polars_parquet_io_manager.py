# File: mta/resources/io_managers/large_socrata_polars_parquet_io_manager.py

import os
import polars as pl
from dagster import IOManager, io_manager, OutputContext, InputContext, get_dagster_logger

class LargeSocrataPolarsParquetIOManager(IOManager):
    """
    An I/O manager designed for incremental/streamed writes:
      1. The user calls `write_chunk(...)` directly for each chunk of data.
      2. We create month-based subdirectories (or year=YYYY/month=MM) as we go.
      3. `handle_output` does minimal (we already wrote everything in `write_chunk`).
    """

    def __init__(self, base_dir: str):
        self._base_dir = base_dir
        self._logger = get_dagster_logger()

    def write_chunk(
        self,
        asset_name: str,
        year: int,
        month: int,
        batch_num: int,
        df: pl.DataFrame
    ):
        """
        Immediately write this chunk to a Parquet file:
          base_dir/asset_name/year=YYYY/month=MM/asset_name_YYYYMM_<batch_num>.parquet
        """
        year_str = f"{year:04d}"
        month_str = f"{month:02d}"
        out_dir = os.path.join(
            self._base_dir,
            asset_name,
            f"year={year_str}",
            f"month={month_str}"
        )
        os.makedirs(out_dir, exist_ok=True)

        file_name = f"{asset_name}_{year_str}{month_str}_{batch_num}.parquet"
        file_path = os.path.join(out_dir, file_name)
        self._logger.info(
            f"Writing batch {batch_num} ({df.shape[0]} rows) to {file_path}"
        )

        df.write_parquet(file_path)

    def handle_output(self, context: OutputContext, obj):
        """
        Called automatically after the asset returns its final object. 
        In our approach, we do minimal or nothing here, because we've 
        already written all chunks as they arrived.
        """
        self._logger.info(
            f"[LargeSocrataPolarsParquetIOManager] Asset '{context.asset_key}' has completed. "
            f"No further writing needed (chunks were already written)."
        )

    def load_input(self, context: InputContext):
        # If you ever need to read back these chunked files, you’d implement that logic here.
        raise NotImplementedError("Reading from chunked parquet not implemented.")

@io_manager(
    config_schema={"base_dir": str},
    required_resource_keys=set(),  # Must be a set or frozenset, not {}
)
def large_socrata_polars_parquet_io_manager(init_context):
    base_dir = init_context.resource_config["base_dir"]
    return LargeSocrataPolarsParquetIOManager(base_dir=base_dir)
