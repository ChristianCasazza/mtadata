import os
import sys
from pathlib import Path

# Add the root of the project to the system path to resolve imports from the pipeline module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipeline.constants import SINGLE_PATH_ASSETS_PATHS, PARTITIONED_ASSETS_PATHS, WAREHOUSE_PATH
from pipeline.utils.duckdb_wrapper import DuckDBWrapper

def create_duckdb_and_views():
    """
    Creates a persistent DuckDB file at WAREHOUSE_PATH and registers each
    asset as a DuckDB view. Handles both single-path and partitioned assets.
    Creates the directory and database if they don't exist, and replaces
    any existing DuckDB file with a new one.
    """
    # Ensure WAREHOUSE_PATH directory exists
    warehouse_dir = Path(WAREHOUSE_PATH).parent
    warehouse_dir.mkdir(parents=True, exist_ok=True)

    # If a DuckDB file exists at WAREHOUSE_PATH, delete it
    if os.path.exists(WAREHOUSE_PATH):
        print(f"Existing DuckDB file at {WAREHOUSE_PATH} deleted.")
        os.remove(WAREHOUSE_PATH)

    # Create new DuckDB connection using DuckDBWrapper
    duckdb_wrapper = DuckDBWrapper(WAREHOUSE_PATH)
    print(f"New DuckDB file created at {WAREHOUSE_PATH}")

    # Register non-partitioned assets
    non_partitioned = {**SINGLE_PATH_ASSETS_PATHS}
    if non_partitioned:
        table_names = list(non_partitioned.keys())
        repo_root = os.path.dirname(WAREHOUSE_PATH)  # Use the warehouse's parent dir as repo_root
        base_path = os.path.relpath(os.path.dirname(list(non_partitioned.values())[0]), repo_root)
        duckdb_wrapper.bulk_register_data(
            repo_root=repo_root,
            base_path=base_path,
            table_names=table_names,
            wildcard="*.parquet",
            as_table=False,  # Register as views
            show_tables=False
        )

    # Register partitioned assets
    partitioned = {**PARTITIONED_ASSETS_PATHS}
    if partitioned:
        table_names = list(partitioned.keys())
        repo_root = os.path.dirname(WAREHOUSE_PATH)  # Use the warehouse's parent dir as repo_root
        base_path = os.path.relpath(os.path.dirname(list(partitioned.values())[0]), repo_root)
        duckdb_wrapper.bulk_register_partitioned_data(
            repo_root=repo_root,
            base_path=base_path,
            table_names=table_names,
            wildcard="year=*/month=*/*.parquet",
            as_table=False,  # Register as views
            show_tables=False
        )

    # Close the connection
    duckdb_wrapper.con.close()
    print("Connection to DuckDB closed.")

if __name__ == "__main__":
    # Call the function to create the DuckDB file and views
    create_duckdb_and_views()