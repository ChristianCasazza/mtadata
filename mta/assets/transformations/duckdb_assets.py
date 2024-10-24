from dagster import asset, AssetExecutionContext, AssetIn
import duckdb
import os
from mta.constants import MTA_ASSETS_PATHS, WEATHER_ASSETS_PATHS, OTHER_MTA_ASSETS_PATHS, LAKE_PATH

@asset(
    ins={
        "mta_hourly_subway_socrata": AssetIn(),
        "mta_daily_ridership": AssetIn(),
        "mta_bus_speeds": AssetIn(),
        "mta_bus_wait_time": AssetIn(),
        "mta_operations_statement": AssetIn(),
        "daily_weather_asset": AssetIn(),
        "hourly_weather_asset": AssetIn(),
    },
    compute_kind="DuckDB",
)
def duckdb_warehouse(
    context: AssetExecutionContext,
    mta_hourly_subway_socrata,
    mta_daily_ridership,
    mta_bus_speeds,
    mta_bus_wait_time,
    mta_operations_statement,
    daily_weather_asset,
    hourly_weather_asset,
):
    # Define the DuckDB file path
    duckdb_file_path = LAKE_PATH
    # Merge MTA and Weather asset paths
    parquet_base_paths = {**MTA_ASSETS_PATHS, **WEATHER_ASSETS_PATHS, **OTHER_MTA_ASSETS_PATHS}
    # Lists to track created and ignored views
    created_views = []
    ignored_views = []
    
    # Check if the DuckDB file already exists, delete it if so
    if os.path.exists(duckdb_file_path):
        context.log.info(f"Found existing DuckDB file at {duckdb_file_path}, deleting it.")
        os.remove(duckdb_file_path)

    # Connect to DuckDB (this will create a new file if it doesn't exist)
    con = duckdb.connect(duckdb_file_path)
    
    try:
        # Loop through each asset name and create a corresponding view based on the parquet files
        for table_name, parquet_dir in parquet_base_paths.items():
            parquet_glob = os.path.join(parquet_dir, "*.parquet")
            # Check if the directory exists and has parquet files
            if not os.path.exists(parquet_dir):
                ignored_views.append(table_name)
                context.log.info(f"Directory does not exist for {table_name}: {parquet_dir}, skipping view creation.")
                continue
            # Check if any parquet files exist in the directory
            if not any(f.endswith('.parquet') for f in os.listdir(parquet_dir)):
                ignored_views.append(table_name)
                context.log.info(f"No parquet files found for {table_name} at {parquet_dir}, skipping view creation.")
                continue
            # Create the view in DuckDB by selecting all from the parquet files
            view_query = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{parquet_glob}');"
            con.execute(view_query)
            context.log.info(f"View created for {table_name} based on parquet files at {parquet_glob}")
            created_views.append(table_name)
    finally:
        # Close the connection
        con.close()
        context.log.info("Connection to DuckDB closed.")
    
    # Log summary of created and ignored views
    context.log.info(f"Created views: {', '.join(created_views) if created_views else 'None'}")
    context.log.info(f"Ignored views (no files or directory not found): {', '.join(ignored_views) if ignored_views else 'None'}")
    
    return None
