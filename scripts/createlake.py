import duckdb
import os
import sys

# Add the root of the project to the system path to resolve imports from the mta module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipeline.constants import MTA_ASSETS_PATHS, WEATHER_ASSETS_PATHS, OTHER_MTA_ASSETS_PATHS
from pipeline.constants import WAREHOUSE_PATH


def create_duckdb_and_views():
    # Define the DuckDB file path
    duckdb_file_path = WAREHOUSE_PATH

    # Merge MTA and Weather asset paths
    parquet_base_paths = {**MTA_ASSETS_PATHS, **WEATHER_ASSETS_PATHS, **OTHER_MTA_ASSETS_PATHS}

    # Lists to track created and ignored views
    created_views = []
    ignored_views = []

    # Check if the DuckDB file already exists, delete it if so
    if os.path.exists(duckdb_file_path):
        print(f"Found existing DuckDB file at {duckdb_file_path}, deleting it.")
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
                print(f"Directory does not exist for {table_name}: {parquet_dir}, skipping view creation.")
                continue

            # Check if any parquet files exist in the directory
            if not any(f.endswith('.parquet') for f in os.listdir(parquet_dir)):
                ignored_views.append(table_name)
                print(f"No parquet files found for {table_name} at {parquet_dir}, skipping view creation.")
                continue
            
            # Create the view in DuckDB by selecting all from the parquet files
            view_query = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{parquet_glob}');"
            con.execute(view_query)

            print(f"View created for {table_name} based on parquet files at {parquet_glob}")
            created_views.append(table_name)

    finally:
        # Close the connection
        con.close()
        print("Connection to DuckDB closed.")
    
    # Print summary of created and ignored views
    print("\nSummary:")
    print(f"Created views: {', '.join(created_views) if created_views else 'None'}")
    print(f"Ignored views (no files or directory not found): {', '.join(ignored_views) if ignored_views else 'None'}")

if __name__ == "__main__":
    # Call the function to create the DuckDB file and views
    create_duckdb_and_views()
