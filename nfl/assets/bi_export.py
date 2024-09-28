from dagster import asset
from dagster_dbt import get_asset_key_for_model
from ..resources import DuckDBBIExport  # Ensure this is correctly imported
from .dbt import dbt_project_assets  # Importing dbt_project_assets

@asset(
    compute_kind="DuckDB",
    deps=[
        get_asset_key_for_model([dbt_project_assets], "penalties_per_team"),
        get_asset_key_for_model([dbt_project_assets], "penalty_totals"),
    ]
)
def analytics_cube():
    # Hardcoded values for testing
    db_raw_path = "/home/christianocean/quiv/data/database_raw.duckdb"
    sql_folder = "/home/christianocean/quiv/nfl/dbt/models"
    new_duckdb_path = "/home/christianocean/quiv/nflstats/sources/nflstats/analytics.duckdb"
    
    # Initialize the DuckDBBIExport class
    exporter = DuckDBBIExport(db_raw_path, sql_folder, new_duckdb_path)
    exporter.export_tables()  # This method should handle the export logic
    
    # Instead of returning the exporter, return a summary or DataFrame
    return {"status": "Export completed", "path": new_duckdb_path}  # Returning a dict as output
