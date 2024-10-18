from dagster import asset
from nfl.constants import NFL_DUCKDB_PATH, BI_DUCKDB_PATH, SQL_FOLDER
from nfl.assets.dbt import dbt_project_assets  # Import the dbt assets
from nfl.resources.io.DuckDBBIExport import duckdb_bi_export_io_manager

@asset(
    io_manager_key="duckdb_bi_export_io_manager",
    deps=[dbt_project_assets],  # Depend on dbt_project_assets
    compute_kind="DuckDB"
)
def mta_stats(context):
    """
    Asset to run the DuckDB export logic for analytics.
    """
    logger = context.log
    # Log information about the paths
    logger.info(f"Starting export from {NFL_DUCKDB_PATH} to {BI_DUCKDB_PATH} using SQL from {SQL_FOLDER}.")

    # The I/O manager will handle the export logic, so no need to instantiate the manager directly
    context.resources.duckdb_bi_export_io_manager.handle_output(context, obj=None)  # Let the IO manager handle the export

    # Return a status or path as the result of the asset
    return {"status": "Export completed", "path": BI_DUCKDB_PATH}