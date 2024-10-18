from dagster import AssetExecutionContext, asset, AssetIn
from dagster_dbt import DbtCliResource, dbt_assets
from mta.resources.dbt_project import dbt_project

@dbt_assets(
    manifest=dbt_project.manifest_path  # Make sure the manifest is generated properly from dbt_project
)
def dbt_project_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(
    ins={"create_duckdb_and_views": AssetIn()},  # Replace with any MTA-specific dependencies
    compute_kind="DuckDB"
)
def run_dbt_assets(create_duckdb_and_views):
    # Run DBT models after assets are materialized
    return dbt_project_assets  # Customize based on your pipeline logic
