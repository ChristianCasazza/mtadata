from dagster import AssetExecutionContext, asset, AssetIn
from dagster_dbt import DbtCliResource, dbt_assets
from mta.resources.dbt_project import dbt_project

@dbt_assets(
    manifest=dbt_project.manifest_path
)
def dbt_project_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(
    ins={"duckdb_warehouse": AssetIn()},
    compute_kind="DuckDB"
)
def run_dbt_assets(duckdb_warehouse):
    # Run DBT models after assets are materialized
    return dbt_project_assets  # Customize based on your pipeline logic