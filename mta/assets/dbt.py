from dagster import AssetExecutionContext, asset, AssetIn
from dagster_dbt import DbtCliResource, dbt_assets
from ..resources.dbt_project import dbt_project

@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_project_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(
    ins={
        "update_pbp_with_players": AssetIn()
    },
    compute_kind="DuckDB"
)
def run_dbt_assets(nfl_pbp_2024, nfl_players, update_pbp_with_players):
    # Here we can directly call the dbt_project_assets to ensure that DBT models run
    # after the previous assets have been successfully materialized.
    return dbt_project_assets  # You can customize this as needed
