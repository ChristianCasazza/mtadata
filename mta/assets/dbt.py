from dagster import AssetExecutionContext, asset, AssetIn
from dagster_dbt import DbtCliResource, dbt_assets
from ..resources.dbt_project import dbt_project


# Define dbt assets using the manifest path provided by the DbtCliResource
@dbt_assets(manifest=dbt_project.get_manifest_path())
def dbt_project_assets():
    pass  # The decorator takes care of materializing the dbt assets
