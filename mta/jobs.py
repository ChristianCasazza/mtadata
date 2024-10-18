from dagster import define_asset_job

# Define a job that ensures create_duckdb_and_views runs before dbt assets
dbt_job = define_asset_job("dbt_with_duckdb", selection=["create_duckdb_and_views", "*my_dbt_assets*"])
