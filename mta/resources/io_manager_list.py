from mta.constants import MTA_ASSETS_PATHS, WEATHER_ASSETS_PATHS
from mta.resources.io_managers.polars_parquet_io_manager import PolarsParquetIOManager

def create_polars_parquet_io_manager(asset_name: str):
    """Generates a Polars Parquet IO manager based on asset name and path."""
    base_dir = MTA_ASSETS_PATHS.get(asset_name) or WEATHER_ASSETS_PATHS.get(asset_name)
    if not base_dir:
        raise ValueError(f"No base directory found for asset: {asset_name}")
    return PolarsParquetIOManager(base_dir=base_dir)

# Combine NYC and Weather asset names
asset_names = list(MTA_ASSETS_PATHS.keys()) + list(WEATHER_ASSETS_PATHS.keys())

# Dynamically create IO managers using the asset name
polars_parquet_io_managers = {
    f"{asset_name}_polars_parquet_io_manager": create_polars_parquet_io_manager(asset_name)
    for asset_name in asset_names
}

# Manually add keys 
polars_parquet_io_managers["daily_weather_io_manager"] = create_polars_parquet_io_manager("daily_weather_asset")
polars_parquet_io_managers["hourly_weather_io_manager"] = create_polars_parquet_io_manager("hourly_weather_asset")
