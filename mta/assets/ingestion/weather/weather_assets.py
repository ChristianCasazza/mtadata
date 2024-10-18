# mta/assets/ingestion/weather/weather_assets.py
from dagster import asset
from mta.utils.open_mateo_free_api import *
from .constants import OpenMateoDailyWeatherConstants, OpenMateoHourlyWeatherConstants
from mta.resources.io_managers.polars_parquet_io_manager import PolarsParquetIOManager


WEATHER_ASSETS_NAMES = [
    "daily_weather_asset",
    "hourly_weather_asset"
]



@asset(io_manager_key="daily_weather_io_manager")
def daily_weather_asset(context):
    config = OpenMateoDailyWeatherConfig(
        start_date=OpenMateoDailyWeatherConstants.START_DATE,
        end_date=OpenMateoDailyWeatherConstants.END_DATE,
        latitude=OpenMateoDailyWeatherConstants.LATITUDE,
        longitude=OpenMateoDailyWeatherConstants.LONGITUDE,
        timezone=OpenMateoDailyWeatherConstants.TIMEZONE,
        temperature_unit=OpenMateoDailyWeatherConstants.TEMPERATURE_UNIT  # Use temperature unit
    )
    
    client = OpenMateoDailyWeatherClient(config)
    daily_df = client.fetch_daily_data()

    # Log the fetched daily weather data
    context.log.info(f"Fetched daily weather data:\n{daily_df.head()}")
    
    # Return the DataFrame for storage by the IO manager
    return daily_df


@asset(io_manager_key="hourly_weather_io_manager")
def hourly_weather_asset(context):
    config = OpenMateoHourlyWeatherConfig(
        start_date=OpenMateoHourlyWeatherConstants.START_DATE,
        end_date=OpenMateoHourlyWeatherConstants.END_DATE,
        latitude=OpenMateoHourlyWeatherConstants.LATITUDE,
        longitude=OpenMateoHourlyWeatherConstants.LONGITUDE,
        timezone=OpenMateoHourlyWeatherConstants.TIMEZONE,
        temperature_unit=OpenMateoHourlyWeatherConstants.TEMPERATURE_UNIT  # Use temperature unit
    )
    
    client = OpenMateoHourlyWeatherClient(config)
    hourly_df = client.fetch_hourly_data()

    # Log the fetched hourly weather data
    context.log.info(f"Fetched hourly weather data:\n{hourly_df.head()}")

    # Return the DataFrame for storage by the IO manager
    return hourly_df