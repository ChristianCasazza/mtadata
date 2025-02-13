# File: mta/assets/ingestion/weather/weather_assets.py

from dagster import asset
from mta.utils.open_mateo_free_api import (
    OpenMateoDailyWeatherConfig,
    OpenMateoHourlyWeatherConfig,
    OpenMateoDailyWeatherClient,
    OpenMateoHourlyWeatherClient,
)

# Daily Weather Configuration
class OpenMateoDailyWeatherConstants:
    START_DATE = "2020-09-29"
    END_DATE = "2024-10-05"
    LATITUDE = 40.7143
    LONGITUDE = -74.006
    TIMEZONE = "America/New_York"
    TEMPERATURE_UNIT = "fahrenheit"

# Hourly Weather Configuration
class OpenMateoHourlyWeatherConstants:
    START_DATE = "2022-02-01"
    END_DATE = "2024-10-01"
    LATITUDE = 40.7143
    LONGITUDE = -74.006
    TIMEZONE = "America/New_York"
    TEMPERATURE_UNIT = "fahrenheit"

@asset(name="daily_weather_asset", compute_kind="Polars")
def daily_weather_asset(context):
    config = OpenMateoDailyWeatherConfig(
        start_date=OpenMateoDailyWeatherConstants.START_DATE,
        end_date=OpenMateoDailyWeatherConstants.END_DATE,
        latitude=OpenMateoDailyWeatherConstants.LATITUDE,
        longitude=OpenMateoDailyWeatherConstants.LONGITUDE,
        timezone=OpenMateoDailyWeatherConstants.TIMEZONE,
        temperature_unit=OpenMateoDailyWeatherConstants.TEMPERATURE_UNIT,
    )

    client = OpenMateoDailyWeatherClient(config)
    daily_df = client.fetch_daily_data()

    context.log.info(f"Fetched daily weather data:\n{daily_df.head()}")
    return daily_df  # Returns a Polars DataFrame => written by SingleFilePolarsParquetIOManager


@asset(name="hourly_weather_asset", compute_kind="Polars")
def hourly_weather_asset(context):
    config = OpenMateoHourlyWeatherConfig(
        start_date=OpenMateoHourlyWeatherConstants.START_DATE,
        end_date=OpenMateoHourlyWeatherConstants.END_DATE,
        latitude=OpenMateoHourlyWeatherConstants.LATITUDE,
        longitude=OpenMateoHourlyWeatherConstants.LONGITUDE,
        timezone=OpenMateoHourlyWeatherConstants.TIMEZONE,
        temperature_unit=OpenMateoHourlyWeatherConstants.TEMPERATURE_UNIT,
    )

    client = OpenMateoHourlyWeatherClient(config)
    hourly_df = client.fetch_hourly_data()

    context.log.info(f"Fetched hourly weather data:\n{hourly_df.head()}")
    return hourly_df
