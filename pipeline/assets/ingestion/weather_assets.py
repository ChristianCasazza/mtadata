# mta/assets/ingestion/weather_assets.py

# Replace MaterializeResult with Output
from dagster import asset, Output

from pipeline.utils.open_mateo_free_api import (
    OpenMateoDailyWeatherConfig,
    OpenMateoHourlyWeatherConfig,
    OpenMateoDailyWeatherClient,
    OpenMateoHourlyWeatherClient,
)

class OpenMateoDailyWeatherConstants:
    START_DATE = "2020-09-29"
    END_DATE = "2024-10-05"
    LATITUDE = 40.7143
    LONGITUDE = -74.006
    TIMEZONE = "America/New_York"
    TEMPERATURE_UNIT = "fahrenheit"

class OpenMateoHourlyWeatherConstants:
    START_DATE = "2022-02-01"
    END_DATE = "2024-10-01"
    LATITUDE = 40.7143
    LONGITUDE = -74.006
    TIMEZONE = "America/New_York"
    TEMPERATURE_UNIT = "fahrenheit"

@asset(
    name="daily_weather_asset",
    compute_kind="Polars",
    group_name="weather",
    tags={"domain": "weather", "type": "ingestion", "source": "open-meteo"},
)
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

    head_sample = daily_df.head(5).to_dicts() if daily_df.shape[0] else []
    return Output(
        value=daily_df,
        metadata={
            "dagster/row_count": daily_df.shape[0],
            "sample_rows": str(head_sample),
        },
    )

@asset(
    name="hourly_weather_asset",
    compute_kind="Polars",
    group_name="weather",
    tags={"domain": "weather", "type": "ingestion", "source": "open-meteo"},
)
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

    head_sample = hourly_df.head(5).to_dicts() if hourly_df.shape[0] else []
    return Output(
        value=hourly_df,
        metadata={
            "dagster/row_count": hourly_df.shape[0],
            "sample_rows": str(head_sample),
        },
    )
