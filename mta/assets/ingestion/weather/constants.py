# mta/assets/ingestion/weather/constants.py
from mta.utils.open_mateo_free_api import OpenMateoDailyWeatherConfig, OpenMateoHourlyWeatherConfig

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