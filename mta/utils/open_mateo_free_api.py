# mta/utils/open_mateo_free_api.py
import requests
import polars as pl
from datetime import datetime

import requests
import polars as pl
from datetime import datetime

class OpenMateoDailyWeatherConfig:
    def __init__(self, start_date, end_date, latitude, longitude, timezone='America/New_York', temperature_unit='fahrenheit'):
        self.start_date = start_date
        self.end_date = end_date
        self.latitude = latitude
        self.longitude = longitude
        self.timezone = timezone
        self.temperature_unit = temperature_unit  # Added temperature_unit


class OpenMateoDailyWeatherClient:
    def __init__(self, config: OpenMateoDailyWeatherConfig):
        self.config = config
        self.daily_vars = [
            "weathercode", 
            "temperature_2m_max", 
            "temperature_2m_min", 
            "temperature_2m_mean", 
            "apparent_temperature_max", 
            "apparent_temperature_min", 
            "apparent_temperature_mean", 
            "sunrise", 
            "sunset",
            "precipitation_sum", 
            "rain_sum", 
            "snowfall_sum", 
            "precipitation_hours"
        ]

    def fetch_daily_data(self):
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": self.config.latitude,
            "longitude": self.config.longitude,
            "start_date": self.config.start_date,
            "end_date": self.config.end_date,
            "daily": ','.join(self.daily_vars),
            "timezone": self.config.timezone,
            "temperature_unit": self.config.temperature_unit  # Include temperature_unit in the request
        }

        response = requests.get(url, params=params)
        if response.status_code == 200:
            return self.process_response(response.json())
        else:
            print(f"Error: {response.status_code}, {response.text}")

    def process_response(self, data):
        daily = data['daily']
        daily_data = {
            "date": [datetime.strptime(date, "%Y-%m-%d") for date in daily['time']],
            "weather_code": daily['weathercode'],
            "temperature_max": daily['temperature_2m_max'],
            "temperature_min": daily['temperature_2m_min'],
            "temperature_mean": daily['temperature_2m_mean'],
            "apparent_temperature_max": daily['apparent_temperature_max'],
            "apparent_temperature_min": daily['apparent_temperature_min'],
            "apparent_temperature_mean": daily['apparent_temperature_mean'],
            "sunrise": daily['sunrise'],
            "sunset": daily['sunset'],
            "precipitation_sum": daily['precipitation_sum'],
            "rain_sum": daily['rain_sum'],
            "snowfall_sum": daily['snowfall_sum'],
            "precipitation_hours": daily['precipitation_hours']
        }
        
        return pl.DataFrame(daily_data)


class OpenMateoHourlyWeatherConfig:
    def __init__(self, start_date, end_date, latitude, longitude, timezone='America/New_York', temperature_unit='fahrenheit'):
        self.start_date = start_date
        self.end_date = end_date
        self.latitude = latitude
        self.longitude = longitude
        self.timezone = timezone
        self.temperature_unit = temperature_unit  # Added temperature_unit


class OpenMateoHourlyWeatherClient:
    def __init__(self, config: OpenMateoHourlyWeatherConfig):
        self.config = config
        self.hourly_vars = [
            "temperature_2m", 
            "precipitation", 
            "rain", 
            "weathercode"
        ]

    def fetch_hourly_data(self):
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": self.config.latitude,
            "longitude": self.config.longitude,
            "start_date": self.config.start_date,
            "end_date": self.config.end_date,
            "hourly": ','.join(self.hourly_vars),
            "timezone": self.config.timezone,
            "temperature_unit": self.config.temperature_unit  # Include temperature_unit in the request
        }

        response = requests.get(url, params=params)
        if response.status_code == 200:
            return self.process_response(response.json())
        else:
            print(f"Error: {response.status_code}, {response.text}")

    def process_response(self, data):
        hourly = data['hourly']
        hourly_data = {
            "date": [datetime.strptime(date, "%Y-%m-%dT%H:%M") for date in hourly['time']],
            "temperature_2m": hourly['temperature_2m'],
            "precipitation": hourly['precipitation'],
            "rain": hourly['rain'],
            "weather_code": hourly['weathercode']
        }
        
        return pl.DataFrame(hourly_data)
