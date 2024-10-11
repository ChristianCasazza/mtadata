from datetime import datetime

# Define the dynamic start date
START_DATE = datetime(2024, 9, 10).isoformat()

# Constants for MTA Hourly Subway API configuration
MTA_HOURLY_SUBWAY_ENDPOINT = "https://data.ny.gov/resource/wujg-7c2s.geojson"
MTA_HOURLY_SUBWAY_ORDER = "transit_timestamp ASC"
MTA_HOURLY_SUBWAY_WHERE = f"transit_timestamp >= '{START_DATE}'"
