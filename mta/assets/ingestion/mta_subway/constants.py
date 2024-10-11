# constants.py

from datetime import datetime
from mta.utils.data_fetching import SocrataAPIConfig

# Define the dynamic start date for hourly subway data
class MTAHourlySubwayConfig(SocrataAPIConfig):
    SOCRATA_ENDPOINT: str = "https://data.ny.gov/resource/wujg-7c2s.geojson"
    order: str = "transit_timestamp ASC"
    limit: int = 500000
    where: str = f"transit_timestamp >= '{datetime(2024, 9, 10).isoformat()}'"
    offset: int = 0

# Define a new config for daily subway data
class MTADailySubwayConfig(SocrataAPIConfig):
    SOCRATA_ENDPOINT: str = "https://data.ny.gov/resource/vxuj-8kew.json"
    order: str = "Date ASC"  # Using transit_date for the daily dataset
    limit: int = 500000
    where: str = f"Date >= '{datetime(2020, 3, 1).isoformat()}'"
    offset: int = 0


# Add a new MTA Bus Speeds Config
class MTABusSpeedsConfig(SocrataAPIConfig):
    SOCRATA_ENDPOINT: str = "https://data.ny.gov/resource/6ksi-7cxr.json"
    order: str = "month ASC"
    limit: int = 500000
    where: str = f"month >= '{datetime(2020, 1, 1).isoformat()}'"
    offset: int = 0


class MTAWaitTimeBusConfig(SocrataAPIConfig):
    SOCRATA_ENDPOINT: str = "https://data.ny.gov/resource/swky-c3v4.json"
    order: str = "month ASC"
    limit: int = 500000
    where: str = f"month >= '{datetime(2020, 1, 1).isoformat()}'"
    offset: int = 0

class MTAOperationsStatementConfig(SocrataAPIConfig):
    SOCRATA_ENDPOINT: str = "https://data.ny.gov/resource/yg77-3tkj.json"
    order: str = "Month ASC"
    limit: int = 500000
    offset: int = 0
