import requests
import polars as pl
from datetime import timedelta

ENDPOINT = 'https://data.ny.gov/resource/wujg-7c2s.geojson'
PAGE_SIZE = 50000
HEADERS = {'X-App-Token': 'uHoP8dT0q1BTcacXLCcxrDp8z'}  # Replace with proper environment loading

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + timedelta(days=4)
    return next_month - timedelta(days=next_month.day)

def fetch_nyc_data_geojson(start_date, end_date, page_size=PAGE_SIZE):
    offset = 0
    all_data = []

    while True:
        print(f"Fetching data from {start_date} to {end_date} with offset: {offset}")
        response = requests.get(
            ENDPOINT,
            params={
                '$limit': page_size,
                '$offset': offset,
                '$where': f"transit_timestamp between '{start_date}' and '{end_date}'"
            },
            headers=HEADERS
        )
        response.raise_for_status()
        data = response.json()
        features = data.get('features', [])

        if not features:
            print(f"No more data to fetch for the period {start_date} to {end_date}.")
            break

        for feature in features:
            properties = feature.get('properties', {})
            all_data.append(properties)

        offset += page_size

    return all_data

def process_and_cast_df(df):
    def parse_timestamp(ts):
        try:
            return pl.col(ts).str.to_datetime(format="%Y-%m-%dT%H:%M:%S.%f", strict=False)
        except:
            try:
                return pl.col(ts).str.to_datetime(format="%Y-%m-%dT%H:%M:%S", strict=False)
            except:
                print(f"Failed to parse timestamp: {ts}")
                return None

    df = df.with_columns([
        parse_timestamp("transit_timestamp").alias("transit_timestamp"),
        pl.col("latitude").cast(pl.Float64),
        pl.col("longitude").cast(pl.Float64),
        pl.col("ridership").cast(pl.Float64).cast(pl.Int64),
        pl.col("transfers").cast(pl.Float64).cast(pl.Int64),
        pl.format('POINT({} {})', pl.col('longitude'), pl.col('latitude')).alias('geom_wkt')
    ])
    return df
