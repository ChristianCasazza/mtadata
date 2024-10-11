import requests
import polars as pl
from datetime import timedelta

ENDPOINT = 'https://data.ny.gov/resource/wujg-7c2s.geojson'
PAGE_SIZE = 500000
HEADERS = {'X-App-Token': 'uHoP8dT0q1BTcacXLCcxrDp8z'}  # Replace with proper environment loading

def fetch_nyc_data_geojson(start_date, offset, page_size=PAGE_SIZE):
    all_data = []

    while True:
        print(f"Fetching data from {start_date} with offset: {offset}")
        response = requests.get(
            ENDPOINT,
            params={
                '$limit': page_size,
                '$offset': offset,
                '$where': f"transit_timestamp >= '{start_date}'",
                '$order': 'transit_timestamp ASC'  # Ensure consistent order
            },
            headers=HEADERS
        )
        response.raise_for_status()
        data = response.json()
        features = data.get('features', [])

        if not features:
            print(f"No more data to fetch at offset {offset}.")
            break

        for feature in features:
            properties = feature.get('properties', {})
            all_data.append(properties)

        break  # Only one batch per function call

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
