# mta/assets/ingestion/mta_subway/mta_assets.py

from mta.utils.socrata_api import SocrataAPI, SocrataAPIConfig
from dagster import asset
import gc
import polars as pl
from datetime import datetime
import os
import requests

def get_io_manager(context):
    """Helper function to dynamically fetch the correct IO manager for the asset."""
    asset_name = context.asset_key.path[-1]  # Get the last part of the asset key, which is the asset name
    io_manager_key = f"{asset_name}_polars_parquet_io_manager"  # Construct the io_manager_key
    return getattr(context.resources, io_manager_key)



BASE_URL = "https://fastopendata.org/mta/raw/hourly_subway/"
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","..", "data", "opendata"))
LOCAL_DOWNLOAD_PATH = os.path.join(BASE_PATH, "mta_hourly_subway_socrata")
years = ["2022", "2023", "2024"]
months = [f"{i:02d}" for i in range(1, 13)]  # Months from 01 to 12

def download_file(file_url, local_path):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    response = requests.get(file_url, stream=True)
    if response.status_code == 200:
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return local_path
    else:
        raise Exception(f"Failed to download: {file_url} (Status code: {response.status_code})")

def download_files_from_year_month(base_url, local_base_path, year, month):
    file_name = f"{year}_{month}.parquet"
    file_url = f"{base_url}year%3D{year}/month%3D{month}/{file_name}"
    local_file_path = os.path.join(local_base_path, file_name)
    return download_file(file_url, local_file_path)

@asset(
    compute_kind="Python"
)
def mta_hourly_subway_socrata(context):
    downloaded_files = []
    for year in years:
        for month in months:
            context.log.info(f"Downloading data for year: {year}, month: {month}")
            try:
                file_path = download_files_from_year_month(BASE_URL, LOCAL_DOWNLOAD_PATH, year, month)
                context.log.info(f"Downloaded file to: {file_path}")
                downloaded_files.append(file_path)
            except Exception as e:
                context.log.error(f"Error downloading file for {year}-{month}: {e}")
    return downloaded_files


class MTADailyRidershipConfig(SocrataAPIConfig):
    SOCRATA_ENDPOINT: str = "https://data.ny.gov/resource/vxuj-8kew.json"
    order: str = "Date ASC"
    limit: int = 500000
    where: str = f"Date >= '{datetime(2020, 3, 1).isoformat()}'"
    offset: int = 0

def process_mta_daily_df(df: pl.DataFrame) -> pl.DataFrame:
    # Log the dataframe columns to verify the date column exists
    print(f"Columns before processing: {df.columns}")
    
    # Ensure column names are lower-cased and spaces replaced with underscores
    df = df.rename({col: col.lower().replace(" ", "_") for col in df.columns})
    
    # Log the dataframe after renaming to confirm changes
    print(f"Columns after renaming: {df.columns}")

    # Handle the date parsing with the correct format
    df = df.with_columns([
        pl.col("date").str.strptime(pl.Date, format="%Y-%m-%dT%H:%M:%S.%f", strict=False).alias("date")
    ])
    
    # Log after date parsing to verify correctness
    print(f"Processed date column: {df.select('date').head()}")

    # Cast columns to appropriate types and drop original string columns after casting
    df = df.with_columns([
        pl.col("subways_total_estimated_ridership").cast(pl.Float64).alias("subways_total_ridership"),
        pl.col("subways_of_comparable_pre_pandemic_day").cast(pl.Float64).alias("subways_pct_pre_pandemic"),
        pl.col("buses_total_estimated_ridersip").cast(pl.Float64).alias("buses_total_ridership"),
        pl.col("buses_of_comparable_pre_pandemic_day").cast(pl.Float64).alias("buses_pct_pre_pandemic"),
        pl.col("lirr_total_estimated_ridership").cast(pl.Float64).alias("lirr_total_ridership"),
        pl.col("lirr_of_comparable_pre_pandemic_day").cast(pl.Float64).alias("lirr_pct_pre_pandemic"),
        pl.col("metro_north_total_estimated_ridership").cast(pl.Float64).alias("metro_north_total_ridership"),
        pl.col("metro_north_of_comparable_pre_pandemic_day").cast(pl.Float64).alias("metro_north_pct_pre_pandemic"),
        pl.col("access_a_ride_total_scheduled_trips").cast(pl.Float64).alias("access_a_ride_total_trips"),
        pl.col("access_a_ride_of_comparable_pre_pandemic_day").cast(pl.Float64).alias("access_a_ride_pct_pre_pandemic"),
        pl.col("bridges_and_tunnels_total_traffic").cast(pl.Float64).alias("bridges_tunnels_total_traffic"),
        pl.col("bridges_and_tunnels_of_comparable_pre_pandemic_day").cast(pl.Float64).alias("bridges_tunnels_pct_pre_pandemic"),
        pl.col("staten_island_railway_total_estimated_ridership").cast(pl.Float64).alias("staten_island_railway_total_ridership"),
        pl.col("staten_island_railway_of_comparable_pre_pandemic_day").cast(pl.Float64).alias("staten_island_railway_pct_pre_pandemic"),
    ]).drop([
        "subways_total_estimated_ridership",
        "subways_of_comparable_pre_pandemic_day",
        "buses_total_estimated_ridersip",
        "buses_of_comparable_pre_pandemic_day",
        "lirr_total_estimated_ridership",
        "lirr_of_comparable_pre_pandemic_day",
        "metro_north_total_estimated_ridership",
        "metro_north_of_comparable_pre_pandemic_day",
        "access_a_ride_total_scheduled_trips",
        "access_a_ride_of_comparable_pre_pandemic_day",
        "bridges_and_tunnels_total_traffic",
        "bridges_and_tunnels_of_comparable_pre_pandemic_day",
        "staten_island_railway_total_estimated_ridership",
        "staten_island_railway_of_comparable_pre_pandemic_day"
    ])

    return df

@asset(
    io_manager_key="mta_daily_ridership_polars_parquet_io_manager",
    compute_kind="Polars"
)
def mta_daily_ridership(context):
    config = MTADailyRidershipConfig()
    api_client = SocrataAPI(config)
    offset = config.offset
    batch_number = 1

    while True:
        context.log.info(f"Fetching daily data with offset: {offset}")
        updated_config = config.copy(update={"offset": offset})
        api_client.config = updated_config
        data = api_client.fetch_data()

        if not data:
            context.log.info(f"No more daily data to fetch at offset {offset}.")
            return

        raw_df = pl.DataFrame(data)
        processed_df = process_mta_daily_df(raw_df)

        if not processed_df.is_empty():
            io_manager = get_io_manager(context)
            io_manager.handle_output(context, processed_df, batch_number)

        del raw_df, processed_df, data
        gc.collect()

        offset += config.limit
        batch_number += 1


class MTABusSpeedsConfig(SocrataAPIConfig):
    SOCRATA_ENDPOINT: str = "https://data.ny.gov/resource/6ksi-7cxr.json"
    order: str = "month ASC"
    limit: int = 500000
    where: str = f"month >= '{datetime(2020, 1, 1).isoformat()}'"
    offset: int = 0

def process_mta_bus_speeds_df(df: pl.DataFrame) -> pl.DataFrame:
    # Log the dataframe columns to verify the month column exists
    print(f"Columns before processing: {df.columns}")

    # Ensure column names are lower-cased and spaces replaced with underscores
    df = df.rename({col: col.lower().replace(" ", "_").replace("-", "_") for col in df.columns})

    # Log the dataframe after renaming to confirm changes
    print(f"Columns after renaming: {df.columns}")

    # Handle the month parsing with the full datetime format and convert it to Date
    df = df.with_columns([
        pl.col("month").str.strptime(pl.Date, format="%Y-%m-%dT%H:%M:%S.%f", strict=False).alias("month")
    ])

    # Log after month parsing to verify correctness
    print(f"Processed month column: {df.select('month').head()}")

    # Cast the rest of the columns to their appropriate types
    df = df.with_columns([
        pl.col("borough").cast(pl.Utf8),
        pl.col("day_type").cast(pl.Int64),
        pl.col("trip_type").cast(pl.Utf8),
        pl.col("route_id").cast(pl.Utf8),
        pl.col("period").cast(pl.Utf8),
        pl.col("total_mileage").cast(pl.Float64),
        pl.col("total_operating_time").cast(pl.Float64),
        pl.col("average_speed").cast(pl.Float64)
    ])

    return df

@asset(
    io_manager_key="mta_bus_speeds_polars_parquet_io_manager",
    compute_kind="Polars"
)
def mta_bus_speeds(context):
    config = MTABusSpeedsConfig()
    api_client = SocrataAPI(config)
    offset = config.offset
    batch_number = 1

    while True:
        context.log.info(f"Fetching bus speeds data with offset: {offset}")
        updated_config = config.copy(update={"offset": offset})
        api_client.config = updated_config
        data = api_client.fetch_data()

        if not data:
            context.log.info(f"No more bus speeds data to fetch at offset {offset}.")
            context.log.info("All data has been fetched successfully.")
            return

        raw_df = pl.DataFrame(data)
        processed_df = process_mta_bus_speeds_df(raw_df)

        if not processed_df.is_empty():
            io_manager = get_io_manager(context)
            io_manager.handle_output(context, processed_df, batch_number)

        del raw_df, processed_df, data
        gc.collect()

        offset += config.limit
        batch_number += 1



class MTAWaitTimeBusConfig(SocrataAPIConfig):
    SOCRATA_ENDPOINT: str = "https://data.ny.gov/resource/swky-c3v4.json"
    order: str = "month ASC"
    limit: int = 500000
    where: str = f"month >= '{datetime(2020, 1, 1).isoformat()}'"
    offset: int = 0

def process_mta_bus_wait_time_df(df: pl.DataFrame) -> pl.DataFrame:
    # Log the dataframe columns to verify the month column exists
    print(f"Columns before processing: {df.columns}")

    # Ensure column names are lower-cased and spaces replaced with underscores
    df = df.rename({col: col.lower().replace(" ", "_").replace("-", "_") for col in df.columns})

    # Log the dataframe after renaming to confirm changes
    print(f"Columns after renaming: {df.columns}")

    # Handle the month parsing with the full datetime format and convert it to Date
    df = df.with_columns([
        pl.col("month").str.strptime(pl.Date, format="%Y-%m-%dT%H:%M:%S.%f", strict=False).alias("month")
    ])

    # Log after month parsing to verify correctness
    print(f"Processed month column: {df.select('month').head()}")

    # Cast the rest of the columns to their appropriate types
    df = df.with_columns([
        pl.col("borough").cast(pl.Utf8),
        pl.col("day_type").cast(pl.Int64),
        pl.col("trip_type").cast(pl.Utf8),
        pl.col("route_id").cast(pl.Utf8),
        pl.col("period").cast(pl.Utf8),
        pl.col("number_of_trips_passing_wait").cast(pl.Float64),
        pl.col("number_of_scheduled_trips").cast(pl.Float64),
        pl.col("wait_assessment").cast(pl.Float64)
    ])

    return df

@asset(
    io_manager_key="mta_bus_wait_time_polars_parquet_io_manager",
    compute_kind="Polars"
)
def mta_bus_wait_time(context):
    config = MTAWaitTimeBusConfig()
    api_client = SocrataAPI(config)
    offset = config.offset
    batch_number = 1

    while True:
        context.log.info(f"Fetching bus wait time data with offset: {offset}")
        updated_config = config.copy(update={"offset": offset})
        api_client.config = updated_config
        data = api_client.fetch_data()

        if not data:
            context.log.info(f"No more bus wait time data to fetch at offset {offset}.")
            context.log.info("All data has been fetched successfully.")
            return

        raw_df = pl.DataFrame(data)
        processed_df = process_mta_bus_wait_time_df(raw_df)

        if not processed_df.is_empty():
            io_manager = get_io_manager(context)
            io_manager.handle_output(context, processed_df, batch_number)

        del raw_df, processed_df, data
        gc.collect()

        offset += config.limit
        batch_number += 1


class MTAOperationsStatementConfig(SocrataAPIConfig):
    SOCRATA_ENDPOINT: str = "https://data.ny.gov/resource/yg77-3tkj.json"
    order: str = "Month ASC"
    limit: int = 500000
    offset: int = 0

import polars as pl

def process_mta_operations_statement_df(df: pl.DataFrame) -> pl.DataFrame:
    # Log the dataframe columns to verify the correct columns exist
    print(f"Columns before processing: {df.columns}")

    # Ensure column names are lower-cased, spaces/special characters replaced, and "month" renamed to "timestamp"
    df = df.rename({
        col: col.lower().replace(" ", "_").replace("-", "_")
        for col in df.columns
    }).rename({"month": "timestamp"})

    # Log the dataframe after renaming to confirm changes
    print(f"Columns after renaming: {df.columns}")

    # Handle the timestamp parsing for the "timestamp" column
    df = df.with_columns([
        pl.col("timestamp").str.strptime(pl.Date, format="%Y-%m-%dT%H:%M:%S.%f", strict=False).alias("timestamp")
    ])

    # Log after parsing to verify correctness
    print(f"Processed timestamp column: {df.select('timestamp').head()}")

    # Define the SQL query to transform the "agency" column using CASE and alias it
    query = """
        SELECT *,
            CASE 
                WHEN agency = 'LIRR' THEN 'Long Island Rail Road'
                WHEN agency = 'BT' THEN 'Bridges and Tunnels'
                WHEN agency = 'FMTAC' THEN 'First Mutual Transportation Assurance Company'
                WHEN agency = 'NYCT' THEN 'New York City Transit'
                WHEN agency = 'SIR' THEN 'Staten Island Railway'
                WHEN agency = 'MTABC' THEN 'MTA Bus Company'
                WHEN agency = 'GCMCOC' THEN 'Grand Central Madison Concourse Operating Company'
                WHEN agency = 'MNR' THEN 'Metro-North Railroad'
                WHEN agency = 'MTAHQ' THEN 'Metropolitan Transportation Authority Headquarters'
                WHEN agency = 'CD' THEN 'MTA Construction and Development'
                WHEN agency = 'CRR' THEN 'Connecticut Railroads'
                ELSE 'Unknown Agency'
            END AS agency_full_name  -- Use an alias to avoid duplicate column names
        FROM self
    """

    # Execute the SQL query
    df = df.sql(query)

    # Log the dataframe after altering the agency column using SQL
    print(f"Agency full name column added using SQL: {df.select('agency', 'agency_full_name').head()}")

    # Cast columns to appropriate types
    df = df.with_columns([
        pl.col("fiscal_year").cast(pl.Int64),
        pl.col("financial_plan_year").cast(pl.Int64),
        pl.col("amount").cast(pl.Float64),
        pl.col("scenario").cast(pl.Utf8),
        pl.col("expense_type").cast(pl.Utf8),
        pl.col("agency").cast(pl.Utf8),  # Keep the original agency column
        pl.col("agency_full_name").cast(pl.Utf8),  # The new full name column
        pl.col("type").cast(pl.Utf8),
        pl.col("subtype").cast(pl.Utf8),
        pl.col("general_ledger").cast(pl.Utf8)
    ])

    return df



@asset(
    io_manager_key="mta_operations_statement_polars_parquet_io_manager",
    compute_kind="Polars"
)
def mta_operations_statement(context):
    config = MTAOperationsStatementConfig()
    api_client = SocrataAPI(config)
    offset = config.offset
    batch_number = 1

    while True:
        context.log.info(f"Fetching operations statement data with offset: {offset}")
        updated_config = config.copy(update={"offset": offset})
        api_client.config = updated_config
        data = api_client.fetch_data()

        if not data:
            context.log.info(f"No more operations statement data to fetch at offset {offset}.")
            context.log.info("All data has been fetched successfully.")
            return

        raw_df = pl.DataFrame(data)
        processed_df = process_mta_operations_statement_df(raw_df)

        if not processed_df.is_empty():
            io_manager = get_io_manager(context)
            io_manager.handle_output(context, processed_df, batch_number)

        del raw_df, processed_df, data
        gc.collect()

        offset += config.limit
        batch_number += 1


