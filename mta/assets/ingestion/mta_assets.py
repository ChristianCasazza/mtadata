# mta/assets/ingestion/mta_subway/mta_assets.py
import os
import gc
import requests
import polars as pl
from datetime import datetime
from dagster import asset
from mta.constants import HOURLY_PATH 
from mta.resources.socrata_resource import SocrataResource

def get_io_manager(context):
    """Helper function to dynamically fetch the correct IO manager for the asset."""
    asset_name = context.asset_key.path[-1]  # Get the last part of the asset key, which is the asset name
    io_manager_key = f"{asset_name}_polars_parquet_io_manager"  # Construct the io_manager_key
    return getattr(context.resources, io_manager_key)


MTA_ASSETS_NAMES = [
    "mta_daily_ridership",
    "mta_bus_speeds",
    "mta_bus_wait_time",
    "mta_operations_statement",
    "sf_air_traffic_cargo",
    "sf_air_traffic_passenger_stats",  
    "sf_air_traffic_landings",       
]

OTHER_MTA_ASSETS_NAMES = [
    "mta_hourly_subway_socrata"
]


BASE_URL = "https://fastopendata.org/mta/raw/hourly_subway/"
LOCAL_DOWNLOAD_PATH = HOURLY_PATH
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
    compute_kind="Python",
    io_manager_key="hourly_mta_io_manager",  # <--- override here
)
def mta_hourly_subway_socrata(context):
    downloaded_files = []
    for year in years:
        for month in months:
            context.log.info(f"Downloading data for year: {year}, month: {month}")
            try:
                file_path = download_files_from_year_month(
                    BASE_URL, LOCAL_DOWNLOAD_PATH, year, month
                )
                context.log.info(f"Downloaded file to: {file_path}")
                downloaded_files.append(file_path)
            except Exception as e:
                context.log.error(f"Error downloading file for {year}-{month}: {e}")
    # Return the list of local file paths
    return downloaded_files



# ------------------------------------------------------------------
# 2) MTA DAILY RIDERSHIP - Socrata-based Ingestion
# ------------------------------------------------------------------
# Original transformation logic is in process_mta_daily_df

def process_mta_daily_df(df: pl.DataFrame) -> pl.DataFrame:
    # Log columns
    print(f"[mta_daily] Columns before processing: {df.columns}")

    # rename columns to lower_case_with_underscores
    df = df.rename({col: col.lower().replace(" ", "_") for col in df.columns})

    print(f"[mta_daily] Columns after renaming: {df.columns}")

    # parse 'date' column
    if "date" in df.columns:
        df = df.with_columns(
            pl.col("date").str.strptime(pl.Date, format="%Y-%m-%dT%H:%M:%S.%f", strict=False).alias("date")
        )
    # log
    if "date" in df.columns:
        print(f"[mta_daily] Processed date column sample: {df.select('date').head()}")

    # cast columns
    old_new_cols = [
        ("subways_total_estimated_ridership", "subways_total_ridership"),
        ("subways_of_comparable_pre_pandemic_day", "subways_pct_pre_pandemic"),
        ("buses_total_estimated_ridersip", "buses_total_ridership"),
        ("buses_of_comparable_pre_pandemic_day", "buses_pct_pre_pandemic"),
        ("lirr_total_estimated_ridership", "lirr_total_ridership"),
        ("lirr_of_comparable_pre_pandemic_day", "lirr_pct_pre_pandemic"),
        ("metro_north_total_estimated_ridership", "metro_north_total_ridership"),
        ("metro_north_of_comparable_pre_pandemic_day", "metro_north_pct_pre_pandemic"),
        ("access_a_ride_total_scheduled_trips", "access_a_ride_total_trips"),
        ("access_a_ride_of_comparable_pre_pandemic_day", "access_a_ride_pct_pre_pandemic"),
        ("bridges_and_tunnels_total_traffic", "bridges_tunnels_total_traffic"),
        ("bridges_and_tunnels_of_comparable_pre_pandemic_day", "bridges_tunnels_pct_pre_pandemic"),
        ("staten_island_railway_total_estimated_ridership", "staten_island_railway_total_ridership"),
        ("staten_island_railway_of_comparable_pre_pandemic_day", "staten_island_railway_pct_pre_pandemic"),
    ]

    exprs = []
    drop_these = []
    for old_col, new_col in old_new_cols:
        if old_col in df.columns:
            exprs.append(pl.col(old_col).cast(pl.Float64).alias(new_col))
            drop_these.append(old_col)

    if exprs:
        df = df.with_columns(exprs).drop(drop_these)

    return df

@asset(
    name="mta_daily_ridership",
    compute_kind="Polars",
)
def mta_daily_ridership(socrata: SocrataResource) -> pl.DataFrame:
    """
    Fetch daily ridership from Socrata in multiple pages, apply process_mta_daily_df
    transformations, return final Polars DataFrame.
    """
    endpoint = "https://data.ny.gov/resource/vxuj-8kew.json"
    limit = 500000
    offset = 0

    frames = []
    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "Date ASC",
            "$where": "Date >= '2020-03-01T00:00:00'",
        }
        data = socrata.fetch_data(endpoint, params)
        if not data:
            print("[mta_daily] No more data at offset", offset)
            break

        df = pl.DataFrame(data)
        processed_df = process_mta_daily_df(df)
        frames.append(processed_df)

        offset += limit
        del df, processed_df, data
        gc.collect()

    if frames:
        return pl.concat(frames, how="vertical")
    else:
        return pl.DataFrame([])


# ------------------------------------------------------------------
# 3) MTA BUS SPEEDS
# ------------------------------------------------------------------
def process_mta_bus_speeds_df(df: pl.DataFrame) -> pl.DataFrame:
    print(f"[mta_bus_speeds] Columns before: {df.columns}")
    df = df.rename({col: col.lower().replace(" ", "_").replace("-", "_") for col in df.columns})
    print(f"[mta_bus_speeds] Columns after rename: {df.columns}")

    # parse 'month' as a Date
    if "month" in df.columns:
        df = df.with_columns(
            pl.col("month").str.strptime(pl.Date, format="%Y-%m-%dT%H:%M:%S.%f", strict=False).alias("month")
        )
    # cast
    casts = [
        ("borough", pl.Utf8),
        ("day_type", pl.Int64),
        ("trip_type", pl.Utf8),
        ("route_id", pl.Utf8),
        ("period", pl.Utf8),
        ("total_mileage", pl.Float64),
        ("total_operating_time", pl.Float64),
        ("average_speed", pl.Float64),
    ]
    exprs = []
    for col_name, dtype in casts:
        if col_name in df.columns:
            exprs.append(pl.col(col_name).cast(dtype))
    if exprs:
        df = df.with_columns(exprs)

    return df

@asset(name="mta_bus_speeds", compute_kind="Polars")
def mta_bus_speeds(socrata: SocrataResource) -> pl.DataFrame:
    endpoint = "https://data.ny.gov/resource/6ksi-7cxr.json"
    limit = 500000
    offset = 0

    combined = []
    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "month ASC",
            "$where": "month >= '2020-01-01T00:00:00'",
        }
        data = socrata.fetch_data(endpoint, params)
        if not data:
            print("[mta_bus_speeds] no more data at offset", offset)
            break

        df = pl.DataFrame(data)
        processed_df = process_mta_bus_speeds_df(df)
        combined.append(processed_df)

        offset += limit
        del df, processed_df, data
        gc.collect()

    if combined:
        return pl.concat(combined, how="vertical")
    return pl.DataFrame([])


# ------------------------------------------------------------------
# 4) MTA BUS WAIT TIME
# ------------------------------------------------------------------
def process_mta_bus_wait_time_df(df: pl.DataFrame) -> pl.DataFrame:
    print(f"[mta_bus_wait_time] columns before: {df.columns}")
    df = df.rename({col: col.lower().replace(" ", "_").replace("-", "_") for col in df.columns})
    print(f"[mta_bus_wait_time] columns after rename: {df.columns}")

    if "month" in df.columns:
        df = df.with_columns(
            pl.col("month").str.strptime(pl.Date, format="%Y-%m-%dT%H:%M:%S.%f", strict=False).alias("month")
        )

    casts = [
        ("borough", pl.Utf8),
        ("day_type", pl.Int64),
        ("trip_type", pl.Utf8),
        ("route_id", pl.Utf8),
        ("period", pl.Utf8),
        ("number_of_trips_passing_wait", pl.Float64),
        ("number_of_scheduled_trips", pl.Float64),
        ("wait_assessment", pl.Float64),
    ]
    exprs = []
    for col_name, dtype in casts:
        if col_name in df.columns:
            exprs.append(pl.col(col_name).cast(dtype))
    if exprs:
        df = df.with_columns(exprs)

    return df

@asset(name="mta_bus_wait_time", compute_kind="Polars")
def mta_bus_wait_time(socrata: SocrataResource) -> pl.DataFrame:
    endpoint = "https://data.ny.gov/resource/swky-c3v4.json"
    limit = 500000
    offset = 0

    frames = []
    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "month ASC",
            "$where": "month >= '2020-01-01T00:00:00'"
        }
        data = socrata.fetch_data(endpoint, params)
        if not data:
            print("[mta_bus_wait_time] no more data at offset", offset)
            break

        df = pl.DataFrame(data)
        processed_df = process_mta_bus_wait_time_df(df)
        frames.append(processed_df)

        offset += limit
        del df, processed_df, data
        gc.collect()

    if frames:
        return pl.concat(frames, how="vertical")
    return pl.DataFrame([])


# ------------------------------------------------------------------
# 5) MTA OPERATIONS STATEMENT
# ------------------------------------------------------------------
def process_mta_operations_statement_df(df: pl.DataFrame) -> pl.DataFrame:
    print(f"[mta_ops_statement] columns before: {df.columns}")
    df = df.rename(
        {col: col.lower().replace(" ", "_").replace("-", "_") for col in df.columns}
    ).rename({"month": "timestamp"})

    print(f"[mta_ops_statement] columns after rename: {df.columns}")

    if "timestamp" in df.columns:
        df = df.with_columns([
            pl.col("timestamp").str.strptime(pl.Date, format="%Y-%m-%dT%H:%M:%S.%f", strict=False).alias("timestamp")
        ])

    # Use df.sql(...) to transform 'agency'
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
        END AS agency_full_name
    FROM self
    """
    df = df.sql(query)

    casts = [
        ("fiscal_year", pl.Int64),
        ("financial_plan_year", pl.Int64),
        ("amount", pl.Float64),
        ("scenario", pl.Utf8),
        ("expense_type", pl.Utf8),
        ("agency", pl.Utf8),
        ("agency_full_name", pl.Utf8),
        ("type", pl.Utf8),
        ("subtype", pl.Utf8),
        ("general_ledger", pl.Utf8),
    ]
    exprs = []
    for col_name, dtype in casts:
        if col_name in df.columns:
            exprs.append(pl.col(col_name).cast(dtype))

    if exprs:
        df = df.with_columns(exprs)

    return df

@asset(name="mta_operations_statement", compute_kind="Polars")
def mta_operations_statement(socrata: SocrataResource) -> pl.DataFrame:
    endpoint = "https://data.ny.gov/resource/yg77-3tkj.json"
    limit = 500000
    offset = 0

    frames = []
    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "Month ASC"
        }
        data = socrata.fetch_data(endpoint, params)
        if not data:
            print("[mta_ops_statement] no more data at offset", offset)
            break

        df = pl.DataFrame(data)
        processed = process_mta_operations_statement_df(df)
        frames.append(processed)

        offset += limit
        del df, processed, data
        gc.collect()

    if frames:
        return pl.concat(frames, how="vertical")
    return pl.DataFrame([])


