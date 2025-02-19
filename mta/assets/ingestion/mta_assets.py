# mta/assets/ingestion/mta_subway/mta_assets.py

import os
import gc
import requests
import polars as pl
from datetime import datetime

# Replace MaterializeResult with Output
from dagster import asset, Output

from mta.constants import HOURLY_PATH
from mta.resources.socrata_resource import SocrataResource


def process_mta_daily_df(df: pl.DataFrame) -> (pl.DataFrame, list, list, str):
    orig_cols = df.columns
    df = df.rename({col: col.lower().replace(" ", "_") for col in df.columns})
    renamed_cols = df.columns

    # parse 'date' column
    date_sample = "N/A"
    if "date" in df.columns:
        df = df.with_columns(
            pl.col("date").str.strptime(pl.Date, format="%Y-%m-%dT%H:%M:%S.%f", strict=False).alias("date")
        )
        date_sample_df = df.select("date").head(3).to_dicts()
        date_sample = str(date_sample_df)

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

    return df, orig_cols, renamed_cols, date_sample

@asset(
    name="mta_daily_ridership",
    compute_kind="Polars",
    group_name="MTA",
    tags={"domain": "mta", "type": "ingestion", "source": "socrata"},
)
def mta_daily_ridership(context, socrata: SocrataResource):
    endpoint = "https://data.ny.gov/resource/vxuj-8kew.json"
    limit = 500000
    offset = 0

    frames = []
    last_orig_cols = []
    last_renamed_cols = []
    date_sample = "N/A"

    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "Date ASC",
            "$where": "Date >= '2020-03-01T00:00:00'",
        }
        data = socrata.fetch_data(endpoint, params)
        if not data:
            context.log.info(f"[mta_daily] No more data at offset {offset}")
            break

        df = pl.DataFrame(data)
        processed_df, orig_cols, renamed_cols, date_sample = process_mta_daily_df(df)
        frames.append(processed_df)

        last_orig_cols = orig_cols
        last_renamed_cols = renamed_cols

        offset += limit
        del df, processed_df, data
        gc.collect()

    final_df = pl.concat(frames, how="vertical") if frames else pl.DataFrame([])

    return Output(
        value=final_df,
        metadata={
            "dagster/row_count": final_df.shape[0],
            "original_columns": str(last_orig_cols),
            "renamed_columns": str(last_renamed_cols),
            "date_col_sample": date_sample,
        },
    )

def process_mta_bus_speeds_df(df: pl.DataFrame) -> (pl.DataFrame, list, list):
    orig_cols = df.columns
    df = df.rename({col: col.lower().replace(" ", "_").replace("-", "_") for col in df.columns})
    renamed_cols = df.columns

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

    return df, orig_cols, renamed_cols

@asset(
    name="mta_bus_speeds",
    compute_kind="Polars",
    group_name="MTA",
    tags={"domain": "mta", "type": "ingestion", "source": "socrata"},
)
def mta_bus_speeds(context, socrata: SocrataResource):
    endpoint = "https://data.ny.gov/resource/6ksi-7cxr.json"
    limit = 500000
    offset = 0

    combined = []
    last_orig_cols = []
    last_renamed_cols = []

    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "month ASC",
            "$where": "month >= '2020-01-01T00:00:00'",
        }
        data = socrata.fetch_data(endpoint, params)
        if not data:
            context.log.info(f"[mta_bus_speeds] no more data at offset {offset}")
            break

        df = pl.DataFrame(data)
        processed_df, orig_cols, renamed_cols = process_mta_bus_speeds_df(df)
        combined.append(processed_df)

        last_orig_cols = orig_cols
        last_renamed_cols = renamed_cols

        offset += limit
        del df, processed_df, data
        gc.collect()

    final_df = pl.concat(combined, how="vertical") if combined else pl.DataFrame([])
    return Output(
        value=final_df,
        metadata={
            "dagster/row_count": final_df.shape[0],
            "original_columns": str(last_orig_cols),
            "renamed_columns": str(last_renamed_cols),
        },
    )

def process_mta_bus_wait_time_df(df: pl.DataFrame) -> (pl.DataFrame, list, list):
    orig_cols = df.columns
    df = df.rename({col: col.lower().replace(" ", "_").replace("-", "_") for col in df.columns})
    renamed_cols = df.columns

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

    return df, orig_cols, renamed_cols

@asset(
    name="mta_bus_wait_time",
    compute_kind="Polars",
    group_name="MTA",
    tags={"domain": "mta", "type": "ingestion", "source": "socrata"},
)
def mta_bus_wait_time(context, socrata: SocrataResource):
    endpoint = "https://data.ny.gov/resource/swky-c3v4.json"
    limit = 500000
    offset = 0

    frames = []
    last_orig_cols = []
    last_renamed_cols = []

    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "month ASC",
            "$where": "month >= '2020-01-01T00:00:00'"
        }
        data = socrata.fetch_data(endpoint, params)
        if not data:
            context.log.info(f"[mta_bus_wait_time] no more data at offset {offset}")
            break

        df = pl.DataFrame(data)
        processed_df, orig_cols, renamed_cols = process_mta_bus_wait_time_df(df)
        frames.append(processed_df)

        last_orig_cols = orig_cols
        last_renamed_cols = renamed_cols

        offset += limit
        del df, processed_df, data
        gc.collect()

    final_df = pl.concat(frames, how="vertical") if frames else pl.DataFrame([])
    return Output(
        value=final_df,
        metadata={
            "dagster/row_count": final_df.shape[0],
            "original_columns": str(last_orig_cols),
            "renamed_columns": str(last_renamed_cols),
        },
    )

def process_mta_operations_statement_df(df: pl.DataFrame) -> (pl.DataFrame, list, list):
    orig_cols = df.columns
    df = df.rename(
        {col: col.lower().replace(" ", "_").replace("-", "_") for col in df.columns}
    ).rename({"month": "timestamp"})
    renamed_cols = df.columns

    if "timestamp" in df.columns:
        df = df.with_columns([
            pl.col("timestamp").str.strptime(pl.Date, format="%Y-%m-%dT%H:%M:%S.%f", strict=False).alias("timestamp")
        ])

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

    return df, orig_cols, renamed_cols

@asset(
    name="mta_operations_statement",
    compute_kind="Polars",
    group_name="MTA",
    tags={"domain": "mta", "type": "ingestion", "source": "socrata"},
)
def mta_operations_statement(context, socrata: SocrataResource):
    endpoint = "https://data.ny.gov/resource/yg77-3tkj.json"
    limit = 500000
    offset = 0

    frames = []
    last_orig_cols = []
    last_renamed_cols = []

    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "Month ASC"
        }
        data = socrata.fetch_data(endpoint, params)
        if not data:
            context.log.info(f"[mta_ops_statement] no more data at offset {offset}")
            break

        df = pl.DataFrame(data)
        processed_df, orig_cols, renamed_cols = process_mta_operations_statement_df(df)
        frames.append(processed_df)

        last_orig_cols = orig_cols
        last_renamed_cols = renamed_cols

        offset += limit
        del df, processed_df, data
        gc.collect()

    final_df = pl.concat(frames, how="vertical") if frames else pl.DataFrame([])
    return Output(
        value=final_df,
        metadata={
            "dagster/row_count": final_df.shape[0],
            "original_columns": str(last_orig_cols),
            "renamed_columns": str(last_renamed_cols),
        },
    )



BASE_URL = "https://fastopendata.org/mta/raw/hourly_subway/"
LOCAL_DOWNLOAD_PATH = HOURLY_PATH
years = ["2022", "2023", "2024"]
months = [f"{i:02d}" for i in range(1, 13)]  # 01..12

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
    group_name="MTA",
    io_manager_key="hourly_mta_io_manager",  # <--- Overrides the default io_manager for its own custom one here
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

    # Return Output with metadata
    return Output(
        value=downloaded_files,
        metadata={
            "dagster/row_count": len(downloaded_files),
            "downloaded_file_paths": str(downloaded_files),
        },
    )