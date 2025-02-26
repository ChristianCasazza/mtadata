# mta/assets/ingestion/mta_subway/mta_assets.py

import os
import gc
import requests
import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dagster import asset, OpExecutionContext
from mta.constants import HOURLY_PATH, BASE_PATH
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


##########################################################
# 1) MTA Subway Hourly Ridership
#    - Data Processing
##########################################################

def process_mta_subway_hourly_ridership_df(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cast columns to their appropriate types for MTA Subway Hourly Ridership.
    Handles cases where 'transfers' and 'ridership' have decimal strings.
    """

    cast_map = {
        "transit_timestamp": pl.Datetime,
        "transit_mode": pl.Utf8,
        "station_complex_id": pl.Utf8,
        "station_complex": pl.Utf8,
        "borough": pl.Utf8,
        "payment_method": pl.Utf8,
        "fare_class_category": pl.Utf8,
        "ridership": pl.Float64,     # Handles cases like "2.0"
        "transfers": pl.Float64,     # Handles cases like "0.0" -> Float64
        "latitude": pl.Float64,
        "longitude": pl.Float64,
    }

    exprs = []
    for col_name, dtype in cast_map.items():
        if col_name in df.columns:
            if dtype == pl.Datetime:
                exprs.append(pl.col(col_name).str.strptime(pl.Datetime, strict=False))
            else:
                exprs.append(pl.col(col_name).cast(dtype))

    if exprs:
        df = df.with_columns(exprs)

    return df


@asset(
    name="mta_subway_hourly_ridership",
    group_name="MTA",
    io_manager_key="large_socrata_io_manager",  # Our new manager
    required_resource_keys={"socrata", "large_socrata_io_manager"},
)
def mta_subway_hourly_ridership(context: OpExecutionContext):
    """
    Fetch MTA Subway hourly ridership (March 2022 -> Jan 2025) in 500K-row chunks,
    writing each chunk to Parquet immediately.
    """
    endpoint = "https://data.ny.gov/resource/wujg-7c2s.geojson"
    start_date = datetime(2022, 3, 1)
    end_date = datetime(2025, 1, 1)  # Example date range
    asset_name = "mta_subway_hourly_ridership"
    manager = context.resources.large_socrata_io_manager

    current = start_date
    total_chunks = 0

    while current < end_date:
        next_month = current + relativedelta(months=1)
        year = current.year
        month = current.month
        offset = 0
        batch_num = 1

        while True:
            where_clause = (
                f"transit_timestamp >= '{current:%Y-%m-%dT00:00:00}' "
                f"AND transit_timestamp < '{next_month:%Y-%m-%dT00:00:00}'"
            )
            query_params = {
                "$limit": 500000,
                "$offset": offset,
                "$order": "transit_timestamp ASC",
                "$where": where_clause,
            }

            records = context.resources.socrata.fetch_data(endpoint, query_params)
            if not records:
                context.log.info(
                    f"No more data for {asset_name} {year}-{month:02d} offset={offset}."
                )
                break

            df = pl.DataFrame(records)
            # Process columns
            df = process_mta_subway_hourly_ridership_df(df)

            context.log.info(
                f"Fetched {df.shape[0]} rows => Writing chunk {batch_num}, offset={offset}"
            )

            manager.write_chunk(asset_name, year, month, batch_num, df)
            del df  # free memory

            offset += 500000
            batch_num += 1
            total_chunks += 1

        current = next_month

    context.log.info(f"Done. Wrote {total_chunks} total chunks for {asset_name}.")
    return f"Wrote {total_chunks} chunks"


##########################################################
# 2) MTA Subway Origin-Destination 2023
#    - Data Processing
##########################################################

def process_mta_subway_origin_destination_2023_df(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cast columns to their appropriate types for MTA Subway Origin-Destination 2023.
    Columns (from data dictionary):
      - year: Number -> Int64
      - month: Number -> Int64
      - day_of_week: Text -> Utf8
      - hour_of_day: Number -> Int64
      - timestamp: Floating Timestamp -> pl.Datetime
      - origin_station_complex_id: Number -> Int64
      - origin_station_complex_name: Text -> Utf8
      - origin_latitude: Number -> Float64
      - origin_longitude: Number -> Float64
      - destination_station_complex_id: Number -> Int64
      - destination_station_complex_name: Text -> Utf8
      - destination_latitude: Number -> Float64
      - destination_longitude: Number -> Float64
      - estimated_average_ridership: Number -> Float64
    """
    cast_map = {
        "year": pl.Int64,
        "month": pl.Int64,
        "day_of_week": pl.Utf8,
        "hour_of_day": pl.Int64,
        "timestamp": pl.Datetime,
        "origin_station_complex_id": pl.Int64,
        "origin_station_complex_name": pl.Utf8,
        "origin_latitude": pl.Float64,
        "origin_longitude": pl.Float64,
        "destination_station_complex_id": pl.Int64,
        "destination_station_complex_name": pl.Utf8,
        "destination_latitude": pl.Float64,
        "destination_longitude": pl.Float64,
        "estimated_average_ridership": pl.Float64,
    }

    exprs = []
    for col_name, dtype in cast_map.items():
        if col_name in df.columns:
            if dtype == pl.Datetime:
                exprs.append(pl.col(col_name).str.strptime(pl.Datetime, strict=False))
            else:
                exprs.append(pl.col(col_name).cast(dtype))

    if exprs:
        df = df.with_columns(exprs)

    return df

@asset(
    name="mta_subway_origin_destination_2023",
    group_name="MTA",
    io_manager_key="large_socrata_io_manager",
    required_resource_keys={"socrata", "large_socrata_io_manager"},
)
def mta_subway_origin_destination_2023(context: OpExecutionContext):
    """
    Fetch 2023 MTA Subway origin-destination data in 500K-row chunks, writing immediately.
    Uses 'Timestamp' column for monthly partitioning: Jan 2023 -> Jan 2024
    """
    endpoint = "https://data.ny.gov/resource/uhf3-t34z.json"
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 1, 1)  
    asset_name = "mta_subway_origin_destination_2023"
    manager = context.resources.large_socrata_io_manager

    current = start_date
    total_chunks = 0

    while current < end_date:
        next_month = current + relativedelta(months=1)
        year = current.year
        month = current.month
        offset = 0
        batch_num = 1

        while True:
            where_clause = (
                f"Timestamp >= '{current:%Y-%m-%dT00:00:00}' "
                f"AND Timestamp < '{next_month:%Y-%m-%dT00:00:00}'"
            )
            query_params = {
                "$limit": 500000,
                "$offset": offset,
                "$order": "Timestamp ASC",
                "$where": where_clause,
            }

            records = context.resources.socrata.fetch_data(endpoint, query_params)
            if not records:
                context.log.info(
                    f"No more data for {asset_name} {year}-{month:02d} offset={offset}."
                )
                break

            df = pl.DataFrame(records)
            # Process columns for OD 2023
            df = process_mta_subway_origin_destination_2023_df(df)

            context.log.info(
                f"Fetched {df.shape[0]} rows => Writing chunk {batch_num}, offset={offset}"
            )

            manager.write_chunk(asset_name, year, month, batch_num, df)
            del df

            offset += 500000
            batch_num += 1
            total_chunks += 1

        current = next_month

    context.log.info(f"Done. Wrote {total_chunks} total chunks for {asset_name}.")
    return f"Wrote {total_chunks} chunks"


##########################################################
# 3) MTA Subway Origin-Destination 2024
#    - Data Processing
##########################################################

def process_mta_subway_origin_destination_2024_df(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cast columns to their appropriate types for MTA Subway Origin-Destination 2024.
    Identical to 2023 in terms of columns, so we can reuse the same schema.
    """
    cast_map = {
        "year": pl.Int64,
        "month": pl.Int64,
        "day_of_week": pl.Utf8,
        "hour_of_day": pl.Int64,
        "timestamp": pl.Datetime,
        "origin_station_complex_id": pl.Int64,
        "origin_station_complex_name": pl.Utf8,
        "origin_latitude": pl.Float64,
        "origin_longitude": pl.Float64,
        "destination_station_complex_id": pl.Int64,
        "destination_station_complex_name": pl.Utf8,
        "destination_latitude": pl.Float64,
        "destination_longitude": pl.Float64,
        "estimated_average_ridership": pl.Float64,
    }

    exprs = []
    for col_name, dtype in cast_map.items():
        if col_name in df.columns:
            if dtype == pl.Datetime:
                exprs.append(pl.col(col_name).str.strptime(pl.Datetime, strict=False))
            else:
                exprs.append(pl.col(col_name).cast(dtype))

    if exprs:
        df = df.with_columns(exprs)

    return df

@asset(
    name="mta_subway_origin_destination_2024",
    group_name="MTA",
    io_manager_key="large_socrata_io_manager",
    required_resource_keys={"socrata", "large_socrata_io_manager"},
)
def mta_subway_origin_destination_2024(context: OpExecutionContext):
    """
    Fetch 2024 MTA Subway origin-destination data in 500K-row chunks, writing immediately.
    Uses 'Timestamp' column for monthly partitioning: Jan 2024 -> Jan 2025
    """
    endpoint = "https://data.ny.gov/resource/jsu2-fbtj.json"
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2025, 1, 1)  
    asset_name = "mta_subway_origin_destination_2024"
    manager = context.resources.large_socrata_io_manager

    current = start_date
    total_chunks = 0

    while current < end_date:
        next_month = current + relativedelta(months=1)
        year = current.year
        month = current.month
        offset = 0
        batch_num = 1

        while True:
            where_clause = (
                f"Timestamp >= '{current:%Y-%m-%dT00:00:00}' "
                f"AND Timestamp < '{next_month:%Y-%m-%dT00:00:00}'"
            )
            query_params = {
                "$limit": 500000,
                "$offset": offset,
                "$order": "Timestamp ASC",
                "$where": where_clause,
            }

            records = context.resources.socrata.fetch_data(endpoint, query_params)
            if not records:
                context.log.info(
                    f"No more data for {asset_name} {year}-{month:02d} offset={offset}."
                )
                break

            df = pl.DataFrame(records)
            # Process columns for OD 2024
            df = process_mta_subway_origin_destination_2024_df(df)

            context.log.info(
                f"Fetched {df.shape[0]} rows => Writing chunk {batch_num}, offset={offset}"
            )

            manager.write_chunk(asset_name, year, month, batch_num, df)
            del df

            offset += 500000
            batch_num += 1
            total_chunks += 1

        current = next_month

    context.log.info(f"Done. Wrote {total_chunks} total chunks for {asset_name}.")
    return f"Wrote {total_chunks} chunks"