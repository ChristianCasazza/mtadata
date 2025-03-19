# pipeline/assets/ingestion/mta_assets.py

import os
import gc
import requests
import polars as pl
from datetime import datetime

# Replace MaterializeResult with Output
from dagster import asset, Output

from pipeline.constants import HOURLY_PATH
from pipeline.resources.socrata_resource import SocrataResource
from .processing.mta_processing import *

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

@asset(
    name="mta_subway_hourly_ridership",
    io_manager_key="fastopendata_partitioned_parquet_io_manager",
    group_name="MTA",
    tags={"domain": "mta", "type": "ingestion", "source": "fastopendata"}
)
def mta_subway_hourly_ridership():
    """
    Asset that wants data from March 2022 to December 2024, for example.
    Instead of returning a Polars DataFrame, we return a dict with the
    start/end that the IO manager can use to fetch data from R2.
    """
    return {
        "start_year": 2022,
        "start_month": 3,
        "end_year": 2024,
        "end_month": 12
    }

