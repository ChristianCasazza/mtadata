# File: mta/assets/ingestion/crime_assets.py

import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dagster import asset, OpExecutionContext
from mta.resources.socrata_resource import SocrataResource

def process_crime_df(df: pl.DataFrame) -> pl.DataFrame:
    """
    Processes the crime DataFrame:
    - Casts columns to correct types
    - Adds a 'boro' column based on 'arrest_boro' using Polars SQL.
    """

    cast_map = {
        "arrest_key": pl.Utf8,
        "arrest_date": pl.Datetime,
        "pd_cd": pl.Int64,
        "pd_desc": pl.Utf8,
        "ky_cd": pl.Int64,
        "ofns_desc": pl.Utf8,
        "law_code": pl.Utf8,
        "law_cat_cd": pl.Utf8,
        "arrest_boro": pl.Utf8,
        "arrest_precinct": pl.Int64,
        "jurisdiction_code": pl.Int64,
        "age_group": pl.Utf8,
        "perp_sex": pl.Utf8,
        "perp_race": pl.Utf8,
        "x_coord_cd": pl.Utf8,
        "y_coord_cd": pl.Utf8,
        "latitude": pl.Float64,
        "longitude": pl.Float64,
    }

    # Cast columns
    cast_expressions = [
        pl.col(col).cast(dtype) for col, dtype in cast_map.items() if col in df.columns
    ]
    if cast_expressions:
        df = df.with_columns(cast_expressions)

    # Add 'boro' column using Polars SQL
    boro_query = """
    SELECT *,
        CASE arrest_boro
            WHEN 'B' THEN 'Bronx'
            WHEN 'S' THEN 'Staten Island'
            WHEN 'K' THEN 'Brooklyn'
            WHEN 'M' THEN 'Manhattan'
            WHEN 'Q' THEN 'Queens'
            ELSE 'Unknown'
        END AS boro
    FROM self
    """
    df = df.sql(boro_query)
    return df


@asset(
    name="crime_nypd_arrests",
    group_name="NYC_Crime",
    io_manager_key="large_socrata_io_manager",
    required_resource_keys={"socrata", "large_socrata_io_manager"},
)
def crime_nypd_arrests(context: OpExecutionContext):
    """
    Fetches NYC arrests (Jan 2013 -> Mar 2013) in monthly 500K-row chunks.
    - Processes data with proper casting and a new 'boro' column.
    - Immediately writes each chunk using the large_socrata_io_manager.
    """

    endpoint = "https://data.cityofnewyork.us/resource/8h9b-rp9u.geojson"
    start_date = datetime(2013, 1, 1)
    end_date = datetime(2013, 3, 31)
    asset_name = "crime_nypd_arrests"
    manager = context.resources.large_socrata_io_manager

    current = start_date
    total_chunks = 0

    while current <= end_date:
        next_month = current + relativedelta(months=1)
        year, month = current.year, current.month
        offset, batch_num = 0, 1

        context.log.info(f"🚀 Fetching data for {year}-{month:02d}...")

        while True:
            where_clause = (
                f"arrest_date >= '{current:%Y-%m-%dT00:00:00}' "
                f"AND arrest_date < '{next_month:%Y-%m-%dT00:00:00}'"
            )
            query_params = {
                "$limit": 500000,
                "$offset": offset,
                "$order": "arrest_date ASC",
                "$where": where_clause,
            }

            records = context.resources.socrata.fetch_data(endpoint, query_params)
            if not records:
                context.log.info(f"✅ No more data for {year}-{month:02d} offset={offset}.")
                break

            df = pl.DataFrame(records)
            processed_df = process_crime_df(df)

            # Write the chunk using the large_socrata_io_manager
            try:
                manager.write_chunk(asset_name, year, month, batch_num, processed_df)
                context.log.info(f"💾 Saved batch {batch_num} for {year}-{month:02d}.")
            except Exception as e:
                context.log.error(f"❌ Failed to save batch {batch_num} for {year}-{month:02d}: {e}")
                raise

            # Clean up memory
            del df, processed_df
            offset += 500000
            batch_num += 1
            total_chunks += 1

        current = next_month

    context.log.info(
        f"🏁 Completed backfill: {start_date:%Y-%m-%d} to {end_date:%Y-%m-%d}. Chunks written: {total_chunks}."
    )
    return f"Wrote {total_chunks} chunks"