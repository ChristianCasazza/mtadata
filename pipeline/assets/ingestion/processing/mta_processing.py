# pipeline/assets/ingestion/processing/mta_processing.py

import polars as pl

def process_mta_daily_df(df: pl.DataFrame) -> (pl.DataFrame, list, list, str):
    orig_cols = df.columns
    df = df.rename({col: col.lower().replace(" ", "_") for col in df.columns})
    renamed_cols = df.columns

    # parse 'date' column
    date_sample = "N/A"
    if "date" in df.columns:
        df = df.with_columns(
            pl.col("date").str.strptime(pl.Date, format="%Y-%m-%dT%H:%M:%S%.f", strict=False).alias("date")
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


def process_mta_operations_statement_df(df: pl.DataFrame) -> (pl.DataFrame, list, list):
    orig_cols = df.columns
    df = df.rename(
        {col: col.lower().replace(" ", "_").replace("-", "_") for col in df.columns}
    ).rename({"month": "timestamp"})
    renamed_cols = df.columns

    if "timestamp" in df.columns:
        df = df.with_columns([
            pl.col("timestamp").str.strptime(pl.Date, format="%Y-%m-%dT%H:%M:%S%.f", strict=False).alias("timestamp")
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