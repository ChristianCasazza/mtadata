import polars as pl

def process_mta_hourly_subway_df(df: pl.DataFrame) -> pl.DataFrame:
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



def process_mta_daily_subway_df(df: pl.DataFrame) -> pl.DataFrame:
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

    # Cast columns to appropriate types
    df = df.with_columns([
        pl.col("fiscal_year").cast(pl.Int64),
        pl.col("financial_plan_year").cast(pl.Int64),
        pl.col("amount").cast(pl.Float64),
        pl.col("scenario").cast(pl.Utf8),
        pl.col("expense_type").cast(pl.Utf8),
        pl.col("agency").cast(pl.Utf8),
        pl.col("type").cast(pl.Utf8),
        pl.col("subtype").cast(pl.Utf8),
        pl.col("general_ledger").cast(pl.Utf8)
    ])

    return df