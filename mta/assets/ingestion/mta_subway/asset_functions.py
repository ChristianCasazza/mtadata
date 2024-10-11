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
