# mta/assets/ingestion/sf_assets.py

import gc
import polars as pl
from dagster import asset

from mta.resources.socrata_resource import SocrataResource


# ------------------------------------------------------------------
# SF Air Traffic Cargo
# ------------------------------------------------------------------
def process_sf_air_traffic_cargo_df(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df

    exprs = []
    if "activity_period" in df.columns:
        exprs.append(pl.col("activity_period").cast(pl.Int64))
    if "activity_period_start_date" in df.columns:
        exprs.append(pl.col("activity_period_start_date").str.strptime(pl.Datetime, strict=False))
    if "cargo_weight_lbs" in df.columns:
        exprs.append(pl.col("cargo_weight_lbs").cast(pl.Float64))
    if "cargo_metric_tons" in df.columns:
        exprs.append(pl.col("cargo_metric_tons").cast(pl.Float64))
    if "data_as_of" in df.columns:
        exprs.append(pl.col("data_as_of").str.strptime(pl.Datetime, strict=False))
    if "data_loaded_at" in df.columns:
        exprs.append(pl.col("data_loaded_at").str.strptime(pl.Datetime, strict=False))

    if exprs:
        df = df.with_columns(exprs)
    return df

@asset(name="sf_air_traffic_cargo", compute_kind="Polars")
def sf_air_traffic_cargo(socrata: SocrataResource) -> pl.DataFrame:
    endpoint = "https://data.sfgov.org/resource/u397-j8nr.json"
    limit = 500000
    offset = 0

    frames = []
    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "Activity_Period_Start_Date ASC",
        }
        data = socrata.fetch_data(endpoint, params)
        if not data:
            break

        df = pl.DataFrame(data)
        processed = process_sf_air_traffic_cargo_df(df)
        frames.append(processed)

        offset += limit
        del df, processed, data
        gc.collect()

    if frames:
        return pl.concat(frames, how="vertical")
    return pl.DataFrame([])


# ------------------------------------------------------------------
# SF Air Traffic Passenger Stats
# ------------------------------------------------------------------
def process_sf_air_traffic_passenger_stats_df(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df

    # Conditionally rename if old columns exist
    rename_map = {}
    if "Activity Period" in df.columns:
        rename_map["Activity Period"] = "activity_period"
    if "Activity Period Start Date" in df.columns:
        rename_map["Activity Period Start Date"] = "activity_period_start_date"
    if "Operating Airline" in df.columns:
        rename_map["Operating Airline"] = "operating_airline"
    if "Operating Airline IATA Code" in df.columns:
        rename_map["Operating Airline IATA Code"] = "operating_airline_iata_code"
    if "Published Airline" in df.columns:
        rename_map["Published Airline"] = "published_airline"
    if "Published Airline IATA Code" in df.columns:
        rename_map["Published Airline IATA Code"] = "published_airline_iata_code"
    if "GEO Summary" in df.columns:
        rename_map["GEO Summary"] = "geo_summary"
    if "GEO Region" in df.columns:
        rename_map["GEO Region"] = "geo_region"
    if "Activity Type Code" in df.columns:
        rename_map["Activity Type Code"] = "activity_type_code"
    if "Price Category Code" in df.columns:
        rename_map["Price Category Code"] = "price_category_code"
    if "Terminal" in df.columns:
        rename_map["Terminal"] = "terminal"
    if "Boarding Area" in df.columns:
        rename_map["Boarding Area"] = "boarding_area"
    if "Passenger Count" in df.columns:
        rename_map["Passenger Count"] = "passenger_count"
    if "data_as_of" in df.columns:
        rename_map["data_as_of"] = "data_as_of"
    if "data_loaded_at" in df.columns:
        rename_map["data_loaded_at"] = "data_loaded_at"

    if rename_map:
        df = df.rename(rename_map)

    # cast columns
    exprs = []
    if "activity_period" in df.columns:
        exprs.append(pl.col("activity_period").cast(pl.Int64))
    if "activity_period_start_date" in df.columns:
        exprs.append(pl.col("activity_period_start_date").str.strptime(pl.Datetime, strict=False))
    if "passenger_count" in df.columns:
        exprs.append(pl.col("passenger_count").cast(pl.Float64))
    if "data_as_of" in df.columns:
        exprs.append(pl.col("data_as_of").str.strptime(pl.Datetime, strict=False))
    if "data_loaded_at" in df.columns:
        exprs.append(pl.col("data_loaded_at").str.strptime(pl.Datetime, strict=False))

    if exprs:
        df = df.with_columns(exprs)

    return df

@asset(name="sf_air_traffic_passenger_stats", compute_kind="Polars")
def sf_air_traffic_passenger_stats(socrata: SocrataResource) -> pl.DataFrame:
    endpoint = "https://data.sfgov.org/resource/rkru-6vcg.json"
    limit = 500000
    offset = 0

    combined = []
    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "Activity_Period_Start_Date ASC",
        }
        data = socrata.fetch_data(endpoint, params)
        if not data:
            break

        df = pl.DataFrame(data)
        processed = process_sf_air_traffic_passenger_stats_df(df)
        combined.append(processed)

        offset += limit
        del df, processed, data
        gc.collect()

    if combined:
        return pl.concat(combined, how="vertical")
    return pl.DataFrame([])


# ------------------------------------------------------------------
# SF Air Traffic Landings
# ------------------------------------------------------------------
def process_sf_air_traffic_landings_df(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df

    rename_map = {}
    if "Activity Period" in df.columns:
        rename_map["Activity Period"] = "activity_period"
    if "Activity Period Start Date" in df.columns:
        rename_map["Activity Period Start Date"] = "activity_period_start_date"
    if "Operating Airline" in df.columns:
        rename_map["Operating Airline"] = "operating_airline"
    if "Operating Airline IATA Code" in df.columns:
        rename_map["Operating Airline IATA Code"] = "operating_airline_iata_code"
    if "Published Airline" in df.columns:
        rename_map["Published Airline"] = "published_airline"
    if "Published Airline IATA Code" in df.columns:
        rename_map["Published Airline IATA Code"] = "published_airline_iata_code"
    if "GEO Summary" in df.columns:
        rename_map["GEO Summary"] = "geo_summary"
    if "GEO Region" in df.columns:
        rename_map["GEO Region"] = "geo_region"
    if "Landing Aircraft Type" in df.columns:
        rename_map["Landing Aircraft Type"] = "landing_aircraft_type"
    if "Aircraft Body Type" in df.columns:
        rename_map["Aircraft Body Type"] = "aircraft_body_type"
    if "Aircraft Manufacturer" in df.columns:
        rename_map["Aircraft Manufacturer"] = "aircraft_manufacturer"
    if "Aircraft Model" in df.columns:
        rename_map["Aircraft Model"] = "aircraft_model"
    if "Aircraft Version" in df.columns:
        rename_map["Aircraft Version"] = "aircraft_version"
    if "Landing Count" in df.columns:
        rename_map["Landing Count"] = "landing_count"
    if "Total Landed Weight" in df.columns:
        rename_map["Total Landed Weight"] = "total_landed_weight"

    if rename_map:
        df = df.rename(rename_map)

    exprs = []
    if "activity_period" in df.columns:
        exprs.append(pl.col("activity_period").cast(pl.Int64))
    if "activity_period_start_date" in df.columns:
        exprs.append(pl.col("activity_period_start_date").str.strptime(pl.Datetime, strict=False))
    if "landing_count" in df.columns:
        exprs.append(pl.col("landing_count").cast(pl.Float64))
    if "total_landed_weight" in df.columns:
        exprs.append(pl.col("total_landed_weight").cast(pl.Float64))

    if exprs:
        df = df.with_columns(exprs)

    return df

@asset(name="sf_air_traffic_landings", compute_kind="Polars")
def sf_air_traffic_landings(socrata: SocrataResource) -> pl.DataFrame:
    endpoint = "https://data.sfgov.org/resource/fpux-q53t.json"
    limit = 500000
    offset = 0

    frames = []
    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "Activity_Period_Start_Date ASC",
        }
        data = socrata.fetch_data(endpoint, params)
        if not data:
            break

        df = pl.DataFrame(data)
        processed = process_sf_air_traffic_landings_df(df)
        frames.append(processed)

        offset += limit
        del df, processed, data
        gc.collect()

    if frames:
        return pl.concat(frames, how="vertical")
    return pl.DataFrame([])
